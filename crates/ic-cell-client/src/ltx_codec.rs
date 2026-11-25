//! Byte-compatible codec for the LTX file format (`github.com/superfly/ltx`,
//! magic `LTX1`, format version 3) as emitted by litefs / `celld`.
//!
//! File layout (all integers big-endian):
//!
//! ```text
//! header (100) | { page header (6) | lz4 size u32 | lz4 block }* | zero page header (6)
//!   | { uvarint pgno, uvarint offset, uvarint size }* | uvarint 0 | index size u64
//!   | post-apply checksum u64 | file checksum u64
//! ```
//!
//! The file checksum is `FLAG | CRC64-ISO` over every byte before the file
//! checksum field, except that page payloads are hashed *uncompressed*.

use crate::PageDelta;
use crc::{Crc, Digest, CRC_64_GO_ISO};
use ic_cell_canister::replication::{lock_pgno, ltx_page_checksum, LTX_CHECKSUM_FLAG};
use std::collections::BTreeMap;

pub const MAGIC: &[u8; 4] = b"LTX1";
pub const HEADER_SIZE: usize = 100;
pub const PAGE_HEADER_SIZE: usize = 6;
pub const TRAILER_SIZE: usize = 16;
pub const HEADER_FLAG_NO_CHECKSUM: u32 = 1 << 1;
pub const PAGE_HEADER_FLAG_SIZE: u16 = 1 << 0;

static CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LtxError {
    #[error("invalid LTX file: {0}")]
    Invalid(String),
    #[error("LTX checksum mismatch: {0}")]
    Checksum(String),
    #[error("unsupported LTX feature: {0}")]
    Unsupported(String),
}

fn invalid<T>(msg: impl Into<String>) -> Result<T, LtxError> {
    Err(LtxError::Invalid(msg.into()))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LtxHeader {
    pub flags: u32,
    pub page_size: u32,
    /// Database size in pages after this transaction.
    pub commit: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    /// Milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    pub pre_apply_checksum: u64,
    pub wal_offset: i64,
    pub wal_size: i64,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub node_id: u64,
}

impl LtxHeader {
    pub fn is_snapshot(&self) -> bool {
        self.min_txid == 1
    }

    pub fn has_checksums(&self) -> bool {
        self.flags & HEADER_FLAG_NO_CHECKSUM == 0
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0u8; HEADER_SIZE];
        b[0..4].copy_from_slice(MAGIC);
        b[4..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.commit.to_be_bytes());
        b[16..24].copy_from_slice(&self.min_txid.to_be_bytes());
        b[24..32].copy_from_slice(&self.max_txid.to_be_bytes());
        b[32..40].copy_from_slice(&self.timestamp_ms.to_be_bytes());
        b[40..48].copy_from_slice(&self.pre_apply_checksum.to_be_bytes());
        b[48..56].copy_from_slice(&self.wal_offset.to_be_bytes());
        b[56..64].copy_from_slice(&self.wal_size.to_be_bytes());
        b[64..68].copy_from_slice(&self.wal_salt1.to_be_bytes());
        b[68..72].copy_from_slice(&self.wal_salt2.to_be_bytes());
        b[72..80].copy_from_slice(&self.node_id.to_be_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self, LtxError> {
        if b.len() < HEADER_SIZE {
            return invalid("short header");
        }
        if &b[0..4] != MAGIC {
            return invalid("bad magic (expected LTX1)");
        }
        let u32_at = |o: usize| u32::from_be_bytes(b[o..o + 4].try_into().unwrap());
        let u64_at = |o: usize| u64::from_be_bytes(b[o..o + 8].try_into().unwrap());
        let h = Self {
            flags: u32_at(4),
            page_size: u32_at(8),
            commit: u32_at(12),
            min_txid: u64_at(16),
            max_txid: u64_at(24),
            timestamp_ms: u64_at(32) as i64,
            pre_apply_checksum: u64_at(40),
            wal_offset: u64_at(48) as i64,
            wal_size: u64_at(56) as i64,
            wal_salt1: u32_at(64),
            wal_salt2: u32_at(68),
            node_id: u64_at(72),
        };
        h.validate()?;
        Ok(h)
    }

    /// Mirrors `ltx.Header.Validate`.
    pub fn validate(&self) -> Result<(), LtxError> {
        if self.flags & !HEADER_FLAG_NO_CHECKSUM != 0 {
            return invalid(format!("invalid header flags {:#x}", self.flags));
        }
        if !(512..=65_536).contains(&self.page_size) || !self.page_size.is_power_of_two() {
            return invalid(format!("invalid page size {}", self.page_size));
        }
        if self.min_txid == 0 || self.max_txid == 0 || self.min_txid > self.max_txid {
            return invalid(format!(
                "invalid txid range {}..={}",
                self.min_txid, self.max_txid
            ));
        }
        if self.wal_offset < 0 || self.wal_size < 0 {
            return invalid("negative WAL offset/size");
        }
        if (self.wal_salt1 != 0 || self.wal_salt2 != 0) && self.wal_offset == 0 {
            return invalid("WAL salt set without WAL offset");
        }
        if self.wal_offset == 0 && self.wal_size != 0 {
            return invalid("WAL size set without WAL offset");
        }
        if self.is_snapshot() || !self.has_checksums() {
            if self.pre_apply_checksum != 0 {
                return invalid(
                    "pre-apply checksum must be zero for snapshots / no-checksum files",
                );
            }
        } else if self.pre_apply_checksum & LTX_CHECKSUM_FLAG == 0 {
            return invalid("pre-apply checksum missing checksum flag");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LtxFile {
    pub header: LtxHeader,
    /// Uncompressed pages in file order (ascending pgno).
    pub pages: Vec<PageDelta>,
    pub post_apply_checksum: u64,
    pub file_checksum: u64,
}

/// LTX database checksum over a full set of pages (lock page excluded).
pub fn database_checksum(page_size: u32, pages: &[PageDelta]) -> u64 {
    let lock = lock_pgno(page_size);
    LTX_CHECKSUM_FLAG
        | pages
            .iter()
            .filter(|p| p.pgno != lock)
            .fold(0u64, |acc, p| acc ^ ltx_page_checksum(p.pgno, &p.data))
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    digest: Digest<'static, u64>,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8], LtxError> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.buf.len())
            .ok_or_else(|| LtxError::Invalid(format!("unexpected EOF reading {what}")))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn take_hashed(&mut self, n: usize, what: &str) -> Result<&'a [u8], LtxError> {
        let s = self.take(n, what)?;
        self.digest.update(s);
        Ok(s)
    }

    fn uvarint_hashed(&mut self) -> Result<u64, LtxError> {
        let mut value = 0u64;
        for i in 0..10 {
            let byte = self.take_hashed(1, "uvarint")?[0];
            if i == 9 && byte > 1 {
                return invalid("uvarint overflow");
            }
            value |= u64::from(byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        invalid("uvarint overflow")
    }
}

/// Decodes and fully validates an LTX file: header rules, page ordering, the
/// page index, the snapshot database checksum, and the file checksum.
pub fn decode(bytes: &[u8]) -> Result<LtxFile, LtxError> {
    let mut r = Reader {
        buf: bytes,
        pos: 0,
        digest: CRC64.digest(),
    };
    let header = LtxHeader::from_bytes(r.take_hashed(HEADER_SIZE, "header")?)?;
    let page_size = header.page_size as usize;
    let lock = lock_pgno(header.page_size);
    let snapshot = header.is_snapshot();

    let mut pages: Vec<PageDelta> = Vec::new();
    let mut frames: Vec<(u32, u64, u64)> = Vec::new();
    let mut db_raw = 0u64;

    loop {
        let frame_offset = r.pos as u64;
        let ph = r.take_hashed(PAGE_HEADER_SIZE, "page header")?;
        let pgno = u32::from_be_bytes(ph[0..4].try_into().unwrap());
        let flags = u16::from_be_bytes(ph[4..6].try_into().unwrap());
        if pgno == 0 && flags == 0 {
            break;
        }
        if pgno == 0 {
            return invalid("page number zero");
        }
        if flags & !PAGE_HEADER_FLAG_SIZE != 0 {
            return invalid(format!("invalid page header flags {flags:#x}"));
        }
        if flags & PAGE_HEADER_FLAG_SIZE == 0 {
            return Err(LtxError::Unsupported(
                "legacy LZ4-frame page encoding (pre size-prefix LTX)".into(),
            ));
        }
        if pgno > header.commit {
            return invalid(format!("page {pgno} beyond commit {}", header.commit));
        }
        match pages.last() {
            Some(prev) if pgno <= prev.pgno => {
                return invalid(format!("pages out of order: {} then {pgno}", prev.pgno));
            }
            _ => {}
        }
        if snapshot {
            let mut expected = pages.last().map_or(1, |p| p.pgno + 1);
            if expected == lock {
                expected += 1;
            }
            if pgno != expected {
                return invalid(format!(
                    "snapshot page {pgno} not contiguous (expected {expected})"
                ));
            }
        }

        let size_bytes = r.take_hashed(4, "page size prefix")?;
        let compressed_len = u32::from_be_bytes(size_bytes.try_into().unwrap()) as usize;
        let compressed = r.take(compressed_len, "page data")?;
        let mut data = vec![0u8; page_size];
        let n = lz4_flex::block::decompress_into(compressed, &mut data)
            .map_err(|e| LtxError::Invalid(format!("page {pgno}: lz4 decode failed: {e}")))?;
        if n != page_size {
            return invalid(format!(
                "page {pgno}: decompressed {n} bytes, expected {page_size}"
            ));
        }
        r.digest.update(&data);

        if pgno != lock {
            db_raw ^= ltx_page_checksum(pgno, &data);
        }
        frames.push((
            pgno,
            frame_offset,
            (PAGE_HEADER_SIZE + 4 + compressed_len) as u64,
        ));
        pages.push(PageDelta { pgno, data });
    }

    if snapshot {
        let last = pages.last().map_or(0, |p| p.pgno);
        let expected_last = if header.commit == lock {
            lock - 1
        } else {
            header.commit
        };
        if last != expected_last {
            return invalid(format!(
                "snapshot ends at page {last}, expected {expected_last}"
            ));
        }
    }

    // Page index.
    let index_start = r.pos;
    let mut idx = 0usize;
    loop {
        let pgno = r.uvarint_hashed()?;
        if pgno == 0 {
            break;
        }
        let offset = r.uvarint_hashed()?;
        let size = r.uvarint_hashed()?;
        let Some(&(fp, fo, fs)) = frames.get(idx) else {
            return invalid("page index has more entries than pages");
        };
        if (pgno, offset, size) != (u64::from(fp), fo, fs) {
            return invalid(format!(
                "page index entry {idx} ({pgno},{offset},{size}) != frame ({fp},{fo},{fs})"
            ));
        }
        idx += 1;
    }
    if idx != frames.len() {
        return invalid(format!(
            "page index has {idx} entries, file has {} pages",
            frames.len()
        ));
    }
    let index_len = (r.pos - index_start) as u64;
    let declared = u64::from_be_bytes(r.take_hashed(8, "index size")?.try_into().unwrap());
    if declared != index_len {
        return invalid(format!("index size {declared} != actual {index_len}"));
    }

    // Trailer.
    let post_apply_checksum =
        u64::from_be_bytes(r.take_hashed(8, "post-apply checksum")?.try_into().unwrap());
    let file_checksum = u64::from_be_bytes(r.take(8, "file checksum")?.try_into().unwrap());
    if r.pos != bytes.len() {
        return invalid(format!(
            "{} trailing bytes after trailer",
            bytes.len() - r.pos
        ));
    }

    if header.has_checksums() {
        if post_apply_checksum & LTX_CHECKSUM_FLAG == 0 {
            return invalid("post-apply checksum missing checksum flag");
        }
    } else if post_apply_checksum != 0 {
        return invalid("post-apply checksum must be zero when checksums are disabled");
    }
    if file_checksum & LTX_CHECKSUM_FLAG == 0 {
        return invalid("file checksum missing checksum flag");
    }
    let computed = LTX_CHECKSUM_FLAG | r.digest.finalize();
    if computed != file_checksum {
        return Err(LtxError::Checksum(format!(
            "file checksum {file_checksum:#018x} != computed {computed:#018x}"
        )));
    }
    if snapshot && header.has_checksums() {
        let db = LTX_CHECKSUM_FLAG | db_raw;
        if db != post_apply_checksum {
            return Err(LtxError::Checksum(format!(
                "snapshot database checksum {db:#018x} != post-apply {post_apply_checksum:#018x}"
            )));
        }
    }

    Ok(LtxFile {
        header,
        pages,
        post_apply_checksum,
        file_checksum,
    })
}

fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Encodes an LTX file. Pages are sorted by pgno; duplicates are rejected.
/// For snapshots the post-apply checksum is computed from the pages when
/// `post_apply_checksum` is `None`; for incremental files it must be given
/// (unless checksums are disabled).
pub fn encode(
    header: &LtxHeader,
    pages: &[PageDelta],
    post_apply_checksum: Option<u64>,
) -> Result<Vec<u8>, LtxError> {
    header.validate()?;
    let lock = lock_pgno(header.page_size);
    let mut sorted: BTreeMap<u32, &[u8]> = BTreeMap::new();
    for p in pages {
        if p.pgno == 0 || p.pgno == lock || p.pgno > header.commit {
            return invalid(format!("cannot encode page {}", p.pgno));
        }
        if p.data.len() != header.page_size as usize {
            return invalid(format!("page {} has wrong size", p.pgno));
        }
        if sorted.insert(p.pgno, &p.data).is_some() {
            return invalid(format!("duplicate page {}", p.pgno));
        }
    }
    if header.is_snapshot() {
        let expected = (1..=header.commit).filter(|p| *p != lock).count();
        if sorted.len() != expected || sorted.keys().any(|p| *p > header.commit) {
            return invalid("snapshot must contain every page up to commit");
        }
    }

    let post = if !header.has_checksums() {
        0
    } else if let Some(c) = post_apply_checksum {
        c
    } else if header.is_snapshot() {
        let owned: Vec<PageDelta> = sorted
            .iter()
            .map(|(p, d)| PageDelta {
                pgno: *p,
                data: d.to_vec(),
            })
            .collect();
        database_checksum(header.page_size, &owned)
    } else {
        return invalid("post-apply checksum required for incremental LTX files");
    };

    let mut digest = CRC64.digest();
    let mut out = Vec::new();
    let hb = header.to_bytes();
    out.extend_from_slice(&hb);
    digest.update(&hb);

    let mut index = Vec::new();
    for (pgno, data) in &sorted {
        let offset = out.len() as u64;
        let compressed = lz4_flex::block::compress(data);
        let mut ph = [0u8; PAGE_HEADER_SIZE + 4];
        ph[0..4].copy_from_slice(&pgno.to_be_bytes());
        ph[4..6].copy_from_slice(&PAGE_HEADER_FLAG_SIZE.to_be_bytes());
        ph[6..10].copy_from_slice(&(compressed.len() as u32).to_be_bytes());
        out.extend_from_slice(&ph);
        digest.update(&ph);
        out.extend_from_slice(&compressed);
        digest.update(data);
        put_uvarint(&mut index, u64::from(*pgno));
        put_uvarint(&mut index, offset);
        put_uvarint(&mut index, (PAGE_HEADER_SIZE + 4 + compressed.len()) as u64);
    }
    let end = [0u8; PAGE_HEADER_SIZE];
    out.extend_from_slice(&end);
    digest.update(&end);

    put_uvarint(&mut index, 0);
    out.extend_from_slice(&index);
    digest.update(&index);
    let index_len = (index.len() as u64).to_be_bytes();
    out.extend_from_slice(&index_len);
    digest.update(&index_len);

    let post_bytes = post.to_be_bytes();
    out.extend_from_slice(&post_bytes);
    digest.update(&post_bytes);
    let file_checksum = LTX_CHECKSUM_FLAG | digest.finalize();
    out.extend_from_slice(&file_checksum.to_be_bytes());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(pgno: u32, size: u32, fill: u8) -> PageDelta {
        let mut data = vec![fill; size as usize];
        data[0] = pgno as u8;
        PageDelta { pgno, data }
    }

    fn snapshot_header(commit: u32) -> LtxHeader {
        LtxHeader {
            page_size: 4096,
            commit,
            min_txid: 1,
            max_txid: 1,
            timestamp_ms: 1_700_000_000_000,
            ..Default::default()
        }
    }

    #[test]
    fn snapshot_roundtrip() {
        let pages: Vec<_> = (1..=3).map(|p| page(p, 4096, 0xab)).collect();
        let bytes = encode(&snapshot_header(3), &pages, None).unwrap();
        let file = decode(&bytes).unwrap();
        assert_eq!(file.pages, pages);
        assert_eq!(file.post_apply_checksum, database_checksum(4096, &pages));
    }

    #[test]
    fn incremental_roundtrip_with_gaps() {
        let header = LtxHeader {
            page_size: 4096,
            commit: 10,
            min_txid: 5,
            max_txid: 6,
            pre_apply_checksum: LTX_CHECKSUM_FLAG | 0x1234,
            ..Default::default()
        };
        let pages = vec![page(2, 4096, 1), page(9, 4096, 2)];
        let bytes = encode(&header, &pages, Some(LTX_CHECKSUM_FLAG | 0x99)).unwrap();
        let file = decode(&bytes).unwrap();
        assert_eq!(file.header, header);
        assert_eq!(file.pages, pages);
        assert_eq!(file.post_apply_checksum, LTX_CHECKSUM_FLAG | 0x99);
    }

    #[test]
    fn detects_corruption_and_trailing_bytes() {
        let pages: Vec<_> = (1..=2).map(|p| page(p, 4096, 0)).collect();
        let bytes = encode(&snapshot_header(2), &pages, None).unwrap();

        let mut flipped = bytes.clone();
        flipped[20] ^= 1; // inside max_txid
        assert!(decode(&flipped).is_err());

        let mut wrong_post = bytes.clone();
        let n = wrong_post.len();
        wrong_post[n - 9] ^= 1;
        assert!(matches!(decode(&wrong_post), Err(LtxError::Checksum(_))));

        let mut extra = bytes.clone();
        extra.push(0);
        assert!(decode(&extra).is_err());

        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn rejects_non_contiguous_snapshot_and_bad_header() {
        let pages = vec![page(1, 4096, 0), page(3, 4096, 0)];
        assert!(encode(&snapshot_header(3), &pages, None).is_err());

        let mut h = snapshot_header(1);
        h.page_size = 1000;
        assert!(h.validate().is_err());
        let mut h = snapshot_header(1);
        h.pre_apply_checksum = LTX_CHECKSUM_FLAG;
        assert!(h.validate().is_err());
    }
}
