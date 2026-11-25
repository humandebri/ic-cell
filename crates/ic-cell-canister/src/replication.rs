use crate::storage::db_err;
use crate::types::{CellError, LtxDeltaRequest, PageDelta, ReplicationState};
use crc::{Crc, CRC_64_GO_ISO};
use ic_sqlite_vfs::Db;
use std::cell::RefCell;
use std::collections::BTreeMap;

const MAGIC_V1: [u8; 4] = *b"ICRP";
const MAGIC_V2: [u8; 4] = *b"ICR2";
pub const ENCODED_STATE_LEN: usize = 40;

/// Top bit set on every valid LTX checksum (`ltx.ChecksumFlag`).
pub const LTX_CHECKSUM_FLAG: u64 = 1 << 63;
/// SQLite `PENDING_BYTE`; the page that contains it is never written.
const PENDING_BYTE: u64 = 0x4000_0000;

/// Per-upload and global caps for staged (multi-message) LTX deltas.
pub const MAX_STAGED_BYTES_PER_UPLOAD: u64 = 128 * 1024 * 1024;
pub const MAX_STAGED_BYTES_TOTAL: u64 = 256 * 1024 * 1024;
pub const MAX_STAGED_UPLOADS: usize = 16;

const CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

const EMPTY_STATE: ReplicationState = ReplicationState {
    epoch: 0,
    last_txid: 0,
    last_checksum: 0,
    page_count: 0,
    ltx_checksum: 0,
};

#[derive(Default)]
struct StagedUpload {
    pages: BTreeMap<u32, Vec<u8>>,
    bytes: u64,
}

thread_local! {
    static REPLICATION_STATE: RefCell<ReplicationState> = const { RefCell::new(EMPTY_STATE) };
    /// Heap-only staging area. It does not survive upgrades; clients restart
    /// an interrupted staged upload from scratch.
    static STAGED: RefCell<BTreeMap<String, StagedUpload>> = const { RefCell::new(BTreeMap::new()) };
}

pub fn get_replication_state() -> ReplicationState {
    REPLICATION_STATE.with(|s| s.borrow().clone())
}

pub fn encode_state() -> [u8; ENCODED_STATE_LEN] {
    let state = get_replication_state();
    let mut buf = [0u8; ENCODED_STATE_LEN];
    buf[0..4].copy_from_slice(&MAGIC_V2);
    buf[4..12].copy_from_slice(&state.epoch.to_le_bytes());
    buf[12..20].copy_from_slice(&state.last_txid.to_le_bytes());
    buf[20..28].copy_from_slice(&state.last_checksum.to_le_bytes());
    buf[28..32].copy_from_slice(&state.page_count.to_le_bytes());
    buf[32..40].copy_from_slice(&state.ltx_checksum.to_le_bytes());
    buf
}

/// Restores state written by [`encode_state`]. Also accepts the legacy 32-byte
/// `ICRP` layout (without an LTX checksum).
pub fn decode_and_restore_state(buf: &[u8]) {
    if buf.len() < 32 {
        return;
    }
    let ltx_checksum = match &buf[0..4] {
        m if m == MAGIC_V2 && buf.len() >= ENCODED_STATE_LEN => {
            u64::from_le_bytes(buf[32..40].try_into().unwrap())
        }
        m if m == MAGIC_V1 => 0,
        _ => return,
    };
    let state = ReplicationState {
        epoch: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        last_txid: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
        last_checksum: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        page_count: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
        ltx_checksum,
    };
    REPLICATION_STATE.with(|s| *s.borrow_mut() = state);
}

pub fn reset_replication_state_for_tests() {
    REPLICATION_STATE.with(|s| *s.borrow_mut() = EMPTY_STATE);
    STAGED.with(|s| s.borrow_mut().clear());
}

/// `ltx.LockPgno`: the page holding SQLite's pending byte.
pub fn lock_pgno(page_size: u32) -> u32 {
    (PENDING_BYTE / u64::from(page_size)) as u32 + 1
}

/// `ltx.ChecksumPage`: `FLAG | CRC64-ISO(be32(pgno) || data)`.
pub fn ltx_page_checksum(pgno: u32, data: &[u8]) -> u64 {
    let mut digest = CRC64.digest();
    digest.update(&pgno.to_be_bytes());
    digest.update(data);
    LTX_CHECKSUM_FLAG | digest.finalize()
}

/// Computes the LTX database checksum of the current SQLite image by scanning
/// every page. Used only when no incremental checksum is known yet.
pub fn ltx_checksum_full_scan(page_size: u32) -> Result<u64, CellError> {
    let count = Db::page_count().map_err(db_err)?;
    let lock = lock_pgno(page_size);
    let mut raw = 0u64;
    for pgno in 1..=count {
        if pgno == lock {
            continue;
        }
        let page = Db::read_raw_page(pgno).map_err(db_err)?;
        raw ^= ltx_page_checksum(pgno, &page);
    }
    Ok(LTX_CHECKSUM_FLAG | raw)
}

/// Stages pages for a delta that does not fit into one ingress message.
/// Returns the number of bytes staged so far for this upload.
pub fn stage_ltx_pages(upload_id: String, pages: Vec<PageDelta>) -> Result<u64, CellError> {
    if upload_id.is_empty() || upload_id.len() > 256 {
        return Err(CellError::InvalidArgument(
            "upload_id must be 1..=256 bytes".into(),
        ));
    }
    STAGED.with(|staged| {
        let mut staged = staged.borrow_mut();
        let total: u64 = staged.values().map(|u| u.bytes).sum();
        if !staged.contains_key(&upload_id) && staged.len() >= MAX_STAGED_UPLOADS {
            return Err(CellError::InvalidArgument(format!(
                "too many concurrent staged LTX uploads (max {MAX_STAGED_UPLOADS})"
            )));
        }
        let upload = staged.entry(upload_id).or_default();
        let mut added = 0u64;
        for page in &pages {
            if page.pgno == 0 {
                return Err(CellError::InvalidArgument("pgno must be 1-based".into()));
            }
            added += page.data.len() as u64;
        }
        if upload.bytes + added > MAX_STAGED_BYTES_PER_UPLOAD || total + added > MAX_STAGED_BYTES_TOTAL
        {
            return Err(CellError::InvalidArgument(format!(
                "staged LTX upload exceeds limit ({MAX_STAGED_BYTES_PER_UPLOAD} bytes per upload, {MAX_STAGED_BYTES_TOTAL} total)"
            )));
        }
        for page in pages {
            let len = page.data.len() as u64;
            if let Some(old) = upload.pages.insert(page.pgno, page.data) {
                upload.bytes -= old.len() as u64;
            }
            upload.bytes += len;
        }
        Ok(upload.bytes)
    })
}

/// Drops a staged upload. Returns the number of discarded pages.
pub fn abort_staged_ltx(upload_id: &str) -> u64 {
    STAGED.with(|s| {
        s.borrow_mut()
            .remove(upload_id)
            .map(|u| u.pages.len() as u64)
            .unwrap_or(0)
    })
}

/// Applies a staged upload plus any pages carried inline in `req` (inline
/// pages win over staged pages with the same pgno). The staged upload is
/// consumed whether or not the apply succeeds.
pub fn commit_staged_ltx_delta(
    upload_id: &str,
    mut req: LtxDeltaRequest,
) -> Result<ReplicationState, CellError> {
    let upload = STAGED
        .with(|s| s.borrow_mut().remove(upload_id))
        .ok_or_else(|| CellError::NotFound {
            key: format!("staged LTX upload {upload_id}"),
        })?;
    let mut pages = upload.pages;
    for page in req.pages.drain(..) {
        pages.insert(page.pgno, page.data);
    }
    req.pages = pages
        .into_iter()
        .map(|(pgno, data)| PageDelta { pgno, data })
        .collect();
    apply_ltx_delta(req)
}

/// Applies an LTX-style page delta directly to the underlying `ic-sqlite-vfs`
/// database image, verifying epoch monotonicity, TXID continuity, and
/// checksums.
///
/// Two independent checksum families are supported:
/// * `pre_apply_checksum` / `post_apply_checksum`: the `ic-sqlite-vfs` image
///   checksum (`0` = skip).
/// * `ltx_pre_apply_checksum` / `ltx_post_apply_checksum`: checksums with the
///   exact semantics of the LTX file format (`None` = skip). These are
///   verified *before* any page is written, so a mismatch never mutates state.
///
/// A delta with `min_txid == 1` is an LTX snapshot: it must contain every page
/// `1..=commit_pages` (except the lock page) and may reset a mirror whose
/// `last_txid <= max_txid`.
///
/// On wasm32 (IC canister), an image `post_apply_checksum` mismatch traps so
/// the replica atomically rolls back all stable-memory writes.
pub fn apply_ltx_delta(req: LtxDeltaRequest) -> Result<ReplicationState, CellError> {
    let mismatch = |reason: String| Err(CellError::ReplicationMismatch { reason });

    if req.min_txid == 0 || req.max_txid < req.min_txid {
        return mismatch(format!(
            "invalid txid range: {}..={}",
            req.min_txid, req.max_txid
        ));
    }

    let current = get_replication_state();
    let snapshot = req.min_txid == 1;

    if req.epoch < current.epoch {
        return mismatch(format!(
            "stale replication epoch: got {}, current {}",
            req.epoch, current.epoch
        ));
    }

    if current.last_txid > 0 {
        if snapshot {
            if req.max_txid < current.last_txid {
                return mismatch(format!(
                    "snapshot max_txid {} is behind current last_txid {}",
                    req.max_txid, current.last_txid
                ));
            }
        } else {
            let expected_min = current.last_txid + 1;
            if req.min_txid != expected_min {
                return mismatch(format!(
                    "non-contiguous txid: expected min_txid={}, got {}",
                    expected_min, req.min_txid
                ));
            }
            if req.pre_apply_checksum != 0 && req.pre_apply_checksum != current.last_checksum {
                return mismatch(format!(
                    "pre_apply_checksum mismatch: expected {}, got {}",
                    current.last_checksum, req.pre_apply_checksum
                ));
            }
        }
    }

    let page_size = req.page_size;
    let lock = lock_pgno(page_size);
    let mut delta: BTreeMap<u32, &[u8]> = BTreeMap::new();
    for page in &req.pages {
        if page.pgno == 0 || page.pgno > req.commit_pages {
            return mismatch(format!(
                "pgno {} out of range 1..={}",
                page.pgno, req.commit_pages
            ));
        }
        if page.data.len() != page_size as usize {
            return mismatch(format!(
                "page {} has {} bytes, expected {}",
                page.pgno,
                page.data.len(),
                page_size
            ));
        }
        if delta.insert(page.pgno, page.data.as_slice()).is_some() {
            return mismatch(format!("duplicate page {}", page.pgno));
        }
    }

    if snapshot {
        if let Some(missing) = (1..=req.commit_pages).find(|p| *p != lock && !delta.contains_key(p))
        {
            return mismatch(format!(
                "snapshot (min_txid=1) is missing page {missing} of {}",
                req.commit_pages
            ));
        }
    }

    let next_ltx_checksum = if snapshot
        || req.ltx_pre_apply_checksum.is_some()
        || req.ltx_post_apply_checksum.is_some()
        || current.ltx_checksum != 0
    {
        Some(next_ltx_checksum(&req, &current, &delta, snapshot)?)
    } else {
        None
    };

    let page_refs: Vec<(u32, &[u8])> = delta.iter().map(|(p, d)| (*p, *d)).collect();
    let applied_checksum =
        Db::apply_page_delta(page_size, req.commit_pages, &page_refs).map_err(db_err)?;

    if req.post_apply_checksum != 0 && req.post_apply_checksum != applied_checksum {
        let msg = format!(
            "post_apply_checksum mismatch: expected {}, got {}",
            req.post_apply_checksum, applied_checksum
        );
        #[cfg(target_arch = "wasm32")]
        ic_cdk::trap(&msg);

        #[cfg(not(target_arch = "wasm32"))]
        return Err(CellError::ReplicationMismatch { reason: msg });
    }

    let next_state = ReplicationState {
        epoch: req.epoch,
        last_txid: req.max_txid,
        last_checksum: applied_checksum,
        page_count: req.commit_pages,
        ltx_checksum: next_ltx_checksum.unwrap_or(0),
    };

    REPLICATION_STATE.with(|s| *s.borrow_mut() = next_state.clone());

    Ok(next_state)
}

/// Computes (and verifies) the LTX database checksum that results from
/// applying `delta`, without writing anything.
fn next_ltx_checksum(
    req: &LtxDeltaRequest,
    current: &ReplicationState,
    delta: &BTreeMap<u32, &[u8]>,
    snapshot: bool,
) -> Result<u64, CellError> {
    let lock = lock_pgno(req.page_size);
    let mismatch = |reason: String| Err(CellError::ReplicationMismatch { reason });

    let raw = if snapshot {
        delta
            .iter()
            .filter(|(p, _)| **p != lock)
            .fold(0u64, |acc, (p, d)| acc ^ ltx_page_checksum(*p, d))
    } else {
        let base = if current.ltx_checksum != 0 {
            current.ltx_checksum
        } else {
            ltx_checksum_full_scan(req.page_size)?
        };
        if let Some(expected) = req.ltx_pre_apply_checksum {
            if expected != base {
                return mismatch(format!(
                    "ltx_pre_apply_checksum mismatch: expected {expected:#018x}, mirror has {base:#018x}"
                ));
            }
        }

        let old_count = Db::page_count().map_err(db_err)?;
        let mut raw = base;
        let old_page_checksum = |pgno: u32| -> Result<u64, CellError> {
            let old = Db::read_raw_page(pgno).map_err(db_err)?;
            Ok(ltx_page_checksum(pgno, &old))
        };
        for (pgno, data) in delta {
            if *pgno == lock {
                continue;
            }
            if *pgno <= old_count {
                raw ^= old_page_checksum(*pgno)?;
            }
            raw ^= ltx_page_checksum(*pgno, data);
        }
        // Truncation drops trailing pages from the checksum.
        for pgno in (req.commit_pages + 1)..=old_count {
            if pgno != lock {
                raw ^= old_page_checksum(pgno)?;
            }
        }
        // Growth without explicit pages yields zero-filled pages.
        if req.commit_pages > old_count {
            let zero = vec![0u8; req.page_size as usize];
            for pgno in (old_count + 1)..=req.commit_pages {
                if pgno != lock && !delta.contains_key(&pgno) {
                    raw ^= ltx_page_checksum(pgno, &zero);
                }
            }
        }
        raw
    };

    let next = LTX_CHECKSUM_FLAG | raw;
    if let Some(expected) = req.ltx_post_apply_checksum {
        if expected != next {
            return mismatch(format!(
                "ltx_post_apply_checksum mismatch: expected {expected:#018x}, delta yields {next:#018x}"
            ));
        }
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_go_iso_check_value() {
        // Go: crc64.Checksum([]byte("123456789"), crc64.MakeTable(crc64.ISO))
        assert_eq!(CRC64.checksum(b"123456789"), 0xb909_56c7_75a4_1001);
    }

    #[test]
    fn lock_page_numbers_match_ltx() {
        assert_eq!(lock_pgno(4096), 262_145);
        assert_eq!(lock_pgno(16_384), 65_537);
    }

    #[test]
    fn state_roundtrip_and_legacy_decode() {
        reset_replication_state_for_tests();
        REPLICATION_STATE.with(|s| {
            *s.borrow_mut() = ReplicationState {
                epoch: 3,
                last_txid: 9,
                last_checksum: 42,
                page_count: 7,
                ltx_checksum: LTX_CHECKSUM_FLAG | 5,
            }
        });
        let encoded = encode_state();
        reset_replication_state_for_tests();
        decode_and_restore_state(&encoded);
        assert_eq!(get_replication_state().ltx_checksum, LTX_CHECKSUM_FLAG | 5);

        let mut legacy = [0u8; 32];
        legacy[0..4].copy_from_slice(&MAGIC_V1);
        legacy[12..20].copy_from_slice(&11u64.to_le_bytes());
        decode_and_restore_state(&legacy);
        let restored = get_replication_state();
        assert_eq!((restored.last_txid, restored.ltx_checksum), (11, 0));
        reset_replication_state_for_tests();
    }
}
