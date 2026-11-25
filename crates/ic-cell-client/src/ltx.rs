//! Phase 5: LTX Replication Adapter
//!
//! Bridges `celld` LTX files / page-delta frames to
//! `ic-cell-canister::apply_ltx_delta`. Deltas larger than one ingress message
//! are staged across several calls and committed atomically.

use crate::ltx_codec::{self, LtxFile};
use crate::{CellError, IcCellTransport, LtxDeltaRequest, PageDelta, ReplicationState};
use std::sync::atomic::{AtomicU64, Ordering};

pub const IC_SQLITE_PAGE_SIZE: u32 = 16_384;

/// Page payload bytes sent per message. ICP ingress messages are capped at
/// 2 MiB, so keep headroom for Candid framing.
pub const DEFAULT_MAX_MESSAGE_PAGE_BYTES: usize = 1_536 * 1024;

static UPLOAD_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LtxFrame {
    pub epoch: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub page_size: u32,
    pub commit_pages: u32,
    /// `ic-sqlite-vfs` image checksum before the delta (`0` = skip).
    pub pre_apply_checksum: u64,
    /// `ic-sqlite-vfs` image checksum after the delta (`0` = skip).
    pub post_apply_checksum: u64,
    /// LTX-format database checksum before the delta.
    pub ltx_pre_apply_checksum: Option<u64>,
    /// LTX-format database checksum after the delta.
    pub ltx_post_apply_checksum: Option<u64>,
    pub pages: Vec<PageDelta>,
}

impl LtxFrame {
    /// Builds a frame from a decoded LTX file. The LTX checksums are carried
    /// through so the canister verifies them with LTX semantics.
    pub fn from_ltx(file: LtxFile, epoch: u64) -> Self {
        let h = &file.header;
        let checksums = h.has_checksums();
        Self {
            epoch,
            min_txid: h.min_txid,
            max_txid: h.max_txid,
            page_size: h.page_size,
            commit_pages: h.commit,
            pre_apply_checksum: 0,
            post_apply_checksum: 0,
            ltx_pre_apply_checksum: (checksums && !h.is_snapshot()).then_some(h.pre_apply_checksum),
            ltx_post_apply_checksum: checksums.then_some(file.post_apply_checksum),
            pages: file.pages,
        }
    }

    fn into_request(self, pages: Vec<PageDelta>) -> LtxDeltaRequest {
        LtxDeltaRequest {
            epoch: self.epoch,
            min_txid: self.min_txid,
            max_txid: self.max_txid,
            page_size: self.page_size,
            commit_pages: self.commit_pages,
            pre_apply_checksum: self.pre_apply_checksum,
            post_apply_checksum: self.post_apply_checksum,
            ltx_pre_apply_checksum: self.ltx_pre_apply_checksum,
            ltx_post_apply_checksum: self.ltx_post_apply_checksum,
            pages,
        }
    }
}

pub struct LtxReplicationAdapter<'a, T: IcCellTransport> {
    transport: &'a T,
    max_message_page_bytes: usize,
}

impl<'a, T: IcCellTransport> LtxReplicationAdapter<'a, T> {
    pub fn new(transport: &'a T) -> Self {
        Self {
            transport,
            max_message_page_bytes: DEFAULT_MAX_MESSAGE_PAGE_BYTES,
        }
    }

    pub fn with_max_message_page_bytes(mut self, bytes: usize) -> Self {
        self.max_message_page_bytes = bytes.max(1);
        self
    }

    /// Decodes a raw LTX file (verifying its file checksum) and replicates it.
    pub fn replicate_ltx_bytes(
        &self,
        bytes: &[u8],
        epoch: u64,
    ) -> Result<ReplicationState, CellError> {
        let file = ltx_codec::decode(bytes).map_err(|e| CellError::ReplicationMismatch {
            reason: e.to_string(),
        })?;
        self.replicate(LtxFrame::from_ltx(file, epoch))
    }

    pub fn replicate(&self, mut frame: LtxFrame) -> Result<ReplicationState, CellError> {
        if frame.page_size != IC_SQLITE_PAGE_SIZE {
            return Err(CellError::ReplicationMismatch {
                reason: format!(
                    "celld LTX page_size {} != required ic-sqlite-vfs page_size {}",
                    frame.page_size, IC_SQLITE_PAGE_SIZE
                ),
            });
        }

        let total: usize = frame.pages.iter().map(|p| p.data.len()).sum();
        if total <= self.max_message_page_bytes {
            let pages = std::mem::take(&mut frame.pages);
            return self.transport.apply_ltx_delta(frame.into_request(pages));
        }

        let upload_id = format!(
            "ltx-{}-{}-{}-{}",
            frame.epoch,
            frame.min_txid,
            frame.max_txid,
            UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let pages = std::mem::take(&mut frame.pages);
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        for page in pages {
            if batch_bytes + page.data.len() > self.max_message_page_bytes && !batch.is_empty() {
                self.stage(&upload_id, std::mem::take(&mut batch))?;
                batch_bytes = 0;
            }
            batch_bytes += page.data.len();
            batch.push(page);
        }
        if !batch.is_empty() {
            self.stage(&upload_id, batch)?;
        }
        self.transport
            .commit_staged_ltx_delta(upload_id, frame.into_request(Vec::new()))
    }

    fn stage(&self, upload_id: &str, pages: Vec<PageDelta>) -> Result<(), CellError> {
        if let Err(err) = self.transport.stage_ltx_pages(upload_id.to_string(), pages) {
            // Best effort: free canister heap; the original error is what matters.
            let _ = self.transport.abort_staged_ltx(upload_id.to_string());
            return Err(err);
        }
        Ok(())
    }

    pub fn state(&self) -> Result<ReplicationState, CellError> {
        self.transport.get_replication_state()
    }
}
