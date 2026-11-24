//! Phase 5: LTX Replication Adapter
//!
//! Bridges `celld` LTX page-delta frames to `ic-cell-canister::apply_ltx_delta`.

use crate::{CellError, IcCellTransport, LtxDeltaRequest, PageDelta, ReplicationState};

pub const IC_SQLITE_PAGE_SIZE: u32 = 16_384;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LtxFrame {
    pub epoch: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub page_size: u32,
    pub commit_pages: u32,
    pub pre_apply_checksum: u64,
    pub post_apply_checksum: u64,
    pub pages: Vec<PageDelta>,
}

pub struct LtxReplicationAdapter<'a, T: IcCellTransport> {
    transport: &'a T,
}

impl<'a, T: IcCellTransport> LtxReplicationAdapter<'a, T> {
    pub fn new(transport: &'a T) -> Self {
        Self { transport }
    }

    pub fn replicate(&self, frame: LtxFrame) -> Result<ReplicationState, CellError> {
        if frame.page_size != IC_SQLITE_PAGE_SIZE {
            return Err(CellError::ReplicationMismatch {
                reason: format!(
                    "celld LTX page_size {} != required ic-sqlite-vfs page_size {}",
                    frame.page_size, IC_SQLITE_PAGE_SIZE
                ),
            });
        }
        self.transport.apply_ltx_delta(LtxDeltaRequest {
            epoch: frame.epoch,
            min_txid: frame.min_txid,
            max_txid: frame.max_txid,
            page_size: frame.page_size,
            commit_pages: frame.commit_pages,
            pre_apply_checksum: frame.pre_apply_checksum,
            post_apply_checksum: frame.post_apply_checksum,
            pages: frame.pages,
        })
    }

    pub fn state(&self) -> Result<ReplicationState, CellError> {
        self.transport.get_replication_state()
    }
}
