use crate::storage::db_err;
use crate::types::{CellError, LtxDeltaRequest, ReplicationState};
use ic_sqlite_vfs::Db;
use std::cell::RefCell;

const MAGIC: [u8; 4] = *b"ICRP";

thread_local! {
    static REPLICATION_STATE: RefCell<ReplicationState> = const {
        RefCell::new(ReplicationState {
            epoch: 0,
            last_txid: 0,
            last_checksum: 0,
            page_count: 0,
        })
    };
}

pub fn get_replication_state() -> ReplicationState {
    REPLICATION_STATE.with(|s| s.borrow().clone())
}

pub fn encode_state() -> [u8; 32] {
    let state = get_replication_state();
    let mut buf = [0u8; 32];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4..12].copy_from_slice(&state.epoch.to_le_bytes());
    buf[12..20].copy_from_slice(&state.last_txid.to_le_bytes());
    buf[20..28].copy_from_slice(&state.last_checksum.to_le_bytes());
    buf[28..32].copy_from_slice(&state.page_count.to_le_bytes());
    buf
}

pub fn decode_and_restore_state(buf: &[u8]) {
    if buf.len() < 32 || buf[0..4] != MAGIC {
        return;
    }
    let state = ReplicationState {
        epoch: u64::from_le_bytes(buf[4..12].try_into().unwrap()),
        last_txid: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
        last_checksum: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
        page_count: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
    };
    REPLICATION_STATE.with(|s| {
        *s.borrow_mut() = state;
    });
}

pub fn reset_replication_state_for_tests() {
    REPLICATION_STATE.with(|s| {
        *s.borrow_mut() = ReplicationState {
            epoch: 0,
            last_txid: 0,
            last_checksum: 0,
            page_count: 0,
        };
    });
}

/// Applies an LTX-style page delta directly to the underlying `ic-sqlite-vfs`
/// database image, verifying epoch monotonicity, TXID continuity, and
/// pre/post checksums.
///
/// On wasm32 (IC canister), a `post_apply_checksum` mismatch triggers an IC
/// trap so the replica atomically rolls back all stable-memory writes.
pub fn apply_ltx_delta(req: LtxDeltaRequest) -> Result<ReplicationState, CellError> {
    if req.min_txid == 0 || req.max_txid < req.min_txid {
        return Err(CellError::ReplicationMismatch {
            reason: format!("invalid txid range: {}..={}", req.min_txid, req.max_txid),
        });
    }

    let current = get_replication_state();

    if req.epoch < current.epoch {
        return Err(CellError::ReplicationMismatch {
            reason: format!(
                "stale replication epoch: got {}, current {}",
                req.epoch, current.epoch
            ),
        });
    }

    if current.last_txid > 0 {
        let expected_min = current.last_txid + 1;
        if req.min_txid != expected_min {
            return Err(CellError::ReplicationMismatch {
                reason: format!(
                    "non-contiguous txid: expected min_txid={}, got {}",
                    expected_min, req.min_txid
                ),
            });
        }
        if req.pre_apply_checksum != 0 && req.pre_apply_checksum != current.last_checksum {
            return Err(CellError::ReplicationMismatch {
                reason: format!(
                    "pre_apply_checksum mismatch: expected {}, got {}",
                    current.last_checksum, req.pre_apply_checksum
                ),
            });
        }
    }

    let page_refs: Vec<(u32, &[u8])> = req
        .pages
        .iter()
        .map(|p| (p.pgno, p.data.as_slice()))
        .collect();

    let applied_checksum = Db::apply_page_delta(req.page_size, req.commit_pages, &page_refs)
        .map_err(db_err)?;

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
    };

    REPLICATION_STATE.with(|s| {
        *s.borrow_mut() = next_state.clone();
    });

    Ok(next_state)
}
