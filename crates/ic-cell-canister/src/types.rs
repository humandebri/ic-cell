use candid::CandidType;
use serde::{Deserialize, Serialize};

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CellError {
    CasConflict {
        key: String,
        expected_generation: u64,
        actual_generation: Option<u64>,
    },
    AlreadyExists {
        key: String,
    },
    NotFound {
        key: String,
    },
    StaleEpoch {
        cell_id: String,
        expected_epoch: u64,
        actual_epoch: u64,
    },
    LeaseHeldByAnother {
        cell_id: String,
        current_owner: String,
        expires_at_ns: u64,
    },
    NotOwner {
        cell_id: String,
    },
    ReplicationMismatch {
        reason: String,
    },
    Unauthorized,
    InvalidArgument(String),
    Database(String),
}

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectRecord {
    pub key: String,
    pub value: Vec<u8>,
    pub generation: u64,
    pub updated_at_ns: u64,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub generation: u64,
    pub updated_at_ns: u64,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub cell_id: String,
    pub owner_id: String,
    pub epoch: u64,
    pub expires_at_ns: u64,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ExecResult {
    pub rows_affected: u64,
    pub idempotent_replay: bool,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct QueryResult {
    pub rows: Vec<Vec<SqlValue>>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PageDelta {
    pub pgno: u32,
    pub data: Vec<u8>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LtxDeltaRequest {
    pub epoch: u64,
    pub min_txid: u64,
    pub max_txid: u64,
    pub page_size: u32,
    pub commit_pages: u32,
    pub pre_apply_checksum: u64,
    pub post_apply_checksum: u64,
    pub pages: Vec<PageDelta>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReplicationState {
    pub epoch: u64,
    pub last_txid: u64,
    pub last_checksum: u64,
    pub page_count: u32,
}
