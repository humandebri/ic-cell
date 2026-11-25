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
    PreconditionFailed {
        reason: String,
    },
    Transport(String),
    InvalidArgument(String),
    Database(String),
}

impl std::fmt::Display for CellError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for CellError {}

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

impl From<&ObjectRecord> for ObjectMeta {
    fn from(r: &ObjectRecord) -> Self {
        Self {
            key: r.key.clone(),
            size: r.value.len() as u64,
            generation: r.generation,
            updated_at_ns: r.updated_at_ns,
        }
    }
}

/// Largest payload returned in a single reply. The IC caps replies at 2 MiB;
/// keep headroom for Candid framing.
pub const MAX_REPLY_PAYLOAD_BYTES: u64 = 2_000_000;

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
    /// LTX-format database checksum before this delta (`None` = skip check).
    #[serde(default)]
    pub ltx_pre_apply_checksum: Option<u64>,
    /// LTX-format database checksum after this delta (`None` = skip check).
    #[serde(default)]
    pub ltx_post_apply_checksum: Option<u64>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReplicationState {
    pub epoch: u64,
    pub last_txid: u64,
    pub last_checksum: u64,
    pub page_count: u32,
    /// LTX-format checksum of the mirrored database (`0` = not tracked yet).
    pub ltx_checksum: u64,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct InitArgs {
    /// Principals (besides controllers) allowed to call the canister.
    pub authorized_callers: Option<Vec<candid::Principal>>,
}

/// `(cell_id, owner_id, epoch)` as returned by `acquire_lease`.
pub type Fencing = (String, String, u64);

/// One step of an atomic cell transaction.
#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CellOp {
    KvPut {
        key: String,
        value: Vec<u8>,
    },
    KvDelete {
        key: String,
    },
    KvDeleteAll,
    /// `Some(at_ns)` sets the cell alarm (Unix ns); `None` clears it.
    SetAlarm(Option<u64>),
    /// Aborts the whole transaction unless the key currently holds `expected`
    /// (`None` = key must be absent).
    AssertKv {
        key: String,
        expected: Option<Vec<u8>>,
    },
    /// Runs SQL inside the same transaction; rows (e.g. `RETURNING`) are
    /// returned. SQL tables are shared by all cells.
    Sql {
        sql: String,
        args: Vec<SqlValue>,
    },
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TransactRequest {
    pub fencing: Fencing,
    pub idempotency_key: Option<String>,
    pub ops: Vec<CellOp>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct OpResult {
    pub rows_affected: u64,
    pub rows: Vec<Vec<SqlValue>>,
}

impl OpResult {
    pub fn affected(rows_affected: u64) -> Self {
        Self {
            rows_affected,
            rows: Vec::new(),
        }
    }
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TransactResult {
    pub results: Vec<OpResult>,
    pub idempotent_replay: bool,
    pub epoch: u64,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KvEntry {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct KvListOptions {
    pub prefix: Option<String>,
    /// Exclusive cursor (the last key of the previous page).
    pub start_after: Option<String>,
    pub limit: Option<u32>,
    pub reverse: Option<bool>,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct KvListResult {
    pub entries: Vec<KvEntry>,
    pub truncated: bool,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DueAlarm {
    pub cell_id: String,
    pub at_ns: u64,
}

#[derive(CandidType, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Activation {
    pub lease: LeaseRecord,
    pub entries: Vec<KvEntry>,
    /// `true` when `entries` is the cell's complete key/value state.
    pub complete: bool,
    pub alarm_at_ns: Option<u64>,
}
