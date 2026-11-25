#[cfg(feature = "ic-agent")]
pub mod ic_agent;
pub mod in_process;
pub mod ltx;
pub mod ltx_codec;
pub mod object_store;
#[cfg(feature = "object-store")]
pub mod object_store_adapter;

pub use ic_cell_canister::types::{
    CellError, ExecResult, LeaseRecord, LtxDeltaRequest, ObjectMeta, ObjectRecord, PageDelta,
    QueryResult, ReplicationState, SqlValue,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    Strong,
    Eventual,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FencingToken {
    pub cell_id: String,
    pub owner_id: String,
    pub epoch: u64,
}

impl FencingToken {
    pub fn as_tuple(&self) -> (String, String, u64) {
        (self.cell_id.clone(), self.owner_id.clone(), self.epoch)
    }
}

/// Transport abstraction between `ic-cell-client` and an `ic-cell-canister`.
/// Implementations can target `ic-agent` (live ICP / dfx replica) or
/// `InProcessTransport` for deterministic local testing.
pub trait IcCellTransport {
    fn acquire_lease(
        &self,
        cell_id: String,
        owner_id: String,
        ttl_ns: u64,
    ) -> Result<LeaseRecord, CellError>;

    fn release_lease(&self, cell_id: String, owner_id: String, epoch: u64)
        -> Result<(), CellError>;

    fn get_lease(&self, cell_id: &str) -> Result<Option<LeaseRecord>, CellError>;

    fn put_object(
        &self,
        key: String,
        value: Vec<u8>,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectRecord, CellError>;

    fn get_object(
        &self,
        key: &str,
        consistency: Consistency,
    ) -> Result<Option<ObjectRecord>, CellError>;

    fn head_object(
        &self,
        key: &str,
        consistency: Consistency,
    ) -> Result<Option<ObjectMeta>, CellError>;

    fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        consistency: Consistency,
    ) -> Result<Vec<u8>, CellError>;

    fn list_objects(
        &self,
        prefix: &str,
        consistency: Consistency,
    ) -> Result<Vec<ObjectMeta>, CellError>;

    fn delete_object(
        &self,
        key: String,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<bool, CellError>;

    fn put_object_chunk(
        &self,
        upload_id: String,
        chunk_index: u32,
        data: Vec<u8>,
    ) -> Result<(), CellError>;

    fn commit_chunked_object(
        &self,
        upload_id: String,
        key: String,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectMeta, CellError>;

    fn sql_execute(
        &self,
        sql: String,
        args: Vec<SqlValue>,
        idempotency_key: Option<String>,
        fencing: Option<FencingToken>,
    ) -> Result<ExecResult, CellError>;

    fn sql_query(
        &self,
        sql: String,
        args: Vec<SqlValue>,
        consistency: Consistency,
    ) -> Result<QueryResult, CellError>;

    fn apply_ltx_delta(&self, req: LtxDeltaRequest) -> Result<ReplicationState, CellError>;

    fn get_replication_state(&self) -> Result<ReplicationState, CellError>;

    /// Stages pages of a delta too large for one ingress message.
    fn stage_ltx_pages(&self, upload_id: String, pages: Vec<PageDelta>) -> Result<u64, CellError>;

    /// Atomically applies staged pages plus `req.pages`.
    fn commit_staged_ltx_delta(
        &self,
        upload_id: String,
        req: LtxDeltaRequest,
    ) -> Result<ReplicationState, CellError>;

    fn abort_staged_ltx(&self, upload_id: String) -> Result<u64, CellError>;

    /// Discards chunks of an uncommitted chunked object upload.
    fn abort_chunked_upload(&self, upload_id: &str) -> Result<u64, CellError>;
}

pub struct IcCellClient<T: IcCellTransport> {
    transport: T,
}

impl<T: IcCellTransport> IcCellClient<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn acquire_lease(
        &self,
        cell_id: impl Into<String>,
        owner_id: impl Into<String>,
        ttl_ns: u64,
    ) -> Result<LeaseRecord, CellError> {
        self.transport
            .acquire_lease(cell_id.into(), owner_id.into(), ttl_ns)
    }

    pub fn release_lease(&self, token: &FencingToken) -> Result<(), CellError> {
        self.transport
            .release_lease(token.cell_id.clone(), token.owner_id.clone(), token.epoch)
    }

    pub fn execute(
        &self,
        sql: impl Into<String>,
        args: Vec<SqlValue>,
        idempotency_key: Option<String>,
        fencing: Option<FencingToken>,
    ) -> Result<ExecResult, CellError> {
        self.transport
            .sql_execute(sql.into(), args, idempotency_key, fencing)
    }

    /// Executes a SQL write with an idempotency key, retrying only transient
    /// `CellError::Transport` errors with exponential backoff (ADR-0002).
    /// Non-retryable SQL / CAS / fencing errors are returned immediately.
    pub fn execute_idempotent_with_retry(
        &self,
        sql: impl Into<String>,
        args: Vec<SqlValue>,
        idempotency_key: impl Into<String>,
        fencing: Option<FencingToken>,
        max_retries: usize,
        base_backoff: std::time::Duration,
    ) -> Result<ExecResult, CellError> {
        let sql = sql.into();
        let key = idempotency_key.into();
        let mut attempt = 0;
        loop {
            match self.transport.sql_execute(
                sql.clone(),
                args.clone(),
                Some(key.clone()),
                fencing.clone(),
            ) {
                Ok(res) => return Ok(res),
                Err(CellError::Transport(_)) if attempt < max_retries => {
                    let delay = base_backoff.saturating_mul(1u32 << attempt.min(10));
                    if !delay.is_zero() {
                        std::thread::sleep(delay);
                    }
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn query(
        &self,
        sql: impl Into<String>,
        args: Vec<SqlValue>,
        consistency: Consistency,
    ) -> Result<QueryResult, CellError> {
        self.transport.sql_query(sql.into(), args, consistency)
    }
}
