use crate::{
    CellError, Consistency, ExecResult, FencingToken, IcCellTransport, LeaseRecord,
    LtxDeltaRequest, ObjectMeta, ObjectRecord, QueryResult, ReplicationState, SqlValue,
};
use std::sync::atomic::{AtomicU64, Ordering};

/// In-process transport backed directly by `ic-cell-canister` functions and a
/// controllable logical clock. Ideal for deterministic concurrency, failover,
/// recovery, LTX replication, and object-store tests.
pub struct InProcessTransport {
    clock_ns: AtomicU64,
}

impl InProcessTransport {
    pub fn new(initial_time_ns: u64) -> Self {
        Self {
            clock_ns: AtomicU64::new(initial_time_ns),
        }
    }

    pub fn now_ns(&self) -> u64 {
        self.clock_ns.load(Ordering::SeqCst)
    }

    pub fn advance_ns(&self, delta_ns: u64) -> u64 {
        self.clock_ns.fetch_add(delta_ns, Ordering::SeqCst) + delta_ns
    }
}

impl IcCellTransport for InProcessTransport {
    fn acquire_lease(
        &self,
        cell_id: String,
        owner_id: String,
        ttl_ns: u64,
    ) -> Result<LeaseRecord, CellError> {
        ic_cell_canister::ownership::acquire_or_renew_lease(
            cell_id,
            owner_id,
            ttl_ns,
            self.now_ns(),
        )
    }

    fn release_lease(
        &self,
        cell_id: String,
        owner_id: String,
        epoch: u64,
    ) -> Result<(), CellError> {
        ic_cell_canister::ownership::release_lease(cell_id, owner_id, epoch)
    }

    fn get_lease(&self, cell_id: &str) -> Result<Option<LeaseRecord>, CellError> {
        ic_cell_canister::ownership::get_lease(cell_id)
    }

    fn put_object(
        &self,
        key: String,
        value: Vec<u8>,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectRecord, CellError> {
        ic_cell_canister::storage::put_object_cas(
            key,
            value,
            expected_generation,
            self.now_ns(),
            fencing.map(|f| f.as_tuple()),
        )
    }

    fn get_object(
        &self,
        key: &str,
        consistency: Consistency,
    ) -> Result<Option<ObjectRecord>, CellError> {
        ic_cell_canister::storage::get_object(key, consistency == Consistency::Strong)
    }

    fn head_object(
        &self,
        key: &str,
        consistency: Consistency,
    ) -> Result<Option<ObjectMeta>, CellError> {
        ic_cell_canister::storage::head_object(key, consistency == Consistency::Strong)
    }

    fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        consistency: Consistency,
    ) -> Result<Vec<u8>, CellError> {
        ic_cell_canister::storage::get_object_range(
            key,
            offset,
            length,
            consistency == Consistency::Strong,
        )
    }

    fn list_objects(
        &self,
        prefix: &str,
        consistency: Consistency,
    ) -> Result<Vec<ObjectMeta>, CellError> {
        ic_cell_canister::storage::list_objects(prefix, consistency == Consistency::Strong)
    }

    fn delete_object(
        &self,
        key: String,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<bool, CellError> {
        ic_cell_canister::storage::delete_object_cas(
            key,
            expected_generation,
            fencing.map(|f| f.as_tuple()),
        )
    }

    fn put_object_chunk(
        &self,
        upload_id: String,
        chunk_index: u32,
        data: Vec<u8>,
    ) -> Result<(), CellError> {
        ic_cell_canister::storage::put_object_chunk(upload_id, chunk_index, data)
    }

    fn commit_chunked_object(
        &self,
        upload_id: String,
        key: String,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectRecord, CellError> {
        ic_cell_canister::storage::commit_chunked_object(
            upload_id,
            key,
            expected_generation,
            self.now_ns(),
            fencing.map(|f| f.as_tuple()),
        )
    }

    fn sql_execute(
        &self,
        sql: String,
        args: Vec<SqlValue>,
        idempotency_key: Option<String>,
        fencing: Option<FencingToken>,
    ) -> Result<ExecResult, CellError> {
        ic_cell_canister::storage::execute_sql(
            sql,
            args,
            self.now_ns(),
            idempotency_key,
            fencing.map(|f| f.as_tuple()),
        )
    }

    fn sql_query(
        &self,
        sql: String,
        args: Vec<SqlValue>,
        consistency: Consistency,
    ) -> Result<QueryResult, CellError> {
        ic_cell_canister::storage::query_sql(sql, args, consistency == Consistency::Strong)
    }

    fn apply_ltx_delta(&self, req: LtxDeltaRequest) -> Result<ReplicationState, CellError> {
        ic_cell_canister::replication::apply_ltx_delta(req)
    }

    fn get_replication_state(&self) -> Result<ReplicationState, CellError> {
        Ok(ic_cell_canister::replication::get_replication_state())
    }
}
