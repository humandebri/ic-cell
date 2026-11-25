pub mod auth;
pub mod ownership;
pub mod replication;
pub mod storage;
pub mod types;

use candid::Principal;
use ic_sqlite_vfs::{Db, DefaultMemoryImpl, Memory, MemoryId, MemoryManager};
use std::cell::RefCell;
use types::{
    CellError, ExecResult, InitArgs, LeaseRecord, LtxDeltaRequest, ObjectMeta, ObjectRecord,
    PageDelta, QueryResult, ReplicationState, SqlValue, MAX_REPLY_PAYLOAD_BYTES,
};

const SQLITE_MEMORY_ID: MemoryId = MemoryId::new(120);
const REPLICATION_META_MEMORY_ID: MemoryId = MemoryId::new(121);
const AUTH_MEMORY_ID: MemoryId = MemoryId::new(122);
const WASM_PAGE_SIZE: u64 = 65_536;

thread_local! {
    static MEMORY_MANAGER: RefCell<MemoryManager<DefaultMemoryImpl>> =
        RefCell::new(
            MemoryManager::init_strict(DefaultMemoryImpl::default())
                .expect("stable memory must either be empty or use MemoryManager layout"),
        );
}

fn init_db() {
    MEMORY_MANAGER.with(|manager| {
        Db::init(manager.borrow().get(SQLITE_MEMORY_ID)).expect("Db::init failed");
    });
}

fn write_region(id: MemoryId, bytes: &[u8]) {
    MEMORY_MANAGER.with(|manager| {
        let mem = manager.borrow().get(id);
        let needed = (bytes.len() as u64).div_ceil(WASM_PAGE_SIZE).max(1);
        if mem.size() < needed {
            let grown = mem.grow(needed - mem.size());
            if grown < 0 {
                ic_cdk::trap("failed to grow stable memory region");
            }
        }
        mem.write(0, bytes);
    });
}

fn read_region(id: MemoryId, len: usize) -> Option<Vec<u8>> {
    MEMORY_MANAGER.with(|manager| {
        let mem = manager.borrow().get(id);
        if mem.size() == 0 {
            return None;
        }
        let len = len.min((mem.size() * WASM_PAGE_SIZE) as usize);
        let mut buf = vec![0u8; len];
        mem.read(0, &mut buf);
        Some(buf)
    })
}

fn save_replication_meta() {
    write_region(REPLICATION_META_MEMORY_ID, &replication::encode_state());
}

fn load_replication_meta() {
    if let Some(buf) = read_region(REPLICATION_META_MEMORY_ID, replication::ENCODED_STATE_LEN) {
        replication::decode_and_restore_state(&buf);
    }
}

fn save_allowlist() {
    write_region(AUTH_MEMORY_ID, &auth::encode());
}

fn load_allowlist() {
    if let Some(buf) = read_region(AUTH_MEMORY_ID, auth::max_encoded_len()) {
        auth::decode_and_restore(&buf);
    }
}

fn apply_init_args(args: Option<InitArgs>) {
    let callers = args.and_then(|a| a.authorized_callers).unwrap_or_default();
    for principal in callers {
        auth::insert(principal).unwrap_or_else(|e| ic_cdk::trap(&e));
    }
}

fn authorize_caller() -> Result<(), CellError> {
    let caller = ic_cdk::api::msg_caller();
    if ic_cdk::api::is_controller(&caller) || auth::contains(&caller) {
        Ok(())
    } else {
        Err(CellError::Unauthorized)
    }
}

fn authorize_controller() -> Result<(), CellError> {
    if ic_cdk::api::is_controller(&ic_cdk::api::msg_caller()) {
        Ok(())
    } else {
        Err(CellError::Unauthorized)
    }
}

fn now_ns() -> u64 {
    ic_cdk::api::time()
}

#[ic_cdk::init]
fn init(args: Option<InitArgs>) {
    let installer = ic_cdk::api::msg_caller();
    if installer != Principal::anonymous() {
        auth::insert(installer).unwrap_or_else(|e| ic_cdk::trap(&e));
    }
    apply_init_args(args);
    save_allowlist();
    init_db();
    Db::migrate(storage::MIGRATIONS).expect("Db::migrate failed");
}

#[ic_cdk::pre_upgrade]
fn pre_upgrade() {
    save_replication_meta();
    save_allowlist();
}

#[ic_cdk::post_upgrade]
fn post_upgrade(args: Option<InitArgs>) {
    init_db();
    Db::migrate(storage::MIGRATIONS).expect("Db::migrate failed");
    load_replication_meta();
    load_allowlist();
    apply_init_args(args);
    save_allowlist();
}

#[ic_cdk::update]
pub fn add_authorized_caller(principal: Principal) -> Result<bool, CellError> {
    authorize_controller()?;
    let inserted = auth::insert(principal).map_err(CellError::InvalidArgument)?;
    save_allowlist();
    Ok(inserted)
}

#[ic_cdk::update]
pub fn remove_authorized_caller(principal: Principal) -> Result<bool, CellError> {
    authorize_controller()?;
    let removed = auth::remove(&principal);
    save_allowlist();
    Ok(removed)
}

#[ic_cdk::query]
pub fn list_authorized_callers() -> Result<Vec<Principal>, CellError> {
    authorize_caller()?;
    Ok(auth::list())
}

#[ic_cdk::update]
pub fn acquire_lease(
    cell_id: String,
    owner_id: String,
    ttl_ns: u64,
) -> Result<LeaseRecord, CellError> {
    authorize_caller()?;
    ownership::acquire_or_renew_lease(cell_id, owner_id, ttl_ns, now_ns())
}

#[ic_cdk::update]
pub fn release_lease(cell_id: String, owner_id: String, epoch: u64) -> Result<(), CellError> {
    authorize_caller()?;
    ownership::release_lease(cell_id, owner_id, epoch)
}

#[ic_cdk::update]
pub fn get_lease(cell_id: String) -> Result<Option<LeaseRecord>, CellError> {
    authorize_caller()?;
    ownership::get_lease(&cell_id)
}

#[ic_cdk::update]
pub fn put_object(
    key: String,
    value: Vec<u8>,
    expected_generation: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<ObjectRecord, CellError> {
    authorize_caller()?;
    storage::put_object_cas(key, value, expected_generation, now_ns(), fencing)
}

#[ic_cdk::update]
pub fn get_object_strong(key: String) -> Result<Option<ObjectRecord>, CellError> {
    authorize_caller()?;
    fit_reply(storage::get_object(&key, true)?)
}

/// Rejects whole-object reads that would exceed the IC reply limit (which
/// would otherwise trap) and points callers at the ranged API.
fn fit_reply(record: Option<ObjectRecord>) -> Result<Option<ObjectRecord>, CellError> {
    match record {
        Some(r) if r.value.len() as u64 > MAX_REPLY_PAYLOAD_BYTES => Err(CellError::InvalidArgument(format!(
            "object {} is {} bytes, above the {MAX_REPLY_PAYLOAD_BYTES}-byte reply limit; use get_object_range_strong",
            r.key,
            r.value.len()
        ))),
        other => Ok(other),
    }
}

#[ic_cdk::query]
pub fn get_object_eventual(key: String) -> Result<Option<ObjectRecord>, CellError> {
    authorize_caller()?;
    fit_reply(storage::get_object(&key, false)?)
}

#[ic_cdk::update]
pub fn delete_object(
    key: String,
    expected_generation: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<bool, CellError> {
    authorize_caller()?;
    storage::delete_object_cas(key, expected_generation, fencing)
}

#[ic_cdk::update]
pub fn sql_execute(
    sql: String,
    args: Vec<SqlValue>,
    idempotency_key: Option<String>,
    fencing: Option<(String, String, u64)>,
) -> Result<ExecResult, CellError> {
    authorize_caller()?;
    storage::execute_sql(sql, args, now_ns(), idempotency_key, fencing)
}

#[ic_cdk::update]
pub fn sql_query_strong(sql: String, args: Vec<SqlValue>) -> Result<QueryResult, CellError> {
    authorize_caller()?;
    storage::query_sql(sql, args, true)
}

#[ic_cdk::query]
pub fn sql_query_eventual(sql: String, args: Vec<SqlValue>) -> Result<QueryResult, CellError> {
    authorize_caller()?;
    storage::query_sql(sql, args, false)
}

#[ic_cdk::update]
pub fn gc_idempotency(older_than_ns: u64) -> Result<u64, CellError> {
    authorize_caller()?;
    storage::gc_idempotency(older_than_ns)
}

#[ic_cdk::update]
pub fn abort_chunked_upload(upload_id: String) -> Result<u64, CellError> {
    authorize_caller()?;
    storage::abort_chunked_upload(&upload_id)
}

#[ic_cdk::update]
pub fn head_object_strong(key: String) -> Result<Option<ObjectMeta>, CellError> {
    authorize_caller()?;
    storage::head_object(&key, true)
}

#[ic_cdk::update]
pub fn get_object_range_strong(
    key: String,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, CellError> {
    authorize_caller()?;
    if length > MAX_REPLY_PAYLOAD_BYTES {
        return Err(CellError::InvalidArgument(format!(
            "range length {length} exceeds the {MAX_REPLY_PAYLOAD_BYTES}-byte reply limit"
        )));
    }
    storage::get_object_range(&key, offset, length, true)
}

#[ic_cdk::update]
pub fn list_objects_strong(prefix: String) -> Result<Vec<ObjectMeta>, CellError> {
    authorize_caller()?;
    storage::list_objects(&prefix, true)
}

#[ic_cdk::update]
pub fn put_object_chunk(
    upload_id: String,
    chunk_index: u32,
    data: Vec<u8>,
) -> Result<(), CellError> {
    authorize_caller()?;
    storage::put_object_chunk(upload_id, chunk_index, data)
}

#[ic_cdk::update]
pub fn commit_chunked_object(
    upload_id: String,
    key: String,
    expected_generation: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<ObjectMeta, CellError> {
    authorize_caller()?;
    // Return metadata only: echoing the assembled value could exceed the
    // reply limit for exactly the objects that needed chunking.
    storage::commit_chunked_object(upload_id, key, expected_generation, now_ns(), fencing)
        .map(|r| ObjectMeta::from(&r))
}

#[ic_cdk::update]
pub fn apply_ltx_delta(req: LtxDeltaRequest) -> Result<ReplicationState, CellError> {
    authorize_caller()?;
    let state = replication::apply_ltx_delta(req)?;
    save_replication_meta();
    Ok(state)
}

#[ic_cdk::update]
pub fn stage_ltx_pages(upload_id: String, pages: Vec<PageDelta>) -> Result<u64, CellError> {
    authorize_caller()?;
    replication::stage_ltx_pages(upload_id, pages)
}

#[ic_cdk::update]
pub fn commit_staged_ltx_delta(
    upload_id: String,
    req: LtxDeltaRequest,
) -> Result<ReplicationState, CellError> {
    authorize_caller()?;
    let state = replication::commit_staged_ltx_delta(&upload_id, req)?;
    save_replication_meta();
    Ok(state)
}

#[ic_cdk::update]
pub fn abort_staged_ltx(upload_id: String) -> Result<u64, CellError> {
    authorize_caller()?;
    Ok(replication::abort_staged_ltx(&upload_id))
}

/// Replication progress metadata (no user data); intentionally public.
#[ic_cdk::query]
pub fn get_replication_state() -> ReplicationState {
    replication::get_replication_state()
}

ic_cdk::export_candid!();

#[cfg(test)]
mod tests {
    use super::*;
    use ic_sqlite_vfs::db::Db;
    use ic_sqlite_vfs::test_support::VectorMemory;
    use serial_test::serial;

    struct TestDbGuard;

    impl Drop for TestDbGuard {
        fn drop(&mut self) {
            ic_sqlite_vfs::test_support::memory::reset_for_tests();
            ic_sqlite_vfs::test_support::lock::reset_for_tests();
        }
    }

    fn setup_fresh_db() -> TestDbGuard {
        ic_sqlite_vfs::test_support::memory::reset_for_tests();
        ic_sqlite_vfs::test_support::lock::reset_for_tests();
        let manager = MemoryManager::init(VectorMemory::default());
        Db::init(manager.get(SQLITE_MEMORY_ID)).expect("Db::init failed");
        Db::migrate(storage::MIGRATIONS).expect("Db::migrate failed");
        TestDbGuard
    }

    #[test]
    fn did_file_matches_exported_interface() {
        use candid_parser::utils::{service_equal, CandidSource};
        let exported = __export_service();
        let did = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("ic_cell_canister.did");
        service_equal(CandidSource::Text(&exported), CandidSource::File(&did)).unwrap_or_else(
            |e| panic!("ic_cell_canister.did is out of date: {e}\n--- exported ---\n{exported}"),
        );
    }

    #[test]
    #[serial]
    fn cas_create_update_conflict_and_delete() {
        let _guard = setup_fresh_db();

        let created = storage::put_object_cas("k1".into(), b"v1".to_vec(), 0, 100, None)
            .expect("create should succeed");
        assert_eq!(created.generation, 1);

        // Duplicate create fails
        let dup = storage::put_object_cas("k1".into(), b"v2".to_vec(), 0, 101, None);
        assert!(matches!(dup, Err(CellError::AlreadyExists { .. })));

        // Valid CAS update succeeds
        let updated = storage::put_object_cas("k1".into(), b"v2".to_vec(), 1, 102, None)
            .expect("cas update should succeed");
        assert_eq!(updated.generation, 2);
        assert_eq!(updated.value, b"v2");

        // Stale generation fails with CasConflict
        let conflict = storage::put_object_cas("k1".into(), b"v3".to_vec(), 1, 103, None);
        assert_eq!(
            conflict,
            Err(CellError::CasConflict {
                key: "k1".into(),
                expected_generation: 1,
                actual_generation: Some(2),
            })
        );

        // Strong & eventual read both return latest
        let got = storage::get_object("k1", true).unwrap().unwrap();
        assert_eq!(got.value, b"v2");

        // Delete with correct generation succeeds
        assert!(storage::delete_object_cas("k1".into(), 2, None).unwrap());
        assert!(storage::get_object("k1", true).unwrap().is_none());
    }

    #[test]
    #[serial]
    fn lease_failover_and_fencing_epoch() {
        let _guard = setup_fresh_db();

        // Node A acquires lease at t=100 with ttl=50 (expires at 150)
        let lease_a = ownership::acquire_or_renew_lease("cell-1".into(), "node-a".into(), 50, 100)
            .expect("node-a acquire");
        assert_eq!(lease_a.epoch, 1);

        // Node B cannot steal before expiry
        let steal_early =
            ownership::acquire_or_renew_lease("cell-1".into(), "node-b".into(), 50, 149);
        assert!(matches!(
            steal_early,
            Err(CellError::LeaseHeldByAnother { .. })
        ));

        // Node B takes over at t=150 -> epoch increments to 2
        let lease_b = ownership::acquire_or_renew_lease("cell-1".into(), "node-b".into(), 50, 150)
            .expect("node-b takeover");
        assert_eq!(lease_b.epoch, 2);

        // Node A's delayed write with epoch=1 is fenced out
        let fenced = storage::put_object_cas(
            "state".into(),
            b"stale".to_vec(),
            0,
            151,
            Some(("cell-1".into(), "node-a".into(), 1)),
        );
        assert_eq!(
            fenced,
            Err(CellError::StaleEpoch {
                cell_id: "cell-1".into(),
                expected_epoch: 1,
                actual_epoch: 2,
            })
        );

        // Node B's write with epoch=2 succeeds
        let ok = storage::put_object_cas(
            "state".into(),
            b"fresh".to_vec(),
            0,
            152,
            Some(("cell-1".into(), "node-b".into(), 2)),
        )
        .expect("node-b write should succeed");
        assert_eq!(ok.generation, 1);
    }

    #[test]
    #[serial]
    fn sql_execute_idempotency_and_query() {
        let _guard = setup_fresh_db();

        storage::execute_sql(
            "CREATE TABLE memories (key TEXT PRIMARY KEY, value TEXT NOT NULL)".into(),
            vec![],
            100,
            None,
            None,
        )
        .unwrap();

        let r1 = storage::execute_sql(
            "INSERT INTO memories (key, value) VALUES (?1, ?2)".into(),
            vec![
                SqlValue::Text("goal".into()),
                SqlValue::Text("persist on-chain".into()),
            ],
            101,
            Some("req-001".into()),
            None,
        )
        .unwrap();
        assert_eq!(r1.rows_affected, 1);
        assert!(!r1.idempotent_replay);

        // Replaying the same idempotency_key returns cached result without PK violation
        let r2 = storage::execute_sql(
            "INSERT INTO memories (key, value) VALUES (?1, ?2)".into(),
            vec![
                SqlValue::Text("goal".into()),
                SqlValue::Text("persist on-chain".into()),
            ],
            102,
            Some("req-001".into()),
            None,
        )
        .unwrap();
        assert_eq!(r2.rows_affected, 1);
        assert!(r2.idempotent_replay);

        let qr = storage::query_sql(
            "SELECT key, value FROM memories WHERE key = ?1".into(),
            vec![SqlValue::Text("goal".into())],
            true,
        )
        .unwrap();
        assert_eq!(
            qr.rows,
            vec![vec![
                SqlValue::Text("goal".into()),
                SqlValue::Text("persist on-chain".into()),
            ]]
        );

        // GC removes idempotency records older than threshold
        assert_eq!(storage::gc_idempotency(105).unwrap(), 1);
        assert_eq!(storage::gc_idempotency(105).unwrap(), 0);
    }
}
