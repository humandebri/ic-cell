use ic_cell_client::in_process::InProcessTransport;
use ic_cell_client::ltx::{LtxFrame, LtxReplicationAdapter, IC_SQLITE_PAGE_SIZE};
use ic_cell_client::object_store::{IcObjectStore, PutMode};
use ic_cell_client::{CellError, Consistency, IcCellClient, PageDelta, SqlValue};
use ic_sqlite_vfs::test_support::VectorMemory;
use ic_sqlite_vfs::{Db, MemoryId, MemoryManager};
use serial_test::serial;

struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        ic_cell_canister::replication::reset_replication_state_for_tests();
        ic_sqlite_vfs::test_support::memory::reset_for_tests();
        ic_sqlite_vfs::test_support::lock::reset_for_tests();
    }
}

fn init_fresh_db() -> Guard {
    ic_cell_canister::replication::reset_replication_state_for_tests();
    ic_sqlite_vfs::test_support::memory::reset_for_tests();
    ic_sqlite_vfs::test_support::lock::reset_for_tests();
    let mgr = MemoryManager::init(VectorMemory::default());
    Db::init(mgr.get(MemoryId::new(120))).unwrap();
    Db::migrate(ic_cell_canister::storage::MIGRATIONS).unwrap();
    Guard
}

#[test]
#[serial]
fn phase5_ltx_page_delta_replication_and_sql_query_on_mirror() {
    // 1. Source DB (simulating celld's 16 KiB SQLite DB) writes rows and extracts raw pages
    let _src_guard = init_fresh_db();
    let src_transport = InProcessTransport::new(100);
    let src_client = IcCellClient::new(src_transport);

    src_client
        .execute(
            "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, msg TEXT NOT NULL)",
            vec![],
            None,
            None,
        )
        .unwrap();
    src_client
        .execute(
            "INSERT INTO audit_log (id, msg) VALUES (1, 'hello from celld')",
            vec![],
            None,
            None,
        )
        .unwrap();

    let page_count = Db::page_count().unwrap();
    assert!(page_count >= 2);
    let mut pages = Vec::new();
    for pgno in 1..=page_count {
        pages.push(PageDelta {
            pgno,
            data: Db::read_raw_page(pgno).unwrap(),
        });
    }
    let expected_checksum = Db::refresh_checksum().unwrap();
    drop(_src_guard);

    // 2. Fresh mirror canister DB receives ONLY the LTX page delta frame
    let _dst_guard = init_fresh_db();
    let dst_transport = InProcessTransport::new(200);
    let adapter = LtxReplicationAdapter::new(&dst_transport);

    let state = adapter
        .replicate(LtxFrame {
            epoch: 1,
            min_txid: 1,
            max_txid: 2,
            page_size: IC_SQLITE_PAGE_SIZE,
            commit_pages: page_count,
            pre_apply_checksum: 0,
            post_apply_checksum: expected_checksum,
            pages,
        })
        .unwrap();

    assert_eq!(state.last_txid, 2);
    assert_eq!(state.last_checksum, expected_checksum);

    // 3. Query the replicated SQLite image directly inside the canister!
    let dst_client = IcCellClient::new(dst_transport);
    let rows = dst_client
        .query("SELECT id, msg FROM audit_log", vec![], Consistency::Strong)
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![
            SqlValue::Integer(1),
            SqlValue::Text("hello from celld".into()),
        ]]
    );

    // 4. Non-contiguous TXID is rejected
    let gap_err = LtxReplicationAdapter::new(dst_client.transport())
        .replicate(LtxFrame {
            epoch: 1,
            min_txid: 5,
            max_txid: 5,
            page_size: IC_SQLITE_PAGE_SIZE,
            commit_pages: page_count,
            pre_apply_checksum: expected_checksum,
            post_apply_checksum: 0,
            pages: vec![],
        })
        .unwrap_err();
    assert!(matches!(gap_err, CellError::ReplicationMismatch { .. }));
}

#[test]
#[serial]
fn phase6_object_store_chunked_upload_range_read_and_list() {
    let _guard = init_fresh_db();
    let transport = InProcessTransport::new(500);
    // Use small chunk size (10 bytes) to exercise multi-chunk upload path
    let store = IcObjectStore::new(&transport).with_chunk_size(10);

    let payload = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();
    let rec = store
        .put("ltx/cell-1/0001.ltx", payload.clone(), PutMode::Create, None)
        .unwrap();
    assert_eq!(rec.generation, 1);
    assert_eq!(rec.value, payload);

    // Range read
    let slice = store.get_range("ltx/cell-1/0001.ltx", 10, 6).unwrap();
    assert_eq!(slice, b"abcdef");

    // Head metadata
    let meta = store.head("ltx/cell-1/0001.ltx").unwrap().unwrap();
    assert_eq!(meta.size, payload.len() as u64);
    assert_eq!(meta.generation, 1);

    // Prefix list
    store
        .put("ltx/cell-1/0002.ltx", b"delta2".to_vec(), PutMode::Create, None)
        .unwrap();
    store
        .put("other/key", b"x".to_vec(), PutMode::Create, None)
        .unwrap();

    let listed = store.list("ltx/cell-1/").unwrap();
    let keys: Vec<&str> = listed.iter().map(|m| m.key.as_str()).collect();
    assert_eq!(keys, vec!["ltx/cell-1/0001.ltx", "ltx/cell-1/0002.ltx"]);
}
