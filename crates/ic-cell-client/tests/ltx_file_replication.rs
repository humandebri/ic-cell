//! Replicates real LTX *files* (byte format used by litefs / celld) into a
//! mirror canister database, including staged multi-message deltas and
//! LTX-checksum enforcement.

use ic_cell_client::in_process::InProcessTransport;
use ic_cell_client::ltx::{LtxReplicationAdapter, IC_SQLITE_PAGE_SIZE};
use ic_cell_client::ltx_codec::{self, LtxHeader};
use ic_cell_client::{CellError, Consistency, IcCellClient, IcCellTransport, PageDelta, SqlValue};
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

fn all_pages() -> Vec<PageDelta> {
    (1..=Db::page_count().unwrap())
        .map(|pgno| PageDelta {
            pgno,
            data: Db::read_raw_page(pgno).unwrap(),
        })
        .collect()
}

fn header(min_txid: u64, max_txid: u64, commit: u32, pre: u64) -> LtxHeader {
    LtxHeader {
        page_size: IC_SQLITE_PAGE_SIZE,
        commit,
        min_txid,
        max_txid,
        timestamp_ms: 1_764_000_000_000,
        pre_apply_checksum: pre,
        ..Default::default()
    }
}

fn rows(client: &IcCellClient<InProcessTransport>) -> Vec<Vec<SqlValue>> {
    client
        .query(
            "SELECT id, msg FROM audit_log ORDER BY id",
            vec![],
            Consistency::Strong,
        )
        .unwrap()
        .rows
}

#[test]
#[serial]
fn ltx_files_snapshot_then_incremental_with_staging_and_checksums() {
    // --- Source ("celld") database: produce a snapshot and an incremental LTX file.
    let src = init_fresh_db();
    let src_client = IcCellClient::new(InProcessTransport::new(1));
    src_client
        .execute(
            "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, msg TEXT NOT NULL)",
            vec![],
            None,
            None,
        )
        .unwrap();
    // Enough rows to span several 16 KiB pages.
    for i in 1..=40 {
        src_client
            .execute(
                "INSERT INTO audit_log (id, msg) VALUES (?1, ?2)",
                vec![SqlValue::Integer(i), SqlValue::Text("x".repeat(1500))],
                None,
                None,
            )
            .unwrap();
    }
    let snap_pages = all_pages();
    let snap_commit = snap_pages.len() as u32;
    assert!(snap_commit >= 4, "need multiple pages, got {snap_commit}");
    let snap_checksum = ltx_codec::database_checksum(IC_SQLITE_PAGE_SIZE, &snap_pages);
    let snapshot = ltx_codec::encode(&header(1, 1, snap_commit, 0), &snap_pages, None).unwrap();

    src_client
        .execute(
            "UPDATE audit_log SET msg = 'rewritten' WHERE id = 3",
            vec![],
            None,
            None,
        )
        .unwrap();
    src_client
        .execute(
            "INSERT INTO audit_log (id, msg) VALUES (41, 'tail')",
            vec![],
            None,
            None,
        )
        .unwrap();
    let new_pages = all_pages();
    let new_commit = new_pages.len() as u32;
    let new_checksum = ltx_codec::database_checksum(IC_SQLITE_PAGE_SIZE, &new_pages);
    let changed: Vec<PageDelta> = new_pages
        .iter()
        .filter(|p| {
            snap_pages
                .get(p.pgno as usize - 1)
                .map(|o| o.data != p.data)
                .unwrap_or(true)
        })
        .cloned()
        .collect();
    assert!(!changed.is_empty() && changed.len() < new_pages.len());
    let inc_header = header(2, 2, new_commit, snap_checksum);
    let incremental = ltx_codec::encode(&inc_header, &changed, Some(new_checksum)).unwrap();
    let bad_incremental =
        ltx_codec::encode(&inc_header, &changed, Some(new_checksum ^ 0x55)).unwrap();
    let expected_rows = rows(&src_client);
    drop(src);

    // --- Mirror canister.
    let _dst = init_fresh_db();
    let transport = InProcessTransport::new(2);
    // Two pages per message forces the staged multi-message path.
    let adapter = LtxReplicationAdapter::new(&transport)
        .with_max_message_page_bytes(2 * IC_SQLITE_PAGE_SIZE as usize);

    // Corrupted bytes never reach the canister.
    let mut corrupt = snapshot.clone();
    let mid = corrupt.len() / 2;
    corrupt[mid] ^= 0xff;
    assert!(matches!(
        adapter.replicate_ltx_bytes(&corrupt, 1),
        Err(CellError::ReplicationMismatch { .. })
    ));

    let state = adapter.replicate_ltx_bytes(&snapshot, 1).unwrap();
    assert_eq!(state.last_txid, 1);
    assert_eq!(state.ltx_checksum, snap_checksum);
    assert_eq!(
        ic_cell_canister::replication::ltx_checksum_full_scan(IC_SQLITE_PAGE_SIZE).unwrap(),
        snap_checksum
    );

    // A wrong LTX post-apply checksum is rejected before any page is written.
    let before = transport.get_replication_state().unwrap();
    let err = adapter
        .replicate_ltx_bytes(&bad_incremental, 1)
        .unwrap_err();
    assert!(
        matches!(&err, CellError::ReplicationMismatch { reason } if reason.contains("ltx_post_apply_checksum")),
        "{err:?}"
    );
    assert_eq!(transport.get_replication_state().unwrap(), before);
    assert_eq!(
        ic_cell_canister::replication::ltx_checksum_full_scan(IC_SQLITE_PAGE_SIZE).unwrap(),
        snap_checksum
    );

    let state = adapter.replicate_ltx_bytes(&incremental, 1).unwrap();
    assert_eq!(state.last_txid, 2);
    assert_eq!(state.page_count, new_commit);
    assert_eq!(state.ltx_checksum, new_checksum);
    assert_eq!(
        ic_cell_canister::replication::ltx_checksum_full_scan(IC_SQLITE_PAGE_SIZE).unwrap(),
        new_checksum
    );

    let mirror = IcCellClient::new(transport);
    assert_eq!(rows(&mirror), expected_rows);

    // Replaying the same incremental file is a TXID gap now.
    let replay = LtxReplicationAdapter::new(mirror.transport())
        .replicate_ltx_bytes(&incremental, 1)
        .unwrap_err();
    assert!(matches!(replay, CellError::ReplicationMismatch { .. }));
}

#[test]
#[serial]
fn snapshot_must_cover_every_page() {
    let _g = init_fresh_db();
    let pages = all_pages();
    let transport = InProcessTransport::new(1);
    let err = transport
        .apply_ltx_delta(ic_cell_client::LtxDeltaRequest {
            epoch: 1,
            min_txid: 1,
            max_txid: 1,
            page_size: IC_SQLITE_PAGE_SIZE,
            commit_pages: pages.len() as u32 + 1,
            pre_apply_checksum: 0,
            post_apply_checksum: 0,
            ltx_pre_apply_checksum: None,
            ltx_post_apply_checksum: None,
            pages,
        })
        .unwrap_err();
    assert!(
        matches!(&err, CellError::ReplicationMismatch { reason } if reason.contains("missing page")),
        "{err:?}"
    );
}

#[test]
#[serial]
fn staged_upload_abort_and_unknown_commit() {
    let _g = init_fresh_db();
    let transport = InProcessTransport::new(1);
    let page = PageDelta {
        pgno: 1,
        data: Db::read_raw_page(1).unwrap(),
    };
    assert_eq!(
        transport
            .stage_ltx_pages("u1".into(), vec![page.clone()])
            .unwrap(),
        IC_SQLITE_PAGE_SIZE as u64
    );
    // Re-staging the same page replaces it instead of double counting.
    assert_eq!(
        transport.stage_ltx_pages("u1".into(), vec![page]).unwrap(),
        IC_SQLITE_PAGE_SIZE as u64
    );
    assert_eq!(transport.abort_staged_ltx("u1".into()).unwrap(), 1);
    let err = transport
        .commit_staged_ltx_delta(
            "u1".into(),
            ic_cell_client::LtxDeltaRequest {
                epoch: 1,
                min_txid: 1,
                max_txid: 1,
                page_size: IC_SQLITE_PAGE_SIZE,
                commit_pages: 1,
                pre_apply_checksum: 0,
                post_apply_checksum: 0,
                ltx_pre_apply_checksum: None,
                ltx_post_apply_checksum: None,
                pages: vec![],
            },
        )
        .unwrap_err();
    assert!(matches!(err, CellError::NotFound { .. }));
}
