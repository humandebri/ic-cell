use ic_cell_client::in_process::InProcessTransport;
use ic_cell_client::{CellError, Consistency, FencingToken, IcCellClient, SqlValue};
use ic_sqlite_vfs::test_support::VectorMemory;
use ic_sqlite_vfs::{Db, MemoryId, MemoryManager};
use serial_test::serial;

struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        ic_sqlite_vfs::test_support::memory::reset_for_tests();
        ic_sqlite_vfs::test_support::lock::reset_for_tests();
    }
}

fn setup() -> (Guard, InProcessTransport) {
    ic_sqlite_vfs::test_support::memory::reset_for_tests();
    ic_sqlite_vfs::test_support::lock::reset_for_tests();
    let mgr = MemoryManager::init(VectorMemory::default());
    Db::init(mgr.get(MemoryId::new(120))).unwrap();
    Db::migrate(ic_cell_canister::storage::MIGRATIONS).unwrap();
    (Guard, InProcessTransport::new(10_000))
}

#[test]
#[serial]
fn node_failover_fences_previous_owner_and_recovers_state() {
    let (_guard, transport) = setup();
    let client = IcCellClient::new(transport);

    // Node A acquires lease for cell "agent-42"
    let lease_a = client.acquire_lease("agent-42", "node-a", 5_000).unwrap();
    let token_a = FencingToken {
        cell_id: lease_a.cell_id,
        owner_id: lease_a.owner_id,
        epoch: lease_a.epoch,
    };

    client
        .execute(
            "CREATE TABLE tasks (id TEXT PRIMARY KEY, status TEXT NOT NULL)",
            vec![],
            None,
            Some(token_a.clone()),
        )
        .unwrap();

    client
        .execute(
            "INSERT INTO tasks (id, status) VALUES (?1, ?2)",
            vec![SqlValue::Text("t1".into()), SqlValue::Text("running".into())],
            Some("task-t1-init".into()),
            Some(token_a.clone()),
        )
        .unwrap();

    // Node A crashes; time advances past TTL (5_000ns)
    client.transport().advance_ns(5_000);

    // Node B takes over ownership -> epoch increments to 2
    let lease_b = client.acquire_lease("agent-42", "node-b", 5_000).unwrap();
    assert_eq!(lease_b.epoch, 2);
    let token_b = FencingToken {
        cell_id: lease_b.cell_id,
        owner_id: lease_b.owner_id,
        epoch: lease_b.epoch,
    };

    // Delayed write from zombie Node A is rejected with StaleEpoch
    let zombie = client.execute(
        "UPDATE tasks SET status = 'corrupted' WHERE id = 't1'",
        vec![],
        None,
        Some(token_a),
    );
    assert_eq!(
        zombie,
        Err(CellError::StaleEpoch {
            cell_id: "agent-42".into(),
            expected_epoch: 1,
            actual_epoch: 2,
        })
    );

    // Node B recovers task state and completes it
    let res = client
        .execute(
            "UPDATE tasks SET status = 'done' WHERE id = 't1'",
            vec![],
            Some("task-t1-done".into()),
            Some(token_b),
        )
        .unwrap();
    assert_eq!(res.rows_affected, 1);

    let rows = client
        .query(
            "SELECT id, status FROM tasks WHERE id = ?1",
            vec![SqlValue::Text("t1".into())],
            Consistency::Strong,
        )
        .unwrap()
        .rows;
    assert_eq!(
        rows,
        vec![vec![SqlValue::Text("t1".into()), SqlValue::Text("done".into())]]
    );
}
