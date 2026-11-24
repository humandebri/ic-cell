use ic_cell_client::in_process::InProcessTransport;
use ic_cell_client::object_store::{IcObjectStore, PutMode};
use ic_cell_client::CellError;
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
    (Guard, InProcessTransport::new(1_000))
}

#[test]
#[serial]
fn concurrent_cas_rejects_stale_writer_and_preserves_winner() {
    let (_guard, transport) = setup();
    let store = IcObjectStore::new(&transport);

    // Initial conditional create
    let r1 = store
        .put("cells/agent-1/owner", b"node-a".to_vec(), PutMode::Create, None)
        .unwrap();
    assert_eq!(r1.generation, 1);

    // Both Node A and Node B observed generation 1.
    // Node A commits first:
    let r2 = store
        .put(
            "cells/agent-1/owner",
            b"node-a-v2".to_vec(),
            PutMode::Update {
                expected_generation: 1,
            },
            None,
        )
        .unwrap();
    assert_eq!(r2.generation, 2);

    // Node B tries to commit with stale generation 1 -> CasConflict
    let err = store
        .put(
            "cells/agent-1/owner",
            b"node-b-v2".to_vec(),
            PutMode::Update {
                expected_generation: 1,
            },
            None,
        )
        .unwrap_err();
    assert_eq!(
        err,
        CellError::CasConflict {
            key: "cells/agent-1/owner".into(),
            expected_generation: 1,
            actual_generation: Some(2),
        }
    );

    // Winner's value is intact
    let latest = store.get("cells/agent-1/owner").unwrap().unwrap();
    assert_eq!(latest.value, b"node-a-v2");
    assert_eq!(latest.generation, 2);
}
