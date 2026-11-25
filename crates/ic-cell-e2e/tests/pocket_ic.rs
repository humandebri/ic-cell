//! End-to-end tests against the real `ic-cell-canister` wasm on PocketIC.
//!
//! Prerequisites:
//! * `cargo build -p ic-cell-canister --target wasm32-unknown-unknown --release`
//!   (or set `IC_CELL_WASM` to a wasm path)
//! * a PocketIC server binary: `POCKET_IC_BIN`, the one bundled with `dfx`
//!   (`~/.cache/dfinity/versions/*/pocket-ic`), or the crate auto-downloads it.
//!
//! Without the wasm the tests are skipped, unless `IC_CELL_E2E_REQUIRED=1`
//! (set in CI) turns a missing prerequisite into a failure.

use candid::{decode_one, encode_args, encode_one, CandidType, Principal};
use futures::TryStreamExt;
use ic_agent::identity::BasicIdentity;
use ic_agent::Identity;
use ic_cell_canister::types::InitArgs;
use ic_cell_client::ic_agent::IcAgentTransport;
use ic_cell_client::ltx::{LtxReplicationAdapter, IC_SQLITE_PAGE_SIZE};
use ic_cell_client::ltx_codec::{self, LtxHeader};
use ic_cell_client::object_store_adapter::IcObjectStoreAdapter;
use ic_cell_client::{
    CellError, Consistency, ExecResult, IcCellClient, IcCellTransport, ObjectRecord, PageDelta,
    QueryResult, ReplicationState, SqlValue,
};
use ic_sqlite_vfs::test_support::VectorMemory;
use ic_sqlite_vfs::{Db, MemoryId, MemoryManager};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use pocket_ic::{PocketIc, PocketIcBuilder};
use serde::de::DeserializeOwned;
use serial_test::serial;
use std::path::PathBuf;

const CYCLES: u128 = 100_000_000_000_000;

fn required() -> bool {
    std::env::var("IC_CELL_E2E_REQUIRED").is_ok_and(|v| v == "1")
}

fn wasm() -> Option<Vec<u8>> {
    let path = std::env::var_os("IC_CELL_WASM")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/wasm32-unknown-unknown/release/ic_cell_canister.wasm")
        });
    match std::fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(e) if required() => panic!("canister wasm {} unavailable: {e}", path.display()),
        Err(_) => {
            eprintln!(
                "SKIP: {} not found; build it with `cargo build -p ic-cell-canister --target wasm32-unknown-unknown --release`",
                path.display()
            );
            None
        }
    }
}

fn pocket_ic() -> PocketIc {
    let mut builder = PocketIcBuilder::new().with_application_subnet();
    if std::env::var_os("POCKET_IC_BIN").is_none() {
        let dfx_cache = std::env::var_os("HOME")
            .map(|h| PathBuf::from(h).join(".cache/dfinity/versions"))
            .and_then(|dir| std::fs::read_dir(dir).ok())
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|e| e.path().join("pocket-ic"))
            .find(|p| p.is_file());
        if let Some(bin) = dfx_cache {
            builder = builder.with_server_binary(bin);
        }
    }
    builder.build()
}

fn principal(byte: u8) -> Principal {
    Principal::from_slice(&[byte; 10])
}

fn agent_identity() -> BasicIdentity {
    BasicIdentity::from_signing_key(ed25519_consensus::SigningKey::from([7u8; 32]))
}

fn install(
    pic: &PocketIc,
    wasm: &[u8],
    controller: Principal,
    callers: Vec<Principal>,
) -> Principal {
    let cid = pic.create_canister_with_settings(Some(controller), None);
    pic.add_cycles(cid, CYCLES);
    let arg = encode_one(Some(InitArgs {
        authorized_callers: Some(callers),
    }))
    .unwrap();
    pic.install_canister(cid, wasm.to_vec(), arg, Some(controller));
    cid
}

fn update<R: CandidType + DeserializeOwned>(
    pic: &PocketIc,
    cid: Principal,
    sender: Principal,
    method: &str,
    args: Vec<u8>,
) -> R {
    let raw = pic
        .update_call(cid, sender, method, args)
        .unwrap_or_else(|e| panic!("{method} rejected: {e:?}"));
    decode_one(&raw).unwrap_or_else(|e| panic!("{method} decode: {e}"))
}

fn query<R: CandidType + DeserializeOwned>(
    pic: &PocketIc,
    cid: Principal,
    sender: Principal,
    method: &str,
    args: Vec<u8>,
) -> R {
    let raw = pic
        .query_call(cid, sender, method, args)
        .unwrap_or_else(|e| panic!("{method} rejected: {e:?}"));
    decode_one(&raw).unwrap_or_else(|e| panic!("{method} decode: {e}"))
}

fn sql(
    pic: &PocketIc,
    cid: Principal,
    sender: Principal,
    stmt: &str,
    key: Option<&str>,
) -> Result<ExecResult, CellError> {
    update(
        pic,
        cid,
        sender,
        "sql_execute",
        encode_args((
            stmt,
            Vec::<SqlValue>::new(),
            key.map(str::to_string),
            None::<(String, String, u64)>,
        ))
        .unwrap(),
    )
}

fn select(
    pic: &PocketIc,
    cid: Principal,
    sender: Principal,
    stmt: &str,
) -> Result<QueryResult, CellError> {
    update(
        pic,
        cid,
        sender,
        "sql_query_strong",
        encode_args((stmt, Vec::<SqlValue>::new())).unwrap(),
    )
}

#[test]
#[serial]
fn allowlist_admin_api_and_state_survive_upgrades() {
    let Some(wasm) = wasm() else { return };
    let pic = pocket_ic();
    let controller = principal(1);
    let alice = principal(2);
    let bob = principal(3);
    let cid = install(&pic, &wasm, controller, vec![alice]);

    // Alice (allowlisted) writes SQL + an object.
    sql(
        &pic,
        cid,
        alice,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        None,
    )
    .unwrap();
    let first = sql(
        &pic,
        cid,
        alice,
        "INSERT INTO notes VALUES (1, 'durable')",
        Some("req-1"),
    )
    .unwrap();
    assert!(!first.idempotent_replay);
    let put: Result<ObjectRecord, CellError> = update(
        &pic,
        cid,
        alice,
        "put_object",
        encode_args(("cfg", b"v1".to_vec(), 0u64, None::<(String, String, u64)>)).unwrap(),
    );
    assert_eq!(put.unwrap().generation, 1);

    // Bob is rejected on updates *and* queries.
    assert_eq!(
        sql(&pic, cid, bob, "SELECT 1", None),
        Err(CellError::Unauthorized)
    );
    let q: Result<QueryResult, CellError> = query(
        &pic,
        cid,
        bob,
        "sql_query_eventual",
        encode_args(("SELECT * FROM notes", Vec::<SqlValue>::new())).unwrap(),
    );
    assert_eq!(q, Err(CellError::Unauthorized));
    let o: Result<Option<ObjectRecord>, CellError> = query(
        &pic,
        cid,
        bob,
        "get_object_eventual",
        encode_one("cfg").unwrap(),
    );
    assert_eq!(o, Err(CellError::Unauthorized));
    // The anonymous principal is not implicitly allowed either.
    assert_eq!(
        sql(&pic, cid, Principal::anonymous(), "SELECT 1", None),
        Err(CellError::Unauthorized)
    );

    // Only controllers manage the allowlist.
    let by_alice: Result<bool, CellError> = update(
        &pic,
        cid,
        alice,
        "add_authorized_caller",
        encode_one(bob).unwrap(),
    );
    assert_eq!(by_alice, Err(CellError::Unauthorized));
    let added: Result<bool, CellError> = update(
        &pic,
        cid,
        controller,
        "add_authorized_caller",
        encode_one(bob).unwrap(),
    );
    assert_eq!(added, Ok(true));
    assert!(select(&pic, cid, bob, "SELECT body FROM notes").is_ok());
    let removed: Result<bool, CellError> = update(
        &pic,
        cid,
        controller,
        "remove_authorized_caller",
        encode_one(bob).unwrap(),
    );
    assert_eq!(removed, Ok(true));
    assert_eq!(
        sql(&pic, cid, bob, "SELECT 1", None),
        Err(CellError::Unauthorized)
    );

    let list = |pic: &PocketIc| -> Vec<Principal> {
        let mut l: Vec<Principal> = query::<Result<Vec<Principal>, CellError>>(
            pic,
            cid,
            controller,
            "list_authorized_callers",
            encode_args(()).unwrap(),
        )
        .unwrap();
        l.sort();
        l
    };
    let mut expected = vec![controller, alice];
    expected.sort();
    assert_eq!(list(&pic), expected);

    // Upgrade without args: data, idempotency records and allowlist persist.
    pic.upgrade_canister(
        cid,
        wasm.clone(),
        encode_one(None::<InitArgs>).unwrap(),
        Some(controller),
    )
    .expect("upgrade");
    assert_eq!(list(&pic), expected);
    assert_eq!(
        select(&pic, cid, alice, "SELECT body FROM notes")
            .unwrap()
            .rows,
        vec![vec![SqlValue::Text("durable".into())]]
    );
    let replay = sql(
        &pic,
        cid,
        alice,
        "INSERT INTO notes VALUES (1, 'durable')",
        Some("req-1"),
    )
    .unwrap();
    assert!(
        replay.idempotent_replay,
        "idempotency key must survive upgrade"
    );
    let obj: Result<Option<ObjectRecord>, CellError> = update(
        &pic,
        cid,
        alice,
        "get_object_strong",
        encode_one("cfg").unwrap(),
    );
    assert_eq!(obj.unwrap().unwrap().value, b"v1");
    assert_eq!(
        sql(&pic, cid, bob, "SELECT 1", None),
        Err(CellError::Unauthorized)
    );

    // Upgrade args can extend the allowlist.
    let arg = encode_one(Some(InitArgs {
        authorized_callers: Some(vec![bob]),
    }))
    .unwrap();
    pic.upgrade_canister(cid, wasm, arg, Some(controller))
        .expect("upgrade 2");
    assert!(select(&pic, cid, bob, "SELECT body FROM notes").is_ok());
}

/// Builds a snapshot LTX file and a follow-up incremental LTX file from a
/// native ic-sqlite-vfs database (standing in for `celld`'s SQLite).
fn build_ltx_files() -> (Vec<u8>, Vec<u8>, u64, Vec<Vec<SqlValue>>) {
    ic_sqlite_vfs::test_support::memory::reset_for_tests();
    ic_sqlite_vfs::test_support::lock::reset_for_tests();
    let mgr = MemoryManager::init(VectorMemory::default());
    Db::init(mgr.get(MemoryId::new(120))).unwrap();
    Db::migrate(ic_cell_canister::storage::MIGRATIONS).unwrap();
    let src = IcCellClient::new(ic_cell_client::in_process::InProcessTransport::new(1));
    src.execute(
        "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, msg TEXT NOT NULL)",
        vec![],
        None,
        None,
    )
    .unwrap();
    for i in 1..=60 {
        src.execute(
            "INSERT INTO audit_log VALUES (?1, ?2)",
            vec![
                SqlValue::Integer(i),
                SqlValue::Text(format!("{i}:{}", "x".repeat(1200))),
            ],
            None,
            None,
        )
        .unwrap();
    }
    let pages = |count: u32| -> Vec<PageDelta> {
        (1..=count)
            .map(|pgno| PageDelta {
                pgno,
                data: Db::read_raw_page(pgno).unwrap(),
            })
            .collect()
    };
    let header = |min, max, commit, pre| LtxHeader {
        page_size: IC_SQLITE_PAGE_SIZE,
        commit,
        min_txid: min,
        max_txid: max,
        timestamp_ms: 1_764_000_000_000,
        pre_apply_checksum: pre,
        ..Default::default()
    };
    let snap_pages = pages(Db::page_count().unwrap());
    let snap_sum = ltx_codec::database_checksum(IC_SQLITE_PAGE_SIZE, &snap_pages);
    let snapshot =
        ltx_codec::encode(&header(1, 1, snap_pages.len() as u32, 0), &snap_pages, None).unwrap();

    src.execute("DELETE FROM audit_log WHERE id % 7 = 0", vec![], None, None)
        .unwrap();
    src.execute(
        "INSERT INTO audit_log VALUES (100, 'from celld')",
        vec![],
        None,
        None,
    )
    .unwrap();
    let new_pages = pages(Db::page_count().unwrap());
    let new_sum = ltx_codec::database_checksum(IC_SQLITE_PAGE_SIZE, &new_pages);
    let changed: Vec<PageDelta> = new_pages
        .iter()
        .filter(|p| {
            snap_pages
                .get(p.pgno as usize - 1)
                .is_none_or(|o| o.data != p.data)
        })
        .cloned()
        .collect();
    let incremental = ltx_codec::encode(
        &header(2, 2, new_pages.len() as u32, snap_sum),
        &changed,
        Some(new_sum),
    )
    .unwrap();
    let rows = src
        .query(
            "SELECT id, msg FROM audit_log ORDER BY id",
            vec![],
            Consistency::Strong,
        )
        .unwrap()
        .rows;
    ic_sqlite_vfs::test_support::memory::reset_for_tests();
    ic_sqlite_vfs::test_support::lock::reset_for_tests();
    (snapshot, incremental, new_sum, rows)
}

#[test]
#[serial]
fn ltx_files_and_object_store_over_ic_agent() {
    let Some(wasm) = wasm() else { return };
    let (snapshot, incremental, final_sum, expected_rows) = build_ltx_files();

    let mut pic = pocket_ic();
    let controller = principal(1);
    let identity = agent_identity();
    let agent_principal = identity.sender().unwrap();
    let mirror = install(&pic, &wasm, controller, vec![agent_principal]);
    let objects = install(&pic, &wasm, controller, vec![agent_principal]);
    let url = pic.make_live(None);

    // --- LTX replication through ic-agent, forcing the staged path.
    let transport =
        IcAgentTransport::connect_with_identity(url.as_str(), mirror, identity, true).unwrap();
    let adapter = LtxReplicationAdapter::new(&transport)
        .with_max_message_page_bytes(4 * IC_SQLITE_PAGE_SIZE as usize);
    let s1 = adapter.replicate_ltx_bytes(&snapshot, 1).unwrap();
    assert_eq!(s1.last_txid, 1);
    let s2 = adapter.replicate_ltx_bytes(&incremental, 1).unwrap();
    assert_eq!((s2.last_txid, s2.ltx_checksum), (2, final_sum));

    let client = IcCellClient::new(transport);
    let rows = |c: &IcCellClient<IcAgentTransport>| {
        c.query(
            "SELECT id, msg FROM audit_log ORDER BY id",
            vec![],
            Consistency::Strong,
        )
        .unwrap()
        .rows
    };
    assert_eq!(rows(&client), expected_rows);

    // Upgrade the mirror: replication state (incl. LTX checksum) and data persist.
    pic.upgrade_canister(
        mirror,
        wasm.clone(),
        encode_one(None::<InitArgs>).unwrap(),
        Some(controller),
    )
    .expect("upgrade mirror");
    let state: ReplicationState = client.transport().get_replication_state().unwrap();
    assert_eq!(state, s2);
    assert_eq!(rows(&client), expected_rows);
    // Replaying an applied file is rejected (TXID gap) and changes nothing.
    assert!(LtxReplicationAdapter::new(client.transport())
        .replicate_ltx_bytes(&incremental, 1)
        .is_err());
    assert_eq!(client.transport().get_replication_state().unwrap(), s2);

    // Admin helpers on the agent transport: the agent is not a controller.
    assert_eq!(
        client.transport().add_authorized_caller(principal(9)),
        Err(CellError::Unauthorized)
    );
    assert!(client
        .transport()
        .list_authorized_callers()
        .unwrap()
        .contains(&agent_principal));

    // --- object_store::ObjectStore over ic-agent (multi-chunk payloads).
    let store = IcObjectStoreAdapter::new(
        IcAgentTransport::connect_with_identity(url.as_str(), objects, agent_identity(), true)
            .unwrap(),
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let big: Vec<u8> = (0..2_500_000u32).map(|i| (i % 251) as u8).collect();
        let path = Path::from("cells/cell-1/db.ltx");
        let put = store
            .put(&path, PutPayload::from(big.clone()))
            .await
            .unwrap();
        assert_eq!(put.e_tag.as_deref(), Some("1"));
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.len(), big.len());
        assert!(got.as_ref() == big.as_slice());
        let slice = store.get_range(&path, 1_999_990..2_000_010).await.unwrap();
        assert_eq!(slice.as_ref(), &big[1_999_990..2_000_010]);

        let mut mpu = store
            .put_multipart(&Path::from("cells/cell-1/mpu"))
            .await
            .unwrap();
        mpu.put_part(PutPayload::from(vec![1u8; 1_300_000]))
            .await
            .unwrap();
        mpu.put_part(PutPayload::from(vec![2u8; 10])).await.unwrap();
        mpu.complete().await.unwrap();
        let head = store.head(&Path::from("cells/cell-1/mpu")).await.unwrap();
        assert_eq!(head.size, 1_300_010);

        let listed: Vec<_> = store
            .list(Some(&Path::from("cells/cell-1")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 2);
        store.delete(&path).await.unwrap();
        assert!(store.head(&path).await.is_err());
    });
    drop(rt);
    drop(store);
}

#[test]
#[serial]
fn durable_cells_failover_alarms_and_upgrade_on_real_wasm() {
    use ic_cell_client::{
        Activation, CellOp, DueAlarm, KvListOptions, KvListResult, TransactRequest, TransactResult,
    };
    use std::time::Duration;

    let Some(wasm) = wasm() else { return };
    let pic = pocket_ic();
    let controller = principal(1);
    let app = principal(2);
    let cid = install(&pic, &wasm, controller, vec![app]);
    let sec = 1_000_000_000u64;

    let activate = |owner: &str| -> Result<Activation, CellError> {
        update(
            &pic,
            cid,
            app,
            "activate_cell",
            encode_args(("room-1", owner, 5 * sec, 1u64 << 20)).unwrap(),
        )
    };
    let transact = |f: (String, String, u64),
                    key: Option<&str>,
                    ops: Vec<CellOp>|
     -> Result<TransactResult, CellError> {
        update(
            &pic,
            cid,
            app,
            "cell_transact",
            encode_one(TransactRequest {
                fencing: f,
                idempotency_key: key.map(Into::into),
                ops,
            })
            .unwrap(),
        )
    };
    let fencing = |a: &Activation| {
        (
            a.lease.cell_id.clone(),
            a.lease.owner_id.clone(),
            a.lease.epoch,
        )
    };

    // node-a owns the cell and writes atomically.
    let a = activate("node-a").unwrap();
    assert!(a.complete && a.entries.is_empty());
    let now: u64 = pic.get_time().as_nanos_since_unix_epoch();
    let r = transact(
        fencing(&a),
        Some("a-1"),
        vec![
            CellOp::KvPut {
                key: "count".into(),
                value: b"1".to_vec(),
            },
            CellOp::KvPut {
                key: "msg:1".into(),
                value: b"hello".to_vec(),
            },
            CellOp::SetAlarm(Some(now + 30 * sec)),
        ],
    )
    .unwrap();
    assert_eq!(r.epoch, 1);
    assert!(
        transact(fencing(&a), Some("a-1"), vec![])
            .unwrap()
            .idempotent_replay
    );

    // node-b cannot take a live cell.
    assert!(matches!(
        activate("node-b"),
        Err(CellError::LeaseHeldByAnother { .. })
    ));

    // node-a dies; after the TTL, node-b takes over with a higher epoch and sees the state.
    pic.advance_time(Duration::from_secs(6));
    pic.tick();
    let b = activate("node-b").unwrap();
    assert_eq!(b.lease.epoch, 2);
    assert!(b.complete);
    let keys: Vec<&str> = b.entries.iter().map(|e| e.key.as_str()).collect();
    assert_eq!(keys, ["count", "msg:1"]);
    assert_eq!(b.alarm_at_ns, Some(now + 30 * sec));

    // A zombie write from node-a is fenced out and changes nothing.
    assert!(matches!(
        transact(
            fencing(&a),
            None,
            vec![CellOp::KvPut {
                key: "count".into(),
                value: b"999".to_vec()
            }]
        ),
        Err(CellError::StaleEpoch { .. })
    ));
    let got: Result<Vec<Option<Vec<u8>>>, CellError> = update(
        &pic,
        cid,
        app,
        "cell_kv_get",
        encode_args(("room-1", vec!["count".to_string()])).unwrap(),
    );
    assert_eq!(got.unwrap(), vec![Some(b"1".to_vec())]);

    // Internal tables are unreachable through SQL.
    assert!(matches!(
        sql(&pic, cid, app, "UPDATE _ic_cell_leases SET epoch = 0", None),
        Err(CellError::InvalidArgument(_))
    ));

    // Alarms become due on consensus time.
    let due = |pic: &PocketIc| -> Vec<DueAlarm> {
        update::<Result<Vec<DueAlarm>, CellError>>(
            pic,
            cid,
            app,
            "list_due_alarms",
            encode_one(10u32).unwrap(),
        )
        .unwrap()
    };
    assert!(due(&pic).is_empty());
    pic.advance_time(Duration::from_secs(30));
    pic.tick();
    assert_eq!(
        due(&pic)
            .iter()
            .map(|d| d.cell_id.as_str())
            .collect::<Vec<_>>(),
        ["room-1"]
    );

    // Everything survives an upgrade.
    pic.upgrade_canister(
        cid,
        wasm,
        encode_one(None::<InitArgs>).unwrap(),
        Some(controller),
    )
    .expect("upgrade");
    let listed: Result<KvListResult, CellError> = update(
        &pic,
        cid,
        app,
        "cell_kv_list",
        encode_args((
            "room-1",
            KvListOptions {
                prefix: Some("msg:".into()),
                ..Default::default()
            },
        ))
        .unwrap(),
    );
    assert_eq!(listed.unwrap().entries.len(), 1);
    assert_eq!(due(&pic).len(), 1);
    // node-b's lease (epoch 2) is still the fencing authority after upgrade.
    pic.advance_time(Duration::from_secs(10));
    let b2 = activate("node-b").unwrap();
    assert_eq!(b2.lease.epoch, 2, "same owner keeps its epoch on renewal");
    transact(fencing(&b2), None, vec![CellOp::SetAlarm(None)]).unwrap();
    assert!(due(&pic).is_empty());
}

/// Runs `packages/typescript-sdk/e2e/live.ts` (Durable Cells runtime +
/// `@dfinity/agent`) against the real wasm in PocketIC live mode.
#[test]
#[serial]
fn typescript_runtime_against_real_canister() {
    let Some(wasm) = wasm() else { return };
    let sdk = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../packages/typescript-sdk");
    let node_ok = std::process::Command::new("node")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success());
    if !node_ok || !sdk.join("node_modules/@dfinity/agent").exists() {
        assert!(
            !required(),
            "node and `bun install` in packages/typescript-sdk are required"
        );
        eprintln!("SKIP: node or packages/typescript-sdk/node_modules missing");
        return;
    }

    let mut pic = pocket_ic();
    let controller = principal(1);
    let ts_principal = agent_identity().sender().unwrap(); // same seed as live.ts
    let cid = install(&pic, &wasm, controller, vec![ts_principal]);
    let url = pic.make_live(None);

    let out = std::process::Command::new("node")
        .args(["--experimental-strip-types", "--no-warnings", "e2e/live.ts"])
        .current_dir(&sdk)
        .env("IC_URL", url.as_str().trim_end_matches('/'))
        .env("CANISTER_ID", cid.to_text())
        .output()
        .expect("spawn node");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stdout.contains("\"ok\":true"),
        "live.ts failed\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    );
}
