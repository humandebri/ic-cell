#![cfg(feature = "object-store")]

use futures::TryStreamExt;
use ic_cell_client::in_process::InProcessTransport;
use ic_cell_client::object_store_adapter::{Execution, IcObjectStoreAdapter};
use ic_sqlite_vfs::test_support::VectorMemory;
use ic_sqlite_vfs::{Db, MemoryId, MemoryManager};
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use serial_test::serial;

struct Guard;
impl Drop for Guard {
    fn drop(&mut self) {
        ic_sqlite_vfs::test_support::memory::reset_for_tests();
        ic_sqlite_vfs::test_support::lock::reset_for_tests();
    }
}

fn store(chunk_size: usize) -> (Guard, IcObjectStoreAdapter<InProcessTransport>) {
    ic_sqlite_vfs::test_support::memory::reset_for_tests();
    ic_sqlite_vfs::test_support::lock::reset_for_tests();
    let mgr = MemoryManager::init(VectorMemory::default());
    Db::init(mgr.get(MemoryId::new(120))).unwrap();
    Db::migrate(ic_cell_canister::storage::MIGRATIONS).unwrap();
    // The in-process canister lives in thread-local state, so run inline.
    let adapter = IcObjectStoreAdapter::with_options(
        InProcessTransport::new(1_700_000_000_000_000_000),
        chunk_size,
        Execution::Inline,
    );
    (Guard, adapter)
}

fn opts(mode: PutMode) -> PutOptions {
    PutOptions {
        mode,
        ..Default::default()
    }
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn conditional_puts_etags_and_reads() {
    let (_g, s) = store(8);
    let path = Path::from("db/cell-1/0000000001.ltx");
    let payload = b"0123456789abcdefghijklmnopqrstuvwxyz".to_vec();

    let created = s
        .put_opts(
            &path,
            PutPayload::from(payload.clone()),
            opts(PutMode::Create),
        )
        .await
        .unwrap();
    assert_eq!(created.e_tag.as_deref(), Some("1"));

    let dup = s
        .put_opts(
            &path,
            PutPayload::from(b"x".to_vec()),
            opts(PutMode::Create),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(dup, object_store::Error::AlreadyExists { .. }),
        "{dup:?}"
    );

    // Chunked full read (chunk size 8 < 36 bytes).
    let got = s.get(&path).await.unwrap();
    assert_eq!(got.meta.size, payload.len() as u64);
    assert_eq!(got.meta.e_tag.as_deref(), Some("1"));
    assert_eq!(got.bytes().await.unwrap().as_ref(), payload.as_slice());

    assert_eq!(
        s.get_range(&path, 10..20).await.unwrap().as_ref(),
        b"abcdefghij"
    );
    let suffix = s
        .get_opts(
            &path,
            GetOptions {
                range: Some(GetRange::Suffix(4)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(suffix.range, 32..36);
    assert_eq!(suffix.bytes().await.unwrap().as_ref(), b"wxyz");

    // CAS update with the right ETag, then a stale one.
    let v2 = s
        .put_opts(
            &path,
            PutPayload::from(b"v2".to_vec()),
            opts(PutMode::Update(UpdateVersion {
                e_tag: Some("1".into()),
                version: None,
            })),
        )
        .await
        .unwrap();
    assert_eq!(v2.e_tag.as_deref(), Some("2"));
    let stale = s
        .put_opts(
            &path,
            PutPayload::from(b"stale".to_vec()),
            opts(PutMode::Update(UpdateVersion {
                e_tag: Some("1".into()),
                version: None,
            })),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(stale, object_store::Error::Precondition { .. }),
        "{stale:?}"
    );

    // Overwrite works whether or not the object exists.
    let v3 = s
        .put(&path, PutPayload::from(b"v3".to_vec()))
        .await
        .unwrap();
    assert_eq!(v3.e_tag.as_deref(), Some("3"));
    let fresh = s
        .put(&Path::from("db/new"), PutPayload::from(b"n".to_vec()))
        .await
        .unwrap();
    assert_eq!(fresh.e_tag.as_deref(), Some("1"));

    // Conditional reads.
    let not_modified = s
        .get_opts(
            &path,
            GetOptions {
                if_none_match: Some("3".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        not_modified,
        object_store::Error::NotModified { .. }
    ));
    let mismatch = s
        .get_opts(
            &path,
            GetOptions {
                if_match: Some("2".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(mismatch, object_store::Error::Precondition { .. }));

    let head = s.head(&path).await.unwrap();
    assert_eq!((head.size, head.e_tag.as_deref()), (2, Some("3")));

    let missing = s.get(&Path::from("nope")).await.unwrap_err();
    assert!(matches!(missing, object_store::Error::NotFound { .. }));
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn list_copy_rename_delete() {
    let (_g, s) = store(1024);
    for key in ["a/b/1", "a/b/2", "a/b/c/3", "a/bc", "z"] {
        s.put(&Path::from(key), PutPayload::from(key.as_bytes().to_vec()))
            .await
            .unwrap();
    }

    let listed: Vec<String> = s
        .list(Some(&Path::from("a/b")))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    assert_eq!(listed, vec!["a/b/1", "a/b/2", "a/b/c/3"]);

    let all: Vec<_> = s.list(None).try_collect().await.unwrap();
    assert_eq!(all.len(), 5);

    let tree = s.list_with_delimiter(Some(&Path::from("a"))).await.unwrap();
    let prefixes: Vec<String> = tree.common_prefixes.iter().map(|p| p.to_string()).collect();
    let objects: Vec<String> = tree
        .objects
        .iter()
        .map(|m| m.location.to_string())
        .collect();
    assert_eq!(prefixes, vec!["a/b"]);
    assert_eq!(objects, vec!["a/bc"]);

    s.copy(&Path::from("z"), &Path::from("z2")).await.unwrap();
    assert_eq!(
        s.get(&Path::from("z2"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"z"
    );
    let exists = s
        .copy_if_not_exists(&Path::from("z"), &Path::from("z2"))
        .await
        .unwrap_err();
    assert!(matches!(exists, object_store::Error::AlreadyExists { .. }));

    s.rename(&Path::from("z2"), &Path::from("z3"))
        .await
        .unwrap();
    assert!(s.head(&Path::from("z2")).await.is_err());
    assert!(s.head(&Path::from("z3")).await.is_ok());

    s.delete(&Path::from("z3")).await.unwrap();
    // Deleting a missing object is not an error (object_store semantics).
    s.delete(&Path::from("z3")).await.unwrap();
    assert!(matches!(
        s.head(&Path::from("z3")).await.unwrap_err(),
        object_store::Error::NotFound { .. }
    ));
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn multipart_parts_assemble_in_call_order() {
    let (_g, s) = store(4);
    let path = Path::from("big/object");
    let mut upload = s.put_multipart(&path).await.unwrap();
    // Parts larger than the chunk size; drive them out of order.
    let p1 = upload.put_part(PutPayload::from(b"first-part|".to_vec()));
    let p2 = upload.put_part(PutPayload::from(b"second-part|".to_vec()));
    let p3 = upload.put_part(PutPayload::from(b"third".to_vec()));
    p3.await.unwrap();
    p1.await.unwrap();
    p2.await.unwrap();
    let res = upload.complete().await.unwrap();
    assert_eq!(res.e_tag.as_deref(), Some("1"));
    assert_eq!(
        s.get(&path).await.unwrap().bytes().await.unwrap().as_ref(),
        b"first-part|second-part|third"
    );

    // A second multipart upload overwrites; an aborted one leaves no trace.
    let mut again = s.put_multipart(&path).await.unwrap();
    again
        .put_part(PutPayload::from(b"v2".to_vec()))
        .await
        .unwrap();
    assert_eq!(again.complete().await.unwrap().e_tag.as_deref(), Some("2"));

    let mut aborted = s.put_multipart(&Path::from("big/aborted")).await.unwrap();
    aborted
        .put_part(PutPayload::from(b"zzzz".to_vec()))
        .await
        .unwrap();
    aborted.abort().await.unwrap();
    assert!(s.head(&Path::from("big/aborted")).await.is_err());
}
