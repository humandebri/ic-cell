//! [`object_store::ObjectStore`] implementation backed by `ic-cell-canister`
//! (`StorageBackend::Ic`), so `celld` (or anything built on `object_store`)
//! can use the canister as a drop-in object store.
//!
//! Semantics:
//! * ETag and version are the canister generation (`"1"`, `"2"`, ...).
//! * `PutMode::Create` / `PutMode::Update` map to the canister's conditional
//!   create / CAS; `PutMode::Overwrite` is a CAS loop on the current generation.
//! * Reads use strong (update-call) consistency. Ranged and large reads are
//!   split into chunks; a generation change mid-read triggers a retry so a
//!   result never mixes two generations.
//! * Multipart parts are uploaded as canister chunks and committed atomically.
//! * Attributes and tags are not stored.

use crate::object_store::{IcObjectStore, PutMode as IcPutMode, DEFAULT_CHUNK_SIZE};
use crate::{CellError, Consistency, FencingToken, IcCellTransport, ObjectMeta as IcObjectMeta};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UploadPart,
};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

const STORE: &str = "IcCell";
const MAX_CAS_RETRIES: usize = 8;
const MAX_READ_RETRIES: usize = 4;

static MULTIPART_SEQ: AtomicU64 = AtomicU64::new(1);

/// How blocking transport calls are driven from async code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Execution {
    /// Run on Tokio's blocking pool (required for network transports such as
    /// `IcAgentTransport`; needs a multi-threaded Tokio runtime).
    SpawnBlocking,
    /// Run directly on the calling task. Only for in-process transports whose
    /// calls are cheap and must stay on the current thread.
    Inline,
}

struct Inner<T> {
    transport: T,
    chunk_size: usize,
    execution: Execution,
    fencing: RwLock<Option<FencingToken>>,
}

pub struct IcObjectStoreAdapter<T> {
    inner: Arc<Inner<T>>,
}

impl<T> Clone for IcObjectStoreAdapter<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> fmt::Debug for IcObjectStoreAdapter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcObjectStoreAdapter")
            .field("chunk_size", &self.inner.chunk_size)
            .field("execution", &self.inner.execution)
            .finish()
    }
}

impl<T> fmt::Display for IcObjectStoreAdapter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IcObjectStoreAdapter")
    }
}

impl<T: IcCellTransport + Send + Sync + 'static> IcObjectStoreAdapter<T> {
    pub fn new(transport: T) -> Self {
        Self::with_options(transport, DEFAULT_CHUNK_SIZE, Execution::SpawnBlocking)
    }

    pub fn with_options(transport: T, chunk_size: usize, execution: Execution) -> Self {
        Self {
            inner: Arc::new(Inner {
                transport,
                chunk_size: chunk_size.max(1),
                execution,
                fencing: RwLock::new(None),
            }),
        }
    }

    /// Attaches a fencing token to every write (lease-holder writes only).
    pub fn set_fencing(&self, token: Option<FencingToken>) {
        *self.inner.fencing.write().unwrap() = token;
    }

    pub fn transport(&self) -> &T {
        &self.inner.transport
    }

    async fn run<R, F>(&self, f: F) -> object_store::Result<R>
    where
        R: Send + 'static,
        F: FnOnce(&Inner<T>) -> object_store::Result<R> + Send + 'static,
    {
        match self.inner.execution {
            Execution::Inline => f(&self.inner),
            Execution::SpawnBlocking => {
                let inner = Arc::clone(&self.inner);
                tokio::task::spawn_blocking(move || f(&inner))
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(e),
                    })?
            }
        }
    }
}

impl<T: IcCellTransport> Inner<T> {
    fn fencing(&self) -> Option<FencingToken> {
        self.fencing.read().unwrap().clone()
    }

    fn head(&self, path: &Path) -> object_store::Result<Option<IcObjectMeta>> {
        self.transport
            .head_object(path.as_ref(), Consistency::Strong)
            .map_err(|e| map_err(path, e))
    }

    fn head_required(&self, path: &Path) -> object_store::Result<IcObjectMeta> {
        self.head(path)?.ok_or_else(|| not_found(path))
    }

    fn put(&self, path: &Path, value: Vec<u8>, mode: PutMode) -> object_store::Result<PutResult> {
        let store = IcObjectStore::new(&self.transport).with_chunk_size(self.chunk_size);
        let key = path.as_ref().to_string();
        let fencing = self.fencing();
        let record = match mode {
            PutMode::Create => store
                .put(key, value, IcPutMode::Create, fencing)
                .map_err(|e| map_err(path, e))?,
            PutMode::Update(v) => {
                let generation =
                    parse_generation(path, v.e_tag.as_deref().or(v.version.as_deref()))?;
                store
                    .put(
                        key,
                        value,
                        IcPutMode::Update {
                            expected_generation: generation,
                        },
                        fencing,
                    )
                    .map_err(|e| map_err(path, e))?
            }
            PutMode::Overwrite => {
                let mut attempt = 0;
                loop {
                    let current = self.head(path)?.map_or(0, |m| m.generation);
                    let mode = if current == 0 {
                        IcPutMode::Create
                    } else {
                        IcPutMode::Update {
                            expected_generation: current,
                        }
                    };
                    match store.put(key.clone(), value.clone(), mode, fencing.clone()) {
                        Ok(r) => break r,
                        Err(CellError::CasConflict { .. } | CellError::AlreadyExists { .. })
                            if attempt < MAX_CAS_RETRIES =>
                        {
                            attempt += 1;
                        }
                        Err(e) => return Err(map_err(path, e)),
                    }
                }
            }
        };
        Ok(put_result(record.generation))
    }

    /// Reads `range` of the object at `generation`, chunked to stay under the
    /// reply size limit. Returns `None` if the generation moved during the read.
    fn read_range(
        &self,
        path: &Path,
        range: std::ops::Range<u64>,
        generation: u64,
    ) -> object_store::Result<Option<Vec<u8>>> {
        let mut out = Vec::with_capacity((range.end - range.start) as usize);
        let mut offset = range.start;
        while offset < range.end {
            let len = (range.end - offset).min(self.chunk_size as u64);
            let part = self
                .transport
                .get_object_range(path.as_ref(), offset, len, Consistency::Strong)
                .map_err(|e| map_err(path, e))?;
            if part.is_empty() {
                break;
            }
            offset += part.len() as u64;
            out.extend_from_slice(&part);
        }
        match self.head(path)? {
            Some(m) if m.generation == generation => Ok(Some(out)),
            _ => Ok(None),
        }
    }

    fn get(
        &self,
        path: &Path,
        options: &GetOptions,
    ) -> object_store::Result<(ObjectMeta, std::ops::Range<u64>, Vec<u8>)> {
        for _ in 0..MAX_READ_RETRIES {
            let ic_meta = self.head_required(path)?;
            let meta = to_meta(path, &ic_meta);
            if let Some(version) = &options.version {
                if *version != ic_meta.generation.to_string() {
                    return Err(not_found(path));
                }
            }
            options.check_preconditions(&meta)?;
            let range = match &options.range {
                Some(r) => r
                    .as_range(meta.size)
                    .map_err(|e| object_store::Error::Generic {
                        store: STORE,
                        source: Box::new(e),
                    })?,
                None => 0..meta.size,
            };
            if options.head {
                return Ok((meta, range, Vec::new()));
            }
            if let Some(data) = self.read_range(path, range.clone(), ic_meta.generation)? {
                return Ok((meta, range, data));
            }
        }
        Err(object_store::Error::Generic {
            store: STORE,
            source: format!("object {path} kept changing during read").into(),
        })
    }

    fn delete(&self, path: &Path) -> object_store::Result<()> {
        let fencing = self.fencing();
        for _ in 0..MAX_CAS_RETRIES {
            let Some(meta) = self.head(path)? else {
                return Ok(());
            };
            match self.transport.delete_object(
                path.as_ref().to_string(),
                meta.generation,
                fencing.clone(),
            ) {
                Ok(_) => return Ok(()),
                Err(CellError::CasConflict { .. }) => continue,
                Err(e) => return Err(map_err(path, e)),
            }
        }
        Err(object_store::Error::Precondition {
            path: path.to_string(),
            source: "object kept changing during delete".into(),
        })
    }

    fn list(&self, prefix: Option<&Path>) -> object_store::Result<Vec<ObjectMeta>> {
        let raw_prefix = match prefix {
            Some(p) if !p.as_ref().is_empty() => format!("{}/", p.as_ref()),
            _ => String::new(),
        };
        let objects = self
            .transport
            .list_objects(&raw_prefix, Consistency::Strong)
            .map_err(|e| map_err(prefix.unwrap_or(&Path::default()), e))?;
        let mut out = Vec::with_capacity(objects.len());
        for m in objects {
            // Keys that are not valid object_store paths are skipped.
            if let Ok(location) = Path::parse(&m.key) {
                out.push(to_meta(&location, &m));
            }
        }
        out.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(out)
    }
}

#[async_trait]
impl<T: IcCellTransport + Send + Sync + 'static> ObjectStore for IcObjectStoreAdapter<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let location = location.clone();
        let value = Bytes::from(payload).to_vec();
        self.run(move |inner| inner.put(&location, value, opts.mode))
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let upload_id = format!(
            "mpu-{}-{}-{}",
            std::process::id(),
            MULTIPART_SEQ.fetch_add(1, Ordering::Relaxed),
            location
        );
        Ok(Box::new(IcMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            upload_id,
            next_chunk: Arc::new(AtomicU32::new(0)),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let location = location.clone();
        let (meta, range, data) = self
            .run(move |inner| inner.get(&location, &options))
            .await?;
        let payload = stream::once(async move { Ok(Bytes::from(data)) }).boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(payload),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let path = location.clone();
                    store.run(move |inner| inner.delete(&path)).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move { store.run(move |inner| inner.list(prefix.as_ref())).await })
            .flat_map(|res| match res {
                Ok(items) => stream::iter(items.into_iter().map(Ok)).boxed(),
                Err(e) => stream::once(async move { Err(e) }).boxed(),
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let base = prefix.cloned().unwrap_or_default();
        let prefix_owned = prefix.cloned();
        let all = self
            .run(move |inner| inner.list(prefix_owned.as_ref()))
            .await?;
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for meta in all {
            let Some(rest) = meta.location.prefix_match(&base) else {
                continue;
            };
            let rest: Vec<_> = rest.collect();
            match rest.len() {
                0 => continue,
                1 => objects.push(meta),
                _ => {
                    common_prefixes.insert(base.clone().join(rest[0].clone()));
                }
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        let (from, to) = (from.clone(), to.clone());
        self.run(move |inner| {
            let (_, _, data) = inner.get(&from, &GetOptions::default())?;
            let mode = match options.mode {
                CopyMode::Overwrite => PutMode::Overwrite,
                CopyMode::Create => PutMode::Create,
            };
            inner.put(&to, data, mode).map(|_| ())
        })
        .await
    }
}

struct IcMultipartUpload<T> {
    store: IcObjectStoreAdapter<T>,
    location: Path,
    upload_id: String,
    next_chunk: Arc<AtomicU32>,
}

impl<T> fmt::Debug for IcMultipartUpload<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcMultipartUpload")
            .field("location", &self.location)
            .field("upload_id", &self.upload_id)
            .finish()
    }
}

#[async_trait]
impl<T: IcCellTransport + Send + Sync + 'static> MultipartUpload for IcMultipartUpload<T> {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let bytes = Bytes::from(data);
        let chunk_size = self.store.inner.chunk_size;
        // Reserve a contiguous index range now so concurrently driven parts
        // still assemble in call order.
        let chunks = bytes.len().div_ceil(chunk_size).max(1) as u32;
        let base = self.next_chunk.fetch_add(chunks, Ordering::SeqCst);
        let store = self.store.clone();
        let upload_id = self.upload_id.clone();
        let location = self.location.clone();
        Box::pin(async move {
            store
                .run(move |inner| {
                    if bytes.is_empty() {
                        return Ok(());
                    }
                    for (i, chunk) in bytes.chunks(chunk_size).enumerate() {
                        inner
                            .transport
                            .put_object_chunk(upload_id.clone(), base + i as u32, chunk.to_vec())
                            .map_err(|e| map_err(&location, e))?;
                    }
                    Ok(())
                })
                .await
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let upload_id = self.upload_id.clone();
        let location = self.location.clone();
        self.store
            .run(move |inner| {
                let fencing = inner.fencing();
                for _ in 0..MAX_CAS_RETRIES {
                    let current = inner.head(&location)?.map_or(0, |m| m.generation);
                    match inner.transport.commit_chunked_object(
                        upload_id.clone(),
                        location.as_ref().to_string(),
                        current,
                        fencing.clone(),
                    ) {
                        Ok(record) => return Ok(put_result(record.generation)),
                        // Chunks survive a failed CAS, so retrying is safe.
                        Err(CellError::CasConflict { .. } | CellError::AlreadyExists { .. }) => {
                            continue
                        }
                        Err(e) => return Err(map_err(&location, e)),
                    }
                }
                Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "object kept changing during multipart commit".into(),
                })
            })
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let upload_id = self.upload_id.clone();
        let location = self.location.clone();
        self.store
            .run(move |inner| {
                inner
                    .transport
                    .abort_chunked_upload(&upload_id)
                    .map(|_| ())
                    .map_err(|e| map_err(&location, e))
            })
            .await
    }
}

fn put_result(generation: u64) -> PutResult {
    PutResult {
        e_tag: Some(generation.to_string()),
        version: Some(generation.to_string()),
        extensions: Default::default(),
    }
}

fn parse_generation(path: &Path, tag: Option<&str>) -> object_store::Result<u64> {
    tag.and_then(|t| t.trim_matches('"').parse::<u64>().ok())
        .filter(|g| *g > 0)
        .ok_or_else(|| object_store::Error::Precondition {
            path: path.to_string(),
            source: format!("invalid e_tag/version {tag:?}; expected a generation").into(),
        })
}

fn to_meta(location: &Path, m: &IcObjectMeta) -> ObjectMeta {
    let secs = (m.updated_at_ns / 1_000_000_000) as i64;
    let nanos = (m.updated_at_ns % 1_000_000_000) as u32;
    ObjectMeta {
        location: location.clone(),
        last_modified: chrono::DateTime::from_timestamp(secs, nanos).unwrap_or_default(),
        size: m.size,
        e_tag: Some(m.generation.to_string()),
        version: Some(m.generation.to_string()),
    }
}

fn not_found(path: &Path) -> object_store::Error {
    object_store::Error::NotFound {
        path: path.to_string(),
        source: "object not found".into(),
    }
}

fn map_err(path: &Path, err: CellError) -> object_store::Error {
    let path_s = path.to_string();
    match err {
        CellError::NotFound { .. } => object_store::Error::NotFound {
            path: path_s,
            source: Box::new(err),
        },
        CellError::AlreadyExists { .. } => object_store::Error::AlreadyExists {
            path: path_s,
            source: Box::new(err),
        },
        CellError::CasConflict { .. } => object_store::Error::Precondition {
            path: path_s,
            source: Box::new(err),
        },
        other => object_store::Error::Generic {
            store: STORE,
            source: Box::new(other),
        },
    }
}
