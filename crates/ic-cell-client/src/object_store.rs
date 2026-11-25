//! Phase 6: ICP Object Storage Backend (`StorageBackend::Ic`)
//!
//! Provides an `object_store`-compatible surface (conditional create, CAS
//! update, read-after-write consistency, range read, list, and automatic
//! chunked upload for payloads exceeding ICP ingress limits).

use crate::{CellError, Consistency, FencingToken, IcCellTransport, ObjectMeta, ObjectRecord};
use std::sync::atomic::{AtomicU64, Ordering};

/// Default chunk size (1 MiB) safely below the ICP 2 MiB ingress message limit.
pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024;

static GLOBAL_UPLOAD_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutMode {
    Create,
    Update { expected_generation: u64 },
}

pub struct IcObjectStore<'a, T: IcCellTransport> {
    transport: &'a T,
    chunk_size: usize,
    upload_seq: AtomicU64,
}

impl<'a, T: IcCellTransport> IcObjectStore<'a, T> {
    pub fn new(transport: &'a T) -> Self {
        Self {
            transport,
            chunk_size: DEFAULT_CHUNK_SIZE,
            upload_seq: AtomicU64::new(1),
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size.max(1);
        self
    }

    pub fn put(
        &self,
        key: impl Into<String>,
        value: Vec<u8>,
        mode: PutMode,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectRecord, CellError> {
        let key = key.into();
        let expected_generation = match mode {
            PutMode::Create => 0,
            PutMode::Update {
                expected_generation,
            } => expected_generation,
        };

        if value.len() <= self.chunk_size {
            return self
                .transport
                .put_object(key, value, expected_generation, fencing);
        }

        // Unique across adapter instances and processes so concurrent uploads
        // of the same key never share staging chunks.
        let seq = self.upload_seq.fetch_add(1, Ordering::SeqCst);
        let global = GLOBAL_UPLOAD_SEQ.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let upload_id = format!("upload-{}-{nanos}-{global}-{seq}-{key}", std::process::id());

        for (idx, chunk) in value.chunks(self.chunk_size).enumerate() {
            self.transport
                .put_object_chunk(upload_id.clone(), idx as u32, chunk.to_vec())?;
        }

        let meta =
            self.transport
                .commit_chunked_object(upload_id, key, expected_generation, fencing)?;
        Ok(ObjectRecord {
            key: meta.key,
            value,
            generation: meta.generation,
            updated_at_ns: meta.updated_at_ns,
        })
    }

    pub fn get(&self, key: &str) -> Result<Option<ObjectRecord>, CellError> {
        self.transport.get_object(key, Consistency::Strong)
    }

    pub fn get_range(&self, key: &str, offset: u64, length: u64) -> Result<Vec<u8>, CellError> {
        self.transport
            .get_object_range(key, offset, length, Consistency::Strong)
    }

    pub fn head(&self, key: &str) -> Result<Option<ObjectMeta>, CellError> {
        self.transport.head_object(key, Consistency::Strong)
    }

    pub fn list(&self, prefix: &str) -> Result<Vec<ObjectMeta>, CellError> {
        self.transport.list_objects(prefix, Consistency::Strong)
    }

    pub fn delete(
        &self,
        key: impl Into<String>,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<bool, CellError> {
        self.transport
            .delete_object(key.into(), expected_generation, fencing)
    }
}
