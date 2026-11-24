//! Live ICP / local `dfx` replica transport backed by `ic-agent`.

use crate::{
    CellError, Consistency, ExecResult, FencingToken, IcCellTransport, LeaseRecord,
    LtxDeltaRequest, ObjectMeta, ObjectRecord, QueryResult, ReplicationState, SqlValue,
};
use candid::{decode_one, encode_args, CandidType, Principal};
use ic_agent::{Agent, Identity};
use serde::de::DeserializeOwned;
use std::future::Future;
use tokio::runtime::{Handle, Runtime};

pub struct IcAgentTransport {
    agent: Agent,
    canister_id: Principal,
    runtime: Option<Runtime>,
}

impl IcAgentTransport {
    /// Connects to an IC replica with an anonymous identity.
    /// Set `fetch_root_key = true` for local `dfx` / PocketIC replicas; keep `false` for mainnet.
    pub fn connect(
        url: &str,
        canister_id: Principal,
        fetch_root_key: bool,
    ) -> Result<Self, CellError> {
        let agent = Agent::builder()
            .with_url(url)
            .build()
            .map_err(|e| CellError::Transport(format!("failed to build ic-agent: {e}")))?;

        Self::init_with_agent(agent, canister_id, fetch_root_key)
    }

    /// Connects to an IC replica with an authenticated `Identity` (required for
    /// canisters enforcing `authorize_caller()`).
    pub fn connect_with_identity<I: Identity + 'static>(
        url: &str,
        canister_id: Principal,
        identity: I,
        fetch_root_key: bool,
    ) -> Result<Self, CellError> {
        let agent = Agent::builder()
            .with_url(url)
            .with_identity(identity)
            .build()
            .map_err(|e| CellError::Transport(format!("failed to build ic-agent: {e}")))?;

        Self::init_with_agent(agent, canister_id, fetch_root_key)
    }

    pub fn from_agent(agent: Agent, canister_id: Principal) -> Result<Self, CellError> {
        Self::init_with_agent(agent, canister_id, false)
    }

    fn init_with_agent(
        agent: Agent,
        canister_id: Principal,
        fetch_root_key: bool,
    ) -> Result<Self, CellError> {
        let runtime = if Handle::try_current().is_ok() {
            None
        } else {
            Some(
                Runtime::new().map_err(|e| {
                    CellError::Transport(format!("failed to create tokio runtime: {e}"))
                })?,
            )
        };

        let transport = Self {
            agent,
            canister_id,
            runtime,
        };

        if fetch_root_key {
            transport
                .block_on(transport.agent.fetch_root_key())
                .map_err(|e| CellError::Transport(format!("fetch_root_key failed: {e}")))?;
        }

        Ok(transport)
    }

    /// Runs an async future safely whether called from outside Tokio or inside a
    /// multi-threaded Tokio runtime.
    fn block_on<F: Future>(&self, fut: F) -> F::Output {
        if let Ok(handle) = Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(rt) = &self.runtime {
            rt.block_on(fut)
        } else {
            let rt = Runtime::new().expect("tokio runtime creation failed");
            rt.block_on(fut)
        }
    }

    fn call_update<R>(&self, method: &str, args: Vec<u8>) -> Result<R, CellError>
    where
        R: CandidType + DeserializeOwned,
    {
        let raw = self
            .block_on(
                self.agent
                    .update(&self.canister_id, method)
                    .with_arg(args)
                    .call_and_wait(),
            )
            .map_err(|e| CellError::Transport(format!("ic-agent update '{method}' failed: {e}")))?;

        decode_one::<Result<R, CellError>>(&raw)
            .map_err(|e| CellError::Transport(format!("candid decode '{method}' failed: {e}")))?
    }

    fn call_query<R>(&self, method: &str, args: Vec<u8>) -> Result<R, CellError>
    where
        R: CandidType + DeserializeOwned,
    {
        let raw = self
            .block_on(
                self.agent
                    .query(&self.canister_id, method)
                    .with_arg(args)
                    .call(),
            )
            .map_err(|e| CellError::Transport(format!("ic-agent query '{method}' failed: {e}")))?;

        decode_one::<Result<R, CellError>>(&raw)
            .map_err(|e| CellError::Transport(format!("candid decode '{method}' failed: {e}")))?
    }
}

fn enc<T: candid::utils::ArgumentEncoder>(args: T) -> Result<Vec<u8>, CellError> {
    encode_args(args).map_err(|e| CellError::InvalidArgument(format!("candid encode failed: {e}")))
}

impl IcCellTransport for IcAgentTransport {
    fn acquire_lease(
        &self,
        cell_id: String,
        owner_id: String,
        ttl_ns: u64,
    ) -> Result<LeaseRecord, CellError> {
        self.call_update("acquire_lease", enc((cell_id, owner_id, ttl_ns))?)
    }

    fn release_lease(
        &self,
        cell_id: String,
        owner_id: String,
        epoch: u64,
    ) -> Result<(), CellError> {
        self.call_update("release_lease", enc((cell_id, owner_id, epoch))?)
    }

    fn get_lease(&self, cell_id: &str) -> Result<Option<LeaseRecord>, CellError> {
        self.call_update("get_lease", enc((cell_id,))?)
    }

    fn put_object(
        &self,
        key: String,
        value: Vec<u8>,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectRecord, CellError> {
        let f = fencing.map(|t| t.as_tuple());
        self.call_update("put_object", enc((key, value, expected_generation, f))?)
    }

    fn get_object(
        &self,
        key: &str,
        consistency: Consistency,
    ) -> Result<Option<ObjectRecord>, CellError> {
        match consistency {
            Consistency::Strong => self.call_update("get_object_strong", enc((key,))?),
            Consistency::Eventual => self.call_query("get_object_eventual", enc((key,))?),
        }
    }

    fn head_object(
        &self,
        key: &str,
        _consistency: Consistency,
    ) -> Result<Option<ObjectMeta>, CellError> {
        self.call_update("head_object_strong", enc((key,))?)
    }

    fn get_object_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
        _consistency: Consistency,
    ) -> Result<Vec<u8>, CellError> {
        self.call_update("get_object_range_strong", enc((key, offset, length))?)
    }

    fn list_objects(
        &self,
        prefix: &str,
        _consistency: Consistency,
    ) -> Result<Vec<ObjectMeta>, CellError> {
        self.call_update("list_objects_strong", enc((prefix,))?)
    }

    fn delete_object(
        &self,
        key: String,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<bool, CellError> {
        let f = fencing.map(|t| t.as_tuple());
        self.call_update("delete_object", enc((key, expected_generation, f))?)
    }

    fn put_object_chunk(
        &self,
        upload_id: String,
        chunk_index: u32,
        data: Vec<u8>,
    ) -> Result<(), CellError> {
        self.call_update("put_object_chunk", enc((upload_id, chunk_index, data))?)
    }

    fn commit_chunked_object(
        &self,
        upload_id: String,
        key: String,
        expected_generation: u64,
        fencing: Option<FencingToken>,
    ) -> Result<ObjectRecord, CellError> {
        let f = fencing.map(|t| t.as_tuple());
        self.call_update(
            "commit_chunked_object",
            enc((upload_id, key, expected_generation, f))?,
        )
    }

    fn sql_execute(
        &self,
        sql: String,
        args: Vec<SqlValue>,
        idempotency_key: Option<String>,
        fencing: Option<FencingToken>,
    ) -> Result<ExecResult, CellError> {
        let f = fencing.map(|t| t.as_tuple());
        self.call_update("sql_execute", enc((sql, args, idempotency_key, f))?)
    }

    fn sql_query(
        &self,
        sql: String,
        args: Vec<SqlValue>,
        consistency: Consistency,
    ) -> Result<QueryResult, CellError> {
        match consistency {
            Consistency::Strong => self.call_update("sql_query_strong", enc((sql, args))?),
            Consistency::Eventual => self.call_query("sql_query_eventual", enc((sql, args))?),
        }
    }

    fn apply_ltx_delta(&self, req: LtxDeltaRequest) -> Result<ReplicationState, CellError> {
        self.call_update("apply_ltx_delta", enc((req,))?)
    }

    fn get_replication_state(&self) -> Result<ReplicationState, CellError> {
        let raw = self
            .block_on(
                self.agent
                    .query(&self.canister_id, "get_replication_state")
                    .with_arg(enc(())?)
                    .call(),
            )
            .map_err(|e| {
                CellError::Transport(format!("ic-agent query 'get_replication_state' failed: {e}"))
            })?;
        decode_one::<ReplicationState>(&raw).map_err(|e| {
            CellError::Transport(format!("candid decode 'get_replication_state' failed: {e}"))
        })
    }
}
