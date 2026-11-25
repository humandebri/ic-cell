// Generated from crates/ic-cell-canister/ic_cell_canister.did. Do not edit.
export const idlFactory = ({ IDL }) => {
  const InitArgs = IDL.Record({
    'authorized_callers' : IDL.Opt(IDL.Vec(IDL.Principal)),
  });
  const CellError = IDL.Variant({
    'CasConflict' : IDL.Record({
      'key' : IDL.Text,
      'actual_generation' : IDL.Opt(IDL.Nat64),
      'expected_generation' : IDL.Nat64,
    }),
    'NotFound' : IDL.Record({ 'key' : IDL.Text }),
    'Database' : IDL.Text,
    'LeaseHeldByAnother' : IDL.Record({
      'current_owner' : IDL.Text,
      'expires_at_ns' : IDL.Nat64,
      'cell_id' : IDL.Text,
    }),
    'StaleEpoch' : IDL.Record({
      'expected_epoch' : IDL.Nat64,
      'cell_id' : IDL.Text,
      'actual_epoch' : IDL.Nat64,
    }),
    'Unauthorized' : IDL.Null,
    'AlreadyExists' : IDL.Record({ 'key' : IDL.Text }),
    'PreconditionFailed' : IDL.Record({ 'reason' : IDL.Text }),
    'NotOwner' : IDL.Record({ 'cell_id' : IDL.Text }),
    'InvalidArgument' : IDL.Text,
    'Transport' : IDL.Text,
    'ReplicationMismatch' : IDL.Record({ 'reason' : IDL.Text }),
  });
  const LeaseRecord = IDL.Record({
    'epoch' : IDL.Nat64,
    'owner_id' : IDL.Text,
    'expires_at_ns' : IDL.Nat64,
    'cell_id' : IDL.Text,
  });
  const KvEntry = IDL.Record({ 'key' : IDL.Text, 'value' : IDL.Vec(IDL.Nat8) });
  const Activation = IDL.Record({
    'alarm_at_ns' : IDL.Opt(IDL.Nat64),
    'lease' : LeaseRecord,
    'entries' : IDL.Vec(KvEntry),
    'complete' : IDL.Bool,
  });
  const PageDelta = IDL.Record({
    'data' : IDL.Vec(IDL.Nat8),
    'pgno' : IDL.Nat32,
  });
  const LtxDeltaRequest = IDL.Record({
    'commit_pages' : IDL.Nat32,
    'page_size' : IDL.Nat32,
    'max_txid' : IDL.Nat64,
    'ltx_post_apply_checksum' : IDL.Opt(IDL.Nat64),
    'min_txid' : IDL.Nat64,
    'epoch' : IDL.Nat64,
    'pre_apply_checksum' : IDL.Nat64,
    'ltx_pre_apply_checksum' : IDL.Opt(IDL.Nat64),
    'pages' : IDL.Vec(PageDelta),
    'post_apply_checksum' : IDL.Nat64,
  });
  const ReplicationState = IDL.Record({
    'ltx_checksum' : IDL.Nat64,
    'last_checksum' : IDL.Nat64,
    'epoch' : IDL.Nat64,
    'last_txid' : IDL.Nat64,
    'page_count' : IDL.Nat32,
  });
  const KvListOptions = IDL.Record({
    'reverse' : IDL.Opt(IDL.Bool),
    'start_after' : IDL.Opt(IDL.Text),
    'limit' : IDL.Opt(IDL.Nat32),
    'prefix' : IDL.Opt(IDL.Text),
  });
  const KvListResult = IDL.Record({
    'truncated' : IDL.Bool,
    'entries' : IDL.Vec(KvEntry),
  });
  const SqlValue = IDL.Variant({
    'Blob' : IDL.Vec(IDL.Nat8),
    'Null' : IDL.Null,
    'Real' : IDL.Float64,
    'Text' : IDL.Text,
    'Integer' : IDL.Int64,
  });
  const CellOp = IDL.Variant({
    'Sql' : IDL.Record({ 'sql' : IDL.Text, 'args' : IDL.Vec(SqlValue) }),
    'SetAlarm' : IDL.Opt(IDL.Nat64),
    'KvPut' : IDL.Record({ 'key' : IDL.Text, 'value' : IDL.Vec(IDL.Nat8) }),
    'KvDelete' : IDL.Record({ 'key' : IDL.Text }),
    'AssertKv' : IDL.Record({
      'key' : IDL.Text,
      'expected' : IDL.Opt(IDL.Vec(IDL.Nat8)),
    }),
    'KvDeleteAll' : IDL.Null,
  });
  const Fencing = IDL.Tuple(IDL.Text, IDL.Text, IDL.Nat64);
  const TransactRequest = IDL.Record({
    'ops' : IDL.Vec(CellOp),
    'fencing' : Fencing,
    'idempotency_key' : IDL.Opt(IDL.Text),
  });
  const OpResult = IDL.Record({
    'rows' : IDL.Vec(IDL.Vec(SqlValue)),
    'rows_affected' : IDL.Nat64,
  });
  const TransactResult = IDL.Record({
    'results' : IDL.Vec(OpResult),
    'epoch' : IDL.Nat64,
    'idempotent_replay' : IDL.Bool,
  });
  const ObjectMeta = IDL.Record({
    'key' : IDL.Text,
    'size' : IDL.Nat64,
    'generation' : IDL.Nat64,
    'updated_at_ns' : IDL.Nat64,
  });
  const ObjectRecord = IDL.Record({
    'key' : IDL.Text,
    'value' : IDL.Vec(IDL.Nat8),
    'generation' : IDL.Nat64,
    'updated_at_ns' : IDL.Nat64,
  });
  const DueAlarm = IDL.Record({ 'at_ns' : IDL.Nat64, 'cell_id' : IDL.Text });
  const ExecResult = IDL.Record({
    'idempotent_replay' : IDL.Bool,
    'rows_affected' : IDL.Nat64,
  });
  const QueryResult = IDL.Record({ 'rows' : IDL.Vec(IDL.Vec(SqlValue)) });
  return IDL.Service({
    'abort_chunked_upload' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Nat64, 'Err' : CellError })],
        [],
      ),
    'abort_staged_ltx' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Nat64, 'Err' : CellError })],
        [],
      ),
    'acquire_lease' : IDL.Func(
        [IDL.Text, IDL.Text, IDL.Nat64],
        [IDL.Variant({ 'Ok' : LeaseRecord, 'Err' : CellError })],
        [],
      ),
    'activate_cell' : IDL.Func(
        [IDL.Text, IDL.Text, IDL.Nat64, IDL.Nat64],
        [IDL.Variant({ 'Ok' : Activation, 'Err' : CellError })],
        [],
      ),
    'add_authorized_caller' : IDL.Func(
        [IDL.Principal],
        [IDL.Variant({ 'Ok' : IDL.Bool, 'Err' : CellError })],
        [],
      ),
    'apply_ltx_delta' : IDL.Func(
        [LtxDeltaRequest],
        [IDL.Variant({ 'Ok' : ReplicationState, 'Err' : CellError })],
        [],
      ),
    'cell_get_alarm' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Opt(IDL.Nat64), 'Err' : CellError })],
        [],
      ),
    'cell_kv_get' : IDL.Func(
        [IDL.Text, IDL.Vec(IDL.Text)],
        [
          IDL.Variant({
            'Ok' : IDL.Vec(IDL.Opt(IDL.Vec(IDL.Nat8))),
            'Err' : CellError,
          }),
        ],
        [],
      ),
    'cell_kv_list' : IDL.Func(
        [IDL.Text, KvListOptions],
        [IDL.Variant({ 'Ok' : KvListResult, 'Err' : CellError })],
        [],
      ),
    'cell_transact' : IDL.Func(
        [TransactRequest],
        [IDL.Variant({ 'Ok' : TransactResult, 'Err' : CellError })],
        [],
      ),
    'commit_chunked_object' : IDL.Func(
        [IDL.Text, IDL.Text, IDL.Nat64, IDL.Opt(Fencing)],
        [IDL.Variant({ 'Ok' : ObjectMeta, 'Err' : CellError })],
        [],
      ),
    'commit_staged_ltx_delta' : IDL.Func(
        [IDL.Text, LtxDeltaRequest],
        [IDL.Variant({ 'Ok' : ReplicationState, 'Err' : CellError })],
        [],
      ),
    'delete_object' : IDL.Func(
        [IDL.Text, IDL.Nat64, IDL.Opt(Fencing)],
        [IDL.Variant({ 'Ok' : IDL.Bool, 'Err' : CellError })],
        [],
      ),
    'gc_idempotency' : IDL.Func(
        [IDL.Nat64],
        [IDL.Variant({ 'Ok' : IDL.Nat64, 'Err' : CellError })],
        [],
      ),
    'get_lease' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Opt(LeaseRecord), 'Err' : CellError })],
        [],
      ),
    'get_object_eventual' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Opt(ObjectRecord), 'Err' : CellError })],
        ['query'],
      ),
    'get_object_range_strong' : IDL.Func(
        [IDL.Text, IDL.Nat64, IDL.Nat64],
        [IDL.Variant({ 'Ok' : IDL.Vec(IDL.Nat8), 'Err' : CellError })],
        [],
      ),
    'get_object_strong' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Opt(ObjectRecord), 'Err' : CellError })],
        [],
      ),
    'get_replication_state' : IDL.Func([], [ReplicationState], ['query']),
    'head_object_strong' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Opt(ObjectMeta), 'Err' : CellError })],
        [],
      ),
    'list_authorized_callers' : IDL.Func(
        [],
        [IDL.Variant({ 'Ok' : IDL.Vec(IDL.Principal), 'Err' : CellError })],
        ['query'],
      ),
    'list_due_alarms' : IDL.Func(
        [IDL.Nat32],
        [IDL.Variant({ 'Ok' : IDL.Vec(DueAlarm), 'Err' : CellError })],
        [],
      ),
    'list_objects_strong' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : IDL.Vec(ObjectMeta), 'Err' : CellError })],
        [],
      ),
    'put_object' : IDL.Func(
        [IDL.Text, IDL.Vec(IDL.Nat8), IDL.Nat64, IDL.Opt(Fencing)],
        [IDL.Variant({ 'Ok' : ObjectRecord, 'Err' : CellError })],
        [],
      ),
    'put_object_chunk' : IDL.Func(
        [IDL.Text, IDL.Nat32, IDL.Vec(IDL.Nat8)],
        [IDL.Variant({ 'Ok' : IDL.Null, 'Err' : CellError })],
        [],
      ),
    'release_lease' : IDL.Func(
        [IDL.Text, IDL.Text, IDL.Nat64],
        [IDL.Variant({ 'Ok' : IDL.Null, 'Err' : CellError })],
        [],
      ),
    'remove_authorized_caller' : IDL.Func(
        [IDL.Principal],
        [IDL.Variant({ 'Ok' : IDL.Bool, 'Err' : CellError })],
        [],
      ),
    'sql_execute' : IDL.Func(
        [IDL.Text, IDL.Vec(SqlValue), IDL.Opt(IDL.Text), IDL.Opt(Fencing)],
        [IDL.Variant({ 'Ok' : ExecResult, 'Err' : CellError })],
        [],
      ),
    'sql_query_eventual' : IDL.Func(
        [IDL.Text, IDL.Vec(SqlValue)],
        [IDL.Variant({ 'Ok' : QueryResult, 'Err' : CellError })],
        ['query'],
      ),
    'sql_query_strong' : IDL.Func(
        [IDL.Text, IDL.Vec(SqlValue)],
        [IDL.Variant({ 'Ok' : QueryResult, 'Err' : CellError })],
        [],
      ),
    'stage_ltx_pages' : IDL.Func(
        [IDL.Text, IDL.Vec(PageDelta)],
        [IDL.Variant({ 'Ok' : IDL.Nat64, 'Err' : CellError })],
        [],
      ),
  });
};
export const init = ({ IDL }) => {
  const InitArgs = IDL.Record({
    'authorized_callers' : IDL.Opt(IDL.Vec(IDL.Principal)),
  });
  return [IDL.Opt(InitArgs)];
};