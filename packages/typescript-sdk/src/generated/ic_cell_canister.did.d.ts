// Generated from crates/ic-cell-canister/ic_cell_canister.did. Do not edit.
import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export interface Activation {
  'alarm_at_ns' : [] | [bigint],
  'lease' : LeaseRecord,
  'entries' : Array<KvEntry>,
  'complete' : boolean,
}
export type CellError = {
    'CasConflict' : {
      'key' : string,
      'actual_generation' : [] | [bigint],
      'expected_generation' : bigint,
    }
  } |
  { 'NotFound' : { 'key' : string } } |
  { 'Database' : string } |
  {
    'LeaseHeldByAnother' : {
      'current_owner' : string,
      'expires_at_ns' : bigint,
      'cell_id' : string,
    }
  } |
  {
    'StaleEpoch' : {
      'expected_epoch' : bigint,
      'cell_id' : string,
      'actual_epoch' : bigint,
    }
  } |
  { 'Unauthorized' : null } |
  { 'AlreadyExists' : { 'key' : string } } |
  { 'PreconditionFailed' : { 'reason' : string } } |
  { 'NotOwner' : { 'cell_id' : string } } |
  { 'InvalidArgument' : string } |
  { 'Transport' : string } |
  { 'ReplicationMismatch' : { 'reason' : string } };
export type CellOp = { 'Sql' : { 'sql' : string, 'args' : Array<SqlValue> } } |
  { 'SetAlarm' : [] | [bigint] } |
  { 'KvPut' : { 'key' : string, 'value' : Uint8Array | number[] } } |
  { 'KvDelete' : { 'key' : string } } |
  {
    'AssertKv' : { 'key' : string, 'expected' : [] | [Uint8Array | number[]] }
  } |
  { 'KvDeleteAll' : null };
export interface DueAlarm { 'at_ns' : bigint, 'cell_id' : string }
export interface ExecResult {
  'idempotent_replay' : boolean,
  'rows_affected' : bigint,
}
export type Fencing = [string, string, bigint];
export interface InitArgs { 'authorized_callers' : [] | [Array<Principal>] }
export interface KvEntry { 'key' : string, 'value' : Uint8Array | number[] }
export interface KvListOptions {
  'reverse' : [] | [boolean],
  'start_after' : [] | [string],
  'limit' : [] | [number],
  'prefix' : [] | [string],
}
export interface KvListResult {
  'truncated' : boolean,
  'entries' : Array<KvEntry>,
}
export interface LeaseRecord {
  'epoch' : bigint,
  'owner_id' : string,
  'expires_at_ns' : bigint,
  'cell_id' : string,
}
export interface LtxDeltaRequest {
  'commit_pages' : number,
  'page_size' : number,
  'max_txid' : bigint,
  'ltx_post_apply_checksum' : [] | [bigint],
  'min_txid' : bigint,
  'epoch' : bigint,
  'pre_apply_checksum' : bigint,
  'ltx_pre_apply_checksum' : [] | [bigint],
  'pages' : Array<PageDelta>,
  'post_apply_checksum' : bigint,
}
export interface ObjectMeta {
  'key' : string,
  'size' : bigint,
  'generation' : bigint,
  'updated_at_ns' : bigint,
}
export interface ObjectRecord {
  'key' : string,
  'value' : Uint8Array | number[],
  'generation' : bigint,
  'updated_at_ns' : bigint,
}
export interface OpResult {
  'rows' : Array<Array<SqlValue>>,
  'rows_affected' : bigint,
}
export interface PageDelta { 'data' : Uint8Array | number[], 'pgno' : number }
export interface QueryResult { 'rows' : Array<Array<SqlValue>> }
export interface ReplicationState {
  'ltx_checksum' : bigint,
  'last_checksum' : bigint,
  'epoch' : bigint,
  'last_txid' : bigint,
  'page_count' : number,
}
export type SqlValue = { 'Blob' : Uint8Array | number[] } |
  { 'Null' : null } |
  { 'Real' : number } |
  { 'Text' : string } |
  { 'Integer' : bigint };
export interface TransactRequest {
  'ops' : Array<CellOp>,
  'fencing' : Fencing,
  'idempotency_key' : [] | [string],
}
export interface TransactResult {
  'results' : Array<OpResult>,
  'epoch' : bigint,
  'idempotent_replay' : boolean,
}
export interface _SERVICE {
  'abort_chunked_upload' : ActorMethod<
    [string],
    { 'Ok' : bigint } |
      { 'Err' : CellError }
  >,
  'abort_staged_ltx' : ActorMethod<
    [string],
    { 'Ok' : bigint } |
      { 'Err' : CellError }
  >,
  'acquire_lease' : ActorMethod<
    [string, string, bigint],
    { 'Ok' : LeaseRecord } |
      { 'Err' : CellError }
  >,
  'activate_cell' : ActorMethod<
    [string, string, bigint, bigint],
    { 'Ok' : Activation } |
      { 'Err' : CellError }
  >,
  'add_authorized_caller' : ActorMethod<
    [Principal],
    { 'Ok' : boolean } |
      { 'Err' : CellError }
  >,
  'apply_ltx_delta' : ActorMethod<
    [LtxDeltaRequest],
    { 'Ok' : ReplicationState } |
      { 'Err' : CellError }
  >,
  'cell_get_alarm' : ActorMethod<
    [string],
    { 'Ok' : [] | [bigint] } |
      { 'Err' : CellError }
  >,
  'cell_kv_get' : ActorMethod<
    [string, Array<string>],
    { 'Ok' : Array<[] | [Uint8Array | number[]]> } |
      { 'Err' : CellError }
  >,
  'cell_kv_list' : ActorMethod<
    [string, KvListOptions],
    { 'Ok' : KvListResult } |
      { 'Err' : CellError }
  >,
  'cell_transact' : ActorMethod<
    [TransactRequest],
    { 'Ok' : TransactResult } |
      { 'Err' : CellError }
  >,
  'commit_chunked_object' : ActorMethod<
    [string, string, bigint, [] | [Fencing]],
    { 'Ok' : ObjectMeta } |
      { 'Err' : CellError }
  >,
  'commit_staged_ltx_delta' : ActorMethod<
    [string, LtxDeltaRequest],
    { 'Ok' : ReplicationState } |
      { 'Err' : CellError }
  >,
  'delete_object' : ActorMethod<
    [string, bigint, [] | [Fencing]],
    { 'Ok' : boolean } |
      { 'Err' : CellError }
  >,
  'gc_idempotency' : ActorMethod<
    [bigint],
    { 'Ok' : bigint } |
      { 'Err' : CellError }
  >,
  'get_lease' : ActorMethod<
    [string],
    { 'Ok' : [] | [LeaseRecord] } |
      { 'Err' : CellError }
  >,
  'get_object_eventual' : ActorMethod<
    [string],
    { 'Ok' : [] | [ObjectRecord] } |
      { 'Err' : CellError }
  >,
  'get_object_range_strong' : ActorMethod<
    [string, bigint, bigint],
    { 'Ok' : Uint8Array | number[] } |
      { 'Err' : CellError }
  >,
  'get_object_strong' : ActorMethod<
    [string],
    { 'Ok' : [] | [ObjectRecord] } |
      { 'Err' : CellError }
  >,
  'get_replication_state' : ActorMethod<[], ReplicationState>,
  'head_object_strong' : ActorMethod<
    [string],
    { 'Ok' : [] | [ObjectMeta] } |
      { 'Err' : CellError }
  >,
  'list_authorized_callers' : ActorMethod<
    [],
    { 'Ok' : Array<Principal> } |
      { 'Err' : CellError }
  >,
  'list_due_alarms' : ActorMethod<
    [number],
    { 'Ok' : Array<DueAlarm> } |
      { 'Err' : CellError }
  >,
  'list_objects_strong' : ActorMethod<
    [string],
    { 'Ok' : Array<ObjectMeta> } |
      { 'Err' : CellError }
  >,
  'put_object' : ActorMethod<
    [string, Uint8Array | number[], bigint, [] | [Fencing]],
    { 'Ok' : ObjectRecord } |
      { 'Err' : CellError }
  >,
  'put_object_chunk' : ActorMethod<
    [string, number, Uint8Array | number[]],
    { 'Ok' : null } |
      { 'Err' : CellError }
  >,
  'release_lease' : ActorMethod<
    [string, string, bigint],
    { 'Ok' : null } |
      { 'Err' : CellError }
  >,
  'remove_authorized_caller' : ActorMethod<
    [Principal],
    { 'Ok' : boolean } |
      { 'Err' : CellError }
  >,
  'sql_execute' : ActorMethod<
    [string, Array<SqlValue>, [] | [string], [] | [Fencing]],
    { 'Ok' : ExecResult } |
      { 'Err' : CellError }
  >,
  'sql_query_eventual' : ActorMethod<
    [string, Array<SqlValue>],
    { 'Ok' : QueryResult } |
      { 'Err' : CellError }
  >,
  'sql_query_strong' : ActorMethod<
    [string, Array<SqlValue>],
    { 'Ok' : QueryResult } |
      { 'Err' : CellError }
  >,
  'stage_ltx_pages' : ActorMethod<
    [string, Array<PageDelta>],
    { 'Ok' : bigint } |
      { 'Err' : CellError }
  >,
}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];