import {
  CasConflictError,
  IcCellError,
  LeaseHeldError,
  PreconditionFailedError,
  StaleEpochError,
} from "./errors.ts";
import type {
  Activation,
  CellOp,
  CellTransport,
  DueAlarm,
  ExecOptions,
  ExecResult,
  FencingToken,
  IcDbTransport,
  KvListOptions,
  KvListResult,
  LeaseRecord,
  QueryOptions,
  SqlParam,
  TransactResult,
} from "./types.ts";

export type CandidSqlValue =
  | { Null: null }
  | { Integer: bigint }
  | { Real: number }
  | { Text: string }
  | { Blob: Uint8Array };

export type CandidCellError =
  | {
      CasConflict: {
        key: string;
        expected_generation: bigint;
        actual_generation: [] | [bigint];
      };
    }
  | { AlreadyExists: { key: string } }
  | { NotFound: { key: string } }
  | {
      StaleEpoch: {
        cell_id: string;
        expected_epoch: bigint;
        actual_epoch: bigint;
      };
    }
  | {
      LeaseHeldByAnother: {
        cell_id: string;
        current_owner: string;
        expires_at_ns: bigint;
      };
    }
  | { NotOwner: { cell_id: string } }
  | { ReplicationMismatch: { reason: string } }
  | { Unauthorized: null }
  | { PreconditionFailed: { reason: string } }
  | { Transport: string }
  | { InvalidArgument: string }
  | { Database: string };

export type CandidResult<T> = { Ok: T } | { Err: CandidCellError };

export type CandidFencing = [string, string, bigint];

type Opt<T> = [] | [T];

export type CandidCellOp =
  | { KvPut: { key: string; value: Uint8Array } }
  | { KvDelete: { key: string } }
  | { KvDeleteAll: null }
  | { SetAlarm: Opt<bigint> }
  | { AssertKv: { key: string; expected: Opt<Uint8Array> } }
  | { Sql: { sql: string; args: CandidSqlValue[] } };

type CandidLease = {
  cell_id: string;
  owner_id: string;
  epoch: bigint;
  expires_at_ns: bigint;
};

type CandidKvEntry = { key: string; value: Uint8Array | number[] };

/**
 * Minimal actor interface compatible with `@dfinity/agent`'s `Actor.createActor`.
 */
export interface CanisterActor {
  acquire_lease(
    cellId: string,
    ownerId: string,
    ttlNs: bigint,
  ): Promise<
    CandidResult<{
      cell_id: string;
      owner_id: string;
      epoch: bigint;
      expires_at_ns: bigint;
    }>
  >;
  release_lease(cellId: string, ownerId: string, epoch: bigint): Promise<CandidResult<null>>;
  sql_execute(
    sql: string,
    args: CandidSqlValue[],
    idempotencyKey: [] | [string],
    fencing: [] | [CandidFencing],
  ): Promise<CandidResult<{ rows_affected: bigint; idempotent_replay: boolean }>>;
  sql_query_strong(
    sql: string,
    args: CandidSqlValue[],
  ): Promise<CandidResult<{ rows: CandidSqlValue[][] }>>;
  sql_query_eventual(
    sql: string,
    args: CandidSqlValue[],
  ): Promise<CandidResult<{ rows: CandidSqlValue[][] }>>;
  activate_cell(
    cellId: string,
    ownerId: string,
    ttlNs: bigint,
    preloadBytes: bigint,
  ): Promise<
    CandidResult<{
      lease: CandidLease;
      entries: CandidKvEntry[];
      complete: boolean;
      alarm_at_ns: Opt<bigint>;
    }>
  >;
  cell_transact(req: {
    fencing: CandidFencing;
    idempotency_key: Opt<string>;
    ops: CandidCellOp[];
  }): Promise<
    CandidResult<{
      results: { rows_affected: bigint; rows: CandidSqlValue[][] }[];
      idempotent_replay: boolean;
      epoch: bigint;
    }>
  >;
  cell_kv_get(cellId: string, keys: string[]): Promise<CandidResult<Opt<Uint8Array | number[]>[]>>;
  cell_kv_list(
    cellId: string,
    opts: {
      prefix: Opt<string>;
      start_after: Opt<string>;
      limit: Opt<number>;
      reverse: Opt<boolean>;
    },
  ): Promise<CandidResult<{ entries: CandidKvEntry[]; truncated: boolean }>>;
  cell_get_alarm(cellId: string): Promise<CandidResult<Opt<bigint>>>;
  list_due_alarms(limit: number): Promise<CandidResult<{ cell_id: string; at_ns: bigint }[]>>;
}

function opt<T>(v: T | null | undefined): Opt<T> {
  return v === null || v === undefined ? [] : [v];
}

/** `@dfinity/candid` may decode `blob` as `number[]`; normalize it. */
function bytes(v: Uint8Array | number[]): Uint8Array {
  return v instanceof Uint8Array ? v : Uint8Array.from(v);
}

export function encodeCellOp(op: CellOp): CandidCellOp {
  switch (op.kind) {
    case "put":
      return { KvPut: { key: op.key, value: op.value } };
    case "delete":
      return { KvDelete: { key: op.key } };
    case "deleteAll":
      return { KvDeleteAll: null };
    case "setAlarm":
      return { SetAlarm: opt(op.atNs) };
    case "assert":
      return { AssertKv: { key: op.key, expected: opt(op.expected) } };
    case "sql":
      return { Sql: { sql: op.sql, args: op.args.map(encodeSqlParam) } };
  }
}

function decodeLease(l: CandidLease): LeaseRecord {
  return { cellId: l.cell_id, ownerId: l.owner_id, epoch: l.epoch, expiresAtNs: l.expires_at_ns };
}

export function encodeSqlParam(param: SqlParam): CandidSqlValue {
  if (param === null) return { Null: null };
  if (typeof param === "bigint") return { Integer: param };
  if (typeof param === "number") {
    return Number.isSafeInteger(param) ? { Integer: BigInt(param) } : { Real: param };
  }
  if (typeof param === "string") return { Text: param };
  return { Blob: param };
}

export function decodeSqlValue(value: CandidSqlValue): SqlParam {
  if ("Null" in value) return null;
  if ("Integer" in value) return value.Integer;
  if ("Real" in value) return value.Real;
  if ("Text" in value) return value.Text;
  return bytes(value.Blob as Uint8Array | number[]);
}

export function unwrapCandidResult<T>(res: CandidResult<T>): T {
  if ("Ok" in res) return res.Ok;
  const err = res.Err;
  if ("CasConflict" in err) {
    const c = err.CasConflict;
    throw new CasConflictError(c.key, c.expected_generation, c.actual_generation[0] ?? null);
  }
  if ("StaleEpoch" in err) {
    const s = err.StaleEpoch;
    throw new StaleEpochError(s.cell_id, s.expected_epoch, s.actual_epoch);
  }
  if ("Unauthorized" in err) {
    throw new IcCellError("UNAUTHORIZED", "Caller is not authorized");
  }
  if ("Transport" in err) {
    throw new IcCellError("TRANSPORT", err.Transport);
  }
  if ("AlreadyExists" in err) {
    throw new IcCellError("ALREADY_EXISTS", `Key already exists: ${err.AlreadyExists.key}`);
  }
  if ("NotFound" in err) {
    throw new IcCellError("NOT_FOUND", `Key not found: ${err.NotFound.key}`);
  }
  if ("LeaseHeldByAnother" in err) {
    const l = err.LeaseHeldByAnother;
    throw new LeaseHeldError(l.cell_id, l.current_owner, l.expires_at_ns);
  }
  if ("PreconditionFailed" in err) {
    throw new PreconditionFailedError(err.PreconditionFailed.reason);
  }
  if ("NotOwner" in err) {
    throw new IcCellError("NOT_OWNER", `Not owner of cell '${err.NotOwner.cell_id}'`);
  }
  if ("ReplicationMismatch" in err) {
    throw new IcCellError("REPLICATION_MISMATCH", err.ReplicationMismatch.reason);
  }
  if ("InvalidArgument" in err) {
    throw new IcCellError("INVALID_ARGUMENT", err.InvalidArgument);
  }
  throw new IcCellError("DATABASE", err.Database);
}

/**
 * `IcDbTransport` backed by a live `@dfinity/agent` Canister Actor.
 */
export class CandidActorTransport implements IcDbTransport, CellTransport {
  private readonly actor: CanisterActor;

  constructor(actor: CanisterActor) {
    this.actor = actor;
  }

  async acquireLease(cellId: string, ownerId: string, ttlNs: bigint): Promise<LeaseRecord> {
    const raw = unwrapCandidResult(await this.actor.acquire_lease(cellId, ownerId, ttlNs));
    return {
      cellId: raw.cell_id,
      ownerId: raw.owner_id,
      epoch: raw.epoch,
      expiresAtNs: raw.expires_at_ns,
    };
  }

  async releaseLease(token: FencingToken): Promise<void> {
    unwrapCandidResult(await this.actor.release_lease(token.cellId, token.ownerId, token.epoch));
  }

  async execute(sql: string, params: SqlParam[], options?: ExecOptions): Promise<ExecResult> {
    const args = params.map(encodeSqlParam);
    const idem: [] | [string] = options?.idempotencyKey ? [options.idempotencyKey] : [];
    const fencing: [] | [CandidFencing] = options?.fencing
      ? [[options.fencing.cellId, options.fencing.ownerId, options.fencing.epoch]]
      : [];
    const raw = unwrapCandidResult(await this.actor.sql_execute(sql, args, idem, fencing));
    return {
      rowsAffected: raw.rows_affected,
      idempotentReplay: raw.idempotent_replay,
    };
  }

  async query(sql: string, params: SqlParam[], options?: QueryOptions): Promise<SqlParam[][]> {
    const args = params.map(encodeSqlParam);
    const consistency = options?.consistency ?? "strong";
    const res =
      consistency === "strong"
        ? await this.actor.sql_query_strong(sql, args)
        : await this.actor.sql_query_eventual(sql, args);
    const raw = unwrapCandidResult(res);
    return raw.rows.map((row) => row.map(decodeSqlValue));
  }

  async activateCell(
    cellId: string,
    ownerId: string,
    ttlNs: bigint,
    preloadBytes: bigint,
  ): Promise<Activation> {
    const raw = unwrapCandidResult(
      await this.actor.activate_cell(cellId, ownerId, ttlNs, preloadBytes),
    );
    return {
      lease: decodeLease(raw.lease),
      entries: raw.entries.map((e) => ({ key: e.key, value: bytes(e.value) })),
      complete: raw.complete,
      alarmAtNs: raw.alarm_at_ns[0] ?? null,
    };
  }

  async transact(
    fencing: FencingToken,
    ops: CellOp[],
    idempotencyKey?: string,
  ): Promise<TransactResult> {
    const raw = unwrapCandidResult(
      await this.actor.cell_transact({
        fencing: [fencing.cellId, fencing.ownerId, fencing.epoch],
        idempotency_key: opt(idempotencyKey),
        ops: ops.map(encodeCellOp),
      }),
    );
    return {
      results: raw.results.map((r) => ({
        rowsAffected: r.rows_affected,
        rows: r.rows.map((row) => row.map(decodeSqlValue)),
      })),
      idempotentReplay: raw.idempotent_replay,
      epoch: raw.epoch,
    };
  }

  async kvGet(cellId: string, keys: string[]): Promise<(Uint8Array | null)[]> {
    const raw = unwrapCandidResult(await this.actor.cell_kv_get(cellId, keys));
    return raw.map((v) => (v.length === 0 ? null : bytes(v[0])));
  }

  async kvList(cellId: string, options: KvListOptions): Promise<KvListResult> {
    const raw = unwrapCandidResult(
      await this.actor.cell_kv_list(cellId, {
        prefix: opt(options.prefix),
        start_after: opt(options.startAfter),
        limit: opt(options.limit),
        reverse: opt(options.reverse),
      }),
    );
    return {
      entries: raw.entries.map((e) => ({ key: e.key, value: bytes(e.value) })),
      truncated: raw.truncated,
    };
  }

  async getAlarm(cellId: string): Promise<bigint | null> {
    return unwrapCandidResult(await this.actor.cell_get_alarm(cellId))[0] ?? null;
  }

  async listDueAlarms(limit: number): Promise<DueAlarm[]> {
    const raw = unwrapCandidResult(await this.actor.list_due_alarms(limit));
    return raw.map((a) => ({ cellId: a.cell_id, atNs: a.at_ns }));
  }
}
