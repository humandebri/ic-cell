import {
  CasConflictError,
  IcCellError,
  StaleEpochError,
  type ExecOptions,
  type ExecResult,
  type FencingToken,
  type IcDbTransport,
  type LeaseRecord,
  type QueryOptions,
  type SqlParam,
} from "./index.ts";

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
  | { Transport: string }
  | { InvalidArgument: string }
  | { Database: string };

export type CandidResult<T> = { Ok: T } | { Err: CandidCellError };

export type CandidFencing = [string, string, bigint];

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
  return value.Blob;
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
    throw new IcCellError(
      "LEASE_HELD",
      `Lease for '${l.cell_id}' held by '${l.current_owner}' until ${l.expires_at_ns}`,
    );
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
export class CandidActorTransport implements IcDbTransport {
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
}
