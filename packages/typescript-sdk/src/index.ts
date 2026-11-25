export * from "./errors.ts";
export * from "./types.ts";
export { InMemoryCanisterTransport } from "./memory.ts";
export {
  CellNamespace,
  CellStorage,
  HttpForwarder,
  InProcessForwarder,
  realTimers,
  type AlarmInfo,
  type CellForwarder,
  type CellNamespaceOptions,
  type CellObject,
  type CellState,
  type ForwardRequest,
  type ListOptions,
  type Timers,
} from "./runtime.ts";
export { decodeJson, encodeJson, taggedJsonCodec, type ValueCodec } from "./codec.ts";
export {
  CandidActorTransport,
  decodeSqlValue,
  encodeCellOp,
  encodeSqlParam,
  unwrapCandidResult,
  type CandidCellError,
  type CandidCellOp,
  type CandidFencing,
  type CandidResult,
  type CandidSqlValue,
  type CanisterActor,
} from "./candid.ts";
export { idlFactory } from "./generated/ic_cell_canister.did.js";

import type {
  ExecOptions,
  ExecResult,
  FencingToken,
  IcDbTransport,
  LeaseRecord,
  QueryOptions,
  SqlParam,
} from "./types.ts";

/**
 * `IC_DB` binding exposed to Workers / Durable Objects running on `celld`.
 *
 * Defaults to strong (consensus-backed `update`) reads per ADR-0002.
 */
export class IcDb {
  private readonly transport: IcDbTransport;

  constructor(transport: IcDbTransport) {
    this.transport = transport;
  }

  async acquireLease(cellId: string, ownerId: string, ttlNs: bigint): Promise<LeaseRecord> {
    return this.transport.acquireLease(cellId, ownerId, ttlNs);
  }

  async releaseLease(token: FencingToken): Promise<void> {
    return this.transport.releaseLease(token);
  }

  async execute(sql: string, params: SqlParam[] = [], options?: ExecOptions): Promise<ExecResult> {
    return this.transport.execute(sql, params, options);
  }

  async query(sql: string, params: SqlParam[] = [], options?: QueryOptions): Promise<SqlParam[][]> {
    return this.transport.query(sql, params, {
      consistency: options?.consistency ?? "strong",
      columnCount: options?.columnCount,
    });
  }
}
