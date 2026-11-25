export type SqlParam = null | number | bigint | string | Uint8Array;

export type Consistency = "strong" | "eventual";

export interface FencingToken {
  cellId: string;
  ownerId: string;
  epoch: bigint;
}

export interface ExecOptions {
  idempotencyKey?: string;
  fencing?: FencingToken;
}

export interface QueryOptions {
  consistency?: Consistency;
  columnCount?: number;
}

export interface ExecResult {
  rowsAffected: bigint;
  idempotentReplay: boolean;
}

export interface LeaseRecord {
  cellId: string;
  ownerId: string;
  epoch: bigint;
  expiresAtNs: bigint;
}

export interface IcDbTransport {
  acquireLease(cellId: string, ownerId: string, ttlNs: bigint): Promise<LeaseRecord>;
  releaseLease(token: FencingToken): Promise<void>;
  execute(sql: string, params: SqlParam[], options?: ExecOptions): Promise<ExecResult>;
  query(sql: string, params: SqlParam[], options?: QueryOptions): Promise<SqlParam[][]>;
}

// ---- Durable Cells --------------------------------------------------------

/** One step of an atomic, fenced cell transaction (`cell_transact`). */
export type CellOp =
  | { kind: "put"; key: string; value: Uint8Array }
  | { kind: "delete"; key: string }
  | { kind: "deleteAll" }
  | { kind: "setAlarm"; atNs: bigint | null }
  | { kind: "assert"; key: string; expected: Uint8Array | null }
  | { kind: "sql"; sql: string; args: SqlParam[] };

export interface OpResult {
  rowsAffected: bigint;
  rows: SqlParam[][];
}

export interface TransactResult {
  results: OpResult[];
  idempotentReplay: boolean;
  epoch: bigint;
}

export interface KvEntry {
  key: string;
  value: Uint8Array;
}

export interface KvListOptions {
  prefix?: string;
  startAfter?: string;
  limit?: number;
  reverse?: boolean;
}

export interface KvListResult {
  entries: KvEntry[];
  truncated: boolean;
}

export interface DueAlarm {
  cellId: string;
  atNs: bigint;
}

export interface Activation {
  lease: LeaseRecord;
  entries: KvEntry[];
  /** `true` when `entries` is the cell's complete key/value state. */
  complete: boolean;
  alarmAtNs: bigint | null;
}

/** Canister operations the Durable Cell runtime needs. */
export interface CellTransport {
  activateCell(
    cellId: string,
    ownerId: string,
    ttlNs: bigint,
    preloadBytes: bigint,
  ): Promise<Activation>;
  transact(
    fencing: FencingToken,
    ops: CellOp[],
    idempotencyKey?: string,
  ): Promise<TransactResult>;
  kvGet(cellId: string, keys: string[]): Promise<(Uint8Array | null)[]>;
  kvList(cellId: string, options: KvListOptions): Promise<KvListResult>;
  getAlarm(cellId: string): Promise<bigint | null>;
  listDueAlarms(limit: number): Promise<DueAlarm[]>;
  releaseLease(token: FencingToken): Promise<void>;
}
