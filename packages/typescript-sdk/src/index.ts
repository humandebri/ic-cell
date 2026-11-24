export {
  CandidActorTransport,
  decodeSqlValue,
  encodeSqlParam,
  unwrapCandidResult,
  type CandidCellError,
  type CandidFencing,
  type CandidResult,
  type CandidSqlValue,
  type CanisterActor,
} from "./candid.ts";

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

export class IcCellError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "IcCellError";
    this.code = code;
  }
}

export class CasConflictError extends IcCellError {
  readonly key: string;
  readonly expectedGeneration: bigint;
  readonly actualGeneration: bigint | null;

  constructor(key: string, expectedGeneration: bigint, actualGeneration: bigint | null) {
    super(
      "CAS_CONFLICT",
      `CAS conflict on '${key}': expected generation ${expectedGeneration}, actual ${actualGeneration}`,
    );
    this.name = "CasConflictError";
    this.key = key;
    this.expectedGeneration = expectedGeneration;
    this.actualGeneration = actualGeneration;
  }
}

export class StaleEpochError extends IcCellError {
  readonly cellId: string;
  readonly expectedEpoch: bigint;
  readonly actualEpoch: bigint;

  constructor(cellId: string, expectedEpoch: bigint, actualEpoch: bigint) {
    super(
      "STALE_EPOCH",
      `Stale epoch for cell '${cellId}': expected ${expectedEpoch}, actual ${actualEpoch}`,
    );
    this.name = "StaleEpochError";
    this.cellId = cellId;
    this.expectedEpoch = expectedEpoch;
    this.actualEpoch = actualEpoch;
  }
}

export class UnknownOutcomeError extends IcCellError {
  readonly idempotencyKey?: string;
  constructor(message: string, idempotencyKey?: string) {
    super("UNKNOWN_OUTCOME", message);
    this.name = "UnknownOutcomeError";
    this.idempotencyKey = idempotencyKey;
  }
}

export interface IcDbTransport {
  acquireLease(cellId: string, ownerId: string, ttlNs: bigint): Promise<LeaseRecord>;
  releaseLease(token: FencingToken): Promise<void>;
  execute(sql: string, params: SqlParam[], options?: ExecOptions): Promise<ExecResult>;
  query(sql: string, params: SqlParam[], options?: QueryOptions): Promise<SqlParam[][]>;
}

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

/**
 * Lightweight in-memory canister simulator for local examples and unit tests.
 * Supports leases, fencing epochs, idempotency keys, and simple key-value tables.
 */
export class InMemoryCanisterTransport implements IcDbTransport {
  private nowNs: bigint;
  private readonly leases = new Map<string, LeaseRecord>();
  private readonly idempotency = new Map<string, ExecResult>();
  private readonly kvStore = new Map<string, SqlParam[]>();

  constructor(initialTimeNs = 1_000n) {
    this.nowNs = initialTimeNs;
  }

  advanceTime(deltaNs: bigint): void {
    this.nowNs += deltaNs;
  }

  async acquireLease(cellId: string, ownerId: string, ttlNs: bigint): Promise<LeaseRecord> {
    const existing = this.leases.get(cellId);
    const expiresAtNs = this.nowNs + ttlNs;
    if (!existing) {
      const lease: LeaseRecord = { cellId, ownerId, epoch: 1n, expiresAtNs };
      this.leases.set(cellId, lease);
      return lease;
    }
    if (existing.ownerId === ownerId) {
      const renewed: LeaseRecord = { ...existing, expiresAtNs };
      this.leases.set(cellId, renewed);
      return renewed;
    }
    if (this.nowNs >= existing.expiresAtNs) {
      const taken: LeaseRecord = {
        cellId,
        ownerId,
        epoch: existing.epoch + 1n,
        expiresAtNs,
      };
      this.leases.set(cellId, taken);
      return taken;
    }
    throw new IcCellError(
      "LEASE_HELD",
      `Lease for '${cellId}' is held by '${existing.ownerId}' until ${existing.expiresAtNs}`,
    );
  }

  async releaseLease(token: FencingToken): Promise<void> {
    this.verifyFencing(token);
    this.leases.delete(token.cellId);
  }

  async execute(sql: string, params: SqlParam[], options?: ExecOptions): Promise<ExecResult> {
    if (options?.fencing) {
      this.verifyFencing(options.fencing);
    }
    if (options?.idempotencyKey) {
      const prev = this.idempotency.get(options.idempotencyKey);
      if (prev) {
        return { rowsAffected: prev.rowsAffected, idempotentReplay: true };
      }
    }

    const normalized = sql.trim().toUpperCase();
    let rowsAffected = 0n;
    if (normalized.startsWith("INSERT") || normalized.startsWith("REPLACE")) {
      const key = String(params[0] ?? "");
      this.kvStore.set(key, [...params]);
      rowsAffected = 1n;
    } else if (normalized.startsWith("UPDATE")) {
      const key = String(params[params.length - 1] ?? "");
      if (this.kvStore.has(key)) {
        this.kvStore.set(key, [key, ...params.slice(0, -1)]);
        rowsAffected = 1n;
      }
    } else if (normalized.startsWith("DELETE")) {
      const key = String(params[0] ?? "");
      rowsAffected = this.kvStore.delete(key) ? 1n : 0n;
    }

    const result: ExecResult = { rowsAffected, idempotentReplay: false };
    if (options?.idempotencyKey) {
      this.idempotency.set(options.idempotencyKey, result);
    }
    return result;
  }

  async query(_sql: string, params: SqlParam[]): Promise<SqlParam[][]> {
    if (params.length === 0) {
      return Array.from(this.kvStore.values());
    }
    const key = String(params[0] ?? "");
    const row = this.kvStore.get(key);
    return row ? [row] : [];
  }

  private verifyFencing(token: FencingToken): void {
    const current = this.leases.get(token.cellId);
    if (!current) {
      throw new IcCellError("NOT_OWNER", `No active lease for '${token.cellId}'`);
    }
    if (current.epoch !== token.epoch) {
      throw new StaleEpochError(token.cellId, token.epoch, current.epoch);
    }
    if (current.ownerId !== token.ownerId) {
      throw new IcCellError("NOT_OWNER", `Cell '${token.cellId}' is owned by '${current.ownerId}'`);
    }
  }
}
