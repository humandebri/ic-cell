import {
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
  KvEntry,
  KvListOptions,
  KvListResult,
  LeaseRecord,
  OpResult,
  SqlParam,
  TransactResult,
} from "./types.ts";

function bytesEqual(a: Uint8Array | null | undefined, b: Uint8Array | null | undefined): boolean {
  if (!a || !b) return !a && !b;
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * In-memory canister simulator for local examples and unit tests.
 *
 * Implements the same lease / fencing / idempotency / Durable Cell semantics
 * as `ic-cell-canister` (atomic `transact`, per-cell KV, alarms). Raw SQL is
 * only simulated for simple key/value tables; `sql` cell ops are unsupported.
 */
export class InMemoryCanisterTransport implements IcDbTransport, CellTransport {
  private nowNs: bigint;
  private readonly leases = new Map<string, LeaseRecord>();
  private readonly idempotency = new Map<string, ExecResult>();
  private readonly kvStore = new Map<string, SqlParam[]>();
  private readonly cells = new Map<string, Map<string, Uint8Array>>();
  private readonly alarms = new Map<string, bigint>();
  private readonly txnResults = new Map<string, { cellId: string; result: TransactResult }>();
  /** Number of `transact` calls that reached the "canister" (for tests). */
  transactCalls = 0;

  constructor(initialTimeNs = 1_000n) {
    this.nowNs = initialTimeNs;
  }

  now(): bigint {
    return this.nowNs;
  }

  advanceTime(deltaNs: bigint): void {
    this.nowNs += deltaNs;
  }

  setTime(nowNs: bigint): void {
    this.nowNs = nowNs;
  }

  async acquireLease(cellId: string, ownerId: string, ttlNs: bigint): Promise<LeaseRecord> {
    if (ttlNs <= 0n) throw new IcCellError("INVALID_ARGUMENT", "ttl_ns must be > 0");
    const existing = this.leases.get(cellId);
    const expiresAtNs = this.nowNs + ttlNs;
    if (!existing) {
      const lease: LeaseRecord = { cellId, ownerId, epoch: 1n, expiresAtNs };
      this.leases.set(cellId, lease);
      return { ...lease };
    }
    if (existing.ownerId === ownerId) {
      const renewed: LeaseRecord = { ...existing, expiresAtNs };
      this.leases.set(cellId, renewed);
      return { ...renewed };
    }
    if (this.nowNs >= existing.expiresAtNs) {
      const taken: LeaseRecord = { cellId, ownerId, epoch: existing.epoch + 1n, expiresAtNs };
      this.leases.set(cellId, taken);
      return { ...taken };
    }
    throw new LeaseHeldError(cellId, existing.ownerId, existing.expiresAtNs);
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

  // ---- Durable Cells ----

  async activateCell(
    cellId: string,
    ownerId: string,
    ttlNs: bigint,
    preloadBytes: bigint,
  ): Promise<Activation> {
    const lease = await this.acquireLease(cellId, ownerId, ttlNs);
    const listed =
      preloadBytes === 0n
        ? { entries: [], truncated: true }
        : this.list(cellId, {}, Number(preloadBytes));
    return {
      lease,
      entries: listed.entries,
      complete: !listed.truncated,
      alarmAtNs: this.alarms.get(cellId) ?? null,
    };
  }

  async transact(
    fencing: FencingToken,
    ops: CellOp[],
    idempotencyKey?: string,
  ): Promise<TransactResult> {
    this.transactCalls++;
    this.verifyFencing(fencing);
    if (idempotencyKey !== undefined) {
      const prev = this.txnResults.get(idempotencyKey);
      if (prev) {
        if (prev.cellId !== fencing.cellId) {
          throw new IcCellError("INVALID_ARGUMENT", "idempotency key belongs to another cell");
        }
        return { ...prev.result, idempotentReplay: true };
      }
    }
    // Apply to copies so a failing op leaves no trace (atomicity).
    const kv = new Map(this.cells.get(fencing.cellId) ?? []);
    let alarm = this.alarms.get(fencing.cellId) ?? null;
    const results: OpResult[] = [];
    for (const op of ops) {
      switch (op.kind) {
        case "put":
          kv.set(op.key, new Uint8Array(op.value));
          results.push({ rowsAffected: 1n, rows: [] });
          break;
        case "delete":
          results.push({ rowsAffected: kv.delete(op.key) ? 1n : 0n, rows: [] });
          break;
        case "deleteAll": {
          const n = BigInt(kv.size);
          kv.clear();
          results.push({ rowsAffected: n, rows: [] });
          break;
        }
        case "setAlarm": {
          const had = alarm !== null;
          alarm = op.atNs;
          results.push({ rowsAffected: op.atNs !== null || had ? 1n : 0n, rows: [] });
          break;
        }
        case "assert":
          if (!bytesEqual(kv.get(op.key) ?? null, op.expected)) {
            throw new PreconditionFailedError(`value of key "${op.key}" does not match`);
          }
          results.push({ rowsAffected: 0n, rows: [] });
          break;
        case "sql":
          throw new IcCellError(
            "UNSUPPORTED",
            "InMemoryCanisterTransport does not execute SQL inside cell transactions",
          );
      }
    }
    this.cells.set(fencing.cellId, kv);
    if (alarm === null) this.alarms.delete(fencing.cellId);
    else this.alarms.set(fencing.cellId, alarm);
    const result: TransactResult = { results, idempotentReplay: false, epoch: fencing.epoch };
    if (idempotencyKey !== undefined) {
      this.txnResults.set(idempotencyKey, { cellId: fencing.cellId, result });
    }
    return result;
  }

  async kvGet(cellId: string, keys: string[]): Promise<(Uint8Array | null)[]> {
    const kv = this.cells.get(cellId);
    return keys.map((k) => {
      const v = kv?.get(k);
      return v ? new Uint8Array(v) : null;
    });
  }

  async kvList(cellId: string, options: KvListOptions): Promise<KvListResult> {
    return this.list(cellId, options, 1_900_000);
  }

  async getAlarm(cellId: string): Promise<bigint | null> {
    return this.alarms.get(cellId) ?? null;
  }

  async listDueAlarms(limit: number): Promise<DueAlarm[]> {
    return Array.from(this.alarms.entries())
      .filter(([, at]) => at <= this.nowNs)
      .sort((a, b) => (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0))
      .slice(0, Math.max(1, limit))
      .map(([cellId, atNs]) => ({ cellId, atNs }));
  }

  /** Test helper: the committed value of a cell key. */
  peek(cellId: string, key: string): Uint8Array | undefined {
    return this.cells.get(cellId)?.get(key);
  }

  private list(cellId: string, options: KvListOptions, byteBudget: number): KvListResult {
    const prefix = options.prefix ?? "";
    const limit = Math.max(1, options.limit ?? 1000);
    let keys = Array.from(this.cells.get(cellId)?.keys() ?? [])
      .filter((k) => k.startsWith(prefix))
      .sort();
    if (options.reverse) keys.reverse();
    if (options.startAfter !== undefined) {
      const after = options.startAfter;
      keys = keys.filter((k) => (options.reverse ? k < after : k > after));
    }
    const entries: KvEntry[] = [];
    let bytes = 0;
    let truncated = false;
    for (const key of keys) {
      const value = this.cells.get(cellId)!.get(key)!;
      const size = key.length + value.length + 16;
      if (entries.length >= limit || (bytes + size > byteBudget && entries.length > 0)) {
        truncated = true;
        break;
      }
      bytes += size;
      entries.push({ key, value: new Uint8Array(value) });
    }
    return { entries, truncated };
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
