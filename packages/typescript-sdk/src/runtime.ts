/**
 * Durable Cells runtime: Durable-Object-style actors whose state lives in
 * `ic-cell-canister`.
 *
 * Guarantees:
 * - **Single instance**: a cell runs on the node holding its lease. Calls that
 *   reach another node are forwarded to the owner (when a forwarder is set).
 * - **Serialized execution**: calls to one cell run one at a time, in order.
 * - **Output gate**: writes made during a call are coalesced into one atomic,
 *   fenced canister transaction that commits before the call returns.
 * - **Fencing**: a node that lost its lease can never commit; its instance is
 *   discarded and the next call re-activates from committed state.
 * - **Alarms**: one alarm per cell, persisted in the canister and fired with
 *   retries; `pollAlarms` lets any node pick up alarms of dead owners.
 */
import { decodeJson, encodeJson, taggedJsonCodec, type ValueCodec } from "./codec.ts";
import {
  CellUnavailableError,
  IcCellError,
  LeaseHeldError,
  StaleEpochError,
  isDeterministicRollbackError,
  isTransientError,
} from "./errors.ts";
import type { CellOp, CellTransport, FencingToken, OpResult, SqlParam } from "./types.ts";

export interface Timers {
  now(): number;
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
}

export const realTimers: Timers = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => {
    const h = setTimeout(fn, ms) as unknown as { unref?: () => void };
    h.unref?.();
    return h;
  },
  clearTimeout: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
};

export interface ListOptions {
  prefix?: string;
  /** Inclusive lower bound. */
  start?: string;
  /** Exclusive cursor. */
  startAfter?: string;
  /** Exclusive upper bound. */
  end?: string;
  limit?: number;
  reverse?: boolean;
}

export interface AlarmInfo {
  retryCount: number;
  isRetry: boolean;
}

export interface CellState {
  readonly id: string;
  readonly storage: CellStorage;
  /** Runs `fn` before any queued call is delivered (e.g. initialization). */
  blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T>;
}

/** Methods a cell class may implement besides its RPC methods. */
export interface CellObject {
  alarm?(info: AlarmInfo): unknown;
}

export interface ForwardRequest {
  cellId: string;
  method: string;
  args: unknown[];
  hops: number;
}

/** Delivers a call to the node that owns the cell. */
export interface CellForwarder {
  forward(ownerId: string, request: ForwardRequest): Promise<unknown>;
}

export interface CellNamespaceOptions<Env> {
  transport: CellTransport;
  /** Unique id of this node; stored as the lease owner. */
  nodeId: string;
  create: (state: CellState, env: Env) => object;
  env: Env;
  leaseTtlMs?: number;
  preloadBytes?: number;
  idleTimeoutMs?: number;
  forwarder?: CellForwarder;
  codec?: ValueCodec;
  timers?: Timers;
  /** Renew leases / evict idle cells / fire alarms with timers (default true). */
  autoTimers?: boolean;
  /** Retries for a transaction whose outcome is unknown (default 3). */
  maxCommitRetries?: number;
  maxAlarmRetries?: number;
}

const MAX_HOPS = 2;
const NS_PER_MS = 1_000_000n;

// ---------------------------------------------------------------------------

interface Snapshot {
  cache: Map<string, Uint8Array | null>;
  complete: boolean;
  alarmAtNs: bigint | null;
  alarmVersion: number;
  pending: number;
}

/** Per-cell storage. Reads are served from a write-through cache that is
 * coherent because only the lease holder writes. */
export class CellStorage {
  private readonly cell: ActiveCell;
  private readonly codec: ValueCodec;
  /** key -> encoded value, or `null` for a key known to be absent. */
  private cache = new Map<string, Uint8Array | null>();
  private complete: boolean;
  private alarmAtNs: bigint | null;
  private pending: CellOp[] = [];
  private inTransaction = false;
  /** Bumped by every setAlarm/deleteAlarm, so re-setting the same time is visible. */
  private alarmVersion = 0;

  constructor(
    cell: ActiveCell,
    codec: ValueCodec,
    entries: { key: string; value: Uint8Array }[],
    complete: boolean,
    alarmAtNs: bigint | null,
  ) {
    this.cell = cell;
    this.codec = codec;
    for (const e of entries) this.cache.set(e.key, e.value);
    this.complete = complete;
    this.alarmAtNs = alarmAtNs;
  }

  get<T = unknown>(key: string): Promise<T | undefined>;
  get<T = unknown>(keys: string[]): Promise<Map<string, T>>;
  async get<T>(keyOrKeys: string | string[]): Promise<T | undefined | Map<string, T>> {
    const keys = Array.isArray(keyOrKeys) ? keyOrKeys : [keyOrKeys];
    await this.load(keys);
    if (!Array.isArray(keyOrKeys)) return this.decode<T>(this.cache.get(keyOrKeys) ?? null);
    const out = new Map<string, T>();
    for (const k of keys) {
      const v = this.cache.get(k);
      if (v) out.set(k, this.codec.decode(v) as T);
    }
    return out;
  }

  put(key: string, value: unknown): Promise<void>;
  put(entries: Record<string, unknown>): Promise<void>;
  async put(keyOrEntries: string | Record<string, unknown>, value?: unknown): Promise<void> {
    const entries =
      typeof keyOrEntries === "string" ? [[keyOrEntries, value] as const] : Object.entries(keyOrEntries);
    for (const [key, v] of entries) {
      if (v === undefined) throw new TypeError(`cannot put undefined for key "${key}"`);
      const encoded = this.codec.encode(v);
      this.cache.set(key, encoded);
      this.pending.push({ kind: "put", key, value: encoded });
    }
  }

  delete(key: string): Promise<boolean>;
  delete(keys: string[]): Promise<number>;
  async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
    const keys = Array.isArray(keyOrKeys) ? keyOrKeys : [keyOrKeys];
    await this.load(keys);
    let deleted = 0;
    for (const key of keys) {
      if (this.cache.get(key)) deleted++;
      this.cache.set(key, null);
      this.pending.push({ kind: "delete", key });
    }
    return Array.isArray(keyOrKeys) ? deleted : deleted === 1;
  }

  async deleteAll(): Promise<void> {
    this.cache = new Map();
    this.complete = true;
    this.pending.push({ kind: "deleteAll" });
  }

  async list<T = unknown>(options: ListOptions = {}): Promise<Map<string, T>> {
    if (!this.complete) await this.loadAll();
    const prefix = options.prefix ?? "";
    let keys = Array.from(this.cache.entries())
      .filter(([k, v]) => v !== null && k.startsWith(prefix))
      .map(([k]) => k)
      .filter(
        (k) =>
          (options.start === undefined || k >= options.start) &&
          (options.startAfter === undefined || k > options.startAfter) &&
          (options.end === undefined || k < options.end),
      )
      .sort();
    if (options.reverse) keys.reverse();
    if (options.limit !== undefined) keys = keys.slice(0, options.limit);
    const out = new Map<string, T>();
    for (const k of keys) out.set(k, this.codec.decode(this.cache.get(k)!) as T);
    return out;
  }

  /** Alarm time in ms since the epoch, or `null`. */
  async getAlarm(): Promise<number | null> {
    return this.alarmAtNs === null ? null : Number(this.alarmAtNs / NS_PER_MS);
  }

  async setAlarm(scheduledTime: number | Date): Promise<void> {
    const ms = typeof scheduledTime === "number" ? scheduledTime : scheduledTime.getTime();
    if (!Number.isFinite(ms)) throw new TypeError("invalid alarm time");
    this.alarmAtNs = BigInt(Math.floor(ms)) * NS_PER_MS;
    this.alarmVersion++;
    this.pending.push({ kind: "setAlarm", atNs: this.alarmAtNs });
  }

  async deleteAlarm(): Promise<void> {
    this.alarmAtNs = null;
    this.alarmVersion++;
    this.pending.push({ kind: "setAlarm", atNs: null });
  }

  /**
   * Runs SQL against the canister's shared SQL tables **in the same atomic
   * transaction** as every write still pending in this call. Returns rows
   * (use `RETURNING` for writes).
   */
  async sql(query: string, params: SqlParam[] = []): Promise<SqlParam[][]> {
    const results = await this.flushWith([{ kind: "sql", sql: query, args: params }]);
    return results[results.length - 1].rows;
  }

  /**
   * Runs `fn` atomically: its writes commit together, and if `fn` throws
   * none of them are applied (local state is rolled back too).
   */
  async transaction<T>(fn: (txn: CellStorage) => Promise<T>): Promise<T> {
    if (this.inTransaction) return fn(this);
    await this.sync();
    const snapshot = this.snapshot();
    this.inTransaction = true;
    try {
      const result = await fn(this);
      this.inTransaction = false;
      await this.sync();
      return result;
    } catch (err) {
      this.inTransaction = false;
      this.restore(snapshot);
      throw err;
    }
  }

  /** Commits pending writes now (normally done automatically per call). */
  async sync(): Promise<void> {
    if (this.pending.length > 0) await this.flushWith([]);
  }

  /** @internal */
  alarmNs(): bigint | null {
    return this.alarmAtNs;
  }

  /** @internal */
  alarmRevision(): number {
    return this.alarmVersion;
  }

  /** @internal */
  hasPending(): boolean {
    return this.pending.length > 0;
  }

  private async flushWith(extra: CellOp[]): Promise<OpResult[]> {
    if (this.inTransaction && extra.length > 0) {
      throw new IcCellError("INVALID_ARGUMENT", "storage.sql() is not allowed inside transaction()");
    }
    const priorPending = this.pending;
    const ops = [...priorPending, ...extra];
    this.pending = [];
    if (ops.length === 0) return [];
    try {
      const result = await this.cell.commit(ops);
      this.cell.onCommitted();
      return result.results;
    } catch (err) {
      if (isDeterministicRollbackError(err)) {
        // The canister rolled back the entire transaction; restore prior pending
        // writes (excluding the failing `extra` op) so catching a SQL error does
        // not discard earlier writes or poison the cell instance.
        this.pending = priorPending;
      }
      throw err;
    }
  }

  private async load(keys: string[]): Promise<void> {
    if (this.complete) return;
    const missing = keys.filter((k) => !this.cache.has(k));
    if (missing.length === 0) return;
    const values = await this.cell.transport.kvGet(this.cell.id, missing);
    missing.forEach((k, i) => {
      // A write made while the read was in flight wins.
      if (!this.cache.has(k)) this.cache.set(k, values[i]);
    });
  }

  private async loadAll(): Promise<void> {
    // Outside a transaction, commit pending writes first; inside a transaction,
    // never commit mid-flight (local uncommitted writes/tombstones in `this.cache`
    // take precedence over canister entries via `!this.cache.has(e.key)`).
    if (!this.inTransaction) await this.sync();
    let startAfter: string | undefined;
    for (;;) {
      const page = await this.cell.transport.kvList(this.cell.id, { startAfter, limit: 1000 });
      for (const e of page.entries) if (!this.cache.has(e.key)) this.cache.set(e.key, e.value);
      if (!page.truncated || page.entries.length === 0) break;
      startAfter = page.entries[page.entries.length - 1].key;
    }
    this.complete = true;
  }

  private decode<T>(v: Uint8Array | null): T | undefined {
    return v ? (this.codec.decode(v) as T) : undefined;
  }

  private snapshot(): Snapshot {
    return {
      cache: new Map(this.cache),
      complete: this.complete,
      alarmAtNs: this.alarmAtNs,
      alarmVersion: this.alarmVersion,
      pending: this.pending.length,
    };
  }

  private restore(s: Snapshot): void {
    this.cache = s.cache;
    this.complete = s.complete;
    this.alarmAtNs = s.alarmAtNs;
    this.alarmVersion = s.alarmVersion;
    this.pending.length = s.pending;
  }
}

// ---------------------------------------------------------------------------

class ActiveCell {
  readonly id: string;
  readonly transport: CellTransport;
  fencing: FencingToken;
  /** Local time (ms) after which the lease must be renewed before use. */
  validUntilMs: number;
  storage!: CellStorage;
  object!: object & CellObject;
  broken: Error | null = null;
  private queue: Promise<unknown> = Promise.resolve();
  /** True while a queued task runs (calls are strictly serialized). */
  running = false;
  private seq = 0;
  private readonly nonce = Math.random().toString(36).slice(2, 10);
  private readonly ns: CellNamespace<unknown>;
  renewTimer: unknown = null;
  idleTimer: unknown = null;
  alarmTimer: unknown = null;
  scheduledAlarmNs: bigint | null = null;
  scheduledRevision = -1;
  alarmRetries = 0;

  constructor(ns: CellNamespace<unknown>, id: string, fencing: FencingToken, validUntilMs: number) {
    this.ns = ns;
    this.id = id;
    this.transport = ns.transport;
    this.fencing = fencing;
    this.validUntilMs = validUntilMs;
  }

  /** Runs `task` after every previously queued task (input gate). */
  enqueue<T>(task: () => Promise<T>): Promise<T> {
    const run = this.queue.then(async () => {
      if (this.broken) throw this.broken;
      this.running = true;
      try {
        return await task();
      } finally {
        this.running = false;
      }
    });
    this.queue = run.catch(() => undefined);
    return run;
  }

  async commit(ops: CellOp[]) {
    if (this.broken) throw this.broken;
    const key = `${this.id}/${this.fencing.epoch}/${this.nonce}/${++this.seq}`;
    const max = this.ns.maxCommitRetries;
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.transport.transact(this.fencing, ops, key);
      } catch (err) {
        if (attempt < max && isTransientError(err)) {
          await this.ns.sleep(Math.min(50 * 2 ** attempt, 1_000));
          continue;
        }
        if (isDeterministicRollbackError(err)) {
          // Canister rolled back cleanly; keep the cell instance alive.
          throw err;
        }
        // Local state may now diverge from the canister: discard the instance.
        this.ns.markBroken(this, err as Error);
        throw err;
      }
    }
  }

  onCommitted(): void {
    this.ns.syncAlarmTimer(this);
  }
}

type Stub<T> = {
  [K in keyof T]: T[K] extends (...args: infer A) => infer R ? (...args: A) => Promise<Awaited<R>> : never;
} & { readonly id: string };

export class CellNamespace<Env = unknown> {
  readonly transport: CellTransport;
  readonly nodeId: string;
  readonly maxCommitRetries: number;
  private readonly create: (state: CellState, env: Env) => object;
  private readonly env: Env;
  private readonly leaseTtlMs: number;
  private readonly preloadBytes: number;
  private readonly idleTimeoutMs: number;
  private readonly forwarder?: CellForwarder;
  private readonly codec: ValueCodec;
  private readonly timers: Timers;
  private readonly autoTimers: boolean;
  private readonly maxAlarmRetries: number;
  private readonly active = new Map<string, ActiveCell>();
  private readonly activating = new Map<string, Promise<ActiveCell>>();
  private alarmPoller: unknown = null;
  private closed = false;

  constructor(options: CellNamespaceOptions<Env>) {
    this.transport = options.transport;
    this.nodeId = options.nodeId;
    this.create = options.create;
    this.env = options.env;
    this.leaseTtlMs = options.leaseTtlMs ?? 10_000;
    this.preloadBytes = options.preloadBytes ?? 1_000_000;
    this.idleTimeoutMs = options.idleTimeoutMs ?? 60_000;
    this.forwarder = options.forwarder;
    this.codec = options.codec ?? taggedJsonCodec;
    this.timers = options.timers ?? realTimers;
    this.autoTimers = options.autoTimers ?? true;
    this.maxCommitRetries = options.maxCommitRetries ?? 3;
    this.maxAlarmRetries = options.maxAlarmRetries ?? 6;
  }

  /** Returns a stub whose methods invoke the cell (locally or remotely). */
  get<T extends object = Record<string, (...args: never[]) => unknown>>(id: string): Stub<T> {
    return new Proxy({} as Stub<T>, {
      get: (_target, prop) => {
        if (prop === "id") return id;
        // Keep the stub from being mistaken for a promise.
        if (prop === "then" || typeof prop === "symbol") return undefined;
        return (...args: unknown[]) => this.call(id, prop, args);
      },
    });
  }

  /** Invokes `method` on cell `id`. */
  async call(id: string, method: string, args: unknown[] = [], hops = 0): Promise<unknown> {
    if (this.closed) throw new IcCellError("CLOSED", `namespace on ${this.nodeId} is shut down`);
    let cell: ActiveCell;
    try {
      cell = await this.ensureActive(id);
    } catch (err) {
      if (err instanceof LeaseHeldError && err.currentOwner !== this.nodeId) {
        return this.forward(err, { cellId: id, method, args, hops });
      }
      throw err;
    }
    this.touch(cell);
    return cell.enqueue(async () => {
      // Re-verify lease validity at execution time so queued read-only calls
      // cannot serve stale cached state after lease expiry.
      if (this.timers.now() >= cell.validUntilMs) {
        await this.renew(cell);
        if (cell.broken) throw cell.broken;
      }
      const fn = (cell.object as Record<string, unknown>)[method];
      if (
        typeof fn !== "function" ||
        method === "constructor" ||
        method === "alarm" ||
        method.startsWith("_") ||
        method in Object.prototype
      ) {
        throw new IcCellError("NO_SUCH_METHOD", `cell has no callable method "${method}"`);
      }
      let result: unknown;
      let failure: unknown = null;
      let failed = false;
      try {
        result = await (fn as (...a: unknown[]) => unknown).apply(cell.object, args);
      } catch (err) {
        failed = true;
        failure = err;
      }
      // Output gate: the caller only sees the result once its writes commit.
      // Writes made before an exception are still committed (as in DO).
      await cell.storage.sync();
      if (failed) throw failure;
      return result;
    });
  }

  /** Entry point for forwarded calls (see `InProcessForwarder` / `HttpForwarder`). */
  handleForwarded(request: ForwardRequest): Promise<unknown> {
    return this.call(request.cellId, request.method, request.args, request.hops);
  }

  /**
   * Handles a POST produced by `HttpForwarder`.
   *
   * This endpoint has no authentication of its own: expose it only on the
   * cluster's private network (or put your own auth in front of it).
   */
  async handleHttp(request: Request): Promise<Response> {
    try {
      const body = decodeJson(await request.text()) as ForwardRequest;
      const result = await this.handleForwarded(body);
      return new Response(encodeJson({ ok: result }), {
        headers: { "content-type": "application/json" },
      });
    } catch (err) {
      const e = err as IcCellError;
      return new Response(
        encodeJson({ error: { name: e.name, code: e.code ?? "ERROR", message: String(e.message ?? e) } }),
        { status: 500, headers: { "content-type": "application/json" } },
      );
    }
  }

  /** Activates cells whose alarms are due and fires them. Returns how many fired. */
  async pollAlarms(limit = 100): Promise<number> {
    const due = await this.transport.listDueAlarms(limit);
    let fired = 0;
    for (const { cellId } of due) {
      try {
        const cell = await this.ensureActive(cellId);
        if (await this.fireAlarm(cell)) fired++;
      } catch {
        // Skip individual cell failures (e.g. LeaseHeldError or handler error)
        // so remaining due alarms still run.
      }
    }
    return fired;
  }

  startAlarmPoller(intervalMs = 5_000): void {
    const tick = () => {
      this.alarmPoller = this.timers.setTimeout(() => {
        this.pollAlarms().catch(() => undefined).finally(() => {
          if (!this.closed) tick();
        });
      }, intervalMs);
    };
    if (this.alarmPoller === null) tick();
  }

  /** Renews every active lease now (also done by timers). */
  async renewLeases(): Promise<void> {
    await Promise.all(Array.from(this.active.values(), (c) => this.renew(c)));
  }

  /** Commits nothing, releases the lease and forgets the instance. */
  async evict(id: string, release = true): Promise<void> {
    const cell = this.active.get(id);
    if (!cell) return;
    // Detach immediately so concurrent calls activate a fresh instance instead
    // of enqueuing onto an instance whose lease is about to be released.
    this.active.delete(id);
    await cell.enqueue(async () => undefined).catch(() => undefined);
    this.drop(cell);
    if (release && !cell.broken) {
      await this.transport.releaseLease(cell.fencing).catch(() => undefined);
    }
  }

  /** Releases all leases so other nodes can take over immediately. */
  async shutdown(): Promise<void> {
    this.closed = true;
    if (this.alarmPoller !== null) this.timers.clearTimeout(this.alarmPoller);
    await Promise.all(Array.from(this.active.keys(), (id) => this.evict(id)));
  }

  /** Ids of cells currently active on this node. */
  activeCells(): string[] {
    return Array.from(this.active.keys());
  }

  /** Runs an alarm now if one is set (also invoked by timers / the poller).
   * Returns `true` if the alarm ran and succeeded. */
  async fireAlarm(cell: ActiveCell): Promise<boolean> {
    const due = cell.storage.alarmNs();
    if (due === null) return false;
    let succeeded = false;
    await cell.enqueue(async () => {
      // The alarm may have been changed while waiting in the queue.
      if (cell.storage.alarmNs() !== due) return;
      const handler = (cell.object as CellObject).alarm;
      const revision = cell.storage.alarmRevision();
      try {
        if (typeof handler === "function") {
          await handler.call(cell.object, {
            retryCount: cell.alarmRetries,
            isRetry: cell.alarmRetries > 0,
          });
        }
        // Clear it unless the handler scheduled (or cleared) the alarm itself.
        if (cell.storage.alarmRevision() === revision) await cell.storage.deleteAlarm();
        await cell.storage.sync();
        cell.alarmRetries = 0;
        succeeded = true;
      } catch (err) {
        await cell.storage.sync().catch(() => undefined);
        cell.alarmRetries++;
        if (cell.alarmRetries > this.maxAlarmRetries) {
          cell.alarmRetries = 0;
          await cell.storage.deleteAlarm();
          await cell.storage.sync();
          return;
        }
        throw err;
      }
    }).catch(() => {
      if (this.autoTimers && this.active.get(cell.id) === cell) {
        const backoff = Math.min(2 ** cell.alarmRetries * 1_000, 60_000);
        cell.alarmTimer = this.timers.setTimeout(() => void this.fireAlarm(cell), backoff);
      }
    });
    return succeeded;
  }

  // ---- internals ----

  /** @internal */
  sleep(ms: number): Promise<void> {
    if (!this.autoTimers || ms <= 0) return Promise.resolve();
    return new Promise((resolve) => {
      this.timers.setTimeout(resolve, ms);
    });
  }

  /** @internal */
  markBroken(cell: ActiveCell, err: Error): void {
    cell.broken = err;
    if (this.active.get(cell.id) === cell) this.drop(cell);
  }

  /** @internal */
  syncAlarmTimer(cell: ActiveCell): void {
    const at = cell.storage.alarmNs();
    const revision = cell.storage.alarmRevision();
    if (at === cell.scheduledAlarmNs && revision === cell.scheduledRevision) return;
    cell.scheduledRevision = revision;
    if (cell.alarmTimer !== null) this.timers.clearTimeout(cell.alarmTimer);
    cell.alarmTimer = null;
    cell.scheduledAlarmNs = at;
    if (at === null || !this.autoTimers) return;
    const delay = Math.max(0, Number(at / NS_PER_MS) - this.timers.now());
    cell.alarmTimer = this.timers.setTimeout(() => void this.fireAlarm(cell), delay);
  }

  private async forward(err: LeaseHeldError, request: ForwardRequest): Promise<unknown> {
    if (!this.forwarder || request.hops >= MAX_HOPS) {
      throw new CellUnavailableError(
        request.cellId,
        `cell '${request.cellId}' is owned by '${err.currentOwner}'` +
          (this.forwarder ? " (too many forwarding hops)" : " and no forwarder is configured"),
      );
    }
    return this.forwarder.forward(err.currentOwner, { ...request, hops: request.hops + 1 });
  }

  private ensureActive(id: string): Promise<ActiveCell> {
    const existing = this.active.get(id);
    if (existing && !existing.broken && this.timers.now() < existing.validUntilMs) {
      return Promise.resolve(existing);
    }
    const inflight = this.activating.get(id);
    if (inflight) return inflight;
    const p = (existing && !existing.broken ? this.revalidate(existing) : this.activate(id)).finally(
      () => this.activating.delete(id),
    );
    this.activating.set(id, p);
    return p;
  }

  /** The local lease deadline passed (e.g. the event loop stalled): renew,
   * keeping the instance only if no other node owned the cell meanwhile. */
  private async revalidate(cell: ActiveCell): Promise<ActiveCell> {
    try {
      await this.renew(cell);
    } catch {
      // renew() already dropped the instance where needed
    }
    if (this.active.get(cell.id) === cell && !cell.broken) return cell;
    return this.activate(cell.id);
  }

  private async activate(id: string): Promise<ActiveCell> {
    const started = this.timers.now();
    const act = await this.transport.activateCell(
      id,
      this.nodeId,
      BigInt(this.leaseTtlMs) * NS_PER_MS,
      BigInt(this.preloadBytes),
    );
    const fencing: FencingToken = {
      cellId: id,
      ownerId: act.lease.ownerId,
      epoch: act.lease.epoch,
    };
    const cell = new ActiveCell(
      this as CellNamespace<unknown>,
      id,
      fencing,
      started + this.leaseTtlMs * 0.8,
    );
    cell.storage = new CellStorage(cell, this.codec, act.entries, act.complete, act.alarmAtNs);
    const state: CellState = {
      id,
      storage: cell.storage,
      // Inside a running call the cell is already exclusive; queuing would deadlock.
      blockConcurrencyWhile: (fn) => (cell.running ? fn() : cell.enqueue(fn)),
    };
    cell.object = this.create(state, this.env) as object & CellObject;
    this.active.set(id, cell);
    this.scheduleRenewal(cell);
    this.touch(cell);
    this.syncAlarmTimer(cell);
    return cell;
  }

  private async renew(cell: ActiveCell): Promise<void> {
    const started = this.timers.now();
    try {
      const act = await this.transport.activateCell(
        cell.id,
        this.nodeId,
        BigInt(this.leaseTtlMs) * NS_PER_MS,
        0n,
      );
      if (act.lease.epoch !== cell.fencing.epoch) {
        // Someone else owned the cell in between: our cache is stale.
        this.markBroken(cell, new StaleEpochError(cell.id, cell.fencing.epoch, act.lease.epoch));
        return;
      }
      cell.validUntilMs = started + this.leaseTtlMs * 0.8;
    } catch (err) {
      if (err instanceof LeaseHeldError || err instanceof StaleEpochError) {
        this.markBroken(cell, err);
      }
      throw err;
    }
  }

  private scheduleRenewal(cell: ActiveCell): void {
    if (!this.autoTimers) return;
    cell.renewTimer = this.timers.setTimeout(() => {
      this.renew(cell)
        .catch(() => undefined)
        .finally(() => {
          if (this.active.get(cell.id) === cell) this.scheduleRenewal(cell);
        });
    }, Math.floor(this.leaseTtlMs / 3));
  }

  private touch(cell: ActiveCell): void {
    if (!this.autoTimers) return;
    if (cell.idleTimer !== null) this.timers.clearTimeout(cell.idleTimer);
    cell.idleTimer = this.timers.setTimeout(() => {
      // Keep cells with a pending alarm resident so the alarm fires locally.
      if (this.active.get(cell.id) === cell && cell.storage.alarmNs() === null) {
        void this.evict(cell.id);
      } else if (this.active.get(cell.id) === cell) {
        this.touch(cell);
      }
    }, this.idleTimeoutMs);
  }

  private drop(cell: ActiveCell): void {
    for (const t of [cell.renewTimer, cell.idleTimer, cell.alarmTimer]) {
      if (t !== null) this.timers.clearTimeout(t);
    }
    cell.renewTimer = cell.idleTimer = cell.alarmTimer = null;
    if (this.active.get(cell.id) === cell) this.active.delete(cell.id);
  }
}

// ---------------------------------------------------------------------------

/** Forwards calls between namespaces in one process (tests, single-host clusters). */
export class InProcessForwarder implements CellForwarder {
  private readonly nodes = new Map<string, CellNamespace<unknown>>();

  register(ns: CellNamespace<any>): void {
    this.nodes.set(ns.nodeId, ns as CellNamespace<unknown>);
  }

  unregister(nodeId: string): void {
    this.nodes.delete(nodeId);
  }

  async forward(ownerId: string, request: ForwardRequest): Promise<unknown> {
    const node = this.nodes.get(ownerId);
    if (!node) {
      throw new CellUnavailableError(request.cellId, `owner node '${ownerId}' is unreachable`);
    }
    return node.handleForwarded(request);
  }
}

/** Forwards calls over HTTP to `CellNamespace.handleHttp` on the owner node. */
export class HttpForwarder implements CellForwarder {
  private readonly resolveUrl: (ownerId: string) => string | undefined;
  private readonly fetchImpl: typeof fetch;

  constructor(resolveUrl: (ownerId: string) => string | undefined, fetchImpl: typeof fetch = fetch) {
    this.resolveUrl = resolveUrl;
    this.fetchImpl = fetchImpl;
  }

  async forward(ownerId: string, request: ForwardRequest): Promise<unknown> {
    const url = this.resolveUrl(ownerId);
    if (!url) throw new CellUnavailableError(request.cellId, `no address for owner '${ownerId}'`);
    let res: Response;
    try {
      res = await this.fetchImpl(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: encodeJson(request),
      });
    } catch (err) {
      throw new CellUnavailableError(request.cellId, `owner '${ownerId}' unreachable: ${err}`);
    }
    const body = decodeJson(await res.text()) as {
      ok?: unknown;
      error?: { name: string; code: string; message: string };
    };
    if (body.error) {
      const e = new IcCellError(body.error.code, body.error.message);
      e.name = body.error.name;
      throw e;
    }
    return body.ok;
  }
}
