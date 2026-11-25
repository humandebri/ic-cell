import test from "node:test";
import assert from "node:assert/strict";
import {
  CandidActorTransport,
  CellNamespace,
  CellUnavailableError,
  HttpForwarder,
  IcCellError,
  InMemoryCanisterTransport,
  InProcessForwarder,
  StaleEpochError,
  taggedJsonCodec,
  type CanisterActor,
  type CellOp,
  type CellState,
  type FencingToken,
  type Timers,
  type TransactResult,
} from "../src/index.ts";

const NS = 1_000_000n;

/** Deterministic timers sharing one clock with the in-memory canister. */
class FakeClock implements Timers {
  private nowMs: number;
  private seq = 0;
  private readonly tasks = new Map<number, { at: number; fn: () => void }>();
  private readonly canister?: InMemoryCanisterTransport;

  constructor(startMs: number, canister?: InMemoryCanisterTransport) {
    this.nowMs = startMs;
    this.canister = canister;
    canister?.setTime(BigInt(startMs) * NS);
  }
  now() {
    return this.nowMs;
  }
  setTimeout(fn: () => void, ms: number) {
    const id = ++this.seq;
    this.tasks.set(id, { at: this.nowMs + ms, fn });
    return id;
  }
  clearTimeout(h: unknown) {
    this.tasks.delete(h as number);
  }
  /** Advances time, running due timers in order and letting promises settle. */
  async advance(ms: number) {
    const target = this.nowMs + ms;
    for (;;) {
      const next = [...this.tasks.entries()]
        .filter(([, t]) => t.at <= target)
        .sort((a, b) => a[1].at - b[1].at || a[0] - b[0])[0];
      if (!next) break;
      this.tasks.delete(next[0]);
      this.set(next[1].at);
      next[1].fn();
      await settle();
    }
    this.set(target);
    await settle();
  }
  private set(ms: number) {
    this.nowMs = Math.max(this.nowMs, ms);
    this.canister?.setTime(BigInt(Math.floor(this.nowMs)) * NS);
  }
}

async function settle() {
  for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r));
}

class Counter {
  private readonly state: CellState;
  constructor(state: CellState) {
    this.state = state;
  }
  async increment(by = 1): Promise<number> {
    const n = ((await this.state.storage.get<number>("n")) ?? 0) + by;
    await this.state.storage.put("n", n);
    return n;
  }
  async get(): Promise<number> {
    return (await this.state.storage.get<number>("n")) ?? 0;
  }
  async putMany(count: number): Promise<void> {
    for (let i = 0; i < count; i++) await this.state.storage.put(`k:${i}`, i);
  }
  async failAfterWrite(): Promise<void> {
    await this.state.storage.put("written-before-throw", true);
    throw new Error("boom");
  }
  async atomicFail(): Promise<void> {
    await this.state.storage.transaction(async (tx) => {
      await tx.put("n", 1_000);
      throw new Error("rollback me");
    });
  }
}

function cluster(opts: { ttlMs?: number; idleMs?: number; preloadBytes?: number } = {}) {
  const canister = new InMemoryCanisterTransport();
  const clock = new FakeClock(1_700_000_000_000, canister);
  const forwarder = new InProcessForwarder();
  const node = (
    nodeId: string,
    timers: Timers = clock,
    create: (s: CellState) => object = (s) => new Counter(s),
  ) => {
    const ns = new CellNamespace({
      transport: canister,
      nodeId,
      create,
      env: {},
      timers,
      forwarder,
      leaseTtlMs: opts.ttlMs ?? 10_000,
      idleTimeoutMs: opts.idleMs ?? 600_000,
      preloadBytes: opts.preloadBytes,
    });
    forwarder.register(ns);
    return ns;
  };
  return { canister, clock, forwarder, node };
}

function value(canister: InMemoryCanisterTransport, cell: string, key: string) {
  const v = canister.peek(cell, key);
  return v ? taggedJsonCodec.decode(v) : undefined;
}

test("concurrent calls from two nodes never lose updates (single instance + serialization)", async () => {
  const { canister, node } = cluster();
  const a = node("node-a");
  const b = node("node-b");
  const calls: Promise<unknown>[] = [];
  for (let i = 0; i < 50; i++) {
    const ns = i % 2 === 0 ? a : b;
    calls.push(ns.get<Counter>("counter-1").increment());
  }
  const results = (await Promise.all(calls)) as number[];
  assert.deepEqual([...results].sort((x, y) => x - y), Array.from({ length: 50 }, (_, i) => i + 1));
  assert.equal(value(canister, "counter-1", "n"), 50);
  // Exactly one node hosts the cell; the other forwarded.
  assert.equal(a.activeCells().length + b.activeCells().length, 1);
});

test("writes in one call coalesce into one atomic commit; reads do not commit", async () => {
  const { canister, node } = cluster();
  const a = node("node-a");
  const stub = a.get<Counter>("c");
  await stub.putMany(10);
  assert.equal(canister.transactCalls, 1);
  await stub.get();
  assert.equal(canister.transactCalls, 1);
  // Output gate: the value is durable as soon as the call resolves.
  assert.equal(await stub.increment(5), 5);
  assert.equal(value(canister, "c", "n"), 5);
});

test("writes before an exception commit; transaction() rolls back on throw", async () => {
  const { canister, node } = cluster();
  const stub = node("node-a").get<Counter>("c");
  await assert.rejects(stub.failAfterWrite(), /boom/);
  assert.equal(value(canister, "c", "written-before-throw"), true);

  await stub.increment(1);
  await assert.rejects(stub.atomicFail(), /rollback me/);
  assert.equal(await stub.get(), 1, "local cache rolled back");
  assert.equal(value(canister, "c", "n"), 1, "nothing committed");
});

test("failover: a dead owner's cell moves after the TTL and the zombie is fenced", async () => {
  const { canister, clock, forwarder, node } = cluster({ ttlMs: 10_000 });
  // node-a has its own frozen clock, so it still believes its lease is valid.
  const frozen = new FakeClock(clock.now());
  const a = node("node-a", frozen);
  const b = node("node-b");
  await a.get<Counter>("room").increment(7);

  // node-a becomes unreachable (crash / partition) without releasing.
  forwarder.unregister("node-a");
  await assert.rejects(b.get<Counter>("room").increment(), CellUnavailableError);

  await clock.advance(10_001);
  assert.equal(await b.get<Counter>("room").increment(), 8, "state recovered on node-b");
  assert.deepEqual(b.activeCells(), ["room"]);

  // The zombie still has an instance and tries to write: fenced out.
  await assert.rejects(a.get<Counter>("room").increment(100), StaleEpochError);
  assert.deepEqual(a.activeCells(), [], "zombie instance discarded");
  assert.equal(value(canister, "room", "n"), 8);

  // Once reachable again, node-a's calls are forwarded to the new owner.
  forwarder.register(a);
  assert.equal(await a.get<Counter>("room").increment(), 9);
  assert.deepEqual(a.activeCells(), []);
});

test("lease renewal keeps the cell; an idle cell releases its lease for instant failover", async () => {
  const { clock, node } = cluster({ ttlMs: 9_000, idleMs: 30_000 });
  const a = node("node-a");
  const b = node("node-b");
  await a.get<Counter>("c").increment();
  await clock.advance(25_000); // well past the TTL, renewed by timers
  assert.equal(await b.get<Counter>("c").increment(), 2, "still forwarded to node-a");
  assert.deepEqual(a.activeCells(), ["c"]);

  await clock.advance(30_001); // idle -> evicted + lease released
  assert.deepEqual(a.activeCells(), []);
  assert.equal(await b.get<Counter>("c").increment(), 3);
  assert.deepEqual(b.activeCells(), ["c"], "taken over without waiting for TTL");
});

test("unknown-outcome commits are retried idempotently", async () => {
  const canister = new InMemoryCanisterTransport();
  let failNext = 1;
  const flaky = Object.create(canister) as InMemoryCanisterTransport;
  flaky.transact = async (f: FencingToken, ops: CellOp[], key?: string): Promise<TransactResult> => {
    const result = await canister.transact(f, ops, key);
    if (failNext-- > 0) throw new IcCellError("TRANSPORT", "reply lost");
    return result;
  };
  const ns = new CellNamespace({
    transport: flaky,
    nodeId: "n",
    create: (s) => new Counter(s),
    env: {},
    autoTimers: false,
  });
  assert.equal(await ns.get<Counter>("c").increment(), 1);
  assert.equal(await ns.get<Counter>("c").increment(), 2);
  assert.equal(value(canister, "c", "n"), 2, "the lost reply was not applied twice");
});

class Reminder {
  private readonly state: CellState;
  constructor(state: CellState) {
    this.state = state;
  }
  async schedule(atMs: number): Promise<void> {
    await this.state.storage.setAlarm(atMs);
  }
  async fired(): Promise<number[]> {
    return (await this.state.storage.get<number[]>("fired")) ?? [];
  }
  async alarm(info: { retryCount: number }): Promise<void> {
    const failures = (await this.state.storage.get<number>("failures")) ?? 0;
    if (failures > 0) {
      await this.state.storage.put("failures", failures - 1);
      throw new Error("transient alarm failure");
    }
    const fired = (await this.state.storage.get<number[]>("fired")) ?? [];
    await this.state.storage.put("fired", [...fired, info.retryCount]);
  }
  async failNext(n: number): Promise<void> {
    await this.state.storage.put("failures", n);
  }
}

test("alarms fire, retry with backoff, and are cleared in the canister", async () => {
  const { canister, clock, node } = cluster();
  const ns = node("node-a", clock, (s) => new Reminder(s));
  const r = ns.get<Reminder>("r1");
  await r.failNext(2);
  await r.schedule(clock.now() + 1_000);
  assert.notEqual(await canister.getAlarm("r1"), null);

  await clock.advance(1_000); // fails (retry 0)
  await clock.advance(2_000); // fails (retry 1)
  await clock.advance(4_000); // succeeds (retry 2)
  assert.deepEqual(await r.fired(), [2]);
  assert.equal(await canister.getAlarm("r1"), null);
});

test("an alarm handler may re-arm the alarm for the very same time", async () => {
  class Loop {
    private readonly state: CellState;
    private readonly at: () => number;
    constructor(state: CellState, at: () => number) {
      this.state = state;
      this.at = at;
    }
    async start() {
      await this.state.storage.setAlarm(this.at());
    }
    async count() {
      return (await this.state.storage.get<number>("ticks")) ?? 0;
    }
    async alarm() {
      const ticks = ((await this.state.storage.get<number>("ticks")) ?? 0) + 1;
      await this.state.storage.put("ticks", ticks);
      if (ticks < 3) await this.state.storage.setAlarm(this.at()); // same ms as the one firing
    }
  }
  const { canister, clock, node } = cluster();
  const fixed = clock.now() + 500;
  const ns = node("n", clock, (s) => new Loop(s, () => fixed));
  await ns.get<Loop>("loop").start();
  await clock.advance(500);
  assert.equal(await ns.get<Loop>("loop").count(), 3);
  assert.equal(await canister.getAlarm("loop"), null);
});

test("pollAlarms lets another node fire alarms of a dead owner", async () => {
  const { canister, clock, forwarder, node } = cluster({ ttlMs: 5_000 });
  const frozen = new FakeClock(clock.now());
  const a = node("node-a", frozen, (s) => new Reminder(s));
  await a.get<Reminder>("r").schedule(clock.now() + 60_000);
  forwarder.unregister("node-a"); // dies before the alarm

  const b = node("node-b", clock, (s) => new Reminder(s));
  await clock.advance(60_000);
  assert.equal(await b.pollAlarms(), 1);
  assert.deepEqual(await b.get<Reminder>("r").fired(), [0]);
  assert.equal(await canister.getAlarm("r"), null);
});

test("blockConcurrencyWhile initializes before calls; storage API semantics", async () => {
  class Store {
    private readonly state: CellState;
    ready = false;
    constructor(state: CellState) {
      this.state = state;
      void state.blockConcurrencyWhile(async () => {
        await state.storage.get("anything");
        this.ready = true;
      });
    }
    async isReady() {
      return this.ready;
    }
    async run() {
      const s = this.state.storage;
      await s.put({ "user:1": { name: "a", born: new Date(0) }, "user:2": 2n, "user:3": new Uint8Array([1, 2]), other: 1 });
      const listed = await s.list({ prefix: "user:", reverse: true, limit: 2 });
      const deleted = await s.delete(["user:1", "missing"]);
      const single = await s.delete("other");
      const many = await s.get(["user:2", "user:3", "missing"]);
      return { keys: [...listed.keys()], deleted, single, many };
    }
    async wipe() {
      await this.state.storage.deleteAll();
      return (await this.state.storage.list()).size;
    }
  }
  const { node } = cluster();
  const stub = node("n", undefined, (s) => new Store(s)).get<Store>("s");
  assert.equal(await stub.isReady(), true);
  const r = await stub.run();
  assert.deepEqual(r.keys, ["user:3", "user:2"]);
  assert.equal(r.deleted, 1);
  assert.equal(r.single, true);
  assert.deepEqual([...r.many.entries()], [["user:2", 2n], ["user:3", new Uint8Array([1, 2])]]);
  assert.equal(await stub.wipe(), 0);
});

test("partial preload falls back to canister reads and full listing", async () => {
  const { canister, node } = cluster({ preloadBytes: 1 });
  const a = node("node-a");
  await a.get<Counter>("big").putMany(30);
  await a.shutdown(); // releases the lease

  const b = node("node-b");
  class Reader {
    private readonly s: CellState;
    constructor(s: CellState) {
      this.s = s;
    }
    async read() {
      return [await this.s.storage.get("k:29"), (await this.s.storage.list({ prefix: "k:" })).size];
    }
  }
  const c = node("node-c", undefined, (s) => new Reader(s));
  void b;
  assert.deepEqual(await c.get<Reader>("big").read(), [29, 30]);
  assert.ok(canister.transactCalls >= 1);
});

test("HttpForwarder round-trips calls and errors through handleHttp", async () => {
  const canister = new InMemoryCanisterTransport();
  const owner = new CellNamespace({
    transport: canister,
    nodeId: "owner",
    create: (s) => new Counter(s),
    env: {},
    autoTimers: false,
  });
  const fakeFetch = (async (_url: string, init: RequestInit) =>
    owner.handleHttp(new Request("http://owner/rpc", init))) as typeof fetch;
  const edge = new CellNamespace({
    transport: canister,
    nodeId: "edge",
    create: (s) => new Counter(s),
    env: {},
    autoTimers: false,
    forwarder: new HttpForwarder((id) => (id === "owner" ? "http://owner/rpc" : undefined), fakeFetch),
  });
  await owner.get<Counter>("c").increment(41);
  assert.equal(await edge.get<Counter>("c").increment(), 42);
  await assert.rejects(edge.get<Counter>("c").failAfterWrite(), /boom/);
  await assert.rejects(
    (edge.get<Record<string, () => Promise<unknown>>>("c") as Record<string, () => Promise<unknown>>).nope(),
    (e: IcCellError) => e.code === "NO_SUCH_METHOD",
  );
  for (const m of ["toString", "hasOwnProperty", "constructor", "alarm"]) {
    await assert.rejects(
      edge.call("c", m, []),
      (e: IcCellError) => e.code === "NO_SUCH_METHOD",
      m,
    );
  }
});

test("tagged JSON codec round-trips rich values", () => {
  const v = {
    big: 2n ** 70n,
    bytes: new Uint8Array([0, 255]),
    when: new Date("2026-01-01T00:00:00Z"),
    map: new Map<unknown, unknown>([[1n, "x"]]),
    set: new Set(["a"]),
    nested: [undefined, Infinity, NaN, { "__ic$": "bigint", v: "spoof" }],
  };
  assert.deepEqual(taggedJsonCodec.decode(taggedJsonCodec.encode(v)), v);
});

test("CandidActorTransport encodes cell ops and decodes blobs", async () => {
  let sent: unknown;
  const actor = {
    cell_transact: async (req: unknown) => {
      sent = req;
      return { Ok: { results: [{ rows_affected: 1n, rows: [[{ Blob: [1, 2] }]] }], idempotent_replay: false, epoch: 3n } };
    },
    activate_cell: async () => ({
      Ok: {
        lease: { cell_id: "c", owner_id: "n", epoch: 3n, expires_at_ns: 9n },
        entries: [{ key: "k", value: [7] }],
        complete: true,
        alarm_at_ns: [5n],
      },
    }),
    cell_kv_get: async () => ({ Ok: [[[1]], []] }),
    activate_cell_err: undefined,
  } as unknown as CanisterActor;
  const t = new CandidActorTransport(actor);
  const res = await t.transact(
    { cellId: "c", ownerId: "n", epoch: 3n },
    [
      { kind: "put", key: "k", value: new Uint8Array([1]) },
      { kind: "setAlarm", atNs: null },
      { kind: "assert", key: "k", expected: null },
      { kind: "sql", sql: "SELECT 1", args: [1, "x"] },
      { kind: "deleteAll" },
    ],
    "idem",
  );
  assert.deepEqual(sent, {
    fencing: ["c", "n", 3n],
    idempotency_key: ["idem"],
    ops: [
      { KvPut: { key: "k", value: new Uint8Array([1]) } },
      { SetAlarm: [] },
      { AssertKv: { key: "k", expected: [] } },
      { Sql: { sql: "SELECT 1", args: [{ Integer: 1n }, { Text: "x" }] } },
      { KvDeleteAll: null },
    ],
  });
  assert.deepEqual(res.results[0].rows, [[new Uint8Array([1, 2])]]);
  const act = await t.activateCell("c", "n", 1n, 1n);
  assert.deepEqual(act.entries, [{ key: "k", value: new Uint8Array([7]) }]);
  assert.equal(act.alarmAtNs, 5n);
  assert.deepEqual(await t.kvGet("c", ["a", "b"]), [new Uint8Array([1]), null]);

  const failing = new CandidActorTransport({
    activate_cell: async () => ({
      Err: { LeaseHeldByAnother: { cell_id: "c", current_owner: "x", expires_at_ns: 1n } },
    }),
  } as unknown as CanisterActor);
  await assert.rejects(failing.activateCell("c", "n", 1n, 0n), (e: IcCellError) => e.code === "LEASE_HELD");
});
