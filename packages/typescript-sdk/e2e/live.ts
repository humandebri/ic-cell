/**
 * Drives the Durable Cells runtime against a *real* ic-cell canister through
 * `@dfinity/agent`. Launched by `crates/ic-cell-e2e` (PocketIC live mode):
 *
 *   IC_URL=http://127.0.0.1:PORT CANISTER_ID=... node --experimental-strip-types e2e/live.ts
 */
import assert from "node:assert/strict";
import { Actor, HttpAgent } from "@dfinity/agent";
import { Ed25519KeyIdentity } from "@dfinity/identity";
import {
  CandidActorTransport,
  CellNamespace,
  InProcessForwarder,
  StaleEpochError,
  idlFactory,
  taggedJsonCodec,
  type CanisterActor,
  type CellState,
} from "../src/index.ts";

const url = process.env.IC_URL!;
const canisterId = process.env.CANISTER_ID!;
// Same seed as the Rust test, so the canister allowlist contains this principal.
const identity = Ed25519KeyIdentity.generate(new Uint8Array(32).fill(7));
const agent = await HttpAgent.create({ host: url, identity, shouldFetchRootKey: true });
const actor = Actor.createActor(idlFactory, { agent, canisterId }) as unknown as CanisterActor;
const transport = new CandidActorTransport(actor);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

class Room {
  private readonly state: CellState;
  constructor(state: CellState) {
    this.state = state;
  }
  async post(text: string): Promise<number> {
    const n = ((await this.state.storage.get<number>("n")) ?? 0) + 1;
    await this.state.storage.put("n", n);
    await this.state.storage.put(`msg:${String(n).padStart(4, "0")}`, { text, at: new Date() });
    return n;
  }
  async messages(): Promise<string[]> {
    const list = await this.state.storage.list<{ text: string }>({ prefix: "msg:" });
    return [...list.values()].map((m) => m.text);
  }
  async bumpSql(): Promise<bigint> {
    await this.state.storage.put("sql-touched", true);
    const rows = await this.state.storage.sql(
      "INSERT INTO room_stats (room, posts) VALUES (?, 1) ON CONFLICT(room) DO UPDATE SET posts = posts + 1 RETURNING posts",
      [this.state.id],
    );
    return rows[0][0] as bigint;
  }
  async remindIn(ms: number): Promise<void> {
    await this.state.storage.setAlarm(Date.now() + ms);
  }
  async alarm(): Promise<void> {
    await this.state.storage.put("reminded", true);
  }
  async reminded(): Promise<boolean> {
    return (await this.state.storage.get<boolean>("reminded")) ?? false;
  }
}

const cluster = new InProcessForwarder();
const makeNode = (nodeId: string) => {
  const ns = new CellNamespace({
    transport,
    nodeId,
    forwarder: cluster,
    env: {},
    leaseTtlMs: 3_000,
    // Timers off: a crashed node must not keep renewing; calls revalidate the
    // lease on demand instead. The alarm is driven by pollAlarms below.
    autoTimers: false,
    create: (s) => new Room(s),
  });
  cluster.register(ns);
  return ns;
};
const a = makeNode("node-a");
const b = makeNode("node-b");

// 1. Concurrent calls through two nodes: one instance, no lost updates.
const posts = await Promise.all(
  Array.from({ length: 12 }, (_, i) => (i % 2 ? a : b).get<Room>("room-1").post(`hello ${i}`)),
);
assert.deepEqual([...posts].sort((x, y) => x - y), Array.from({ length: 12 }, (_, i) => i + 1));
const committed = await transport.kvGet("room-1", ["n"]);
assert.equal(taggedJsonCodec.decode(committed[0]!), 12);
assert.equal((await b.get<Room>("room-1").messages()).length, 12);
const owner = a.activeCells().length ? a : b;
const other = owner === a ? b : a;

// 2. SQL in the same atomic commit as KV writes.
await transport.transact(
  (await transport.activateCell("setup", "setup", 1_000_000_000n, 0n)).lease,
  [{ kind: "sql", sql: "CREATE TABLE IF NOT EXISTS room_stats (room TEXT PRIMARY KEY, posts INTEGER NOT NULL)", args: [] }],
);
assert.equal(await other.get<Room>("room-1").bumpSql(), 1n);
assert.equal(await owner.get<Room>("room-1").bumpSql(), 2n);

// 3. Alarm on the real canister.
await owner.get<Room>("room-1").remindIn(500);
await sleep(1_500);
assert.equal(await owner.pollAlarms(), 1);
assert.equal(await owner.get<Room>("room-1").reminded(), true);
assert.equal(await transport.getAlarm("room-1"), null);

// 4. Failover: the owner dies without releasing; the survivor takes over after the TTL.
cluster.unregister(owner.nodeId);
await sleep(3_500);
assert.equal(await other.get<Room>("room-1").post("after failover"), 13);
assert.deepEqual(other.activeCells(), ["room-1"]);
// A write with the dead owner's epoch-1 token is fenced by the canister.
await assert.rejects(
  transport.transact({ cellId: "room-1", ownerId: owner.nodeId, epoch: 1n }, [
    { kind: "put", key: "n", value: taggedJsonCodec.encode(999) },
  ]),
  StaleEpochError,
);
// The zombie's own runtime refuses to use its stale instance and forwards instead.
cluster.register(owner);
assert.equal(await owner.get<Room>("room-1").post("from revived node"), 14);
assert.deepEqual(owner.activeCells(), []);
const final = await transport.kvGet("room-1", ["n"]);
assert.equal(taggedJsonCodec.decode(final[0]!), 14);
console.log(JSON.stringify({ ok: true, posts: 14 }));
await other.shutdown();
process.exit(0);
