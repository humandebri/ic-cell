import test from "node:test";
import assert from "node:assert/strict";
import {
  CandidActorTransport,
  IcDb,
  InMemoryCanisterTransport,
  StaleEpochError,
  type CanisterActor,
} from "../src/index.ts";

test("IcDb enforces fencing epoch and idempotency keys", async () => {
  const transport = new InMemoryCanisterTransport(100n);
  const db = new IcDb(transport);

  const leaseA = await db.acquireLease("cell-1", "node-a", 50n);
  assert.equal(leaseA.epoch, 1n);

  const r1 = await db.execute(
    "INSERT INTO memories (key, value) VALUES (?, ?)",
    ["goal", "persist on-chain"],
    {
      idempotencyKey: "req-1",
      fencing: { cellId: "cell-1", ownerId: "node-a", epoch: leaseA.epoch },
    },
  );
  assert.equal(r1.rowsAffected, 1n);
  assert.equal(r1.idempotentReplay, false);

  // Idempotent replay
  const r2 = await db.execute(
    "INSERT INTO memories (key, value) VALUES (?, ?)",
    ["goal", "persist on-chain"],
    {
      idempotencyKey: "req-1",
      fencing: { cellId: "cell-1", ownerId: "node-a", epoch: leaseA.epoch },
    },
  );
  assert.equal(r2.idempotentReplay, true);

  // Failover to Node B
  transport.advanceTime(50n);
  const leaseB = await db.acquireLease("cell-1", "node-b", 50n);
  assert.equal(leaseB.epoch, 2n);

  await assert.rejects(
    () =>
      db.execute("INSERT INTO memories (key, value) VALUES (?, ?)", ["goal", "stale"], {
        fencing: { cellId: "cell-1", ownerId: "node-a", epoch: leaseA.epoch },
      }),
    StaleEpochError,
  );
});

test("CandidActorTransport encodes SQL params, routes consistency, and maps Candid errors", async () => {
  const calls: string[] = [];
  const fakeActor: CanisterActor = {
    async acquire_lease(cellId, ownerId, ttlNs) {
      calls.push(`acquire:${cellId}:${ownerId}:${ttlNs}`);
      return {
        Ok: { cell_id: cellId, owner_id: ownerId, epoch: 3n, expires_at_ns: 999n },
      };
    },
    async release_lease() {
      return { Ok: null };
    },
    async sql_execute(_sql, args, idem, fencing) {
      calls.push(`exec:${args.length}:${idem[0] ?? ""}:${fencing[0]?.[2] ?? ""}`);
      if (fencing[0]?.[2] === 1n) {
        return {
          Err: {
            StaleEpoch: { cell_id: "c1", expected_epoch: 1n, actual_epoch: 3n },
          },
        };
      }
      return { Ok: { rows_affected: 1n, idempotent_replay: false } };
    },
    async sql_query_strong() {
      calls.push("query:strong");
      return { Ok: { rows: [[{ Text: "k" }, { Integer: 42n }, { Null: null }]] } };
    },
    async sql_query_eventual() {
      calls.push("query:eventual");
      return { Ok: { rows: [[{ Real: 3.5 }]] } };
    },
  };

  const db = new IcDb(new CandidActorTransport(fakeActor));
  const lease = await db.acquireLease("c1", "node-x", 100n);
  assert.equal(lease.epoch, 3n);

  const execRes = await db.execute("INSERT INTO t VALUES (?, ?)", ["a", 1], {
    idempotencyKey: "k1",
    fencing: { cellId: "c1", ownerId: "node-x", epoch: 3n },
  });
  assert.equal(execRes.rowsAffected, 1n);

  const strongRows = await db.query("SELECT * FROM t", []);
  assert.deepEqual(strongRows, [["k", 42n, null]]);

  const eventualRows = await db.query("SELECT * FROM t", [], { consistency: "eventual" });
  assert.deepEqual(eventualRows, [[3.5]]);

  await assert.rejects(
    () =>
      db.execute("INSERT INTO t VALUES (?)", ["stale"], {
        fencing: { cellId: "c1", ownerId: "node-x", epoch: 1n },
      }),
    StaleEpochError,
  );

  assert.deepEqual(calls, [
    "acquire:c1:node-x:100",
    "exec:2:k1:3",
    "query:strong",
    "query:eventual",
    "exec:1::1",
  ]);
});
