import test from "node:test";
import assert from "node:assert/strict";
import { IcDb, InMemoryCanisterTransport, StaleEpochError } from "../src/index.ts";

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
