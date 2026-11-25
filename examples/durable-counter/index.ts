import {
  CellNamespace,
  InMemoryCanisterTransport,
  InProcessForwarder,
  type CellState,
  type CellTransport,
} from "../../packages/typescript-sdk/src/index.ts";

/**
 * A Durable-Object-style counter. One instance per id runs cluster-wide,
 * calls are serialized, and every call's writes commit atomically to the
 * ic-cell canister before the call returns.
 */
export class DurableCounter {
  private readonly state: CellState;

  constructor(state: CellState) {
    this.state = state;
  }

  async increment(delta = 1): Promise<number> {
    const next = ((await this.state.storage.get<number>("value")) ?? 0) + delta;
    await this.state.storage.put("value", next);
    return next;
  }

  async get(): Promise<number> {
    return (await this.state.storage.get<number>("value")) ?? 0;
  }
}

/** Creates the namespace for one node. Use `CandidActorTransport` for a real canister. */
export function counters(transport: CellTransport, nodeId: string, forwarder?: InProcessForwarder) {
  const ns = new CellNamespace({
    transport,
    nodeId,
    forwarder,
    env: {},
    create: (state) => new DurableCounter(state),
  });
  forwarder?.register(ns);
  return ns;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const canister = new InMemoryCanisterTransport(BigInt(Date.now()) * 1_000_000n);
  const cluster = new InProcessForwarder();
  const nodeA = counters(canister, "node-a", cluster);
  const nodeB = counters(canister, "node-b", cluster);

  // 100 concurrent increments arriving at two different nodes.
  await Promise.all(
    Array.from({ length: 100 }, (_, i) => (i % 2 ? nodeA : nodeB).get<DurableCounter>("hits").increment()),
  );
  console.log("count =", await nodeB.get<DurableCounter>("hits").get(), "(expected 100)");
  console.log("hosted on:", nodeA.activeCells().length ? "node-a" : "node-b");
  await Promise.all([nodeA.shutdown(), nodeB.shutdown()]);
}
