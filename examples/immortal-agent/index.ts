import {
  IcDb,
  InMemoryCanisterTransport,
  type FencingToken,
} from "../../packages/typescript-sdk/src/index.ts";

/**
 * Immortal Agent — 1 Agent = 1 Cell.
 * Even if the execution node crashes, a new node acquires the next fencing
 * epoch and resumes from the committed on-chain state.
 */
export class ImmortalAgent {
  private readonly cellId: string;
  private readonly nodeId: string;
  private readonly env: { IC_DB: IcDb };
  private fencing?: FencingToken;

  constructor(cellId: string, nodeId: string, env: { IC_DB: IcDb }) {
    this.cellId = cellId;
    this.nodeId = nodeId;
    this.env = env;
  }

  async start(ttlNs = 5_000n): Promise<bigint> {
    const lease = await this.env.IC_DB.acquireLease(this.cellId, this.nodeId, ttlNs);
    this.fencing = { cellId: lease.cellId, ownerId: lease.ownerId, epoch: lease.epoch };
    return lease.epoch;
  }

  async saveMemory(key: string, value: string, idempotencyKey?: string): Promise<void> {
    await this.env.IC_DB.execute("INSERT INTO memories (key, value) VALUES (?, ?)", [key, value], {
      idempotencyKey,
      fencing: this.fencing,
    });
  }

  async recallMemory(key: string): Promise<string | null> {
    const rows = await this.env.IC_DB.query("SELECT key, value FROM memories WHERE key = ?", [key]);
    return rows.length > 0 ? String(rows[0][1]) : null;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const transport = new InMemoryCanisterTransport(1_000n);
  const env = { IC_DB: new IcDb(transport) };

  // Node A runs the agent and commits memory on-chain
  const agentOnNodeA = new ImmortalAgent("agent-001", "celld-node-a", env);
  const epoch1 = await agentOnNodeA.start(5_000n);
  await agentOnNodeA.saveMemory("mission", "Run anywhere. Persist on-chain.", "m-1");
  console.log(`[node-a epoch=${epoch1}] saved memory`);

  // Node A disappears; lease expires
  transport.advanceTime(5_000n);

  // Node B boots a fresh instance and recovers the committed memory
  const agentOnNodeB = new ImmortalAgent("agent-001", "celld-node-b", env);
  const epoch2 = await agentOnNodeB.start(5_000n);
  const recalled = await agentOnNodeB.recallMemory("mission");
  console.log(`[node-b epoch=${epoch2}] recovered memory: "${recalled}"`);
}
