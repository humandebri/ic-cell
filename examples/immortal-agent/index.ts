import {
  CellNamespace,
  InMemoryCanisterTransport,
  InProcessForwarder,
  type CellState,
  type CellTransport,
} from "../../packages/typescript-sdk/src/index.ts";

interface Task {
  id: string;
  goal: string;
  steps: string[];
  done: boolean;
}

/**
 * Immortal Agent — 1 Agent = 1 Durable Cell.
 *
 * The agent's memory and task progress live in the ic-cell canister. Work is
 * driven by an alarm, so if the node running the agent dies, another node
 * picks the alarm up (`pollAlarms`) and continues from the last committed step.
 */
export class ImmortalAgent {
  private readonly state: CellState;

  constructor(state: CellState) {
    this.state = state;
  }

  async remember(key: string, value: string): Promise<void> {
    await this.state.storage.put(`memory:${key}`, value);
  }

  async recall(key: string): Promise<string | undefined> {
    return this.state.storage.get<string>(`memory:${key}`);
  }

  async assign(goal: string, steps: number): Promise<string> {
    const id = `task-${Date.now()}`;
    const task: Task = { id, goal, steps: Array.from({ length: steps }, (_, i) => `step ${i + 1}`), done: false };
    await this.state.storage.put(`task:${id}`, task);
    await this.state.storage.put("progress", { task: id, next: 0 });
    await this.state.storage.setAlarm(Date.now());
    return id;
  }

  async status(): Promise<{ next: number; total: number; done: boolean } | null> {
    const progress = await this.state.storage.get<{ task: string; next: number }>("progress");
    if (!progress) return null;
    const task = (await this.state.storage.get<Task>(`task:${progress.task}`))!;
    return { next: progress.next, total: task.steps.length, done: task.done };
  }

  /** Executes one step per alarm; each step commits atomically with its progress. */
  async alarm(): Promise<void> {
    const progress = await this.state.storage.get<{ task: string; next: number }>("progress");
    if (!progress) return;
    const task = (await this.state.storage.get<Task>(`task:${progress.task}`))!;
    if (progress.next >= task.steps.length) {
      await this.state.storage.put(`task:${task.id}`, { ...task, done: true });
      return;
    }
    await this.state.storage.put(`log:${task.id}:${progress.next}`, `did ${task.steps[progress.next]}`);
    await this.state.storage.put("progress", { ...progress, next: progress.next + 1 });
    await this.state.storage.setAlarm(Date.now());
  }
}

export function agents(transport: CellTransport, nodeId: string, forwarder?: InProcessForwarder) {
  const ns = new CellNamespace({
    transport,
    nodeId,
    forwarder,
    env: {},
    leaseTtlMs: 2_000,
    // Timers drive alarms in production; this demo steps them by hand.
    autoTimers: false,
    create: (state) => new ImmortalAgent(state),
  });
  forwarder?.register(ns);
  return ns;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const canister = new InMemoryCanisterTransport();
  // The demo canister follows wall-clock time plus a skew we use to "wait".
  let skewMs = 0;
  const syncClock = () => canister.setTime(BigInt(Date.now() + skewMs) * 1_000_000n);
  const step = async (ns: { pollAlarms(): Promise<number> }) => {
    syncClock();
    return ns.pollAlarms();
  };

  const cluster = new InProcessForwarder();
  const nodeA = agents(canister, "celld-node-a", cluster);
  syncClock();
  const agent = nodeA.get<ImmortalAgent>("agent-001");
  await agent.remember("mission", "Run anywhere. Persist on-chain.");
  await agent.assign("write a report", 5);
  await step(nodeA);
  await step(nodeA);
  console.log("[node-a] progress:", await agent.status());

  // node-a crashes without releasing its lease; time passes beyond the TTL.
  cluster.unregister("celld-node-a");
  skewMs += 3_000;

  const nodeB = agents(canister, "celld-node-b", cluster);
  while ((await step(nodeB)) > 0) {
    // keep stepping until no alarm is due
  }
  const resumed = nodeB.get<ImmortalAgent>("agent-001");
  console.log("[node-b] recalled:", await resumed.recall("mission"));
  console.log("[node-b] progress:", await resumed.status());
  await nodeB.shutdown();
}
