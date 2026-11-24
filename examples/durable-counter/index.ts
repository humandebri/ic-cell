import { IcDb, InMemoryCanisterTransport } from "../../packages/typescript-sdk/src/index.ts";

export class DurableCounter {
  private readonly env: { IC_DB: IcDb };

  constructor(env: { IC_DB: IcDb }) {
    this.env = env;
  }

  async increment(delta = 1): Promise<number> {
    const rows = await this.env.IC_DB.query("SELECT value FROM counters WHERE key = ?", ["counter"]);
    const current = rows.length > 0 ? Number(rows[0][1]) : 0;
    const next = current + delta;
    await this.env.IC_DB.execute("INSERT INTO counters (key, value) VALUES (?, ?)", [
      "counter",
      next,
    ]);
    return next;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const db = new IcDb(new InMemoryCanisterTransport());
  const counter = new DurableCounter({ IC_DB: db });
  console.log("count =", await counter.increment(1));
  console.log("count =", await counter.increment(5));
}
