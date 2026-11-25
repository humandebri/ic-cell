# ic-cell

> **Run anywhere. Persist on-chain.**

**ic-cell** は、[denoland/celld](https://github.com/denoland/celld) の高速なオフチェーン実行環境（Cloudflare Workers / Durable Objects 互換）と、[humandebri/ic-sqlite-vfs](https://github.com/humandebri/ic-sqlite-vfs) による Internet Computer (ICP) 上の永続化・合意済み状態を組み合わせた **ICP-backed Durable Objects** 基盤です。

実行環境は自由に選べる（Self-hosted）一方で、重要な状態は ICP のコンセンサスと Stable Memory 上に永続化されます。

---

## 位置づけ

| システム | 実行環境 | 状態の永続化 |
| --- | --- | --- |
| Cloudflare Durable Objects | Cloudflare | Cloudflare |
| celld | Self-hosted | Object Storage (S3 等) |
| ICP Canister | ICP | ICP |
| **ic-cell** | **Self-hosted** | **ICP** |

---

## なぜ celld × ic-sqlite-vfs なのか

両者はストレージの設計思想に強い共通点を持っています。

| 特性 | celld | ic-sqlite-vfs |
| --- | --- | --- |
| 状態管理 | Cell | Canister |
| データベース | SQLite | SQLite |
| 書き込みモデル | Single writer | 同期的な update 内での直列実行 |
| 永続化 | Object Storage + LTX | IC Stable Memory |
| 整合性の基盤 | CAS・Lease・Epoch | IC Consensus |
| データアクセス | ローカル SQLite | カスタム VFS |
| 障害復旧 | LTX による復元 | Canister の複製状態 |
| 実行環境 | V8 / Workers | Wasm / IC |

`celld` が外部ストレージと所有権管理によって実現している性質の一部を、ICP はプロトコルレベルで提供しています。一方で、`celld` は WAL と LTX を利用するのに対し、`ic-sqlite-vfs` は WAL を無効化しているため、VFS を単純に差し替えるのではなく、適切なレイヤーで接続します。

---

## 開発者体験（目標イメージ）

アプリケーション開発者は Canister の Stable Memory、Candid、VFS の内部構造を意識せず、Durable Objects 互換のスタイルで ICP 上に状態を永続化できます。

```ts
export class Agent extends DurableObject {
  async saveMemory(key: string, value: string) {
    await this.env.IC_DB.execute(
      "INSERT INTO memories (key, value) VALUES (?, ?)",
      [key, value],
    );
  }
}
```

- **celld**: LLM 呼び出し、外部 API、WebSocket、スケジューリング、キャッシュ、ルーティングを担当
- **ICP (Canister)**: 記憶、タスク状態、権限、実行履歴などの「正本（Source of Truth）」を担当

---

## 設計原則

1. **データの正本を単一化する**
   - 二重書き込みによる不整合を避けるため、MVP では **ICP を正本** とし、`celld` 側のローカル SQLite はキャッシュとして扱います。
2. **独立した SDK から始める**
   - 最初から `celld` を fork せず、`ic-cell` として独立した Canister / Client / TypeScript SDK を構築し、段階的に LTX 連携や `StorageBackend::Ic` へ拡張します。
3. **保証境界を明確にする**
   - ICP が保証するのは「確定済み状態の永続化・合意・権限制御」であり、オフチェーン実行（LLM 推論など）自体の計算正当性までは保証しません。

---

## 4つの統合アプローチと採用方針

`ic-cell` では、小さく実用化できる構成から始め、最終的にストレージ・レプリケーション層まで発展させます。

1. **ICP Native Storage Binding（MVP の起点）** — Workers / Durable Objects から `ic-sqlite-vfs` 搭載 Canister を直接操作する SDK (`IC_DB`) を提供
2. **LTX → ICP Replication（中核技術）** — `celld` の SQLite ページ差分（LTX）を ICP に転送し、Canister 内で直接 SQL 照会可能なミラーを維持
3. **ICP Object Storage Backend（互換拡張）** — `celld` の `StorageBackend` に `Ic` を追加し、S3 互換のオブジェクト・CAS・所有権管理を Canister で提供
4. **ICP Ownership Coordinator（必要に応じて検討）** — owner record と fencing epoch のみを Canister に分離

---

## リポジトリ構成

```text
ic-cell/
├── crates/
│   ├── ic-cell-canister/     # ic-sqlite-vfs を使った状態管理・所有権・CAS Canister
│   │   └── src/
│   │       ├── lib.rs          # Canister エントリポイント（認可・アップグレード）
│   │       ├── auth.rs         # Stable Memory に永続化される許可リスト
│   │       ├── cells.rs        # Durable Cells（Cell ごとの KV・アラーム・原子的 transact）
│   │       ├── storage.rs      # SQL / オブジェクト / チャンクアップロード
│   │       ├── ownership.rs    # Lease・Fencing Epoch
│   │       └── replication.rs  # LTX ページ差分適用・LTX チェックサム・ステージング
│   ├── ic-cell-client/       # Rust クライアント
│   │   └── src/
│   │       ├── ic_agent.rs             # ic-agent トランスポート（+ 許可リスト管理）
│   │       ├── ltx_codec.rs            # LTX ファイル（LTX1）のデコーダ / エンコーダ
│   │       ├── ltx.rs                  # LTX レプリケーションアダプタ（自動ステージング）
│   │       ├── object_store.rs         # 同期版オブジェクトストア
│   │       └── object_store_adapter.rs # object_store::ObjectStore 実装
│   └── ic-cell-e2e/          # PocketIC 上で実 wasm を使う E2E テスト
├── packages/
│   └── typescript-sdk/       # IC_DB バインディング + Durable Cells ランタイム（runtime.ts）
├── examples/
│   ├── durable-counter/      # 最小構成サンプル
│   └── immortal-agent/       # サーバーが消えても状態が残る AI Agent デモ
└── docs/
    ├── PLAN.md               # 詳細な実装計画・技術課題
    └── adr/                  # Architecture Decision Records
```

## Durable Cells（Durable Objects 互換）

1 つの ID につきクラスタ全体で 1 インスタンスだけが動き、呼び出しは直列に実行されます。各呼び出しの書き込みは、ic-cell Canister への 1 回の原子的コミットにまとまります（詳細は [ADR-0006](./docs/adr/0006-durable-cells.md)）。

```ts
import { Actor, HttpAgent } from "@dfinity/agent";
import { CandidActorTransport, CellNamespace, HttpForwarder, idlFactory, type CellState } from "@ic-cell/sdk";

class Counter {
  private readonly state: CellState;
  constructor(state: CellState) {
    this.state = state;
  }
  async increment() {
    const n = ((await this.state.storage.get<number>("n")) ?? 0) + 1;
    await this.state.storage.put("n", n); // 呼び出し終了時に fenced + atomic にコミット
    return n;
  }
  async alarm() { /* storage.setAlarm() で予約した処理 */ }
}

const actor = Actor.createActor(idlFactory, { agent: await HttpAgent.create({ host, identity }), canisterId });
const counters = new CellNamespace({
  transport: new CandidActorTransport(actor),
  nodeId: "node-a",                                   // Lease の owner として記録される
  forwarder: new HttpForwarder((owner) => nodeUrls[owner]), // 他ノードが owner なら転送
  create: (state) => new Counter(state),
  env: {},
});
await counters.get<Counter>("room-42").increment();
// 受信側: counters.handleHttp(request) を RPC エンドポイントとして公開
// 死んだ owner のアラーム回収: counters.startAlarmPoller()
```

| DO の性質 | Durable Cells |
| --- | --- |
| ID ごとに 1 インスタンス | ✅ Lease + Fencing、非 owner からは転送 |
| 直列実行 | ✅ 厳密な直列（インターリーブなし） |
| 書き込みの原子性 / 出力ゲート | ✅ 呼び出し単位で 1 回の `cell_transact` |
| `transaction()` / ロールバック | ✅ |
| Storage API（get / put / delete / list / deleteAll） | ✅（Cell ごとに分離された KV） |
| SQL | ✅ 同じトランザクション内で実行（テーブルは全 Cell 共有） |
| Alarm | ✅ 再試行付き。`pollAlarms` でフェイルオーバー |
| 低レイテンシの書き込み | ⚠️ 書き込みを含む呼び出しは 1 回の update call（1〜2 秒）を待つ |
| WebSocket / Hibernation | ✗ 対象外 |

## テスト

```bash
# ネイティブのユニット / 結合テスト（.did と実装の一致チェックを含む）
cargo test --workspace -- --test-threads=1

# PocketIC E2E（実 wasm のデプロイ・アップグレード・ic-agent 経由の LTX / object_store）
cargo build -p ic-cell-canister --target wasm32-unknown-unknown --release
cargo test -p ic-cell-e2e -- --test-threads=1   # TS ランタイムの実 Canister テストを含む

# TypeScript SDK（初回のみ: cd packages/typescript-sdk && bun install）
cd packages/typescript-sdk && bun x tsc -p . && node --experimental-strip-types --test test/*.test.ts
```

PocketIC サーバーは `POCKET_IC_BIN`、`dfx` 同梱のバイナリ（`~/.cache/dfinity/versions/*/pocket-ic`）の順に探し、見つからなければ自動ダウンロードされます。wasm が無い場合 E2E はスキップされます（CI では `IC_CELL_E2E_REQUIRED=1` で必須化）。

## 認可

コントローラは常に許可され、それ以外は許可リストに登録された principal のみが呼び出せます（詳細は [ADR-0005](./docs/adr/0005-caller-authorization.md)）。

```bash
dfx deploy ic_cell_canister --argument '(opt record { authorized_callers = opt vec { principal "<celld-node-principal>" } })'
dfx canister call ic_cell_canister add_authorized_caller '(principal "<principal>")'
```

---

## ロードマップ

詳細なタスク・技術設計・完了条件は [docs/PLAN.md](./docs/PLAN.md) を参照してください。

| Phase | 内容 | ゴール |
| --- | --- | --- |
| **Phase 1** | 状態管理 Canister (`ic-cell-canister`) | `ic-sqlite-vfs` 上で SQL 実行・CAS・Epoch 管理が動作する |
| **Phase 2** | Client & TypeScript SDK | `celld` / Workers から `IC_DB` 経由で読み書きできる |
| **Phase 3** | サンプル実装 (`durable-counter` / `immortal-agent`) | ノード再起動・切替後も Agent の記憶とタスクが復元される |
| **Phase 4** | Epoch・冪等性・障害復旧テスト | 競合 CAS、フェイルオーバー、タイムアウト後の整合性を自動検証 |
| **Phase 5** | LTX Replication Adapter | `celld` のページ差分を ICP 側 SQLite イメージへ安全に適用 |
| **Phase 6** | ICP Object Storage Backend | `celld` の `StorageBackend::Ic` として永続化バックエンド化 |

---

## 主なユースケース

- **Immortal AI Agent**: 1 Agent = 1 Cell。実行ノードが消失しても、別ノードから確定済みの記憶・タスク状態を復元して継続稼働
- **Verifiable SQLite / Time Machine**: オフチェーンで高速に動く DB の履歴・監査・復元をオンチェーンで担保
- **Durable Workflow Engine**: 長期間稼働する自動処理の状態管理
- **Personal Data Pod**: 実行基盤にロックインされない個人所有の永続データベース

---

## 関連リンク

- [実装計画 (docs/PLAN.md)](./docs/PLAN.md)
- [Architecture Decision Records (docs/adr/)](./docs/adr/README.md)
- [denoland/celld](https://github.com/denoland/celld)
- [humandebri/ic-sqlite-vfs](https://github.com/humandebri/ic-sqlite-vfs)
