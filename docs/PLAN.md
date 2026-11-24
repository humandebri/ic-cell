# ic-cell 実装計画 (Implementation Plan)

本ドキュメントは、`denoland/celld` と `humandebri/ic-sqlite-vfs` を組み合わせた **ic-cell** の段階的な開発計画、技術的課題、および各 Phase の完了条件（Definition of Done）を定義します。

---

## 1. 全体戦略

- **MVP 優先（Approach D → C → A）**: 最初から `celld` 本体を fork せず、独立した Canister + SDK (`IC_DB`) で動く最小構成を先に作る。
- **Source of Truth の明確化**: Phase 1〜4 では **ICP Canister を唯一の正本** とし、`celld` 側の SQLite は読み取りキャッシュまたは一時作業領域に限定する。
- **整合性モデルの明文化**: ICP の `query` はコンセンサスを経由しないため、所有権確定・最新状態が必要な読み取りは `update`（または certified query）を使い分ける。

---

## 2. Phase 別実装計画

### Phase 1: 状態管理 Canister (`crates/ic-cell-canister`)

**目的**: `ic-sqlite-vfs` を用いて、Stable Memory 上に永続化される SQLite 状態管理 Canister を構築する。

**主な実装内容**:
- Cargo workspace と `dfx.json` の初期化
- `storage.rs`:
  - `ic-sqlite-vfs` の組み込みと初期化・アップグレード（`pre_upgrade` / `post_upgrade`）対応
  - 汎用 SQL 実行インターフェース（`execute` / `query`）
  - オブジェクト・KV 向け `objects` テーブル（`key`, `value`, `generation`, `updated_at`）と条件付き UPDATE による CAS 実装
- `ownership.rs`:
  - Cell ごとの `owner_id`, `fencing_epoch`, `lease_expires_at` 管理
  - 単一 update トランザクション内での epoch 検証 + 書き込み
- Candid インターフェース（`.did`）の定義

**完了条件 (DoD)**:
- ローカルレプリカ（`dfx` または PocketIC）上で Canister のデプロイ・アップグレード後もデータが保持される
- 単体テストで CAS 成功（更新件数 1）/ CAS 失敗（更新件数 0）と fencing epoch 拒否が確認できる

---

### Phase 2: Rust Client & TypeScript SDK (`crates/ic-cell-client`, `packages/typescript-sdk`)

**目的**: `celld`（Workers / Durable Objects）から Canister を透過的に操作できる `IC_DB` バインディングを提供する。

**主な実装内容**:
- `ic-cell-client`:
  - `ic-agent` ベースの Rust クライアント（後続の LTX アダプタ・テストハーネスでも利用）
  - リトライと冪等性キー（idempotency key）のサポート
- `packages/typescript-sdk`:
  - `IC_DB.execute(sql, params)` / `IC_DB.query(sql, params, { consistency: "strong" | "eventual" })`
  - Durable Object の `env` バインディングとして組み込めるラッパー
  - 認証（Identity / Principal）とエラー型（`CasConflictError`, `StaleEpochError`, `UnknownOutcomeError`）の整理

**完了条件 (DoD)**:
- TypeScript からローカル Canister に対して CRUD・トランザクション・CAS 操作を実行できる
- `celld` ローカル環境上の Worker / Durable Object から `IC_DB` を呼び出せる

---

### Phase 3: サンプルアプリケーション (`examples/`)

**目的**: 開発者体験（DX）と実用性をエンドツーエンドで検証する。

**主な実装内容**:
- `examples/durable-counter`:
  - 最小の Durable Object + `IC_DB` サンプル
- `examples/immortal-agent`:
  - 1 Agent = 1 Cell 構成
  - 会話メモリ・タスク状態・実行履歴を ICP に永続化
  - 実行ノード（`celld` プロセス）を強制終了・別ノードで再起動しても、直前の確定状態からタスクを再開できるデモ

**完了条件 (DoD)**:
- README の手順通りにローカルで起動し、ノード停止 → 再起動後の状態復元を再現できる

---

### Phase 4: 整合性・障害復旧テスト (`tests/`)

**目的**: 分散環境におけるエッジケースを自動テストで担保する。

**主なテストスイート**:
- `tests/concurrent-cas`:
  - Node A / Node B からの同時更新で、古い `generation` / `epoch` が確実に拒否されること
- `tests/node-failover`:
  - Lease 期限切れ後の新オーナー昇格と、旧オーナーからの遅延書き込み（fencing）のブロック
- `tests/recovery`:
  - Canister アップグレード後、およびタイムアウト（結果不明状態）発生後の冪等リトライで重複適用が起きないこと
  - `query`（非合意）と `update`（合意済み）の読み取り一貫性の検証

**完了条件 (DoD)**:
- PocketIC ベースの統合テストが CI で安定してパスする

---

### Phase 5: LTX Replication Adapter

**目的**: `celld` の SQLite ページ差分（LTX）を ICP 上の `ic-sqlite-vfs` へ適用し、オンチェーンで直接 SQL 照会可能なミラーを実現する。

**技術的課題と設計ポイント**:
- **ページサイズ互換**: `ic-sqlite-vfs` は現在 16 KiB ページ前提。`celld` 側 SQLite の `PRAGMA page_size = 16384` 統一、または変換レイヤーの設計
- **WAL 非依存の適用**: `ic-sqlite-vfs` は WAL 無効のため、LTX のコミット済みページ群をトランザクション overlay 経由でアトミックに反映
- **チェックサム検証**: `PreApplyChecksum` / `PostApplyChecksum` と `MinTXID` / `MaxTXID` の連続性チェック
- **接続キャッシュ無効化**: ページ直接書き換え後に Canister 内の SQLite 接続キャッシュを安全にパージする仕組み
- **提案 API**:
  ```rust
  Db::apply_page_delta(
      epoch: u64,
      txid: u64,
      page_size: u32,
      changed_pages: Vec<(u32, Vec<u8>)>,
      pre_checksum: u64,
      post_checksum: u64,
  ) -> Result<(), Error>
  ```

**完了条件 (DoD)**:
- `celld` 側で実行した一連の SQLite トランザクションを LTX 経由で Canister に複製し、Canister 側で同じ SQL クエリ結果が得られる

---

### Phase 6: ICP Object Storage Backend (`StorageBackend::Ic`)

**目的**: `celld` のストレージ層（`crates/celld/bucket.rs`）から ICP を永続化バックエンドとして利用可能にする。

**主な実装内容**:
- `celld` の保証仕様（`docs/guarantees.md`）の充足:
  - Conditional create (`PutMode::Create`)
  - CAS update (`PutMode::Update`)
  - Read-after-write consistency
  - Range read（大きな LTX / スナップショットの分割読み出し）
  - Fencing とタイムアウト時の結果不明状態のハンドリング
- Rust `object_store::ObjectStore` トレイト互換アダプタの実装

**完了条件 (DoD)**:
- `celld` のストレージバックエンドを `Ic` に設定して Cell のライフサイクル（作成・更新・退避・復元）が一通り動作する

---

## 3. 主なリスクと対策

| リスク | 影響 | 対策 |
| --- | --- | --- |
| ICP `update` のレイテンシ（約 1〜2 秒） | 高頻度書き込み時の応答遅延 | MVP は重要状態のみ ICP へ永続化し、高頻度な一時状態は `celld` 側に留める。Phase 5 で非同期/バッチ LTX 複製を導入 |
| Canister メッセージサイズ上限（Ingress 2 MiB 等） | 大きな LTX や BLOB の転送失敗 | チャンク分割アップロードとマニフェスト確定プロトコルを設計 |
| `ic-sqlite-vfs` の 16 KiB ページ・独自メタデータ | LTX ページ差分の直接適用が不可 | Phase 5 着手前に `ic-sqlite-vfs` の内部レイアウトを調査し、必要なら PR / 拡張トレイトを設計 |
| `query` の非合意性 | 古い所有権・状態の誤読 | 所有権・CAS 前提の読み取りは必ず `update` 経由とし、SDK のデフォルトを安全側に倒す |
