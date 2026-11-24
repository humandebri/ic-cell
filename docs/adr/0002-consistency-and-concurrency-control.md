# ADR-0002: ICP 上の CAS・Fencing Epoch および Query/Update の整合性モデル

- **Status**: Proposed
- **Date**: 2026-11-24

## Context

`celld` は Single-writer モデルを前提とし、複数ノードからの競合書き込みやネットワーク分断時の split-brain を防ぐために CAS、Lease、Fencing Epoch を用いている。
ICP の Canister は単一スレッドの同期的な `update` 実行とコンセンサスを提供するが、以下の点に注意が必要である。

1. **Query はコンセンサスを経由しない**: 単一レプリカから応答するため、古い状態が返る可能性があり、所有権の確定や強整合な読み取りには使えない。
2. **タイムアウト時の結果不明状態（Unknown Outcome）**: クライアント側で `update` がタイムアウトしても、ICP 側ではコミットされている可能性がある。単純なリトライは二重適用を引き起こす。

## Decision

1. **単一 `update` トランザクション内での CAS と Fencing 検証**
   - オブジェクト更新は SQLite の条件付き `UPDATE` を単一の同期 `update` 内で実行する。
     ```sql
     UPDATE objects
     SET value = ?1, generation = generation + 1, updated_at = ?4
     WHERE key = ?2 AND generation = ?3;
     ```
   - 更新件数が `1` なら成功、`0` なら `CasConflict` とする。
   - 所有権が必要な操作では、同一トランザクション内で `owner_id` と `fencing_epoch` を検証し、古い epoch からの書き込みを `StaleEpoch` として拒否する。
2. **Query と Update の明示的な使い分け**
   - 所有権の取得・確認、CAS 前提の読み取り、および `consistency: "strong"` 指定時は必ず `update`（または将来的に certified query）を使用する。
   - 通常の `query` は `consistency: "eventual"` として明示的にオプトインする設計とし、SDK のデフォルトは安全側（強整合）に倒す。
3. **冪等性キー（Idempotency Key）の導入**
   - 書き込み系 API はオプションで `idempotency_key` を受け取り、Canister 内で直近の適用済みキーと結果を記録する。
   - タイムアウト（`UnknownOutcomeError`）発生時にクライアントが同一キーでリトライしても、重複実行されず前回の結果を安全に取得できる。

## Consequences

### Positive
- Split-brain や遅延パケットによる旧オーナーの書き込みをプロトコルレベルで防止できる。
- ネットワーク障害時のリトライ安全性が担保される。

### Negative
- 強整合読み取りに `update` を使うため、読み取りにもコンセンサスレイテンシとサイクルコストが発生する。
- 冪等性キーの保存と定期的な GC（有効期限切れレコードの削除）が必要になる。
