# ADR-0004: `celld` 向け ICP Object Storage Backend (`StorageBackend::Ic`) の設計

- **Status**: Proposed
- **Date**: 2026-11-24

## Context

`celld` のストレージ層（`crates/celld/bucket.rs`）は Rust の `object_store` クレートを利用し、`S3` / `Gcs` / `Azure` / `Local` をサポートしている。
ここに `Ic` を追加できれば、既存の `celld` エコシステムと高い互換性を保ったまま、ICP を永続化バックエンドとするセルフホスト型 Workers プラットフォームを実現できる。

ただし、`celld` の保証仕様（`docs/guarantees.md`）を満たすためには、単なる KV 保存ではなく以下の要件をクリアする必要がある。

- Conditional create（存在しない場合のみ作成）
- Conditional update（世代番号による CAS）
- Read-after-write consistency
- Range read（部分読み出し）
- Fencing とタイムアウト時の結果不明状態のハンドリング

## Decision

1. **`object_store::ObjectStore` 互換アダプタを `ic-cell-client` に実装する**
   - `celld` 側への変更を `StorageBackend::Ic` の追加とファクトリ分岐のみに最小化する。
2. **オブジェクトのチャンク分割と Range Read 対応**
   - ICP の 1 メッセージあたりのサイズ上限（Ingress 約 2 MiB / 応答約 2〜3 MiB）を超えるオブジェクト（LTX スナップショット等）を扱うため、Canister 内部でオブジェクトを固定サイズチャンクに分割管理し、offset + length による Range Read を提供する。
   - 大きなオブジェクトの書き込みは「チャンクアップロード → マニフェストの CAS コミット」の 2 段階でアトミックに確定させる。
3. **Read-after-write consistency の保証**
   - `get` / `head` / `list` のうち、`celld` の整合性要件に関わる呼び出しは `update`（または certified data 検証付き query）を経由させる。

## Consequences

### Positive
- 既存の `celld` の仕組み（LTX・Lease・フェイルオーバー）をそのまま ICP バックエンド上で動かせる。
- S3 等の特定クラウド事業者に依存しないセルフホスト構成が実現できる。

### Negative
- S3 と比較して 1 リクエストあたりのレイテンシとストレージ単価（Stable Memory / Cycles）が高いため、ホットキャッシュやコンパクション戦略のチューニングが必要になる。
- チャンク分割アップロードの途中失敗による孤立チャンクの GC（ガベージコレクション）機構が必要になる。

## Implementation Notes (2026-11-25)

- `ic_cell_client::object_store_adapter::IcObjectStoreAdapter` が `object_store` 0.14 の `ObjectStore` トレイトを実装する（feature `object-store`、デフォルト有効）。
- ETag / version は Canister の generation。`PutMode::Create` / `Update` は条件付き作成 / CAS、`Overwrite` は現在 generation に対する CAS ループ。
- 読み取りは strong（update call）で、1 MiB 単位の Range Read に分割し、読み取り中に generation が変わった場合は再試行する（異なる世代のバイトが混ざらない）。
- マルチパートは各パートを Canister チャンクとして送り、`complete` で CAS コミットする。CAS 失敗時もチャンクは保持されるため再コミットできる。
- IC の応答上限（2 MiB）対策として、`commit_chunked_object` は値を含まない `ObjectMeta` を返し、`get_object_*` は 2,000,000 byte を超えるオブジェクトに対してトラップではなく `InvalidArgument` を返す（`get_object_range_strong` を使う）。
- 属性（attributes）とタグは保存しない。
