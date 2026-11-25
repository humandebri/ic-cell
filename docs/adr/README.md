# Architecture Decision Records (ADR)

`ic-cell` における主要なアーキテクチャ上の意思決定を記録します。

## 一覧

| ID | タイトル | ステータス |
| --- | --- | --- |
| [ADR-0001](./0001-source-of-truth-and-mvp-scope.md) | MVP におけるデータの正本（Source of Truth）と独立 SDK アプローチの採用 | Proposed |
| [ADR-0002](./0002-consistency-and-concurrency-control.md) | ICP 上の CAS・Fencing Epoch および Query/Update の整合性モデル | Proposed |
| [ADR-0003](./0003-ltx-replication-to-ic-sqlite-vfs.md) | LTX ページ差分の `ic-sqlite-vfs` への適用方針 | Proposed |
| [ADR-0004](./0004-icp-object-storage-backend.md) | `celld` 向け ICP Object Storage Backend (`StorageBackend::Ic`) の設計 | Proposed |
| [ADR-0005](./0005-caller-authorization.md) | 呼び出し元許可リストの永続化と管理 API | Accepted |
| [ADR-0006](./0006-durable-cells.md) | Durable Cells — Durable Objects 互換の実行モデル | Accepted |

## ステータス定義

- **Proposed**: 提案・設計中
- **Accepted**: 採用・実装対象
- **Deprecated**: 非推奨
- **Superseded**: 後続の ADR によって置き換え済み
