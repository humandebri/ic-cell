# ADR-0001: MVP におけるデータの正本（Source of Truth）と独立 SDK アプローチの採用

- **Status**: Proposed
- **Date**: 2026-11-24

## Context

`denoland/celld` と `humandebri/ic-sqlite-vfs` を組み合わせる方法は複数存在する（ICP Native Storage Binding、LTX Replication、ICP Object Storage Backend、ICP Ownership Coordinator）。
最初から `celld` 本体を fork してストレージ層や VFS を書き換えるアプローチは、以下のリスクを伴う。

1. `celld` 上流の変更追従コストが高い。
2. `celld` 側のローカル SQLite と ICP Canister 側の両方に書き込む構成を初期から採ると、二重書き込みによる状態不整合や障害復旧の複雑さが急増する。
3. 実用的な開発者体験（`IC_DB` バインディングや Immortal Agent）の検証までに時間がかかる。

## Decision

1. **独立 SDK アプローチから開始する（Phase 1〜4）**
   - まずは `celld` を fork せず、`ic-cell` として独立した Canister (`ic-cell-canister`)、Rust Client (`ic-cell-client`)、TypeScript SDK (`packages/typescript-sdk`) を構築する。
   - Durable Object からは `this.env.IC_DB` バインディング経由で Canister を操作する。
2. **MVP では ICP を唯一の正本（Source of Truth）とする**
   - 重要な永続状態はすべて ICP Canister に直接書き込む。
   - `celld` 側のローカル SQLite はキャッシュまたは一時作業領域に限定し、二重書き込みを行わない。
3. **LTX 連携・`StorageBackend::Ic` は後続フェーズ（Phase 5〜6）で導入する**
   - MVP で所有権・CAS・障害復旧のモデルを固めた上で、より深い統合へ進む。

## Consequences

### Positive
- 最小のコード量でエンドツーエンドの動作（Immortal Agent 等）を早期に検証できる。
- 正本が ICP に一元化されるため、ノード障害時の復元ロジックが単純かつ安全になる。
- `celld` 本体のアップデートに引きずられずに開発を進められる。

### Negative
- 書き込みごとに ICP の `update` コール（約 1〜2 秒のコンセンサスレイテンシ）が発生する。
- 高頻度な書き込みワークロードには、Phase 5 の LTX 非同期/バッチ複製が揃うまで不向きとなる。
