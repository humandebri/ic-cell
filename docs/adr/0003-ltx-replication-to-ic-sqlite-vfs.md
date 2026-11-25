# ADR-0003: LTX ページ差分の `ic-sqlite-vfs` への適用方針

- **Status**: Proposed
- **Date**: 2026-11-24

## Context

`celld` はローカル SQLite のコミットを WAL から抽出し、LTX（ページ差分・TXID・Pre/PostApplyChecksum を含む）として複製する。
一方、`ic-sqlite-vfs` は以下の特性を持つ。

- WAL を無効化している
- SQLite ページサイズを 16 KiB 固定として設計されている
- Stable Memory 上の固定オフセットに独自メタデータとトランザクション overlay を持つ

このため、`celld` と `ic-sqlite-vfs` を VFS レベルでそのまま差し替えることはできない。しかし、LTX のページ差分を Canister 側の SQLite イメージに直接反映できれば、DB 全体を転送せずにオンチェーンで SQL 照会可能なミラーを実現できる。

## Decision

1. **VFS の直接差し替えではなく、ページ差分適用 API（`apply_page_delta`）で接続する**
   - `ic-sqlite-vfs` 側（またはその拡張レイヤー）に、LTX から抽出した変更ページ群をアトミックに適用する API を設ける。
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
2. **ページサイズの統一を前提とする**
   - `celld` 側の SQLite データベースは `PRAGMA page_size = 16384`（16 KiB）で作成し、`ic-sqlite-vfs` のページサイズと一致させる。不一致の場合は適用時にエラーで拒否する。
3. **検証とアトミック公開**
   - `epoch`、`MinTXID`/`MaxTXID` の連続性、`PreApplyChecksum` を検証した上で、トランザクション overlay に変更ページを書き込む。
   - 適用後のイメージに対して `PostApplyChecksum` を検証し、一致した場合のみ新バージョンを公開する。
4. **接続キャッシュの無効化**
   - ページイメージを直接書き換えた直後に、Canister 内の SQLite 接続・ページキャッシュを明示的に無効化（または再オープン）し、古いキャッシュによる読み取り不整合を防ぐ。

## Consequences

### Positive
- `celld` のローカル SQLite の高速性を維持したまま、ICP 上に SQL 検索可能な検証済みミラーを持てる。
- 差分転送のみのため、ネットワーク帯域と ICP サイクル消費を大幅に抑えられる。

### Negative
- `ic-sqlite-vfs` の内部レイアウトや overlay 機構への依存が発生するため、上流との協調または拡張 PR が必要になる。
- 1 回の LTX が ICP の Ingress メッセージ上限（2 MiB 等）を超える場合、チャンク分割アップロードとステージング領域が必要になる。

## Implementation Notes (2026-11-25)

- **LTX ファイルのデコード**: `ic_cell_client::ltx_codec` が `superfly/ltx` 形式（magic `LTX1`、ヘッダ 100 byte、6 byte ページヘッダ + サイズ付き LZ4 ブロック、ページインデックス、16 byte トレーラ）をバイト互換でデコード / エンコードする。ファイルチェックサム（CRC-64/ISO、圧縮前ページで計算）・ページ順序・インデックス・スナップショットの DB チェックサムをすべて検証してから Canister に送る。
- **2 系統のチェックサム**: `pre/post_apply_checksum` は従来どおり `ic-sqlite-vfs` イメージのチェックサム（0 = スキップ）。LTX 形式のチェックサムは `ltx_pre/post_apply_checksum : opt nat64` として別フィールドで渡し、Canister は `ltx.ChecksumPage`（`FLAG | CRC64(be32(pgno) || page)` の XOR、lock page 除外）と同じ定義で **書き込み前に** 増分計算・検証する。不一致なら何も変更せずに拒否する。計算結果は `ReplicationState.ltx_checksum` として Stable Memory に永続化される。
- **スナップショット**（`min_txid == 1`）は `1..=commit`（lock page を除く）の全ページを含む必要があり、`max_txid >= last_txid` であれば既存ミラーをリセットできる。
- **大きな差分のステージング**: 1 メッセージに収まらない差分は `stage_ltx_pages` → `commit_staged_ltx_delta` で分割送信し、コミットは 1 メッセージでアトミックに適用する。ステージング領域はヒープ上（1 アップロード 128 MiB / 合計 256 MiB / 同時 16 件まで）で、アップグレードを跨がない。`LtxReplicationAdapter` が自動で使い分ける。
