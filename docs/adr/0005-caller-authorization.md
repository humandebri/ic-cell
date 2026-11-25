# ADR-0005: 呼び出し元許可リストの永続化と管理 API

- Status: Accepted
- Date: 2026-11-25

## Context

初期実装では許可リスト（`AUTHORIZED_CALLERS`）がヒープ上にのみ存在し、さらに「リストが空なら全員許可」という判定だったため、アップグレード後は **誰でも** 読み書きできる状態になっていた。また eventual 系 query（`sql_query_eventual` / `get_object_eventual`）は認可チェック自体を行っていなかった。

## Decision

1. **許可リストは SQLite の外、専用の Stable Memory 領域（`MemoryId 122`）に保存する。**
   LTX レプリケーションは SQLite の生ページを上書きするため、SQLite 内に置いた認可情報はスナップショット適用で置き換わってしまう。
2. **判定ルール**: コントローラは常に許可。それ以外は許可リストに含まれる場合のみ許可。空リストは「コントローラのみ」を意味する。anonymous も暗黙には許可しない。
3. **変更のたびに即時保存**（`pre_upgrade` に依存しない）。`pre_upgrade` でも念のため再保存する。
4. **管理 API（コントローラ専用）**: `add_authorized_caller` / `remove_authorized_caller`。参照は `list_authorized_callers`（許可済み呼び出し元なら可）。
5. **初期化引数** `opt InitArgs { authorized_callers : opt vec principal }` を `init` / `post_upgrade` の両方で受け付け、アップグレード時にも追加できる。インストーラ（anonymous 以外）は自動で登録される。
6. **query も認可する。** `get_replication_state` のみ、ユーザーデータを含まない進捗メタデータとして公開のままとする。

## Consequences

- 旧バージョンからのアップグレード直後は保存済みリストが無いため、コントローラ以外は拒否される。必要な principal はアップグレード引数か `add_authorized_caller` で再登録する。
- 許可リストの上限は 1,024 principal（1 WASM ページに収まる）。
