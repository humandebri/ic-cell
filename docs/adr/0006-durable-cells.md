# ADR-0006: Durable Cells — Durable Objects 互換の実行モデル

- Status: Accepted
- Date: 2026-11-25

## Context

`IC_DB` は SQL の 1 文ごとに update call を発行するだけで、「ID ごとに 1 つだけ動くアクター」「呼び出しの直列化」「read-modify-write の原子性」がなかった。`examples/durable-counter` は SELECT → INSERT の 2 回呼び出しで、並行実行すると更新が失われていた。

## Decision

### Canister（`cells.rs`）
1. **Cell ごとに分離された KV**（`_ic_cell_kv(cell_id, key)`）と **Cell ごとに 1 つのアラーム**（`_ic_cell_alarms`）を持つ。
2. **`cell_transact`**: Fencing Token を必須とし、`KvPut / KvDelete / KvDeleteAll / SetAlarm / AssertKv / Sql` の op 列を **1 つの SQLite トランザクション**で適用する。途中で失敗すれば全体をロールバックする。`Sql` の結果（`RETURNING` を含む）も返す。
3. **冪等性**: `idempotency_key` 付きのトランザクションは結果を Candid で保存し、再送時は保存済みの結果をそのまま返す（`idempotent_replay = true`）。キーは Cell に紐づき、他の Cell からは使えない。
4. **`activate_cell`**: Lease の取得・更新と状態のプリロード（応答上限内）、アラームの取得を 1 往復で行う。
5. **`list_due_alarms`**: Canister 時刻（合意時刻）で期限が来たアラームを返す。どのノードでもポーリングでき、死んだ Owner のアラームを拾える。
6. **内部テーブルの保護**: `_ic_cell_*` を参照するユーザー SQL は `sql_execute` / `sql_query_*` / `Sql` op のすべてで拒否する。拒否しないと Lease を書き換えて Fencing を迂回できてしまう。

### TypeScript ランタイム（`runtime.ts`）
- **`CellNamespace.get(id)`** が返す Stub でメソッドを呼ぶ。Lease を持つノードでのみインスタンスを生成し、他ノードは `CellForwarder`（`InProcessForwarder` / `HttpForwarder` + `handleHttp`）で Owner に転送する。
- **入力ゲート**: 1 Cell への呼び出しは厳密に直列実行する。
- **出力ゲート / 書き込み集約**: 呼び出し中の書き込みはキャッシュに即時反映し、呼び出し終了時に 1 回の `cell_transact` でまとめてコミットする。コミットが確定するまで呼び出し元に結果を返さない。例外の前に行った書き込みもコミットする（DO と同じ）。`transaction()` は失敗時にローカル状態も含めてロールバックする。
- **読み取り**: Owner だけが書くのでキャッシュは一貫している。プリロード済みならローカル読み取りで済み、Canister への往復はない。
- **Lease**: TTL/3 ごとに更新する。ローカルの期限を過ぎたら次の呼び出し時に再検証し、epoch が変わっていればキャッシュを捨てて再アクティベートする。コミット時に Fencing で拒否された場合もインスタンスを破棄する。
- **再試行**: 結果不明（通信エラー）のコミットは同じ冪等キーで再送する。
- **アラーム**: `alarm()` ハンドラを指数バックオフ付きで再試行する（既定 6 回）。ハンドラが同じ時刻で再設定しても見落とさないよう、値ではなくリビジョンで変更を検知する。
- **アイドル退避**: 一定時間呼び出しがない Cell は Lease を解放し、他ノードが即座に引き継げるようにする（アラームが残っている Cell は保持する）。

## Consequences

- ✅ DO と同等の「単一インスタンス・直列実行・原子的コミット・Fencing」を、実 Canister（PocketIC E2E）と TS ランタイムの両方で検証した。
- ⚠️ **書き込みのレイテンシ**: 書き込みを含む呼び出しは、最後に 1 回の update call（1〜2 秒）を待つ。読み取りだけの呼び出しは速い。
- ⚠️ **SQL は全 Cell で共有**される（KV だけが Cell ごとに分離）。Cell ごとの SQL 名前空間は未実装。
- ⚠️ 直列実行のため、Cell が自分自身の Stub を呼ぶとデッドロックする（DO の入力ゲートのインターリーブは未実装）。
- ⚠️ WebSocket / Hibernation は対象外（実行ホスト側の責務）。
- ⚠️ `handleHttp`（ノード間 RPC）には認証がないため、クラスタ内部のネットワークだけに公開する。呼べるのはクラス自身のメソッドだけで、`_` 始まり・`constructor`・`alarm`・`Object.prototype` のメソッドは拒否する。
