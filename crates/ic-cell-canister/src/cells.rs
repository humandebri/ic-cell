//! Durable Cells: Durable-Object-style per-cell storage.
//!
//! * Every cell has an isolated key/value namespace (`_ic_cell_kv`) and at
//!   most one alarm (`_ic_cell_alarms`).
//! * All writes go through [`transact`], which verifies the cell's fencing
//!   token and applies a list of ops in **one** SQLite transaction: either
//!   every op commits or none does.
//! * An idempotency key makes a transaction safe to retry after an unknown
//!   outcome; a replay returns the originally committed results.
//! * [`activate_cell`] acquires/renews the lease and preloads the cell's state
//!   in a single round trip.

use crate::ownership::{acquire_or_renew_lease, verify_fencing_epoch};
use crate::storage::{db_err, query_rows, to_vfs_value};
use crate::types::{
    Activation, CellError, CellOp, DueAlarm, KvEntry, KvListOptions, KvListResult, OpResult,
    SqlValue, TransactRequest, TransactResult, MAX_REPLY_PAYLOAD_BYTES,
};
use ic_sqlite_vfs::db::connection::Connection;
use ic_sqlite_vfs::db::value::{ToSql, Value as VfsValue};
use ic_sqlite_vfs::db::DbError;
use ic_sqlite_vfs::{params, Db};

pub const MAX_KEY_BYTES: usize = 2048;
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;
pub const MAX_OPS_PER_TRANSACTION: usize = 1024;
pub const DEFAULT_LIST_LIMIT: u32 = 1000;
/// Table names starting with this prefix are internal and may not be touched
/// by user SQL (otherwise SQL could rewrite leases and bypass fencing).
pub const RESERVED_TABLE_PREFIX: &str = "_ic_cell_";

const FORBIDDEN_SQL_KEYWORDS: &[&str] = &[
    "begin",
    "commit",
    "end",
    "rollback",
    "savepoint",
    "release",
    "pragma",
    "attach",
    "detach",
    "vacuum",
];

fn leading_sql_keyword(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            match rest.find('\n') {
                Some(i) => s = &rest[i + 1..],
                None => return "",
            }
        } else if let Some(rest) = s.strip_prefix("/*") {
            match rest.find("*/") {
                Some(i) => s = &rest[i + 2..],
                None => return "",
            }
        } else {
            break;
        }
    }
    let end = s
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(s.len());
    &s[..end]
}

/// Rejects user SQL that references internal tables or attempts transaction
/// control / PRAGMA / ATTACH / DETACH / VACUUM.
pub fn check_user_sql(sql: &str) -> Result<(), CellError> {
    if sql.to_ascii_lowercase().contains(RESERVED_TABLE_PREFIX) {
        return Err(CellError::InvalidArgument(format!(
            "SQL may not reference internal tables ({RESERVED_TABLE_PREFIX}*)"
        )));
    }
    let kw = leading_sql_keyword(sql).to_ascii_lowercase();
    if FORBIDDEN_SQL_KEYWORDS.contains(&kw.as_str()) {
        return Err(CellError::InvalidArgument(format!(
            "SQL statement '{kw}' is not allowed inside a cell transaction"
        )));
    }
    Ok(())
}

fn check_key(key: &str) -> Result<(), CellError> {
    if key.len() > MAX_KEY_BYTES {
        return Err(CellError::InvalidArgument(format!(
            "key is {} bytes, max {MAX_KEY_BYTES}",
            key.len()
        )));
    }
    Ok(())
}

fn validate(req: &TransactRequest) -> Result<(), CellError> {
    if req.ops.len() > MAX_OPS_PER_TRANSACTION {
        return Err(CellError::InvalidArgument(format!(
            "{} ops exceed the limit of {MAX_OPS_PER_TRANSACTION}",
            req.ops.len()
        )));
    }
    for op in &req.ops {
        match op {
            CellOp::KvPut { key, value } => {
                check_key(key)?;
                if value.len() > MAX_VALUE_BYTES {
                    return Err(CellError::InvalidArgument(format!(
                        "value is {} bytes, max {MAX_VALUE_BYTES}",
                        value.len()
                    )));
                }
            }
            CellOp::KvDelete { key } | CellOp::AssertKv { key, .. } => check_key(key)?,
            CellOp::Sql { sql, .. } => check_user_sql(sql)?,
            CellOp::SetAlarm(Some(at_ns)) => {
                if *at_ns > i64::MAX as u64 {
                    return Err(CellError::InvalidArgument(
                        "alarm timestamp exceeds i64::MAX".to_string(),
                    ));
                }
            }
            CellOp::KvDeleteAll | CellOp::SetAlarm(None) => {}
        }
    }
    Ok(())
}

/// Carries a domain error out of `Db::update` while still forcing rollback.
const ABORT_CODE: i32 = -77;

fn abort(slot: &mut Option<CellError>, err: CellError) -> DbError {
    *slot = Some(err);
    DbError::Sqlite(ABORT_CODE, "ic-cell transaction aborted".into())
}

fn total_changes(conn: &Connection) -> Result<u64, DbError> {
    Ok(conn.query_scalar::<i64>("SELECT total_changes()", &[])? as u64)
}

fn kv_value(conn: &Connection, cell_id: &str, key: &str) -> Result<Option<Vec<u8>>, DbError> {
    conn.query_optional(
        "SELECT value FROM _ic_cell_kv WHERE cell_id = ?1 AND key = ?2",
        params![cell_id, key],
        |row| row.get::<Vec<u8>>(0),
    )
}

/// Atomically applies `req.ops` for the fenced cell.
pub fn transact(req: TransactRequest, now_ns: u64) -> Result<TransactResult, CellError> {
    validate(&req)?;
    let (cell_id, owner_id, epoch) = req.fencing.clone();
    let mut failure: Option<CellError> = None;

    let outcome = Db::update(|conn| -> Result<TransactResult, DbError> {
        if let Err(err) = verify_fencing_epoch(conn, &cell_id, &owner_id, epoch)? {
            return Err(abort(&mut failure, err));
        }

        if let Some(key) = req.idempotency_key.as_deref() {
            if let Some((stored_cell, blob)) = conn.query_optional(
                "SELECT cell_id, result FROM _ic_cell_txn_results WHERE idempotency_key = ?1",
                params![key],
                |row| Ok((row.get::<String>(0)?, row.get::<Vec<u8>>(1)?)),
            )? {
                if stored_cell != cell_id {
                    return Err(abort(
                        &mut failure,
                        CellError::InvalidArgument(format!(
                            "idempotency key {key} belongs to another cell"
                        )),
                    ));
                }
                let mut replay: TransactResult = candid::decode_one(&blob).map_err(|e| {
                    DbError::Sqlite(1, format!("corrupt stored transaction result: {e}"))
                })?;
                replay.idempotent_replay = true;
                return Ok(replay);
            }
        }

        let mut results = Vec::with_capacity(req.ops.len());
        for op in &req.ops {
            let result = match op {
                CellOp::KvPut { key, value } => {
                    conn.execute(
                        "INSERT INTO _ic_cell_kv (cell_id, key, value) VALUES (?1, ?2, ?3)
                         ON CONFLICT(cell_id, key) DO UPDATE SET value = excluded.value",
                        params![cell_id, key.as_str(), value.as_slice()],
                    )?;
                    OpResult::affected(1)
                }
                CellOp::KvDelete { key } => {
                    conn.execute(
                        "DELETE FROM _ic_cell_kv WHERE cell_id = ?1 AND key = ?2",
                        params![cell_id, key.as_str()],
                    )?;
                    OpResult::affected(conn.changes())
                }
                CellOp::KvDeleteAll => {
                    conn.execute(
                        "DELETE FROM _ic_cell_kv WHERE cell_id = ?1",
                        params![cell_id],
                    )?;
                    OpResult::affected(conn.changes())
                }
                CellOp::SetAlarm(Some(at_ns)) => {
                    conn.execute(
                        "INSERT INTO _ic_cell_alarms (cell_id, at_ns) VALUES (?1, ?2)
                         ON CONFLICT(cell_id) DO UPDATE SET at_ns = excluded.at_ns",
                        params![cell_id, *at_ns as i64],
                    )?;
                    OpResult::affected(1)
                }
                CellOp::SetAlarm(None) => {
                    conn.execute(
                        "DELETE FROM _ic_cell_alarms WHERE cell_id = ?1",
                        params![cell_id],
                    )?;
                    OpResult::affected(conn.changes())
                }
                CellOp::AssertKv { key, expected } => {
                    let actual = kv_value(conn, &cell_id, key)?;
                    if actual != *expected {
                        return Err(abort(
                            &mut failure,
                            CellError::PreconditionFailed {
                                reason: format!("value of key {key:?} does not match"),
                            },
                        ));
                    }
                    OpResult::affected(0)
                }
                CellOp::Sql { sql, args } => {
                    let before = total_changes(conn)?;
                    let rows = run_sql(conn, sql, args)?;
                    let after = total_changes(conn)?;
                    OpResult {
                        rows_affected: after.saturating_sub(before),
                        rows,
                    }
                }
            };
            results.push(result);
        }

        let result = TransactResult {
            results,
            idempotent_replay: false,
            epoch,
        };
        if let Some(key) = req.idempotency_key.as_deref() {
            let blob = candid::encode_one(&result)
                .map_err(|e| DbError::Sqlite(1, format!("encode transaction result: {e}")))?;
            conn.execute(
                "INSERT INTO _ic_cell_txn_results (idempotency_key, cell_id, result, created_at_ns)
                 VALUES (?1, ?2, ?3, ?4)",
                params![key, cell_id, blob, now_ns as i64],
            )?;
        }
        Ok(result)
    });

    match (outcome, failure) {
        (_, Some(err)) => Err(err),
        (Ok(result), None) => Ok(result),
        (Err(err), None) => Err(db_err(err)),
    }
}

fn run_sql(conn: &Connection, sql: &str, args: &[SqlValue]) -> Result<Vec<Vec<SqlValue>>, DbError> {
    let values: Vec<VfsValue<'_>> = args.iter().map(to_vfs_value).collect();
    let refs: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
    query_rows(conn, sql, &refs)
}

pub fn kv_get(cell_id: &str, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>, CellError> {
    for key in keys {
        check_key(key)?;
    }
    let reply_budget = (MAX_REPLY_PAYLOAD_BYTES - 64 * 1024) as usize;
    let mut failure: Option<CellError> = None;
    let outcome = Db::update(|conn| -> Result<Vec<Option<Vec<u8>>>, DbError> {
        let mut out = Vec::with_capacity(keys.len());
        let mut total_bytes = 0usize;
        for k in keys {
            let val = kv_value(conn, cell_id, k)?;
            if let Some(ref bytes) = val {
                total_bytes = total_bytes.saturating_add(bytes.len() + 16);
                if total_bytes > reply_budget {
                    return Err(abort(
                        &mut failure,
                        CellError::InvalidArgument(format!(
                            "kv_get reply exceeds payload budget ({reply_budget} bytes); fetch fewer keys"
                        )),
                    ));
                }
            }
            out.push(val);
        }
        Ok(out)
    });
    match (outcome, failure) {
        (_, Some(err)) => Err(err),
        (Ok(vals), None) => Ok(vals),
        (Err(err), None) => Err(db_err(err)),
    }
}

/// Lists entries in key order. Results are cut off at `limit` entries or at
/// the reply size budget; `truncated` tells the caller to continue with
/// `start_after = last key`.
pub fn kv_list(cell_id: &str, opts: &KvListOptions) -> Result<KvListResult, CellError> {
    kv_list_with_budget(cell_id, opts, MAX_REPLY_PAYLOAD_BYTES - 64 * 1024)
}

fn kv_list_with_budget(
    cell_id: &str,
    opts: &KvListOptions,
    byte_budget: u64,
) -> Result<KvListResult, CellError> {
    const BATCH_SIZE: u32 = 256;
    let limit = opts.limit.unwrap_or(DEFAULT_LIST_LIMIT).max(1);
    let prefix = opts.prefix.clone().unwrap_or_default();
    let reverse = opts.reverse.unwrap_or(false);
    let order = if reverse { "DESC" } else { "ASC" };
    let cursor_cmp = if reverse { "<" } else { ">" };
    let sql = format!(
        "SELECT key, value FROM _ic_cell_kv
         WHERE cell_id = ?1
           AND substr(key, 1, length(?2)) = ?2
           AND (?3 IS NULL OR key {cursor_cmp} ?3)
         ORDER BY key {order}
         LIMIT ?4"
    );

    Db::update(|conn| {
        let mut entries = Vec::new();
        let mut bytes = 0u64;
        let mut truncated = false;
        let mut cursor_owned = opts.start_after.clone();

        loop {
            let remaining = limit.saturating_sub(entries.len() as u32);
            let batch_limit = remaining.min(BATCH_SIZE);
            let fetch_n = i64::from(batch_limit) + 1;
            let cursor: VfsValue<'_> = match &cursor_owned {
                Some(k) => VfsValue::Text(k.as_str()),
                None => VfsValue::Null,
            };
            let rows = conn.query_all(
                &sql,
                &[&cell_id, &prefix.as_str(), &cursor, &fetch_n],
                |row| {
                    Ok(KvEntry {
                        key: row.get::<String>(0)?,
                        value: row.get::<Vec<u8>>(1)?,
                    })
                },
            )?;

            let has_more_in_db = rows.len() as i64 > i64::from(batch_limit);
            let mut hit_budget = false;
            for entry in rows.into_iter().take(batch_limit as usize) {
                let size = (entry.key.len() + entry.value.len() + 16) as u64;
                if bytes + size > byte_budget && !entries.is_empty() {
                    truncated = true;
                    hit_budget = true;
                    break;
                }
                bytes += size;
                cursor_owned = Some(entry.key.clone());
                entries.push(entry);
            }

            if hit_budget {
                break;
            }
            if entries.len() as u32 >= limit {
                if has_more_in_db {
                    truncated = true;
                }
                break;
            }
            if !has_more_in_db {
                break;
            }
        }

        Ok(KvListResult { entries, truncated })
    })
    .map_err(db_err)
}

pub fn get_alarm(cell_id: &str) -> Result<Option<u64>, CellError> {
    Db::update(|conn| {
        conn.query_optional(
            "SELECT at_ns FROM _ic_cell_alarms WHERE cell_id = ?1",
            params![cell_id],
            |row| Ok(row.get::<i64>(0)? as u64),
        )
    })
    .map_err(db_err)
}

/// Alarms due at `now_ns`, oldest first. Any node may poll this to activate
/// cells whose previous owner died.
pub fn list_due_alarms(now_ns: u64, limit: u32) -> Result<Vec<DueAlarm>, CellError> {
    let limit = limit.clamp(1, 1000);
    Db::update(|conn| {
        conn.query_all(
            "SELECT cell_id, at_ns FROM _ic_cell_alarms WHERE at_ns <= ?1 ORDER BY at_ns LIMIT ?2",
            params![now_ns as i64, i64::from(limit)],
            |row| {
                Ok(DueAlarm {
                    cell_id: row.get::<String>(0)?,
                    at_ns: row.get::<i64>(1)? as u64,
                })
            },
        )
    })
    .map_err(db_err)
}

/// Acquires (or renews) the cell lease and preloads up to `preload_bytes` of
/// state plus the alarm in one call.
pub fn activate_cell(
    cell_id: String,
    owner_id: String,
    ttl_ns: u64,
    preload_bytes: u64,
    now_ns: u64,
) -> Result<Activation, CellError> {
    let lease = acquire_or_renew_lease(cell_id.clone(), owner_id, ttl_ns, now_ns)?;
    let budget = preload_bytes.min(MAX_REPLY_PAYLOAD_BYTES - 128 * 1024);
    let listed = if budget == 0 {
        KvListResult {
            entries: Vec::new(),
            truncated: true,
        }
    } else {
        kv_list_with_budget(
            &cell_id,
            &KvListOptions {
                limit: Some(u32::MAX),
                ..Default::default()
            },
            budget,
        )?
    };
    Ok(Activation {
        lease,
        entries: listed.entries,
        complete: !listed.truncated,
        alarm_at_ns: get_alarm(&cell_id)?,
    })
}

/// Drops stored transaction results older than `older_than_ns`.
pub fn gc_txn_results(older_than_ns: u64) -> Result<u64, CellError> {
    Db::update(|conn| {
        conn.execute(
            "DELETE FROM _ic_cell_txn_results WHERE created_at_ns < ?1",
            params![older_than_ns as i64],
        )?;
        Ok(conn.changes())
    })
    .map_err(db_err)
}
