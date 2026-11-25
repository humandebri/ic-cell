use crate::ownership::verify_fencing_epoch;
use crate::types::{CellError, ExecResult, ObjectMeta, ObjectRecord, QueryResult, SqlValue};
use ic_sqlite_vfs::db::connection::Connection;
use ic_sqlite_vfs::db::migrate::Migration;
use ic_sqlite_vfs::db::row::ColumnValue;
use ic_sqlite_vfs::db::value::{ToSql, Value as VfsValue};
use ic_sqlite_vfs::db::DbError;
use ic_sqlite_vfs::{params, Db};

pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        sql: "
            CREATE TABLE _ic_cell_objects (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                generation INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL
            );

            CREATE TABLE _ic_cell_leases (
                cell_id TEXT PRIMARY KEY NOT NULL,
                owner_id TEXT NOT NULL,
                epoch INTEGER NOT NULL,
                expires_at_ns INTEGER NOT NULL
            );

            CREATE TABLE _ic_cell_idempotency (
                idempotency_key TEXT PRIMARY KEY NOT NULL,
                rows_affected INTEGER NOT NULL,
                created_at_ns INTEGER NOT NULL
            );
        ",
    },
    Migration {
        version: 2,
        sql: "
            CREATE TABLE _ic_cell_upload_chunks (
                upload_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (upload_id, chunk_index)
            );
        ",
    },
    Migration {
        version: 3,
        sql: "
            CREATE TABLE _ic_cell_kv (
                cell_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (cell_id, key)
            ) WITHOUT ROWID;

            CREATE TABLE _ic_cell_alarms (
                cell_id TEXT PRIMARY KEY NOT NULL,
                at_ns INTEGER NOT NULL
            );
            CREATE INDEX _ic_cell_alarms_by_time ON _ic_cell_alarms (at_ns);

            CREATE TABLE _ic_cell_txn_results (
                idempotency_key TEXT PRIMARY KEY NOT NULL,
                cell_id TEXT NOT NULL,
                result BLOB NOT NULL,
                created_at_ns INTEGER NOT NULL
            );
        ",
    },
];

pub fn put_object_create(
    key: String,
    value: Vec<u8>,
    now_ns: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<ObjectRecord, CellError> {
    Db::update(|conn| -> Result<Result<ObjectRecord, CellError>, DbError> {
        if let Some((cell_id, owner_id, epoch)) = fencing.as_ref() {
            if let Err(err) = verify_fencing_epoch(conn, cell_id, owner_id, *epoch)? {
                return Ok(Err(err));
            }
        }

        let exists = conn.query_optional(
            "SELECT generation FROM _ic_cell_objects WHERE key = ?1",
            params![key],
            |row| row.get::<i64>(0),
        )?;

        if exists.is_some() {
            return Ok(Err(CellError::AlreadyExists { key }));
        }

        conn.execute(
            "INSERT INTO _ic_cell_objects (key, value, generation, updated_at_ns)
             VALUES (?1, ?2, 1, ?3)",
            params![key, value, now_ns as i64],
        )?;

        Ok(Ok(ObjectRecord {
            key,
            value,
            generation: 1,
            updated_at_ns: now_ns,
        }))
    })
    .map_err(db_err)?
}

pub fn put_object_cas(
    key: String,
    value: Vec<u8>,
    expected_generation: u64,
    now_ns: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<ObjectRecord, CellError> {
    if expected_generation == 0 {
        return put_object_create(key, value, now_ns, fencing);
    }

    Db::update(|conn| -> Result<Result<ObjectRecord, CellError>, DbError> {
        if let Some((cell_id, owner_id, epoch)) = fencing.as_ref() {
            if let Err(err) = verify_fencing_epoch(conn, cell_id, owner_id, *epoch)? {
                return Ok(Err(err));
            }
        }

        conn.execute(
            "UPDATE _ic_cell_objects
             SET value = ?1, generation = generation + 1, updated_at_ns = ?4
             WHERE key = ?2 AND generation = ?3",
            params![value, key, expected_generation as i64, now_ns as i64],
        )?;

        if conn.changes() == 1 {
            return Ok(Ok(ObjectRecord {
                key,
                value,
                generation: expected_generation + 1,
                updated_at_ns: now_ns,
            }));
        }

        let actual_generation = conn
            .query_optional(
                "SELECT generation FROM _ic_cell_objects WHERE key = ?1",
                params![key],
                |row| row.get::<i64>(0),
            )?
            .map(|g| g as u64);

        Ok(Err(CellError::CasConflict {
            key,
            expected_generation,
            actual_generation,
        }))
    })
    .map_err(db_err)?
}

pub fn get_object(key: &str, strong: bool) -> Result<Option<ObjectRecord>, CellError> {
    let read_fn = |conn: &Connection| -> Result<Option<ObjectRecord>, DbError> {
        conn.query_optional(
            "SELECT key, value, generation, updated_at_ns
             FROM _ic_cell_objects WHERE key = ?1",
            params![key],
            |row| {
                Ok(ObjectRecord {
                    key: row.get::<String>(0)?,
                    value: row.get::<Vec<u8>>(1)?,
                    generation: row.get::<i64>(2)? as u64,
                    updated_at_ns: row.get::<i64>(3)? as u64,
                })
            },
        )
    };

    if strong {
        Db::update(|conn| read_fn(conn)).map_err(db_err)
    } else {
        Db::query(read_fn).map_err(db_err)
    }
}

pub fn delete_object_cas(
    key: String,
    expected_generation: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<bool, CellError> {
    Db::update(|conn| -> Result<Result<bool, CellError>, DbError> {
        if let Some((cell_id, owner_id, epoch)) = fencing.as_ref() {
            if let Err(err) = verify_fencing_epoch(conn, cell_id, owner_id, *epoch)? {
                return Ok(Err(err));
            }
        }

        conn.execute(
            "DELETE FROM _ic_cell_objects WHERE key = ?1 AND generation = ?2",
            params![key, expected_generation as i64],
        )?;

        if conn.changes() == 1 {
            return Ok(Ok(true));
        }

        let actual_generation = conn
            .query_optional(
                "SELECT generation FROM _ic_cell_objects WHERE key = ?1",
                params![key],
                |row| row.get::<i64>(0),
            )?
            .map(|g| g as u64);

        Ok(Err(CellError::CasConflict {
            key,
            expected_generation,
            actual_generation,
        }))
    })
    .map_err(db_err)?
}

pub fn execute_sql(
    sql: String,
    args: Vec<SqlValue>,
    now_ns: u64,
    idempotency_key: Option<String>,
    fencing: Option<(String, String, u64)>,
) -> Result<ExecResult, CellError> {
    crate::cells::check_user_sql(&sql)?;
    Db::update(|conn| -> Result<Result<ExecResult, CellError>, DbError> {
        if let Some((cell_id, owner_id, epoch)) = fencing.as_ref() {
            if let Err(err) = verify_fencing_epoch(conn, cell_id, owner_id, *epoch)? {
                return Ok(Err(err));
            }
        }

        if let Some(idem_key) = idempotency_key.as_deref() {
            if let Some(prev_rows) = conn.query_optional(
                "SELECT rows_affected FROM _ic_cell_idempotency WHERE idempotency_key = ?1",
                params![idem_key],
                |row| row.get::<i64>(0),
            )? {
                return Ok(Ok(ExecResult {
                    rows_affected: prev_rows as u64,
                    idempotent_replay: true,
                }));
            }
        }

        let vfs_values: Vec<VfsValue<'_>> = args.iter().map(to_vfs_value).collect();
        let refs: Vec<&dyn ToSql> = vfs_values.iter().map(|v| v as &dyn ToSql).collect();
        conn.execute(&sql, &refs)?;
        let rows_affected = conn.changes();

        if let Some(idem_key) = idempotency_key.as_deref() {
            conn.execute(
                "INSERT INTO _ic_cell_idempotency (idempotency_key, rows_affected, created_at_ns)
                 VALUES (?1, ?2, ?3)",
                params![idem_key, rows_affected as i64, now_ns as i64],
            )?;
        }

        Ok(Ok(ExecResult {
            rows_affected,
            idempotent_replay: false,
        }))
    })
    .map_err(db_err)?
}

pub fn query_sql(sql: String, args: Vec<SqlValue>, strong: bool) -> Result<QueryResult, CellError> {
    crate::cells::check_user_sql(&sql)?;
    let run = |conn: &Connection| -> Result<QueryResult, DbError> {
        let vfs_values: Vec<VfsValue<'_>> = args.iter().map(to_vfs_value).collect();
        let refs: Vec<&dyn ToSql> = vfs_values.iter().map(|v| v as &dyn ToSql).collect();
        Ok(QueryResult {
            rows: query_rows(conn, &sql, &refs)?,
        })
    };

    if strong {
        Db::update(|conn| run(conn)).map_err(db_err)
    } else {
        Db::query(run).map_err(db_err)
    }
}

/// Runs `sql` and returns every row as `SqlValue`s.
pub(crate) fn query_rows(
    conn: &Connection,
    sql: &str,
    refs: &[&dyn ToSql],
) -> Result<Vec<Vec<SqlValue>>, DbError> {
    conn.query_all(sql, refs, |row| {
        let count = row.column_count();
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            out.push(match row.get_value(i)? {
                ColumnValue::Null => SqlValue::Null,
                ColumnValue::Integer(v) => SqlValue::Integer(v),
                ColumnValue::Real(v) => SqlValue::Real(v),
                ColumnValue::Text(v) => SqlValue::Text(v),
                ColumnValue::Blob(v) => SqlValue::Blob(v),
            });
        }
        Ok(out)
    })
}

pub fn gc_idempotency(older_than_ns: u64) -> Result<u64, CellError> {
    Db::update(|conn| {
        conn.execute(
            "DELETE FROM _ic_cell_idempotency WHERE created_at_ns < ?1",
            params![older_than_ns as i64],
        )?;
        Ok(conn.changes())
    })
    .map_err(db_err)
}

pub fn abort_chunked_upload(upload_id: &str) -> Result<u64, CellError> {
    Db::update(|conn| {
        conn.execute(
            "DELETE FROM _ic_cell_upload_chunks WHERE upload_id = ?1",
            params![upload_id],
        )?;
        Ok(conn.changes())
    })
    .map_err(db_err)
}

pub(crate) fn to_vfs_value(v: &SqlValue) -> VfsValue<'_> {
    match v {
        SqlValue::Null => VfsValue::Null,
        SqlValue::Integer(i) => VfsValue::Integer(*i),
        SqlValue::Real(r) => VfsValue::Real(*r),
        SqlValue::Text(s) => VfsValue::Text(s.as_str()),
        SqlValue::Blob(b) => VfsValue::Blob(b.as_slice()),
    }
}

pub fn head_object(key: &str, strong: bool) -> Result<Option<ObjectMeta>, CellError> {
    let read_fn = |conn: &Connection| -> Result<Option<ObjectMeta>, DbError> {
        conn.query_optional(
            "SELECT key, length(value), generation, updated_at_ns
             FROM _ic_cell_objects WHERE key = ?1",
            params![key],
            |row| {
                Ok(ObjectMeta {
                    key: row.get::<String>(0)?,
                    size: row.get::<i64>(1)? as u64,
                    generation: row.get::<i64>(2)? as u64,
                    updated_at_ns: row.get::<i64>(3)? as u64,
                })
            },
        )
    };

    if strong {
        Db::update(|conn| read_fn(conn)).map_err(db_err)
    } else {
        Db::query(read_fn).map_err(db_err)
    }
}

pub fn get_object_range(
    key: &str,
    offset: u64,
    length: u64,
    strong: bool,
) -> Result<Vec<u8>, CellError> {
    let obj = get_object(key, strong)?.ok_or_else(|| CellError::NotFound {
        key: key.to_string(),
    })?;
    let start = usize::try_from(offset).unwrap_or(usize::MAX);
    if start >= obj.value.len() {
        return Ok(Vec::new());
    }
    let len = usize::try_from(length).unwrap_or(usize::MAX);
    let end = start.saturating_add(len).min(obj.value.len());
    Ok(obj.value[start..end].to_vec())
}

pub fn list_objects(prefix: &str, strong: bool) -> Result<Vec<ObjectMeta>, CellError> {
    let escaped = prefix
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let pattern = format!("{escaped}%");
    let read_fn = |conn: &Connection| -> Result<Vec<ObjectMeta>, DbError> {
        conn.query_all(
            "SELECT key, length(value), generation, updated_at_ns
             FROM _ic_cell_objects
             WHERE key LIKE ?1 ESCAPE '\\'
             ORDER BY key ASC",
            params![pattern],
            |row| {
                Ok(ObjectMeta {
                    key: row.get::<String>(0)?,
                    size: row.get::<i64>(1)? as u64,
                    generation: row.get::<i64>(2)? as u64,
                    updated_at_ns: row.get::<i64>(3)? as u64,
                })
            },
        )
    };

    if strong {
        Db::update(|conn| read_fn(conn)).map_err(db_err)
    } else {
        Db::query(read_fn).map_err(db_err)
    }
}

pub fn put_object_chunk(
    upload_id: String,
    chunk_index: u32,
    data: Vec<u8>,
) -> Result<(), CellError> {
    Db::update(|conn| {
        conn.execute(
            "INSERT INTO _ic_cell_upload_chunks (upload_id, chunk_index, data)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(upload_id, chunk_index) DO UPDATE SET data = excluded.data",
            params![upload_id, chunk_index as i64, data],
        )
    })
    .map_err(db_err)
}

pub fn commit_chunked_object(
    upload_id: String,
    key: String,
    expected_generation: u64,
    now_ns: u64,
    fencing: Option<(String, String, u64)>,
) -> Result<ObjectRecord, CellError> {
    // Assemble chunks, apply CAS/create, and delete staging chunks in a single
    // atomic transaction so failed CAS/fencing preserves uploaded chunks.
    Db::update(|conn| -> Result<Result<ObjectRecord, CellError>, DbError> {
        if let Some((cell_id, owner_id, epoch)) = fencing.as_ref() {
            if let Err(err) = verify_fencing_epoch(conn, cell_id, owner_id, *epoch)? {
                return Ok(Err(err));
            }
        }

        let chunks = conn.query_all(
            "SELECT data FROM _ic_cell_upload_chunks
             WHERE upload_id = ?1
             ORDER BY chunk_index ASC",
            params![upload_id],
            |row| row.get::<Vec<u8>>(0),
        )?;
        let total_len: usize = chunks.iter().map(|c| c.len()).sum();
        let mut assembled = Vec::with_capacity(total_len);
        for c in chunks {
            assembled.extend_from_slice(&c);
        }

        let record = if expected_generation == 0 {
            let exists = conn.query_optional(
                "SELECT generation FROM _ic_cell_objects WHERE key = ?1",
                params![key],
                |row| row.get::<i64>(0),
            )?;
            if exists.is_some() {
                return Ok(Err(CellError::AlreadyExists { key }));
            }
            conn.execute(
                "INSERT INTO _ic_cell_objects (key, value, generation, updated_at_ns)
                 VALUES (?1, ?2, 1, ?3)",
                params![key, assembled, now_ns as i64],
            )?;
            ObjectRecord {
                key,
                value: assembled,
                generation: 1,
                updated_at_ns: now_ns,
            }
        } else {
            conn.execute(
                "UPDATE _ic_cell_objects
                 SET value = ?1, generation = generation + 1, updated_at_ns = ?4
                 WHERE key = ?2 AND generation = ?3",
                params![assembled, key, expected_generation as i64, now_ns as i64],
            )?;
            if conn.changes() != 1 {
                let actual_generation = conn
                    .query_optional(
                        "SELECT generation FROM _ic_cell_objects WHERE key = ?1",
                        params![key],
                        |row| row.get::<i64>(0),
                    )?
                    .map(|g| g as u64);
                return Ok(Err(CellError::CasConflict {
                    key,
                    expected_generation,
                    actual_generation,
                }));
            }
            ObjectRecord {
                key,
                value: assembled,
                generation: expected_generation + 1,
                updated_at_ns: now_ns,
            }
        };

        conn.execute(
            "DELETE FROM _ic_cell_upload_chunks WHERE upload_id = ?1",
            params![upload_id],
        )?;

        Ok(Ok(record))
    })
    .map_err(db_err)?
}

pub(crate) fn db_err(err: DbError) -> CellError {
    CellError::Database(err.to_string())
}
