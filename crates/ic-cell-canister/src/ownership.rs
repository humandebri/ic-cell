use crate::storage::db_err;
use crate::types::{CellError, LeaseRecord};
use ic_sqlite_vfs::db::connection::Connection;
use ic_sqlite_vfs::db::DbError;
use ic_sqlite_vfs::{params, Db};

pub fn acquire_or_renew_lease(
    cell_id: String,
    owner_id: String,
    ttl_ns: u64,
    now_ns: u64,
) -> Result<LeaseRecord, CellError> {
    if ttl_ns == 0 {
        return Err(CellError::InvalidArgument("ttl_ns must be > 0".to_string()));
    }

    let expires_at_ns = now_ns.saturating_add(ttl_ns);

    Db::update(|conn| -> Result<Result<LeaseRecord, CellError>, DbError> {
        let existing = conn.query_optional(
            "SELECT owner_id, epoch, expires_at_ns
             FROM _ic_cell_leases WHERE cell_id = ?1",
            params![cell_id],
            |row| {
                Ok((
                    row.get::<String>(0)?,
                    row.get::<i64>(1)? as u64,
                    row.get::<i64>(2)? as u64,
                ))
            },
        )?;

        match existing {
            None => {
                let epoch = 1;
                conn.execute(
                    "INSERT INTO _ic_cell_leases (cell_id, owner_id, epoch, expires_at_ns)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![cell_id, owner_id, epoch as i64, expires_at_ns as i64],
                )?;
                Ok(Ok(LeaseRecord {
                    cell_id,
                    owner_id,
                    epoch,
                    expires_at_ns,
                }))
            }
            Some((current_owner, current_epoch, current_expires_at)) => {
                if current_owner == owner_id {
                    conn.execute(
                        "UPDATE _ic_cell_leases
                         SET expires_at_ns = ?2
                         WHERE cell_id = ?1",
                        params![cell_id, expires_at_ns as i64],
                    )?;
                    Ok(Ok(LeaseRecord {
                        cell_id,
                        owner_id,
                        epoch: current_epoch,
                        expires_at_ns,
                    }))
                } else if now_ns >= current_expires_at {
                    let next_epoch = current_epoch + 1;
                    conn.execute(
                        "UPDATE _ic_cell_leases
                         SET owner_id = ?2, epoch = ?3, expires_at_ns = ?4
                         WHERE cell_id = ?1",
                        params![
                            cell_id,
                            owner_id,
                            next_epoch as i64,
                            expires_at_ns as i64
                        ],
                    )?;
                    Ok(Ok(LeaseRecord {
                        cell_id,
                        owner_id,
                        epoch: next_epoch,
                        expires_at_ns,
                    }))
                } else {
                    Ok(Err(CellError::LeaseHeldByAnother {
                        cell_id,
                        current_owner,
                        expires_at_ns: current_expires_at,
                    }))
                }
            }
        }
    })
    .map_err(db_err)?
}

pub fn release_lease(cell_id: String, owner_id: String, epoch: u64) -> Result<(), CellError> {
    Db::update(|conn| -> Result<Result<(), CellError>, DbError> {
        if let Err(err) = verify_fencing_epoch(conn, &cell_id, &owner_id, epoch)? {
            return Ok(Err(err));
        }
        conn.execute(
            "DELETE FROM _ic_cell_leases WHERE cell_id = ?1",
            params![cell_id],
        )?;
        Ok(Ok(()))
    })
    .map_err(db_err)?
}

pub fn get_lease(cell_id: &str) -> Result<Option<LeaseRecord>, CellError> {
    // Always use update (consensus-backed) for ownership reads (ADR-0002).
    Db::update(|conn| {
        conn.query_optional(
            "SELECT cell_id, owner_id, epoch, expires_at_ns
             FROM _ic_cell_leases WHERE cell_id = ?1",
            params![cell_id],
            |row| {
                Ok(LeaseRecord {
                    cell_id: row.get::<String>(0)?,
                    owner_id: row.get::<String>(1)?,
                    epoch: row.get::<i64>(2)? as u64,
                    expires_at_ns: row.get::<i64>(3)? as u64,
                })
            },
        )
    })
    .map_err(db_err)
}

pub(crate) fn verify_fencing_epoch(
    conn: &Connection,
    cell_id: &str,
    owner_id: &str,
    expected_epoch: u64,
) -> Result<Result<(), CellError>, DbError> {
    let lease = conn.query_optional(
        "SELECT owner_id, epoch FROM _ic_cell_leases WHERE cell_id = ?1",
        params![cell_id],
        |row| Ok((row.get::<String>(0)?, row.get::<i64>(1)? as u64)),
    )?;

    match lease {
        None => Ok(Err(CellError::NotOwner {
            cell_id: cell_id.to_string(),
        })),
        Some((current_owner, actual_epoch)) => {
            if actual_epoch != expected_epoch {
                return Ok(Err(CellError::StaleEpoch {
                    cell_id: cell_id.to_string(),
                    expected_epoch,
                    actual_epoch,
                }));
            }
            if current_owner != owner_id {
                return Ok(Err(CellError::NotOwner {
                    cell_id: cell_id.to_string(),
                }));
            }
            Ok(Ok(()))
        }
    }
}
