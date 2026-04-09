//! CRUD and query operations against physical user tables.
//!
//! Each public(crate) function receives a `&Connection` (and `&mut HashSet<String>` cache
//! where table auto-creation is needed) and executes SQL against the physical
//! `_<table>_current` and `_<table>_history` tables managed by the schema module.
//!
//! Write operations follow the pattern:
//! ```text
//! schema::ensure_table / validate_table_name
//! versioning::begin_write  →  DML  →  versioning::commit
//! (on DML error: versioning::rollback)
//! ```

use std::collections::HashSet;

use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::document::{Document, QueryResult};
use crate::error::{Error, Result};
use crate::{schema, versioning};

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the physical current table for `table_name` exists in the DB.
fn table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
    let physical = format!("_{table_name}_current");
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        rusqlite::params![physical],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Returns `true` if a document with `id` exists in `_T_current`.
fn doc_exists(conn: &Connection, table_name: &str, id: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM _{table_name}_current WHERE _id = ?1"),
        rusqlite::params![id],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

/// Reads a single document from `_T_current` by `id`.
///
/// Returns `Error::DocumentNotFound` if no row is found.
fn read_current_doc(conn: &Connection, table_name: &str, id: &str) -> Result<Document> {
    let result = conn.query_row(
        &format!(
            "SELECT _id, json(_data), _valid_from, _txn_id \
             FROM _{table_name}_current WHERE _id = ?1"
        ),
        rusqlite::params![id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    );

    let (doc_id, data_str, valid_from, txn_id) = match result {
        Ok(row) => row,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            return Err(Error::DocumentNotFound(id.to_string()))
        }
        Err(e) => return Err(Error::Sqlite(e)),
    };

    let data_val: Value = serde_json::from_str(&data_str)?;
    let map = match data_val {
        Value::Object(m) => m,
        _ => unreachable!("stored data is always a JSON object"),
    };
    Ok(Document::new(doc_id, map, valid_from, txn_id))
}

/// Reads all documents from `_T_current`.
fn read_all_current(conn: &Connection, table_name: &str) -> Result<Vec<Document>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT _id, json(_data), _valid_from, _txn_id FROM _{table_name}_current"
    ))?;

    let raw_rows: Vec<(String, String, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    raw_rows
        .into_iter()
        .map(|(id, data_str, valid_from, txn_id)| {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            Ok(Document::new(id, map, valid_from, txn_id))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Public(crate) operations
// ---------------------------------------------------------------------------

/// Inserts a new document into `table`, auto-creating the table if needed.
///
/// `data` must be a JSON object; returns `Error::Json` for non-object inputs.
/// Returns the inserted [`Document`] including its generated UUIDv7 `_id`.
pub(crate) fn insert(
    conn: &Connection,
    cache: &mut HashSet<String>,
    table: &str,
    data: Value,
) -> Result<Document> {
    // Validate: data must be a JSON object
    let map: Map<String, Value> = serde_json::from_value(data)?;

    // Auto-create table (validates name, idempotent)
    schema::ensure_table(conn, cache, table)?;

    // Begin write transaction
    let (txn_id, ts) = versioning::begin_write(conn)?;

    let id = uuid::Uuid::now_v7().hyphenated().to_string();
    let data_json = serde_json::to_string(&Value::Object(map.clone()));

    let execute_result = data_json.map_err(Error::Json).and_then(|json_str| {
        conn.execute(
            &format!(
                "INSERT INTO _{table}_current (_id, _data, _valid_from, _txn_id) \
                 VALUES (?1, jsonb(?2), ?3, ?4)"
            ),
            rusqlite::params![id, json_str, ts, txn_id],
        )
        .map_err(Error::Sqlite)
    });

    match execute_result {
        Ok(_) => {
            versioning::commit(conn)?;
            Ok(Document::new(id, map, ts, txn_id))
        }
        Err(e) => {
            let _ = versioning::rollback(conn);
            Err(e)
        }
    }
}

/// Updates an existing document in `table` using JSON Merge Patch (RFC 7396).
///
/// Verifies the table and document exist before beginning the transaction.
/// The BEFORE UPDATE trigger copies the old row to history automatically.
/// Returns the updated [`Document`].
///
/// `patch` must be a JSON object. Non-object values (arrays, scalars, null) are
/// rejected with `Error::Json` because `json_patch` would produce non-object `_data`,
/// which would cause an `unreachable!` panic during document materialization.
pub(crate) fn update(
    conn: &Connection,
    table: &str,
    id: &str,
    patch: Value,
) -> Result<Document> {
    schema::validate_table_name(table)?;

    // Validate: patch must be a JSON object.
    // Non-object patches (arrays, scalars, null) can produce non-object _data after
    // json_patch, which would panic at the unreachable!() in materialization helpers.
    let patch_map: Map<String, Value> = serde_json::from_value(patch)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    if !doc_exists(conn, table, id)? {
        return Err(Error::DocumentNotFound(id.to_string()));
    }

    let patch_json = serde_json::to_string(&Value::Object(patch_map))?;
    let (txn_id, ts) = versioning::begin_write(conn)?;

    let execute_result = conn
        .execute(
            &format!(
                "UPDATE _{table}_current \
                 SET _data = json_patch(_data, jsonb(?1)), _valid_from = ?2, _txn_id = ?3 \
                 WHERE _id = ?4"
            ),
            rusqlite::params![patch_json, ts, txn_id, id],
        )
        .map_err(Error::Sqlite);

    match execute_result {
        Ok(_) => {
            versioning::commit(conn)?;
            read_current_doc(conn, table, id)
        }
        Err(e) => {
            let _ = versioning::rollback(conn);
            Err(e)
        }
    }
}

/// Deletes a document from `_T_current`.
///
/// The BEFORE DELETE trigger copies the old row to `_T_history` with `_op='DELETE'`.
/// Returns `Error::TableNotFound` / `Error::DocumentNotFound` if the target doesn't exist.
pub(crate) fn delete(conn: &Connection, table: &str, id: &str) -> Result<()> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    if !doc_exists(conn, table, id)? {
        return Err(Error::DocumentNotFound(id.to_string()));
    }

    let (_, _) = versioning::begin_write(conn)?;

    let execute_result = conn
        .execute(
            &format!("DELETE FROM _{table}_current WHERE _id = ?1"),
            rusqlite::params![id],
        )
        .map_err(Error::Sqlite);

    match execute_result {
        Ok(_) => {
            versioning::commit(conn)?;
            Ok(())
        }
        Err(e) => {
            let _ = versioning::rollback(conn);
            Err(e)
        }
    }
}

/// Permanently erases a document from both current state and history.
///
/// Idempotent — returns `Ok(())` if the table or document does not exist.
/// Deletes from `_T_current` (trigger may add to history), then deletes all
/// `_T_history` rows for that `id`, then logs the erasure to `_erasure_log`.
pub(crate) fn erase(conn: &Connection, table: &str, id: &str) -> Result<()> {
    schema::validate_table_name(table)?;

    // Idempotent: if the table doesn't exist, nothing to erase
    if !table_exists(conn, table)? {
        return Ok(());
    }

    let (_, ts) = versioning::begin_write(conn)?;

    let execute_result: Result<()> = (|| {
        // Delete from current (BEFORE DELETE trigger fires if doc exists, adds to history)
        conn.execute(
            &format!("DELETE FROM _{table}_current WHERE _id = ?1"),
            rusqlite::params![id],
        )?;
        // Delete ALL history entries for this id (including what trigger may have just added)
        conn.execute(
            &format!("DELETE FROM _{table}_history WHERE _id = ?1"),
            rusqlite::params![id],
        )?;
        // Log the erasure (even if doc didn't exist — idempotent per GDPR semantics)
        conn.execute(
            "INSERT INTO _erasure_log (table_name, doc_id, erased_at) VALUES (?1, ?2, ?3)",
            rusqlite::params![table, id, ts],
        )?;
        Ok(())
    })();

    match execute_result {
        Ok(()) => {
            versioning::commit(conn)?;
            Ok(())
        }
        Err(e) => {
            let _ = versioning::rollback(conn);
            Err(e)
        }
    }
}

/// Returns a single document from `_T_current` by `id`.
///
/// Returns `Error::TableNotFound` if the table doesn't exist,
/// `Error::DocumentNotFound` if no document with that `id` exists.
pub(crate) fn get(conn: &Connection, table: &str, id: &str) -> Result<Document> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    read_current_doc(conn, table, id)
}

/// Returns all current documents in `table`.
///
/// Returns `Error::TableNotFound` if the table doesn't exist.
/// Returns an empty [`QueryResult`] if the table is empty or all docs were deleted.
pub(crate) fn query(conn: &Connection, table: &str) -> Result<QueryResult> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    let docs = read_all_current(conn, table)?;
    Ok(QueryResult::new(docs))
}

/// Returns every version of every document in `table` (history UNION ALL current).
///
/// History rows have `_op` (e.g. `"UPDATE"` or `"DELETE"`) and `_valid_to`
/// (epoch-ms integer) injected into their data map as metadata fields.
/// Current rows have no `_op` or `_valid_to` metadata injected.
///
/// Returns `Error::TableNotFound` if the table doesn't exist.
pub(crate) fn query_all(conn: &Connection, table: &str) -> Result<QueryResult> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    let mut docs = Vec::new();

    // History rows: include _op and _valid_to metadata
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT _id, json(_data), _valid_from, _txn_id, _valid_to, _op \
             FROM _{table}_history \
             ORDER BY _valid_from"
        ))?;

        let raw: Vec<(String, String, i64, i64, i64, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (id, data_str, valid_from, txn_id, valid_to, op) in raw {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let mut map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            map.insert("_op".to_string(), Value::String(op));
            map.insert("_valid_to".to_string(), Value::Number(valid_to.into()));
            docs.push(Document::new(id, map, valid_from, txn_id));
        }
    }

    // Current rows: no _op / _valid_to metadata
    docs.extend(read_all_current(conn, table)?);

    Ok(QueryResult::new(docs))
}

/// Returns documents as they existed at `timestamp_iso` (ISO 8601 UTC string).
///
/// Inclusive boundary on `_valid_from`: a document inserted AT `ts` is returned.
///
/// Queries:
/// - history rows where `_valid_from <= ts AND _valid_to > ts`
/// - current rows where `_valid_from <= ts`
///
/// Returns `Error::TableNotFound` / `Error::InvalidTimestamp` as appropriate.
pub(crate) fn query_at(conn: &Connection, table: &str, timestamp_iso: &str) -> Result<QueryResult> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    let ts = iso8601_to_epoch_ms(timestamp_iso)?;
    let mut docs = Vec::new();

    // History rows: existed at ts
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT _id, json(_data), _valid_from, _txn_id, _valid_to, _op \
             FROM _{table}_history \
             WHERE _valid_from <= ?1 AND _valid_to > ?1"
        ))?;

        let raw: Vec<(String, String, i64, i64, i64, String)> = stmt
            .query_map(rusqlite::params![ts], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (id, data_str, valid_from, txn_id, valid_to, op) in raw {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let mut map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            map.insert("_op".to_string(), Value::String(op));
            map.insert("_valid_to".to_string(), Value::Number(valid_to.into()));
            docs.push(Document::new(id, map, valid_from, txn_id));
        }
    }

    // Current rows: existed at ts
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT _id, json(_data), _valid_from, _txn_id \
             FROM _{table}_current \
             WHERE _valid_from <= ?1"
        ))?;

        let raw: Vec<(String, String, i64, i64)> = stmt
            .query_map(rusqlite::params![ts], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (id, data_str, valid_from, txn_id) in raw {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            docs.push(Document::new(id, map, valid_from, txn_id));
        }
    }

    Ok(QueryResult::new(docs))
}

/// Returns all versions of documents active during the half-open range `[from_iso, to_iso)`.
///
/// Queries:
/// - history rows where `_valid_from < to AND _valid_to > from`
/// - current rows where `_valid_from < to`
///
/// Returns `Error::TableNotFound` / `Error::InvalidTimestamp` as appropriate.
pub(crate) fn query_between(
    conn: &Connection,
    table: &str,
    from_iso: &str,
    to_iso: &str,
) -> Result<QueryResult> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    let from_ts = iso8601_to_epoch_ms(from_iso)?;
    let to_ts = iso8601_to_epoch_ms(to_iso)?;

    // Guard: reversed or equal ranges produce no valid interval — return empty result.
    if from_ts >= to_ts {
        return Ok(QueryResult::new(vec![]));
    }

    let mut docs = Vec::new();

    // History rows: active during [from, to)
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT _id, json(_data), _valid_from, _txn_id, _valid_to, _op \
             FROM _{table}_history \
             WHERE _valid_from < ?2 AND _valid_to > ?1"
        ))?;

        let raw: Vec<(String, String, i64, i64, i64, String)> = stmt
            .query_map(rusqlite::params![from_ts, to_ts], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (id, data_str, valid_from, txn_id, valid_to, op) in raw {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let mut map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            map.insert("_op".to_string(), Value::String(op));
            map.insert("_valid_to".to_string(), Value::Number(valid_to.into()));
            docs.push(Document::new(id, map, valid_from, txn_id));
        }
    }

    // Current rows: active during [from, to)
    {
        let mut stmt = conn.prepare(&format!(
            "SELECT _id, json(_data), _valid_from, _txn_id \
             FROM _{table}_current \
             WHERE _valid_from < ?1"
        ))?;

        let raw: Vec<(String, String, i64, i64)> = stmt
            .query_map(rusqlite::params![to_ts], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (id, data_str, valid_from, txn_id) in raw {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            docs.push(Document::new(id, map, valid_from, txn_id));
        }
    }

    Ok(QueryResult::new(docs))
}

// ---------------------------------------------------------------------------
// Timestamp parsing helpers
// ---------------------------------------------------------------------------

/// Parses an ISO 8601 UTC timestamp string into epoch milliseconds.
///
/// Expected format: `YYYY-MM-DDTHH:MM:SS.mmmZ` (exactly 24 characters).
pub(crate) fn iso8601_to_epoch_ms(s: &str) -> Result<i64> {
    if s.len() != 24 || !s.ends_with('Z') {
        return Err(Error::InvalidTimestamp(format!(
            "expected format 'YYYY-MM-DDTHH:MM:SS.mmmZ' (24 chars), got: {s:?}"
        )));
    }
    let b = s.as_bytes();
    // Validate separator positions
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':'
        || b[19] != b'.'
    {
        return Err(Error::InvalidTimestamp(format!(
            "malformed ISO 8601 separators in: {s:?}"
        )));
    }

    let year  = parse_digits_i64(&b[0..4])?  as i32;
    let month = parse_digits_i64(&b[5..7])?  as u32;
    let day   = parse_digits_i64(&b[8..10])? as u32;
    let hour  = parse_digits_i64(&b[11..13])?;
    let min   = parse_digits_i64(&b[14..16])?;
    let sec   = parse_digits_i64(&b[17..19])?;
    let ms    = parse_digits_i64(&b[20..23])?;

    if !(1..=12).contains(&month)
        || day < 1
        || hour > 23
        || min > 59
        || sec > 59
    {
        return Err(Error::InvalidTimestamp(format!(
            "out-of-range field in: {s:?}"
        )));
    }

    // Validate day against the actual calendar length for this month/year.
    // For example, 2024-02-31 and 2024-04-31 are rejected even though
    // days_from_civil would silently normalise them.
    let max_day = days_in_month(year, month);
    if day > max_day {
        return Err(Error::InvalidTimestamp(format!(
            "invalid day {day} for {year}-{month:02}: month has {max_day} days"
        )));
    }

    let days = days_from_civil(year, month, day);
    let epoch_secs = days * 86_400 + hour * 3_600 + min * 60 + sec;
    Ok(epoch_secs * 1_000 + ms)
}

/// Returns the number of days in the given month (1-indexed), accounting for leap years.
///
/// Returns 0 for any month value outside 1–12 (caller must validate the month
/// before calling this function).
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            // Gregorian leap-year rule
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 0, // caller has already validated 1..=12
    }
}

/// Converts (year, month, day) to days since Unix epoch (1970-01-01).
///
/// Uses Howard Hinnant's algorithm — the inverse of `civil_from_days` in
/// `document.rs`.
fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let (y, m) = if m <= 2 {
        (y as i64 - 1, m + 9)
    } else {
        (y as i64, m - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * m as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parses a slice of ASCII digit bytes into an `i64`.
fn parse_digits_i64(bytes: &[u8]) -> Result<i64> {
    let mut n: i64 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(Error::InvalidTimestamp(format!(
                "non-digit byte 0x{b:02X} in timestamp"
            )));
        }
        n = n * 10 + (b - b'0') as i64;
    }
    Ok(n)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rusqlite::Connection;
    use serde_json::json;

    use crate::error::Error;
    use crate::schema::init_system_tables;

    use super::*;

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Opens an in-memory SQLite connection with all system tables initialised.
    fn open_conn() -> (Connection, HashSet<String>) {
        let conn = Connection::open_in_memory().unwrap();
        init_system_tables(&conn).unwrap();
        (conn, HashSet::new())
    }

    /// Checks that a UUIDv7-format string is structurally valid (36 chars, hyphens, version=7).
    fn is_uuidv7(s: &str) -> bool {
        if s.len() != 36 {
            return false;
        }
        let bytes = s.as_bytes();
        if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
            return false;
        }
        // Version nibble is the first character of the 3rd group (position 14)
        bytes[14] == b'7'
    }

    /// Returns the number of rows in `_erasure_log` for the given (table, id) pair.
    fn erasure_log_count(conn: &Connection, table: &str, id: &str) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM _erasure_log WHERE table_name=?1 AND doc_id=?2",
            rusqlite::params![table, id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// Returns the number of history rows for `id` in `_<table>_history`.
    fn history_count(conn: &Connection, table: &str, id: &str) -> i64 {
        conn.query_row(
            &format!("SELECT COUNT(*) FROM _{table}_history WHERE _id=?1"),
            rusqlite::params![id],
            |row| row.get(0),
        )
        .unwrap()
    }

    // ------------------------------------------------------------------
    // Insert — VAL-INS-001 to VAL-INS-008, VAL-BOUND-001, VAL-BOUND-002
    // ------------------------------------------------------------------

    /// VAL-INS-001: insert returns Document with valid UUIDv7 _id
    #[test]
    fn insert_returns_document_with_uuidv7_id() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"name": "foo"})).unwrap();
        assert!(is_uuidv7(doc.id()), "id is not valid UUIDv7: {}", doc.id());
    }

    /// VAL-INS-002: returned document data matches input
    #[test]
    fn insert_returned_data_matches_input() {
        let (conn, mut cache) = open_conn();
        let doc = insert(
            &conn,
            &mut cache,
            "events",
            json!({"name": "foo", "count": 42}),
        )
        .unwrap();
        assert_eq!(doc.get("name"), Some(&json!("foo")));
        assert_eq!(doc.get("count"), Some(&json!(42)));
    }

    /// VAL-INS-003: returned document has valid system_time (ISO 8601)
    #[test]
    fn insert_returned_document_has_system_time() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        let ts = doc.system_time();
        // Should be 24-char ISO 8601: "YYYY-MM-DDTHH:MM:SS.mmmZ"
        assert_eq!(ts.len(), 24, "unexpected system_time format: {ts}");
        assert!(ts.ends_with('Z'));
    }

    /// VAL-INS-004: returned document has positive txn_id
    #[test]
    fn insert_returned_document_has_txn_id() {
        let (conn, mut cache) = open_conn();
        let doc1 = insert(&conn, &mut cache, "events", json!({"a": 1})).unwrap();
        let doc2 = insert(&conn, &mut cache, "events", json!({"b": 2})).unwrap();
        assert!(doc1.txn_id() > 0, "txn_id should be positive");
        assert!(doc2.txn_id() >= doc1.txn_id(), "txn_ids should be non-decreasing");
    }

    /// VAL-INS-005: table auto-created on first insert (schema-last)
    #[test]
    fn insert_auto_creates_table() {
        let (conn, mut cache) = open_conn();
        // Table "newtable" does not exist yet
        let result = insert(&conn, &mut cache, "newtable", json!({"x": 1}));
        assert!(result.is_ok(), "insert should auto-create table: {result:?}");
        // Subsequent query should work
        let qr = query(&conn, "newtable").unwrap();
        assert_eq!(qr.len(), 1);
    }

    /// VAL-INS-006: multiple inserts produce distinct UUIDs
    #[test]
    fn insert_produces_distinct_ids() {
        let (conn, mut cache) = open_conn();
        let doc1 = insert(&conn, &mut cache, "events", json!({"n": 1})).unwrap();
        let doc2 = insert(&conn, &mut cache, "events", json!({"n": 2})).unwrap();
        assert_ne!(doc1.id(), doc2.id(), "IDs should be distinct");
    }

    /// VAL-INS-007: UUIDv7 IDs are time-sortable (later insert has lexicographically greater ID)
    #[test]
    fn insert_uuidv7_ids_are_time_sortable() {
        let (conn, mut cache) = open_conn();
        let doc1 = insert(&conn, &mut cache, "events", json!({"n": 1})).unwrap();
        // A tiny sleep is not needed because UUIDv7 sub-ms precision ensures ordering
        let doc2 = insert(&conn, &mut cache, "events", json!({"n": 2})).unwrap();
        assert!(
            doc1.id() <= doc2.id(),
            "UUIDv7 IDs should be time-sortable: {} vs {}",
            doc1.id(),
            doc2.id()
        );
    }

    /// VAL-INS-008: inserted document immediately queryable
    #[test]
    fn insert_immediately_queryable() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 99})).unwrap();

        let fetched = get(&conn, "events", doc.id()).unwrap();
        assert_eq!(fetched.id(), doc.id());
        assert_eq!(fetched.get("x"), Some(&json!(99)));

        let qr = query(&conn, "events").unwrap();
        assert_eq!(qr.len(), 1);
    }

    /// VAL-BOUND-001: insert empty JSON object {} succeeds
    #[test]
    fn insert_empty_object_succeeds() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({})).unwrap();
        assert!(is_uuidv7(doc.id()));
        // Data should be empty object
        assert_eq!(doc.data().len(), 0);
        // Should be queryable
        let fetched = get(&conn, "events", doc.id()).unwrap();
        assert_eq!(fetched.data().len(), 0);
    }

    /// VAL-BOUND-002: insert non-object JSON returns Err
    #[test]
    fn insert_non_object_returns_error() {
        let (conn, mut cache) = open_conn();
        assert!(
            insert(&conn, &mut cache, "events", json!([1, 2, 3])).is_err(),
            "array should be rejected"
        );
        assert!(
            insert(&conn, &mut cache, "events", json!(42)).is_err(),
            "scalar should be rejected"
        );
        assert!(
            insert(&conn, &mut cache, "events", json!("string")).is_err(),
            "string scalar should be rejected"
        );
        assert!(
            insert(&conn, &mut cache, "events", json!(null)).is_err(),
            "null should be rejected"
        );
    }

    // ------------------------------------------------------------------
    // Update — VAL-UPD-001 to VAL-UPD-008
    // ------------------------------------------------------------------

    /// VAL-UPD-001: update patches _data field, preserves unmentioned keys
    #[test]
    fn update_patches_data_field() {
        let (conn, mut cache) = open_conn();
        let doc = insert(
            &conn,
            &mut cache,
            "events",
            json!({"name": "old", "count": 1}),
        )
        .unwrap();
        let updated = update(&conn, "events", doc.id(), json!({"name": "new"})).unwrap();
        assert_eq!(updated.get("name"), Some(&json!("new")));
        assert_eq!(updated.get("count"), Some(&json!(1)), "count should be preserved");
    }

    /// VAL-UPD-002: updated document has new system_time and txn_id
    #[test]
    fn update_returns_document_with_new_metadata() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        let updated = update(&conn, "events", doc.id(), json!({"x": 2})).unwrap();
        assert!(updated.txn_id() >= doc.txn_id(), "txn_id should be non-decreasing");
    }

    /// VAL-UPD-003: original version preserved in history after update
    #[test]
    fn update_preserves_original_in_history() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        update(&conn, "events", doc.id(), json!({"x": 2})).unwrap();
        let hist_count = history_count(&conn, "events", doc.id());
        assert_eq!(hist_count, 1, "should have 1 history entry after update");
    }

    /// VAL-UPD-004: history record has _op='UPDATE' and _valid_to set
    #[test]
    fn update_history_has_correct_op_and_valid_to() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        update(&conn, "events", doc.id(), json!({"x": 2})).unwrap();

        let (op, valid_to): (String, i64) = conn
            .query_row(
                "SELECT _op, _valid_to FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(op, "UPDATE");
        assert!(valid_to > 0, "_valid_to should be a positive epoch-ms value");
    }

    /// VAL-UPD-005: update on non-existent document returns DocumentNotFound
    #[test]
    fn update_nonexistent_doc_returns_error() {
        let (conn, mut cache) = open_conn();
        // Create the table first
        schema::ensure_table(&conn, &mut cache, "events").unwrap();
        let result = update(&conn, "events", "nonexistent-id", json!({"x": 1}));
        assert!(
            matches!(result, Err(Error::DocumentNotFound(_))),
            "expected DocumentNotFound, got {result:?}"
        );
    }

    /// VAL-UPD-006: multiple updates create multiple history entries
    #[test]
    fn multiple_updates_create_multiple_history_entries() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"v": 0})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 1})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 2})).unwrap();
        let hist_count = history_count(&conn, "events", doc.id());
        assert_eq!(hist_count, 2, "should have 2 history entries after 2 updates");
    }

    /// VAL-UPD-007: partial patch preserves existing keys
    #[test]
    fn partial_patch_preserves_existing_keys() {
        let (conn, mut cache) = open_conn();
        let doc = insert(
            &conn,
            &mut cache,
            "events",
            json!({"a": 1, "b": 2, "c": 3}),
        )
        .unwrap();
        let updated = update(&conn, "events", doc.id(), json!({"b": 20})).unwrap();
        assert_eq!(updated.get("a"), Some(&json!(1)));
        assert_eq!(updated.get("b"), Some(&json!(20)));
        assert_eq!(updated.get("c"), Some(&json!(3)));
    }

    /// VAL-UPD-008: json_patch null removes key (RFC 7396)
    #[test]
    fn patch_null_removes_key() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"a": 1, "b": 2})).unwrap();
        let updated = update(&conn, "events", doc.id(), json!({"b": null})).unwrap();
        assert_eq!(updated.get("a"), Some(&json!(1)));
        assert!(updated.get("b").is_none(), "key 'b' should be removed after null patch");
    }

    /// VAL-BOUND-003: update with empty patch returns Ok with unchanged data
    #[test]
    fn update_empty_patch_returns_unchanged_data() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 42})).unwrap();
        let updated = update(&conn, "events", doc.id(), json!({})).unwrap();
        assert_eq!(updated.get("x"), Some(&json!(42)));
    }

    // ------------------------------------------------------------------
    // Delete — VAL-DEL-001 to VAL-DEL-006, VAL-BOUND-006
    // ------------------------------------------------------------------

    /// VAL-DEL-001: deleted document excluded from query()
    #[test]
    fn delete_excluded_from_query() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();
        let qr = query(&conn, "events").unwrap();
        assert!(qr.is_empty(), "query should exclude deleted document");
    }

    /// VAL-DEL-002: deleted document excluded from get()
    #[test]
    fn delete_excluded_from_get() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();
        let result = get(&conn, "events", doc.id());
        assert!(
            matches!(result, Err(Error::DocumentNotFound(_))),
            "get should return DocumentNotFound after delete, got {result:?}"
        );
    }

    /// VAL-DEL-003: deleted document in history with _op='DELETE'
    #[test]
    fn delete_preserved_in_history_with_op_delete() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();

        let op: String = conn
            .query_row(
                "SELECT _op FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(op, "DELETE");
    }

    /// VAL-DEL-004: history _valid_to set on delete
    #[test]
    fn delete_history_valid_to_set() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();

        let valid_to: i64 = conn
            .query_row(
                "SELECT _valid_to FROM _events_history WHERE _id = ?1 AND _op='DELETE'",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(valid_to > 0, "_valid_to should be positive");
    }

    /// VAL-DEL-005: delete non-existent document returns DocumentNotFound
    #[test]
    fn delete_nonexistent_doc_returns_error() {
        let (conn, mut cache) = open_conn();
        schema::ensure_table(&conn, &mut cache, "events").unwrap();
        let result = delete(&conn, "events", "no-such-id");
        assert!(
            matches!(result, Err(Error::DocumentNotFound(_))),
            "expected DocumentNotFound, got {result:?}"
        );
    }

    /// VAL-DEL-006: delete after update preserves both UPDATE and DELETE in history
    #[test]
    fn delete_after_update_preserves_full_history() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"v": 0})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "should have UPDATE + DELETE history entries");

        let ops: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT _op FROM _events_history WHERE _id = ?1 ORDER BY _valid_from",
                )
                .unwrap();
            stmt.query_map(rusqlite::params![doc.id()], |row| row.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert!(ops.contains(&"UPDATE".to_string()));
        assert!(ops.contains(&"DELETE".to_string()));
    }

    /// VAL-BOUND-006: delete from table with no matching doc returns DocumentNotFound
    #[test]
    fn delete_from_table_no_matching_doc() {
        let (conn, mut cache) = open_conn();
        schema::ensure_table(&conn, &mut cache, "t").unwrap();
        let result = delete(&conn, "t", "any-id");
        assert!(
            matches!(result, Err(Error::DocumentNotFound(_))),
            "expected DocumentNotFound, got {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // Erase — VAL-ERASE-001 to VAL-ERASE-005
    // ------------------------------------------------------------------

    /// VAL-ERASE-001: erased document not in current state
    #[test]
    fn erase_removes_from_current_state() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        erase(&conn, "events", doc.id()).unwrap();

        assert!(query(&conn, "events").unwrap().is_empty());
        assert!(matches!(
            get(&conn, "events", doc.id()),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-ERASE-002: erased document not in history
    #[test]
    fn erase_removes_from_history() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        update(&conn, "events", doc.id(), json!({"x": 2})).unwrap();
        erase(&conn, "events", doc.id()).unwrap();

        assert_eq!(
            history_count(&conn, "events", doc.id()),
            0,
            "history should be empty after erase"
        );
    }

    /// VAL-ERASE-003: erasure logged in _erasure_log
    #[test]
    fn erase_logs_to_erasure_log() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        erase(&conn, "events", doc.id()).unwrap();

        let count = erasure_log_count(&conn, "events", doc.id());
        assert_eq!(count, 1, "_erasure_log should have 1 entry");

        // Verify erased_at is set (non-null positive value)
        let erased_at: i64 = conn
            .query_row(
                "SELECT erased_at FROM _erasure_log WHERE table_name=?1 AND doc_id=?2",
                rusqlite::params!["events", doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(erased_at > 0, "erased_at should be a positive timestamp");
    }

    /// VAL-ERASE-004: erase removes ALL history entries for a previously-updated doc
    #[test]
    fn erase_removes_all_history_entries() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"v": 0})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 1})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 2})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 3})).unwrap();

        erase(&conn, "events", doc.id()).unwrap();

        // No current
        assert!(matches!(
            get(&conn, "events", doc.id()),
            Err(Error::DocumentNotFound(_))
        ));
        // No history
        assert_eq!(history_count(&conn, "events", doc.id()), 0);
    }

    /// VAL-ERASE-005: erase is idempotent (non-existent doc returns Ok)
    #[test]
    fn erase_is_idempotent() {
        let (conn, mut cache) = open_conn();
        schema::ensure_table(&conn, &mut cache, "events").unwrap();
        // Erase a doc that never existed — should succeed
        assert!(
            erase(&conn, "events", "never-existed").is_ok(),
            "erase of non-existent doc should return Ok"
        );
        // Erase a table that never existed — should succeed
        assert!(
            erase(&conn, "nonexistent_table", "any-id").is_ok(),
            "erase on non-existent table should return Ok"
        );
        // Double erase
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        erase(&conn, "events", doc.id()).unwrap();
        assert!(
            erase(&conn, "events", doc.id()).is_ok(),
            "double-erase should return Ok"
        );
    }

    // ------------------------------------------------------------------
    // Get — VAL-QRY-011, VAL-QRY-012
    // ------------------------------------------------------------------

    /// VAL-QRY-011: get() returns single document by ID
    #[test]
    fn get_returns_document_by_id() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"name": "Alice"})).unwrap();
        let fetched = get(&conn, "events", doc.id()).unwrap();
        assert_eq!(fetched.id(), doc.id());
        assert_eq!(fetched.get("name"), Some(&json!("Alice")));
    }

    /// VAL-QRY-012: get() on non-existent ID returns DocumentNotFound
    #[test]
    fn get_nonexistent_id_returns_error() {
        let (conn, mut cache) = open_conn();
        schema::ensure_table(&conn, &mut cache, "events").unwrap();
        let result = get(&conn, "events", "no-such-id");
        assert!(
            matches!(result, Err(Error::DocumentNotFound(_))),
            "expected DocumentNotFound, got {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // Query — VAL-QRY-001, VAL-QRY-002, VAL-QRY-003, VAL-QRY-013
    // ------------------------------------------------------------------

    /// VAL-QRY-001: query() returns current versions only (latest)
    #[test]
    fn query_returns_current_versions_only() {
        let (conn, mut cache) = open_conn();
        let doc1 = insert(&conn, &mut cache, "events", json!({"n": 1})).unwrap();
        let doc2 = insert(&conn, &mut cache, "events", json!({"n": 2})).unwrap();
        let doc3 = insert(&conn, &mut cache, "events", json!({"n": 3})).unwrap();
        // Update doc1 — should still only see 3 current docs
        update(&conn, "events", doc1.id(), json!({"n": 10})).unwrap();

        let qr = query(&conn, "events").unwrap();
        assert_eq!(qr.len(), 3, "should see 3 current documents");

        // doc1 should have updated value
        let updated_doc = qr.documents().iter().find(|d| d.id() == doc1.id()).unwrap();
        assert_eq!(updated_doc.get("n"), Some(&json!(10)));

        // Other docs unchanged
        let d2 = qr.documents().iter().find(|d| d.id() == doc2.id()).unwrap();
        assert_eq!(d2.get("n"), Some(&json!(2)));
        let d3 = qr.documents().iter().find(|d| d.id() == doc3.id()).unwrap();
        assert_eq!(d3.get("n"), Some(&json!(3)));
    }

    /// VAL-QRY-002: query() on empty table returns empty QueryResult
    #[test]
    fn query_empty_table_returns_empty() {
        let (conn, mut cache) = open_conn();
        schema::ensure_table(&conn, &mut cache, "events").unwrap();
        let qr = query(&conn, "events").unwrap();
        assert!(qr.is_empty());
        assert_eq!(qr.len(), 0);
    }

    /// VAL-QRY-003: query() excludes deleted documents
    #[test]
    fn query_excludes_deleted_documents() {
        let (conn, mut cache) = open_conn();
        let doc1 = insert(&conn, &mut cache, "events", json!({"n": 1})).unwrap();
        let doc2 = insert(&conn, &mut cache, "events", json!({"n": 2})).unwrap();
        delete(&conn, "events", doc1.id()).unwrap();

        let qr = query(&conn, "events").unwrap();
        assert_eq!(qr.len(), 1);
        assert_eq!(qr.documents()[0].id(), doc2.id());
    }

    /// VAL-QRY-013: query() on non-existent table returns TableNotFound
    #[test]
    fn query_nonexistent_table_returns_error() {
        let (conn, _cache) = open_conn();
        let result = query(&conn, "never_created");
        assert!(
            matches!(result, Err(Error::TableNotFound(_))),
            "expected TableNotFound, got {result:?}"
        );
    }

    /// VAL-BOUND-007: query after all documents deleted returns empty (not TableNotFound)
    #[test]
    fn query_after_all_deleted_returns_empty() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();

        let result = query(&conn, "events");
        assert!(result.is_ok(), "query should return Ok, not error: {result:?}");
        assert!(result.unwrap().is_empty());
    }

    // ------------------------------------------------------------------
    // Regression: history entries across doc lifecycle
    // ------------------------------------------------------------------

    /// History has correct _op sequence: INSERT (no entry) → UPDATE → DELETE
    #[test]
    fn history_op_sequence_insert_update_delete() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"v": 0})).unwrap();
        update(&conn, "events", doc.id(), json!({"v": 1})).unwrap();
        delete(&conn, "events", doc.id()).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "should have UPDATE + DELETE in history");
    }

    /// Update on non-existent table returns TableNotFound
    #[test]
    fn update_nonexistent_table_returns_error() {
        let (conn, _cache) = open_conn();
        let result = update(&conn, "no_table", "some-id", json!({"x": 1}));
        assert!(
            matches!(result, Err(Error::TableNotFound(_))),
            "expected TableNotFound, got {result:?}"
        );
    }

    // ------------------------------------------------------------------
    // Timestamp parsing — iso8601_to_epoch_ms round-trip
    // ------------------------------------------------------------------

    /// Unix epoch round-trips correctly
    #[test]
    fn iso8601_roundtrip_epoch_zero() {
        // epoch_ms_to_iso8601(0) == "1970-01-01T00:00:00.000Z"
        let iso = "1970-01-01T00:00:00.000Z";
        let ms = iso8601_to_epoch_ms(iso).unwrap();
        assert_eq!(ms, 0);
    }

    /// Known timestamp round-trip: 2024-01-01
    #[test]
    fn iso8601_roundtrip_known_date() {
        let iso = "2024-01-01T00:00:00.000Z";
        let ms = iso8601_to_epoch_ms(iso).unwrap();
        assert_eq!(ms, 1_704_067_200_000);
    }

    /// Milliseconds preserved
    #[test]
    fn iso8601_preserves_milliseconds() {
        let ms = iso8601_to_epoch_ms("1970-01-01T00:00:00.999Z").unwrap();
        assert_eq!(ms, 999);
    }

    /// Invalid format returns InvalidTimestamp
    #[test]
    fn iso8601_invalid_format_returns_error() {
        assert!(iso8601_to_epoch_ms("not-a-date").is_err());
        assert!(iso8601_to_epoch_ms("2024-01-01").is_err());
        assert!(iso8601_to_epoch_ms("").is_err());
    }

    // ------------------------------------------------------------------
    // query_all — VAL-QRY-004, VAL-QRY-005
    // ------------------------------------------------------------------

    /// VAL-QRY-004: query_all() returns all versions (history + current)
    #[test]
    fn query_all_returns_all_versions() {
        use std::thread::sleep;
        use std::time::Duration;
        let (conn, mut cache) = open_conn();

        // Insert A, then update A, then insert B
        let a = insert(&conn, &mut cache, "events", json!({"n": "a_v0"})).unwrap();
        sleep(Duration::from_millis(10));
        update(&conn, "events", a.id(), json!({"n": "a_v1"})).unwrap();
        sleep(Duration::from_millis(10));
        insert(&conn, &mut cache, "events", json!({"n": "b"})).unwrap();

        // query_all: 1 history row (a_v0 UPDATE) + 2 current rows (a_v1, b) = 3
        let qr = query_all(&conn, "events").unwrap();
        assert_eq!(qr.len(), 3, "should see 3 total version entries");
    }

    /// query_all on non-existent table returns TableNotFound
    #[test]
    fn query_all_nonexistent_table_returns_error() {
        let (conn, _cache) = open_conn();
        assert!(matches!(
            query_all(&conn, "never_created"),
            Err(Error::TableNotFound(_))
        ));
    }

    /// VAL-QRY-005: query_all after insert→update→delete shows lifecycle
    /// Note: with BEFORE-only triggers (no AFTER INSERT), insert→update→delete
    /// produces 2 history rows: one UPDATE and one DELETE. query_all = 2.
    #[test]
    fn query_all_lifecycle_shows_correct_op_sequence() {
        use std::thread::sleep;
        use std::time::Duration;
        let (conn, mut cache) = open_conn();

        let doc = insert(&conn, &mut cache, "events", json!({"v": 0})).unwrap();
        sleep(Duration::from_millis(10));
        update(&conn, "events", doc.id(), json!({"v": 1})).unwrap();
        sleep(Duration::from_millis(10));
        delete(&conn, "events", doc.id()).unwrap();

        let qr = query_all(&conn, "events").unwrap();
        // 2 history rows (UPDATE + DELETE) + 0 current = 2
        assert_eq!(qr.len(), 2, "should have 2 versioned entries after lifecycle");

        // Check _op values are present
        let ops: Vec<String> = qr
            .documents()
            .iter()
            .filter_map(|d| d.get("_op").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();
        assert!(ops.contains(&"UPDATE".to_string()), "_op=UPDATE should be present");
        assert!(ops.contains(&"DELETE".to_string()), "_op=DELETE should be present");
    }

    // ------------------------------------------------------------------
    // query_at — VAL-QRY-006, VAL-QRY-007, VAL-QRY-008, VAL-BOUND-004
    // ------------------------------------------------------------------

    /// VAL-QRY-007: query_at() before any inserts returns empty
    #[test]
    fn query_at_before_inserts_returns_empty() {
        let (conn, mut cache) = open_conn();
        schema::ensure_table(&conn, &mut cache, "events").unwrap();

        let qr = query_at(&conn, "events", "1970-01-01T00:00:00.000Z").unwrap();
        assert!(qr.is_empty(), "query_at before any data should return empty");
    }

    /// VAL-QRY-006: query_at() returns state at specific timestamp
    #[test]
    fn query_at_returns_state_at_timestamp() {
        use std::thread::sleep;
        use std::time::Duration;
        let (conn, mut cache) = open_conn();

        let doc = insert(&conn, &mut cache, "events", json!({"v": "original"})).unwrap();
        let t1 = doc.system_time(); // ISO timestamp after insert
        sleep(Duration::from_millis(10));
        let updated = update(&conn, "events", doc.id(), json!({"v": "updated"})).unwrap();
        let t2 = updated.system_time();

        // query_at(t1) should return original
        let at_t1 = query_at(&conn, "events", &t1).unwrap();
        assert_eq!(at_t1.len(), 1);
        assert_eq!(at_t1.documents()[0].get("v"), Some(&json!("original")));

        // query_at(t2) should return updated
        let at_t2 = query_at(&conn, "events", &t2).unwrap();
        assert_eq!(at_t2.len(), 1);
        assert_eq!(at_t2.documents()[0].get("v"), Some(&json!("updated")));
    }

    /// VAL-QRY-008: query_at() after delete returns empty for that document
    #[test]
    fn query_at_after_delete_returns_empty() {
        use std::thread::sleep;
        use std::time::Duration;
        let (conn, mut cache) = open_conn();

        let doc = insert(&conn, &mut cache, "events", json!({"v": 1})).unwrap();
        sleep(Duration::from_millis(10));
        delete(&conn, "events", doc.id()).unwrap();

        // A future timestamp should show empty
        let future = "2099-01-01T00:00:00.000Z";
        let qr = query_at(&conn, "events", future).unwrap();
        assert!(
            qr.documents().iter().all(|d| d.id() != doc.id()),
            "deleted doc should not appear in query_at after deletion"
        );
    }

    /// VAL-BOUND-004: query_at at exact insert timestamp is inclusive
    #[test]
    fn query_at_exact_insert_timestamp_inclusive() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"v": "at_insert"})).unwrap();
        let t_insert = doc.system_time();

        // query_at at the exact insert timestamp should include the document
        let qr = query_at(&conn, "events", &t_insert).unwrap();
        let found = qr.documents().iter().any(|d| d.id() == doc.id());
        assert!(found, "document should be visible at its exact insert timestamp (inclusive boundary)");
    }

    // ------------------------------------------------------------------
    // query_between — VAL-QRY-009, VAL-QRY-010
    // ------------------------------------------------------------------

    /// VAL-QRY-010: query_between() before all data returns empty
    #[test]
    fn query_between_before_data_returns_empty() {
        let (conn, mut cache) = open_conn();
        insert(&conn, &mut cache, "events", json!({"v": 1})).unwrap();

        let qr = query_between(&conn, "events", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.001Z").unwrap();
        assert!(qr.is_empty(), "query_between entirely before data should return empty");
    }

    // ------------------------------------------------------------------
    // Fix: update() rejects non-object patches (Issue #1)
    // ------------------------------------------------------------------

    /// update() with an array patch returns Err (not a panic)
    #[test]
    fn update_with_array_patch_returns_error() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        let result = update(&conn, "events", doc.id(), json!([1, 2, 3]));
        assert!(
            result.is_err(),
            "array patch should return Err, got Ok"
        );
    }

    /// update() with a scalar (integer) patch returns Err
    #[test]
    fn update_with_scalar_patch_returns_error() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        let result = update(&conn, "events", doc.id(), json!(42));
        assert!(
            result.is_err(),
            "scalar patch should return Err, got Ok"
        );
    }

    /// update() with a null patch returns Err
    #[test]
    fn update_with_null_patch_returns_error() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        let result = update(&conn, "events", doc.id(), json!(null));
        assert!(
            result.is_err(),
            "null patch should return Err, got Ok"
        );
    }

    /// update() with a string patch returns Err
    #[test]
    fn update_with_string_patch_returns_error() {
        let (conn, mut cache) = open_conn();
        let doc = insert(&conn, &mut cache, "events", json!({"x": 1})).unwrap();
        let result = update(&conn, "events", doc.id(), json!("a string"));
        assert!(
            result.is_err(),
            "string patch should return Err, got Ok"
        );
    }

    // ------------------------------------------------------------------
    // Fix: iso8601_to_epoch_ms() rejects invalid calendar dates (Issue #2)
    // ------------------------------------------------------------------

    /// Feb 31 is invalid (February has at most 29 days)
    #[test]
    fn iso8601_rejects_feb_31() {
        let result = iso8601_to_epoch_ms("2024-02-31T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "2024-02-31 should be InvalidTimestamp, got {result:?}"
        );
    }

    /// Feb 30 is always invalid
    #[test]
    fn iso8601_rejects_feb_30() {
        let result = iso8601_to_epoch_ms("2024-02-30T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "2024-02-30 should be InvalidTimestamp, got {result:?}"
        );
    }

    /// Feb 29 is valid in a leap year
    #[test]
    fn iso8601_accepts_feb_29_leap_year() {
        // 2024 is a leap year (divisible by 4, not by 100)
        let result = iso8601_to_epoch_ms("2024-02-29T00:00:00.000Z");
        assert!(result.is_ok(), "2024-02-29 should be valid: {result:?}");
    }

    /// Feb 29 is invalid in a non-leap year
    #[test]
    fn iso8601_rejects_feb_29_non_leap_year() {
        // 2023 is not a leap year
        let result = iso8601_to_epoch_ms("2023-02-29T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "2023-02-29 should be InvalidTimestamp, got {result:?}"
        );
    }

    /// April 31 is invalid (April has 30 days)
    #[test]
    fn iso8601_rejects_apr_31() {
        let result = iso8601_to_epoch_ms("2024-04-31T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "2024-04-31 should be InvalidTimestamp, got {result:?}"
        );
    }

    /// Month 13 is invalid
    #[test]
    fn iso8601_rejects_month_13() {
        let result = iso8601_to_epoch_ms("2024-13-01T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "2024-13-01 should be InvalidTimestamp, got {result:?}"
        );
    }

    /// Day 0 is invalid
    #[test]
    fn iso8601_rejects_day_zero() {
        let result = iso8601_to_epoch_ms("2024-01-00T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "2024-01-00 should be InvalidTimestamp, got {result:?}"
        );
    }

    /// Century year 1900 is not a leap year (divisible by 100 but not 400)
    #[test]
    fn iso8601_rejects_feb_29_century_non_leap() {
        let result = iso8601_to_epoch_ms("1900-02-29T00:00:00.000Z");
        assert!(
            matches!(result, Err(Error::InvalidTimestamp(_))),
            "1900-02-29 should be InvalidTimestamp (1900 not a leap year), got {result:?}"
        );
    }

    /// Year 2000 IS a leap year (divisible by 400)
    #[test]
    fn iso8601_accepts_feb_29_year_2000() {
        let result = iso8601_to_epoch_ms("2000-02-29T00:00:00.000Z");
        assert!(result.is_ok(), "2000-02-29 should be valid (year 2000 is a leap year): {result:?}");
    }

    // ------------------------------------------------------------------
    // Fix: query_between() returns empty for from >= to (Issue #3)
    // ------------------------------------------------------------------

    /// query_between with from == to returns empty (degenerate range)
    #[test]
    fn query_between_equal_timestamps_returns_empty() {
        let (conn, mut cache) = open_conn();
        insert(&conn, &mut cache, "events", json!({"v": 1})).unwrap();

        let ts = "2024-01-01T00:00:00.000Z";
        let qr = query_between(&conn, "events", ts, ts).unwrap();
        assert!(
            qr.is_empty(),
            "query_between with equal timestamps should return empty, got {} docs",
            qr.len()
        );
    }

    /// query_between with from > to (reversed range) returns empty
    #[test]
    fn query_between_reversed_range_returns_empty() {
        let (conn, mut cache) = open_conn();
        insert(&conn, &mut cache, "events", json!({"v": 1})).unwrap();

        // from is after to
        let from = "2099-01-01T00:00:00.000Z";
        let to   = "2024-01-01T00:00:00.000Z";
        let qr = query_between(&conn, "events", from, to).unwrap();
        assert!(
            qr.is_empty(),
            "query_between with reversed range should return empty, got {} docs",
            qr.len()
        );
    }

    /// VAL-QRY-009: query_between() returns versions active during range
    #[test]
    fn query_between_returns_versions_active_during_range() {
        use std::thread::sleep;
        use std::time::Duration;
        let (conn, mut cache) = open_conn();

        let doc = insert(&conn, &mut cache, "events", json!({"v": "v0"})).unwrap();
        let t1 = doc.system_time();
        sleep(Duration::from_millis(10));
        update(&conn, "events", doc.id(), json!({"v": "v1"})).unwrap();

        // query_between(t1, far_future) should return both versions
        // (1 history: v0 active from t1 to t2) + (1 current: v1)
        let qr = query_between(&conn, "events", &t1, "2099-01-01T00:00:00.000Z").unwrap();
        assert_eq!(qr.len(), 2, "should find 2 versions: the history version and the current version");

        // Verify both v0 and v1 data are present
        let values: Vec<&str> = qr
            .documents()
            .iter()
            .filter_map(|d| d.get("v").and_then(|v| v.as_str()))
            .collect();
        assert!(values.contains(&"v0"), "should include original v0");
        assert!(values.contains(&"v1"), "should include updated v1");
    }
}
