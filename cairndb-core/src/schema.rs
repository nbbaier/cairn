//! Physical schema management: DDL generation, trigger SQL, indexes, and system tables.
//!
//! This module is responsible for:
//! - Validating table names against SQL injection patterns
//! - Creating `_T_current` and `_T_history` physical tables with versioning triggers
//! - Maintaining an in-memory cache of known table names for performance
//! - Creating system tables (`_transactions`, `_schema_registry`, `_erasure_log`) on DB init

use std::collections::HashSet;

use rusqlite::Connection;

use crate::error::{Error, Result};

/// Validates that `name` is a legal table identifier.
///
/// Accepted pattern: `^[a-zA-Z_][a-zA-Z0-9_]*$`
///
/// Rejects empty strings, names starting with digits, and names containing
/// special characters (spaces, semicolons, quotes, hyphens, dots, etc.).
pub(crate) fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidTableName(
            "table name must not be empty".to_string(),
        ));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap(); // safe: name is non-empty
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(Error::InvalidTableName(format!(
            "table name must start with a letter or underscore, got: '{name}'"
        )));
    }
    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(Error::InvalidTableName(format!(
                "table name contains invalid character '{ch}' in: '{name}'"
            )));
        }
    }
    Ok(())
}

/// Creates physical tables, versioning triggers, and a composite index for `name`.
///
/// Tables created:
/// - `_<name>_current` — latest version of each document
/// - `_<name>_history` — append-only log of superseded versions
///
/// All DDL uses `IF NOT EXISTS`, making this call idempotent.
///
/// If `name` is already in `cache`, the function returns immediately without
/// executing any SQL (performance optimisation).
pub(crate) fn ensure_table(
    conn: &Connection,
    cache: &mut HashSet<String>,
    name: &str,
) -> Result<()> {
    validate_table_name(name)?;
    if cache.contains(name) {
        return Ok(());
    }
    let sql = build_table_sql(name);
    conn.execute_batch(&sql)?;
    cache.insert(name.to_string());
    Ok(())
}

/// Creates the three system tables required by every cairndb database.
///
/// All DDL uses `IF NOT EXISTS`, so this function is safe to call multiple times.
///
/// Tables created:
/// - `_transactions` — transaction audit log (AUTOINCREMENT primary key)
/// - `_schema_registry` — schema inference metadata (reserved for future use in v0.1)
/// - `_erasure_log` — GDPR erasure audit trail
pub(crate) fn init_system_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(SYSTEM_TABLES_SQL)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Generates the DDL SQL for a user table pair plus its triggers and index.
fn build_table_sql(name: &str) -> String {
    // `name` has already been validated by `ensure_table`, so interpolation is safe.
    format!(
        r#"
CREATE TABLE IF NOT EXISTS _{name}_current (
    _id       TEXT    PRIMARY KEY,
    _data     JSONB   NOT NULL,
    _valid_from INTEGER NOT NULL,
    _txn_id   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS _{name}_history (
    _id         TEXT    NOT NULL,
    _data       JSONB   NOT NULL,
    _valid_from INTEGER NOT NULL,
    _valid_to   INTEGER NOT NULL,
    _txn_id     INTEGER NOT NULL,
    _op         TEXT    NOT NULL
);
CREATE TRIGGER IF NOT EXISTS _{name}_before_update
BEFORE UPDATE ON _{name}_current
FOR EACH ROW
BEGIN
    INSERT INTO _{name}_history (_id, _data, _valid_from, _valid_to, _txn_id, _op)
    VALUES (
        OLD._id,
        OLD._data,
        OLD._valid_from,
        (SELECT timestamp FROM _cairn_tx_context),
        (SELECT txn_id   FROM _cairn_tx_context),
        'UPDATE'
    );
END;
CREATE TRIGGER IF NOT EXISTS _{name}_before_delete
BEFORE DELETE ON _{name}_current
FOR EACH ROW
BEGIN
    INSERT INTO _{name}_history (_id, _data, _valid_from, _valid_to, _txn_id, _op)
    VALUES (
        OLD._id,
        OLD._data,
        OLD._valid_from,
        (SELECT timestamp FROM _cairn_tx_context),
        (SELECT txn_id   FROM _cairn_tx_context),
        'DELETE'
    );
END;
CREATE INDEX IF NOT EXISTS _{name}_history_idx
    ON _{name}_history (_id, _valid_from, _valid_to);
"#
    )
}

/// DDL for the system tables, created on every database open.
///
/// NOTE: `_cairn_tx_context` is intentionally a regular (non-TEMP) table.
/// SQLite restricts triggers on `main`-schema tables from referencing objects
/// in the `temp` schema, so the transaction context must live in `main`.
/// The versioning module manages its lifecycle: rows are inserted before each
/// write operation and deleted after commit/rollback. Because `Database` uses
/// a single `Mutex<Connection>`, only one writer is active at a time, so a
/// regular single-row table is safe and correct.
const SYSTEM_TABLES_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS _transactions (
    txn_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    metadata  TEXT
);
CREATE TABLE IF NOT EXISTS _schema_registry (
    "table"        TEXT    NOT NULL,
    key_path       TEXT    NOT NULL,
    inferred_type  TEXT,
    first_seen     INTEGER,
    last_seen      INTEGER,
    PRIMARY KEY ("table", key_path)
);
CREATE TABLE IF NOT EXISTS _erasure_log (
    table_name TEXT,
    doc_id     TEXT,
    erased_at  INTEGER
);
CREATE TABLE IF NOT EXISTS _cairn_tx_context (
    txn_id    INTEGER NOT NULL,
    timestamp INTEGER NOT NULL
);
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn open_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    /// Populate `_cairn_tx_context` with the given txn context.
    ///
    /// Assumes `init_system_tables` (or `ensure_table`) has already created the table.
    fn setup_tx_context(conn: &Connection, txn_id: i64, timestamp: i64) {
        conn.execute_batch(&format!(
            "DELETE FROM _cairn_tx_context;
             INSERT INTO _cairn_tx_context VALUES ({txn_id}, {timestamp});"
        ))
        .unwrap();
    }

    // ------------------------------------------------------------------
    // validate_table_name — VAL-SCHEMA-008, VAL-SCHEMA-007, VAL-BOUND-005
    // ------------------------------------------------------------------

    #[test]
    fn validate_accepts_valid_names() {
        assert!(validate_table_name("events").is_ok());
        assert!(validate_table_name("user_sessions").is_ok());
        assert!(validate_table_name("_private").is_ok());
        assert!(validate_table_name("Table1").is_ok());
        assert!(validate_table_name("abc123").is_ok());
        assert!(validate_table_name("A").is_ok());
        assert!(validate_table_name("_").is_ok());
    }

    #[test]
    fn validate_rejects_empty_name() {
        // VAL-BOUND-005
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn validate_rejects_special_characters() {
        // VAL-SCHEMA-007
        assert!(validate_table_name("with space").is_err());
        assert!(validate_table_name("semi;colon").is_err());
        assert!(validate_table_name("sql'injection").is_err());
        assert!(validate_table_name("--comment").is_err());
        assert!(validate_table_name("has-hyphen").is_err());
        assert!(validate_table_name("has.dot").is_err());
    }

    #[test]
    fn validate_rejects_digit_first_char() {
        // VAL-SCHEMA-007
        assert!(validate_table_name("1starts_with_digit").is_err());
        assert!(validate_table_name("9table").is_err());
    }

    // ------------------------------------------------------------------
    // ensure_table — VAL-SCHEMA-001
    // ------------------------------------------------------------------

    #[test]
    fn ensure_table_creates_current_and_history() {
        let conn = open_conn();
        let mut cache = HashSet::new();
        ensure_table(&conn, &mut cache, "events").unwrap();

        let count_current: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='_events_current'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count_current, 1, "_events_current not created");

        let count_history: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='_events_history'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count_history, 1, "_events_history not created");
    }

    // ------------------------------------------------------------------
    // ensure_table — VAL-SCHEMA-004 (composite index)
    // ------------------------------------------------------------------

    #[test]
    fn ensure_table_creates_composite_index() {
        let conn = open_conn();
        let mut cache = HashSet::new();
        ensure_table(&conn, &mut cache, "events").unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='index' AND name='_events_history_idx'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "_events_history_idx not found");
    }

    // ------------------------------------------------------------------
    // ensure_table — VAL-SCHEMA-005 (idempotent)
    // ------------------------------------------------------------------

    #[test]
    fn ensure_table_is_idempotent() {
        let conn = open_conn();
        let mut cache = HashSet::new();
        ensure_table(&conn, &mut cache, "events").unwrap();
        // Second call must not error
        let result = ensure_table(&conn, &mut cache, "events");
        assert!(result.is_ok(), "second ensure_table call failed: {result:?}");
        // Table still exists
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='_events_current'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn ensure_table_idempotent_without_cache() {
        // Two separate caches simulate two different processes opening the same DB.
        let conn = open_conn();
        let mut cache1 = HashSet::new();
        let mut cache2 = HashSet::new();
        ensure_table(&conn, &mut cache1, "events").unwrap();
        // Second call with a fresh cache — hits IF NOT EXISTS
        let result = ensure_table(&conn, &mut cache2, "events");
        assert!(result.is_ok());
    }

    #[test]
    fn cache_populated_after_ensure_table() {
        let conn = open_conn();
        let mut cache = HashSet::new();
        assert!(!cache.contains("events"));
        ensure_table(&conn, &mut cache, "events").unwrap();
        assert!(cache.contains("events"));
    }

    // ------------------------------------------------------------------
    // BEFORE UPDATE trigger — VAL-SCHEMA-002
    // ------------------------------------------------------------------

    #[test]
    fn before_update_trigger_copies_row_to_history() {
        let conn = open_conn();
        // _cairn_tx_context is a system table; must be created before triggers fire
        init_system_tables(&conn).unwrap();
        let mut cache = HashSet::new();
        ensure_table(&conn, &mut cache, "events").unwrap();

        // Provide a tx context so the trigger can read it
        setup_tx_context(&conn, 1, 500);

        // Insert a row into _events_current
        conn.execute(
            "INSERT INTO _events_current (_id, _data, _valid_from, _txn_id) \
             VALUES ('doc1', jsonb('{\"name\":\"Alice\"}'), 500, 1)",
            [],
        )
        .unwrap();

        // Update the row with a new tx context
        setup_tx_context(&conn, 2, 1000);
        conn.execute(
            "UPDATE _events_current \
             SET _data = jsonb('{\"name\":\"Bob\"}'), _valid_from = 1000, _txn_id = 2 \
             WHERE _id = 'doc1'",
            [],
        )
        .unwrap();

        // History should have one entry with _op = 'UPDATE'
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id='doc1' AND _op='UPDATE'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "expected 1 UPDATE history row");

        // _valid_to should equal the tx context timestamp (1000)
        let valid_to: i64 = conn
            .query_row(
                "SELECT _valid_to FROM _events_history WHERE _id='doc1' AND _op='UPDATE'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(valid_to, 1000, "_valid_to should match tx context timestamp");
    }

    // ------------------------------------------------------------------
    // BEFORE DELETE trigger — VAL-SCHEMA-003
    // ------------------------------------------------------------------

    #[test]
    fn before_delete_trigger_copies_row_to_history() {
        let conn = open_conn();
        // _cairn_tx_context is a system table; must be created before triggers fire
        init_system_tables(&conn).unwrap();
        let mut cache = HashSet::new();
        ensure_table(&conn, &mut cache, "events").unwrap();

        setup_tx_context(&conn, 1, 500);

        conn.execute(
            "INSERT INTO _events_current (_id, _data, _valid_from, _txn_id) \
             VALUES ('doc1', jsonb('{\"name\":\"Alice\"}'), 500, 1)",
            [],
        )
        .unwrap();

        // Delete the row with a new tx context
        setup_tx_context(&conn, 2, 2000);
        conn.execute(
            "DELETE FROM _events_current WHERE _id='doc1'",
            [],
        )
        .unwrap();

        // History should have one entry with _op = 'DELETE'
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id='doc1' AND _op='DELETE'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "expected 1 DELETE history row");

        // _valid_to should equal the tx context timestamp (2000)
        let valid_to: i64 = conn
            .query_row(
                "SELECT _valid_to FROM _events_history WHERE _id='doc1' AND _op='DELETE'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(valid_to, 2000, "_valid_to should match tx context timestamp");
    }

    // ------------------------------------------------------------------
    // init_system_tables — VAL-SCHEMA-006
    // ------------------------------------------------------------------

    #[test]
    fn init_system_tables_creates_all_three_tables() {
        let conn = open_conn();
        init_system_tables(&conn).unwrap();

        for table in &["_transactions", "_schema_registry", "_erasure_log"] {
            let count: i64 = conn
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM sqlite_master \
                         WHERE type='table' AND name='{table}'"
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "System table '{table}' was not created");
        }
    }

    #[test]
    fn init_system_tables_is_idempotent() {
        let conn = open_conn();
        init_system_tables(&conn).unwrap();
        // Second call must not error
        assert!(init_system_tables(&conn).is_ok());
    }

    // ------------------------------------------------------------------
    // ensure_table rejects invalid names
    // ------------------------------------------------------------------

    #[test]
    fn ensure_table_rejects_invalid_name() {
        let conn = open_conn();
        let mut cache = HashSet::new();
        assert!(ensure_table(&conn, &mut cache, "").is_err());
        assert!(ensure_table(&conn, &mut cache, "bad name").is_err());
        assert!(ensure_table(&conn, &mut cache, "sql'inject").is_err());
        assert!(ensure_table(&conn, &mut cache, "1bad").is_err());
    }
}
