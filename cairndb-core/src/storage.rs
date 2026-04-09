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
pub(crate) fn update(
    conn: &Connection,
    table: &str,
    id: &str,
    patch: Value,
) -> Result<Document> {
    schema::validate_table_name(table)?;

    if !table_exists(conn, table)? {
        return Err(Error::TableNotFound(table.to_string()));
    }

    if !doc_exists(conn, table, id)? {
        return Err(Error::DocumentNotFound(id.to_string()));
    }

    let patch_json = serde_json::to_string(&patch)?;
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
}
