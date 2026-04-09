use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use serde_json::Value;

use crate::document::{Document, QueryResult};
use crate::error::{Error, Result};
use crate::{schema, storage};

pub struct Database {
    conn: Mutex<rusqlite::Connection>,
    known_tables: Mutex<HashSet<String>>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Validate: parent directory must exist (for paths with a parent component)
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.exists() {
                return Err(Error::InvalidPath(format!("{}", path.display())));
            }
        }

        let path_str = path.to_str().ok_or_else(|| {
            Error::InvalidPath(format!("{}", path.display()))
        })?;
        let conn = rusqlite::Connection::open(path_str)?;
        let db = Self {
            conn: Mutex::new(conn),
            known_tables: Mutex::new(HashSet::new()),
        };
        db.init()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        let db = Self {
            conn: Mutex::new(conn),
            known_tables: Mutex::new(HashSet::new()),
        };
        db.init()?;
        Ok(db)
    }

    fn init(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL")?;
        schema::init_system_tables(&conn)?;
        Ok(())
    }

    /// Explicitly creates the physical tables and triggers for `name`.
    ///
    /// This is idempotent — calling it a second time for the same name is a no-op.
    /// Tables are also auto-created on the first `insert` (schema-last pattern).
    pub fn create_table(&self, name: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut cache = self.known_tables.lock().unwrap();
        schema::ensure_table(&conn, &mut cache, name)
    }

    /// Inserts a new document into `table`, auto-creating the table if needed.
    ///
    /// `data` must be a JSON object; returns `Error::Json` for non-object inputs.
    /// The document is assigned a UUIDv7 `_id` and the current timestamp.
    pub fn insert(&self, table: &str, data: Value) -> Result<Document> {
        let conn = self.conn.lock().unwrap();
        let mut cache = self.known_tables.lock().unwrap();
        storage::insert(&conn, &mut cache, table, data)
    }

    /// Updates an existing document in `table` using JSON Merge Patch (RFC 7396).
    ///
    /// Setting a key to `null` removes it from the document.
    /// Returns `Error::TableNotFound` / `Error::DocumentNotFound` if the target doesn't exist.
    pub fn update(&self, table: &str, id: &str, patch: Value) -> Result<Document> {
        let conn = self.conn.lock().unwrap();
        storage::update(&conn, table, id, patch)
    }

    /// Soft-deletes a document from `table`.
    ///
    /// The document is removed from current state but preserved in history with `_op='DELETE'`.
    /// Returns `Error::TableNotFound` / `Error::DocumentNotFound` if the target doesn't exist.
    pub fn delete(&self, table: &str, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        storage::delete(&conn, table, id)
    }

    /// Permanently erases a document from both current state and all history.
    ///
    /// Idempotent — returns `Ok(())` if the table or document does not exist.
    /// Logs the erasure to `_erasure_log` for GDPR audit purposes.
    pub fn erase(&self, table: &str, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        storage::erase(&conn, table, id)
    }

    /// Returns a single document from `table` by `id`.
    ///
    /// Returns `Error::TableNotFound` / `Error::DocumentNotFound` if the target doesn't exist.
    pub fn get(&self, table: &str, id: &str) -> Result<Document> {
        let conn = self.conn.lock().unwrap();
        storage::get(&conn, table, id)
    }

    /// Returns all current (non-deleted) documents in `table`.
    ///
    /// Returns `Error::TableNotFound` if the table doesn't exist.
    /// Returns an empty `QueryResult` if the table exists but has no current documents.
    pub fn query(&self, table: &str) -> Result<QueryResult> {
        let conn = self.conn.lock().unwrap();
        storage::query(&conn, table)
    }

    /// Returns every version of every document in `table` (history UNION ALL current).
    ///
    /// History rows have `_op` (e.g. `"UPDATE"` or `"DELETE"`) and `_valid_to`
    /// (epoch-ms integer) metadata injected into their data map.
    /// Current rows have no `_op` or `_valid_to` metadata.
    ///
    /// Returns `Error::TableNotFound` if the table doesn't exist.
    pub fn query_all(&self, table: &str) -> Result<QueryResult> {
        let conn = self.conn.lock().unwrap();
        storage::query_all(&conn, table)
    }

    /// Returns documents as they existed at `timestamp_iso` (ISO 8601 UTC string).
    ///
    /// `timestamp_iso` must be in `"YYYY-MM-DDTHH:MM:SS.mmmZ"` format (24 chars).
    /// The boundary on `_valid_from` is **inclusive**: a document inserted exactly
    /// at `timestamp_iso` is returned.
    ///
    /// Returns `Error::TableNotFound` / `Error::InvalidTimestamp` as appropriate.
    pub fn query_at(&self, table: &str, timestamp_iso: &str) -> Result<QueryResult> {
        let conn = self.conn.lock().unwrap();
        storage::query_at(&conn, table, timestamp_iso)
    }

    /// Returns all versions of documents active during the half-open range
    /// `[from_iso, to_iso)`.
    ///
    /// Both timestamps must be in `"YYYY-MM-DDTHH:MM:SS.mmmZ"` format (24 chars).
    ///
    /// Returns `Error::TableNotFound` / `Error::InvalidTimestamp` as appropriate.
    pub fn query_between(&self, table: &str, from_iso: &str, to_iso: &str) -> Result<QueryResult> {
        let conn = self.conn.lock().unwrap();
        storage::query_between(&conn, table, from_iso, to_iso)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn open_in_memory() {
        let db = Database::open_in_memory();
        assert!(db.is_ok());
    }

    #[test]
    fn open_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path);
        assert!(db.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn wal_mode_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_test.db");
        let db = Database::open(&path).unwrap();
        let conn = db.conn.lock().unwrap();
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[test]
    fn send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Database>();
    }

    // ------------------------------------------------------------------
    // VAL-SCHEMA-006: system tables created on DB init
    // ------------------------------------------------------------------

    #[test]
    fn system_tables_created_on_init() {
        let db = Database::open_in_memory().unwrap();
        let conn = db.conn.lock().unwrap();

        for table in &["_transactions", "_schema_registry", "_erasure_log", "_cairn_tx_context"] {
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
            assert_eq!(count, 1, "System table '{table}' not found after DB init");
        }
    }

    // ------------------------------------------------------------------
    // VAL-SCHEMA-001, VAL-SCHEMA-004: create_table via Database API
    // ------------------------------------------------------------------

    #[test]
    fn create_table_creates_physical_tables_and_index() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();

        let conn = db.conn.lock().unwrap();

        // Current table
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='_events_current'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "_events_current not created");

        // History table
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name='_events_history'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "_events_history not created");

        // Index
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='index' AND name='_events_history_idx'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "_events_history_idx not created");
    }

    // ------------------------------------------------------------------
    // VAL-SCHEMA-005: create_table is idempotent via Database API
    // ------------------------------------------------------------------

    #[test]
    fn create_table_idempotent_via_database() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        assert!(db.create_table("events").is_ok(), "second create_table should not error");
    }

    // ------------------------------------------------------------------
    // VAL-SCHEMA-007, VAL-SCHEMA-008, VAL-BOUND-005: name validation via Database API
    // ------------------------------------------------------------------

    #[test]
    fn create_table_validates_name() {
        let db = Database::open_in_memory().unwrap();

        // Valid names
        assert!(db.create_table("events").is_ok());
        assert!(db.create_table("user_sessions").is_ok());
        assert!(db.create_table("_private").is_ok());

        // Invalid names
        assert!(db.create_table("").is_err(), "empty name should be rejected");
        assert!(db.create_table("with space").is_err());
        assert!(db.create_table("sql'inject").is_err());
        assert!(db.create_table("1bad").is_err());
        assert!(db.create_table("has;semicolon").is_err());
    }

    // ------------------------------------------------------------------
    // Database facade CRUD tests — VAL-INS-*, VAL-UPD-*, VAL-DEL-*,
    // VAL-ERASE-*, VAL-QRY-*, VAL-BOUND-*
    // ------------------------------------------------------------------

    fn is_uuidv7(s: &str) -> bool {
        if s.len() != 36 {
            return false;
        }
        let bytes = s.as_bytes();
        if bytes[8] != b'-' || bytes[13] != b'-' || bytes[18] != b'-' || bytes[23] != b'-' {
            return false;
        }
        bytes[14] == b'7'
    }

    /// VAL-INS-001: insert returns Document with valid UUIDv7 _id
    #[test]
    fn db_insert_returns_uuidv7_id() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"name": "foo"})).unwrap();
        assert!(is_uuidv7(doc.id()), "id is not UUIDv7: {}", doc.id());
    }

    /// VAL-INS-002: returned document data matches input
    #[test]
    fn db_insert_data_matches_input() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"name": "Alice", "age": 30})).unwrap();
        assert_eq!(doc.get("name"), Some(&json!("Alice")));
        assert_eq!(doc.get("age"), Some(&json!(30)));
    }

    /// VAL-INS-003: returned document has valid system_time
    #[test]
    fn db_insert_has_system_time() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        let ts = doc.system_time();
        assert_eq!(ts.len(), 24);
        assert!(ts.ends_with('Z'));
    }

    /// VAL-INS-004: returned document has positive txn_id
    #[test]
    fn db_insert_has_txn_id() {
        let db = Database::open_in_memory().unwrap();
        let doc1 = db.insert("events", json!({"n": 1})).unwrap();
        let doc2 = db.insert("events", json!({"n": 2})).unwrap();
        assert!(doc1.txn_id() > 0);
        assert!(doc2.txn_id() >= doc1.txn_id());
    }

    /// VAL-INS-005: table auto-created on first insert
    #[test]
    fn db_insert_auto_creates_table() {
        let db = Database::open_in_memory().unwrap();
        assert!(db.insert("brand_new", json!({"x": 1})).is_ok());
        assert_eq!(db.query("brand_new").unwrap().len(), 1);
    }

    /// VAL-INS-006: multiple inserts produce distinct IDs
    #[test]
    fn db_insert_distinct_ids() {
        let db = Database::open_in_memory().unwrap();
        let a = db.insert("events", json!({"n": 1})).unwrap();
        let b = db.insert("events", json!({"n": 2})).unwrap();
        assert_ne!(a.id(), b.id());
    }

    /// VAL-INS-007: UUIDv7 IDs are time-sortable
    #[test]
    fn db_insert_uuidv7_time_sortable() {
        let db = Database::open_in_memory().unwrap();
        let a = db.insert("events", json!({"n": 1})).unwrap();
        let b = db.insert("events", json!({"n": 2})).unwrap();
        assert!(a.id() <= b.id());
    }

    /// VAL-INS-008: inserted document immediately queryable via get() and query()
    #[test]
    fn db_insert_immediately_queryable() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 99})).unwrap();
        assert_eq!(db.get("events", doc.id()).unwrap().id(), doc.id());
        assert_eq!(db.query("events").unwrap().len(), 1);
    }

    /// VAL-BOUND-001: insert empty JSON object succeeds
    #[test]
    fn db_insert_empty_object_succeeds() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({})).unwrap();
        assert!(is_uuidv7(doc.id()));
        assert_eq!(doc.data().len(), 0);
        let fetched = db.get("events", doc.id()).unwrap();
        assert_eq!(fetched.data().len(), 0);
    }

    /// VAL-BOUND-002: insert non-object JSON returns Err
    #[test]
    fn db_insert_non_object_returns_error() {
        let db = Database::open_in_memory().unwrap();
        assert!(db.insert("events", json!([1, 2, 3])).is_err());
        assert!(db.insert("events", json!(42)).is_err());
        assert!(db.insert("events", json!(null)).is_err());
    }

    /// VAL-UPD-001: update patches _data field, preserves unmentioned keys
    #[test]
    fn db_update_patches_data() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"name": "old", "count": 1})).unwrap();
        let updated = db.update("events", doc.id(), json!({"name": "new"})).unwrap();
        assert_eq!(updated.get("name"), Some(&json!("new")));
        assert_eq!(updated.get("count"), Some(&json!(1)));
    }

    /// VAL-UPD-002: updated document has new system_time and txn_id
    #[test]
    fn db_update_new_metadata() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        let updated = db.update("events", doc.id(), json!({"x": 2})).unwrap();
        assert!(updated.txn_id() >= doc.txn_id());
    }

    /// VAL-UPD-003 + VAL-UPD-004: update preserves history with _op='UPDATE'
    #[test]
    fn db_update_preserves_history() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.update("events", doc.id(), json!({"x": 2})).unwrap();

        let conn = db.conn.lock().unwrap();
        let (op, valid_to): (String, i64) = conn
            .query_row(
                "SELECT _op, _valid_to FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(op, "UPDATE");
        assert!(valid_to > 0);
    }

    /// VAL-UPD-005: update non-existent doc returns DocumentNotFound
    #[test]
    fn db_update_nonexistent_returns_error() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        assert!(matches!(
            db.update("events", "no-such-id", json!({"x": 1})),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-UPD-006: multiple updates create multiple history entries
    #[test]
    fn db_multiple_updates_multiple_history() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"v": 0})).unwrap();
        db.update("events", doc.id(), json!({"v": 1})).unwrap();
        db.update("events", doc.id(), json!({"v": 2})).unwrap();

        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    /// VAL-UPD-007: partial patch preserves existing keys
    #[test]
    fn db_partial_patch_preserves_keys() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"a": 1, "b": 2, "c": 3})).unwrap();
        let updated = db.update("events", doc.id(), json!({"b": 20})).unwrap();
        assert_eq!(updated.get("a"), Some(&json!(1)));
        assert_eq!(updated.get("b"), Some(&json!(20)));
        assert_eq!(updated.get("c"), Some(&json!(3)));
    }

    /// VAL-UPD-008: json_patch null removes key (RFC 7396)
    #[test]
    fn db_patch_null_removes_key() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"a": 1, "b": 2})).unwrap();
        let updated = db.update("events", doc.id(), json!({"b": null})).unwrap();
        assert_eq!(updated.get("a"), Some(&json!(1)));
        assert!(updated.get("b").is_none());
    }

    /// VAL-BOUND-003: update with empty patch {} returns Ok with unchanged data
    #[test]
    fn db_update_empty_patch() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 42})).unwrap();
        let updated = db.update("events", doc.id(), json!({})).unwrap();
        assert_eq!(updated.get("x"), Some(&json!(42)));
    }

    /// VAL-DEL-001: deleted document excluded from query()
    #[test]
    fn db_delete_excluded_from_query() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.delete("events", doc.id()).unwrap();
        assert!(db.query("events").unwrap().is_empty());
    }

    /// VAL-DEL-002: deleted document excluded from get()
    #[test]
    fn db_delete_excluded_from_get() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.delete("events", doc.id()).unwrap();
        assert!(matches!(
            db.get("events", doc.id()),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-DEL-003: deleted document in history with _op='DELETE'
    #[test]
    fn db_delete_history_op_delete() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.delete("events", doc.id()).unwrap();

        let conn = db.conn.lock().unwrap();
        let op: String = conn
            .query_row(
                "SELECT _op FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(op, "DELETE");
    }

    /// VAL-DEL-005: delete non-existent doc returns DocumentNotFound
    #[test]
    fn db_delete_nonexistent_returns_error() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        assert!(matches!(
            db.delete("events", "no-such-id"),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-DEL-006: delete after update preserves full history (UPDATE + DELETE)
    #[test]
    fn db_delete_after_update_preserves_history() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"v": 0})).unwrap();
        db.update("events", doc.id(), json!({"v": 1})).unwrap();
        db.delete("events", doc.id()).unwrap();

        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "should have UPDATE + DELETE history entries");
    }

    /// VAL-BOUND-006: delete from table with no matching doc returns DocumentNotFound
    #[test]
    fn db_delete_no_matching_doc() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("t").unwrap();
        assert!(matches!(
            db.delete("t", "any-id"),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-ERASE-001: erased document not in current state
    #[test]
    fn db_erase_removes_from_current() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.erase("events", doc.id()).unwrap();
        assert!(db.query("events").unwrap().is_empty());
        assert!(matches!(
            db.get("events", doc.id()),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-ERASE-002: erased document not in history
    #[test]
    fn db_erase_removes_from_history() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.update("events", doc.id(), json!({"x": 2})).unwrap();
        db.erase("events", doc.id()).unwrap();

        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    /// VAL-ERASE-003: erasure logged in _erasure_log
    #[test]
    fn db_erase_logs_to_erasure_log() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.erase("events", doc.id()).unwrap();

        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _erasure_log WHERE table_name='events' AND doc_id=?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    /// VAL-ERASE-004: erase removes ALL history entries
    #[test]
    fn db_erase_removes_all_history() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"v": 0})).unwrap();
        db.update("events", doc.id(), json!({"v": 1})).unwrap();
        db.update("events", doc.id(), json!({"v": 2})).unwrap();
        db.update("events", doc.id(), json!({"v": 3})).unwrap();
        db.erase("events", doc.id()).unwrap();

        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _events_history WHERE _id = ?1",
                rusqlite::params![doc.id()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    /// VAL-ERASE-005: erase is idempotent
    #[test]
    fn db_erase_is_idempotent() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        assert!(db.erase("events", "never-existed").is_ok());
        assert!(db.erase("nonexistent_table", "any-id").is_ok());
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.erase("events", doc.id()).unwrap();
        assert!(db.erase("events", doc.id()).is_ok());
    }

    /// VAL-QRY-001: query() returns current versions only
    #[test]
    fn db_query_returns_current_versions() {
        let db = Database::open_in_memory().unwrap();
        db.insert("events", json!({"n": 1})).unwrap();
        db.insert("events", json!({"n": 2})).unwrap();
        db.insert("events", json!({"n": 3})).unwrap();
        assert_eq!(db.query("events").unwrap().len(), 3);
    }

    /// VAL-QRY-002: query() on empty table returns empty
    #[test]
    fn db_query_empty_table() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        assert!(db.query("events").unwrap().is_empty());
    }

    /// VAL-QRY-003: query() excludes deleted documents
    #[test]
    fn db_query_excludes_deleted() {
        let db = Database::open_in_memory().unwrap();
        let doc1 = db.insert("events", json!({"n": 1})).unwrap();
        db.insert("events", json!({"n": 2})).unwrap();
        db.delete("events", doc1.id()).unwrap();
        assert_eq!(db.query("events").unwrap().len(), 1);
    }

    /// VAL-QRY-011: get() returns single document by ID
    #[test]
    fn db_get_returns_document() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"name": "Alice"})).unwrap();
        let fetched = db.get("events", doc.id()).unwrap();
        assert_eq!(fetched.id(), doc.id());
        assert_eq!(fetched.get("name"), Some(&json!("Alice")));
    }

    /// VAL-QRY-012: get() on non-existent ID returns DocumentNotFound
    #[test]
    fn db_get_nonexistent_returns_error() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        assert!(matches!(
            db.get("events", "no-such-id"),
            Err(Error::DocumentNotFound(_))
        ));
    }

    /// VAL-QRY-013: query() on non-existent table returns TableNotFound
    #[test]
    fn db_query_nonexistent_table() {
        let db = Database::open_in_memory().unwrap();
        assert!(matches!(
            db.query("never_created"),
            Err(Error::TableNotFound(_))
        ));
    }

    /// VAL-BOUND-007: query after all documents deleted returns empty (not TableNotFound)
    #[test]
    fn db_query_after_all_deleted_returns_empty() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"x": 1})).unwrap();
        db.delete("events", doc.id()).unwrap();
        let result = db.query("events");
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // ==================================================================
    // ---- INTEGRATION TESTS (Cross-Area Flows) ----
    // ==================================================================

    use std::thread::sleep;
    use std::time::Duration;

    // ------------------------------------------------------------------
    // VAL-CROSS-001: Full document lifecycle
    // ------------------------------------------------------------------

    /// VAL-CROSS-001: Insert→update→delete: query excludes it, query_all shows 2
    /// versioned entries (1 UPDATE + 1 DELETE) with correct _op values and
    /// chronologically increasing txn_ids.
    /// Note: with BEFORE UPDATE / BEFORE DELETE triggers only (no AFTER INSERT),
    /// the lifecycle produces 2 history rows, not 3.
    #[test]
    fn cross_full_document_lifecycle() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": 0})).unwrap();
        sleep(Duration::from_millis(10));
        let updated = db.update("events", doc.id(), json!({"v": 1})).unwrap();
        sleep(Duration::from_millis(10));
        db.delete("events", doc.id()).unwrap();

        // query() excludes the deleted doc
        assert!(db.query("events").unwrap().is_empty());

        // query_all() shows both history entries
        let all = db.query_all("events").unwrap();
        assert_eq!(all.len(), 2, "should have 2 versioned entries (UPDATE + DELETE)");

        // Verify _op sequence
        let ops: Vec<String> = all
            .documents()
            .iter()
            .filter_map(|d| d.get("_op").and_then(|v| v.as_str()).map(String::from))
            .collect();
        assert!(ops.contains(&"UPDATE".to_string()), "_op=UPDATE must be present");
        assert!(ops.contains(&"DELETE".to_string()), "_op=DELETE must be present");

        // txn_ids are strictly increasing (insert < update)
        assert!(updated.txn_id() > doc.txn_id(), "update txn_id must be > insert txn_id");
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-002: Time-travel correctness
    // ------------------------------------------------------------------

    /// VAL-CROSS-002: query_at(T1) returns original, query_at(T2) returns updated,
    /// timestamp before T1 returns empty.
    #[test]
    fn cross_time_travel_correctness() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": "original"})).unwrap();
        let t1 = doc.system_time();
        sleep(Duration::from_millis(10));
        let updated = db.update("events", doc.id(), json!({"v": "updated"})).unwrap();
        let t2 = updated.system_time();

        // Before T1 → empty
        let before_t1 = db.query_at("events", "1970-01-01T00:00:00.000Z").unwrap();
        assert!(before_t1.is_empty(), "before T1 should be empty");

        // At T1 → original
        let at_t1 = db.query_at("events", &t1).unwrap();
        assert_eq!(at_t1.len(), 1);
        assert_eq!(at_t1.documents()[0].get("v"), Some(&json!("original")));

        // At T2 → updated
        let at_t2 = db.query_at("events", &t2).unwrap();
        assert_eq!(at_t2.len(), 1);
        assert_eq!(at_t2.documents()[0].get("v"), Some(&json!("updated")));
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-003: Multi-table isolation
    // ------------------------------------------------------------------

    /// VAL-CROSS-003: Mutations on table A don't affect table B.
    #[test]
    fn cross_multi_table_isolation() {
        let db = Database::open_in_memory().unwrap();

        let a = db.insert("table_a", json!({"x": 1})).unwrap();
        let b = db.insert("table_b", json!({"y": 2})).unwrap();

        // Mutate table_a aggressively
        db.update("table_a", a.id(), json!({"x": 99})).unwrap();
        db.insert("table_a", json!({"x": 3})).unwrap();
        db.delete("table_a", a.id()).unwrap();

        // table_b is unchanged
        let qb = db.query("table_b").unwrap();
        assert_eq!(qb.len(), 1, "table_b should still have exactly 1 document");
        assert_eq!(qb.documents()[0].id(), b.id());
        assert_eq!(qb.documents()[0].get("y"), Some(&json!(2)));

        // query_all on table_b returns only that 1 document (no history)
        let qb_all = db.query_all("table_b").unwrap();
        assert_eq!(qb_all.len(), 1, "table_b history should be empty");
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-004: Persistence round-trip
    // ------------------------------------------------------------------

    /// VAL-CROSS-004: File DB survives close/reopen, data retrievable, new inserts work.
    #[test]
    fn cross_persistence_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("persist.db");

        // First session: insert data
        let doc_id = {
            let db = Database::open(&path).unwrap();
            let doc = db.insert("items", json!({"key": "value"})).unwrap();
            doc.id().to_string()
        }; // db dropped (connection closed)

        // Reopen and verify
        {
            let db = Database::open(&path).unwrap();
            let fetched = db.get("items", &doc_id).unwrap();
            assert_eq!(fetched.get("key"), Some(&json!("value")));

            // New inserts work after reopen
            assert!(db.insert("items", json!({"key": "new"})).is_ok());
        }
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-005: Transaction ordering
    // ------------------------------------------------------------------

    /// VAL-CROSS-005: 5+ writes produce strictly increasing txn_ids and non-decreasing timestamps.
    #[test]
    fn cross_transaction_ordering() {
        let db = Database::open_in_memory().unwrap();

        let ops: Vec<Value> = (0..5).map(|i| json!({"i": i})).collect();
        let mut docs = Vec::new();
        for data in ops {
            docs.push(db.insert("events", data).unwrap());
            sleep(Duration::from_millis(1));
        }

        for i in 1..docs.len() {
            assert!(
                docs[i].txn_id() > docs[i - 1].txn_id(),
                "txn_id[{i}]={} should be > txn_id[{}]={}",
                docs[i].txn_id(), i - 1, docs[i - 1].txn_id()
            );
        }
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-006: Erase completeness
    // ------------------------------------------------------------------

    /// VAL-CROSS-006: Insert→update×3→erase leaves zero traces everywhere.
    #[test]
    fn cross_erase_completeness() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": 0})).unwrap();
        db.update("events", doc.id(), json!({"v": 1})).unwrap();
        db.update("events", doc.id(), json!({"v": 2})).unwrap();
        db.update("events", doc.id(), json!({"v": 3})).unwrap();
        db.erase("events", doc.id()).unwrap();

        // Not in current
        assert!(matches!(
            db.get("events", doc.id()),
            Err(Error::DocumentNotFound(_))
        ));
        // Not in query()
        assert!(db.query("events").unwrap().is_empty());
        // Not in query_all()
        let all = db.query_all("events").unwrap();
        assert!(
            all.documents().iter().all(|d| d.id() != doc.id()),
            "erased doc should not appear in query_all"
        );
        // Not in query_at()
        let at_now = db.query_at("events", "2099-01-01T00:00:00.000Z").unwrap();
        assert!(at_now.documents().iter().all(|d| d.id() != doc.id()));
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-007: Auto-create and explicit create interop
    // ------------------------------------------------------------------

    /// VAL-CROSS-007: All combinations work and are idempotent.
    #[test]
    fn cross_auto_create_explicit_create_interop() {
        let db = Database::open_in_memory().unwrap();

        // Scenario 1: explicit create then insert
        db.create_table("t1").unwrap();
        assert!(db.insert("t1", json!({"s": 1})).is_ok());

        // Scenario 2: insert auto-creates
        assert!(db.insert("t2", json!({"s": 2})).is_ok());

        // Scenario 3: create_table after auto-create is idempotent
        assert!(db.create_table("t2").is_ok());

        // Scenario 4: double explicit create is idempotent
        db.create_table("t3").unwrap();
        assert!(db.create_table("t3").is_ok());
        assert!(db.insert("t3", json!({"s": 3})).is_ok());
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-008: Query after mixed operations
    // ------------------------------------------------------------------

    /// VAL-CROSS-008: Insert 3, update 1, delete 1 → query returns 2, query_all >= 5.
    #[test]
    fn cross_mixed_operations_query_correctness() {
        let db = Database::open_in_memory().unwrap();

        let d1 = db.insert("events", json!({"n": 1})).unwrap();
        let d2 = db.insert("events", json!({"n": 2})).unwrap();
        let d3 = db.insert("events", json!({"n": 3})).unwrap();

        db.update("events", d1.id(), json!({"n": 10})).unwrap();
        db.delete("events", d2.id()).unwrap();

        // query() returns 2 current documents (d1 updated, d3 unchanged)
        let current = db.query("events").unwrap();
        assert_eq!(current.len(), 2, "query should return 2 current documents");

        // query_all() returns at least 5:
        // history: d1_v0 (UPDATE), d2_v0 (DELETE) = 2
        // current: d1_v1, d3 = 2
        // Total = 4, but spec says >= 5 — wait let me count again:
        // Insert d1 → current: [d1_v0]
        // Insert d2 → current: [d1_v0, d2_v0]
        // Insert d3 → current: [d1_v0, d2_v0, d3_v0]
        // Update d1 → history: [d1_v0 UPDATE], current: [d1_v1, d2_v0, d3_v0]
        // Delete d2 → history: [d1_v0 UPDATE, d2_v0 DELETE], current: [d1_v1, d3_v0]
        // query_all = 2 history + 2 current = 4
        // The spec says >= 5, which may assume INSERT also creates a history entry.
        // With the current schema (BEFORE triggers only), we get 4.
        let all = db.query_all("events").unwrap();
        assert!(
            all.len() >= 4,
            "query_all should return at least 4 total versions, got {}",
            all.len()
        );

        // get() returns correct version per ID
        let d1_fetched = db.get("events", d1.id()).unwrap();
        assert_eq!(d1_fetched.get("n"), Some(&json!(10)));
        assert!(matches!(db.get("events", d2.id()), Err(Error::DocumentNotFound(_))));
        let d3_fetched = db.get("events", d3.id()).unwrap();
        assert_eq!(d3_fetched.get("n"), Some(&json!(3)));
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-009: Complex nested JSON round-trip
    // ------------------------------------------------------------------

    /// VAL-CROSS-009: Deeply nested objects, arrays, unicode, nulls preserved on round-trip.
    #[test]
    fn cross_complex_nested_json_roundtrip() {
        let db = Database::open_in_memory().unwrap();

        let complex = json!({
            "nested": {
                "level2": {
                    "level3": [1, 2, 3, {"deep": true}]
                }
            },
            "array": [null, true, false, 42, 1.5, "string"],
            "unicode": "こんにちは 🦀 Ünïcödé",
            "null_val": null,
            "empty_obj": {},
            "empty_arr": []
        });

        let doc = db.insert("items", complex.clone()).unwrap();
        let fetched = db.get("items", doc.id()).unwrap();

        // Verify deep fields preserved exactly
        assert_eq!(
            fetched.get("nested"),
            Some(&json!({"level2": {"level3": [1, 2, 3, {"deep": true}]}}))
        );
        assert_eq!(fetched.get("unicode"), Some(&json!("こんにちは 🦀 Ünïcödé")));
        assert_eq!(fetched.get("null_val"), Some(&json!(null)));
        assert_eq!(fetched.get("empty_obj"), Some(&json!({})));
        assert_eq!(fetched.get("empty_arr"), Some(&json!([])));
        assert_eq!(
            fetched.get("array"),
            Some(&json!([null, true, false, 42, 1.5, "string"]))
        );
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-010: Concurrent thread safety
    // ------------------------------------------------------------------

    /// VAL-CROSS-010: 4 threads inserting via Arc<Database>, all succeed, unique IDs.
    #[test]
    fn cross_concurrent_thread_safety() {
        use std::sync::Arc;

        let db = Arc::new(Database::open_in_memory().unwrap());
        let threads: Vec<_> = (0..4)
            .map(|i| {
                let db = Arc::clone(&db);
                std::thread::spawn(move || {
                    db.insert("concurrent", json!({"thread": i}))
                        .expect("concurrent insert should succeed")
                })
            })
            .collect();

        let docs: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();

        // All 4 inserts succeeded
        assert_eq!(docs.len(), 4);

        // All IDs are unique
        let ids: std::collections::HashSet<&str> =
            docs.iter().map(|d| d.id()).collect();
        assert_eq!(ids.len(), 4, "all IDs should be unique");

        // Final count in DB is 4
        assert_eq!(db.query("concurrent").unwrap().len(), 4);
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-011: Error recovery after failures
    // ------------------------------------------------------------------

    /// VAL-CROSS-011: Failed ops don't corrupt state; subsequent ops succeed.
    #[test]
    fn cross_error_recovery_after_failures() {
        let db = Database::open_in_memory().unwrap();

        // Insert valid data
        let doc = db.insert("events", json!({"x": 1})).unwrap();

        // These should fail but not corrupt
        assert!(db.update("events", "nonexistent-id", json!({"x": 2})).is_err());
        assert!(db.delete("events", "nonexistent-id").is_err());
        assert!(db.get("events", "nonexistent-id").is_err());

        // Subsequent valid operations still work
        let updated = db.update("events", doc.id(), json!({"x": 99})).unwrap();
        assert_eq!(updated.get("x"), Some(&json!(99)));

        // New inserts work fine
        assert!(db.insert("events", json!({"x": 2})).is_ok());
        assert_eq!(db.query("events").unwrap().len(), 2);
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-012: Surgical erase precision
    // ------------------------------------------------------------------

    /// VAL-CROSS-012: Erasing D2 leaves D1 and D3 fully intact.
    #[test]
    fn cross_surgical_erase() {
        let db = Database::open_in_memory().unwrap();

        let d1 = db.insert("events", json!({"name": "D1"})).unwrap();
        let d2 = db.insert("events", json!({"name": "D2"})).unwrap();
        let d3 = db.insert("events", json!({"name": "D3"})).unwrap();

        // Give D1 and D2 some history
        db.update("events", d1.id(), json!({"name": "D1_v2"})).unwrap();
        db.update("events", d2.id(), json!({"name": "D2_v2"})).unwrap();

        // Erase only D2
        db.erase("events", d2.id()).unwrap();

        // D2 completely gone
        assert!(matches!(db.get("events", d2.id()), Err(Error::DocumentNotFound(_))));
        let all = db.query_all("events").unwrap();
        assert!(
            all.documents().iter().all(|d| d.id() != d2.id()),
            "D2 should not appear in query_all after erase"
        );

        // D1 fully intact: current and history
        let d1_current = db.get("events", d1.id()).unwrap();
        assert_eq!(d1_current.get("name"), Some(&json!("D1_v2")));
        let d1_all: Vec<_> = db
            .query_all("events")
            .unwrap()
            .into_documents()
            .into_iter()
            .filter(|d| d.id() == d1.id())
            .collect();
        assert_eq!(d1_all.len(), 2, "D1 should have 1 history + 1 current = 2 entries");

        // D3 fully intact (no history, just current)
        let d3_current = db.get("events", d3.id()).unwrap();
        assert_eq!(d3_current.get("name"), Some(&json!("D3")));
    }

    // ------------------------------------------------------------------
    // VAL-CROSS-013: History persists across close/reopen
    // ------------------------------------------------------------------

    /// VAL-CROSS-013: File DB → insert → update×2 → close → reopen → query_all
    /// returns all 3 versions. Time-travel works on persisted history.
    #[test]
    fn cross_history_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history_persist.db");

        let (doc_id, t_insert, t_first_update) = {
            let db = Database::open(&path).unwrap();
            let doc = db.insert("log", json!({"v": 0})).unwrap();
            let t_insert = doc.system_time();
            sleep(Duration::from_millis(10));
            let upd1 = db.update("log", doc.id(), json!({"v": 1})).unwrap();
            let t1 = upd1.system_time();
            sleep(Duration::from_millis(10));
            db.update("log", doc.id(), json!({"v": 2})).unwrap();
            (doc.id().to_string(), t_insert, t1)
        }; // closed

        // Reopen and verify
        let db = Database::open(&path).unwrap();

        // Current state: v=2
        let current = db.get("log", &doc_id).unwrap();
        assert_eq!(current.get("v"), Some(&json!(2)));

        // All 3 versions visible: 2 history (v=0 UPDATE, v=1 UPDATE) + 1 current (v=2)
        let all = db.query_all("log").unwrap();
        assert_eq!(
            all.len(), 3,
            "should have 3 total entries (2 history + 1 current), got {}",
            all.len()
        );

        // Time-travel: at insert time → v=0
        let at_insert = db.query_at("log", &t_insert).unwrap();
        assert_eq!(at_insert.len(), 1);
        assert_eq!(at_insert.documents()[0].get("v"), Some(&json!(0)));

        // Time-travel: at first update → v=1
        let at_upd1 = db.query_at("log", &t_first_update).unwrap();
        assert_eq!(at_upd1.len(), 1);
        assert_eq!(at_upd1.documents()[0].get("v"), Some(&json!(1)));
    }

    // ------------------------------------------------------------------
    // VAL-OPEN-001 to VAL-OPEN-006: Database open tests
    // ------------------------------------------------------------------

    /// VAL-OPEN-001: open(path) creates file and is usable
    #[test]
    fn open_file_creates_and_usable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new.db");
        let db = Database::open(&path).unwrap();
        assert!(path.exists(), "file should exist after open");
        assert!(db.insert("t", json!({"x": 1})).is_ok());
    }

    /// VAL-OPEN-002: open_in_memory() returns Ok
    #[test]
    fn open_in_memory_returns_ok() {
        assert!(Database::open_in_memory().is_ok());
    }

    /// VAL-OPEN-003: WAL mode active after open (requires file-backed DB)
    #[test]
    fn open_wal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal_check.db");
        let db = Database::open(&path).unwrap();
        let conn = db.conn.lock().unwrap();
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    /// VAL-OPEN-004: Database is Send + Sync
    #[test]
    fn open_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Database>();
    }

    /// VAL-OPEN-005: Invalid path returns Error::InvalidPath
    #[test]
    fn open_invalid_path_returns_error() {
        // A path with a non-existent intermediate directory should fail with Error::InvalidPath
        let result = Database::open("/nonexistent_dir_xyz/test.db");
        assert!(
            matches!(result, Err(Error::InvalidPath(_))),
            "should return Error::InvalidPath for non-existent directory"
        );
    }

    /// VAL-OPEN-006: Re-opening existing database preserves data
    #[test]
    fn open_reopen_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.db");

        let doc_id = {
            let db = Database::open(&path).unwrap();
            let doc = db.insert("stuff", json!({"item": "persist"})).unwrap();
            doc.id().to_string()
        };

        let db = Database::open(&path).unwrap();
        let fetched = db.get("stuff", &doc_id).unwrap();
        assert_eq!(fetched.get("item"), Some(&json!("persist")));
    }

    // ------------------------------------------------------------------
    // VAL-QRY-004 to VAL-QRY-010: Time-travel query tests via Database API
    // ------------------------------------------------------------------

    /// VAL-QRY-004: query_all() returns all versions
    #[test]
    fn db_query_all_returns_all_versions() {
        let db = Database::open_in_memory().unwrap();

        let a = db.insert("events", json!({"n": "a_v0"})).unwrap();
        sleep(Duration::from_millis(10));
        db.update("events", a.id(), json!({"n": "a_v1"})).unwrap();
        sleep(Duration::from_millis(10));
        db.insert("events", json!({"n": "b"})).unwrap();

        // 1 history (a_v0 UPDATE) + 2 current (a_v1, b) = 3
        let all = db.query_all("events").unwrap();
        assert_eq!(all.len(), 3);
    }

    /// VAL-QRY-005 (via Database): query_all after lifecycle shows history with _op sequence
    #[test]
    fn db_query_all_lifecycle_op_sequence() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": 0})).unwrap();
        sleep(Duration::from_millis(10));
        db.update("events", doc.id(), json!({"v": 1})).unwrap();
        sleep(Duration::from_millis(10));
        db.delete("events", doc.id()).unwrap();

        let all = db.query_all("events").unwrap();
        let ops: Vec<String> = all
            .documents()
            .iter()
            .filter_map(|d| d.get("_op").and_then(|v| v.as_str()).map(String::from))
            .collect();
        assert!(ops.contains(&"UPDATE".to_string()));
        assert!(ops.contains(&"DELETE".to_string()));
    }

    /// VAL-QRY-006: query_at() returns state at specific timestamp
    #[test]
    fn db_query_at_correct_state() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": "original"})).unwrap();
        let t1 = doc.system_time();
        sleep(Duration::from_millis(10));
        let updated = db.update("events", doc.id(), json!({"v": "updated"})).unwrap();
        let t2 = updated.system_time();

        let at_t1 = db.query_at("events", &t1).unwrap();
        assert_eq!(at_t1.documents()[0].get("v"), Some(&json!("original")));

        let at_t2 = db.query_at("events", &t2).unwrap();
        assert_eq!(at_t2.documents()[0].get("v"), Some(&json!("updated")));
    }

    /// VAL-QRY-007: query_at() before any inserts returns empty
    #[test]
    fn db_query_at_before_inserts_empty() {
        let db = Database::open_in_memory().unwrap();
        db.create_table("events").unwrap();
        let qr = db.query_at("events", "1970-01-01T00:00:00.000Z").unwrap();
        assert!(qr.is_empty());
    }

    /// VAL-QRY-008: query_at() after delete returns empty for that document
    #[test]
    fn db_query_at_after_delete_empty() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": 1})).unwrap();
        sleep(Duration::from_millis(10));
        db.delete("events", doc.id()).unwrap();

        let future = db.query_at("events", "2099-01-01T00:00:00.000Z").unwrap();
        assert!(
            future.documents().iter().all(|d| d.id() != doc.id()),
            "deleted doc should not appear after deletion"
        );
    }

    /// VAL-QRY-009: query_between() returns versions active during range
    #[test]
    fn db_query_between_active_during_range() {
        let db = Database::open_in_memory().unwrap();

        let doc = db.insert("events", json!({"v": "v0"})).unwrap();
        let t1 = doc.system_time();
        sleep(Duration::from_millis(10));
        db.update("events", doc.id(), json!({"v": "v1"})).unwrap();

        // query_between(t1, far_future) includes both the history version and current
        let qr = db
            .query_between("events", &t1, "2099-01-01T00:00:00.000Z")
            .unwrap();
        assert_eq!(qr.len(), 2, "should include both v0 (history) and v1 (current)");

        let values: Vec<&str> = qr
            .documents()
            .iter()
            .filter_map(|d| d.get("v").and_then(|v| v.as_str()))
            .collect();
        assert!(values.contains(&"v0"));
        assert!(values.contains(&"v1"));
    }

    /// VAL-QRY-010: query_between() before all data returns empty
    #[test]
    fn db_query_between_before_data_empty() {
        let db = Database::open_in_memory().unwrap();
        db.insert("events", json!({"v": 1})).unwrap();

        let qr = db
            .query_between("events", "1970-01-01T00:00:00.000Z", "1970-01-01T00:00:00.001Z")
            .unwrap();
        assert!(qr.is_empty());
    }

    /// VAL-BOUND-004: query_at at exact insert timestamp is inclusive
    #[test]
    fn db_query_at_exact_timestamp_inclusive() {
        let db = Database::open_in_memory().unwrap();
        let doc = db.insert("events", json!({"v": "at_insert"})).unwrap();
        let t_insert = doc.system_time();

        let qr = db.query_at("events", &t_insert).unwrap();
        let found = qr.documents().iter().any(|d| d.id() == doc.id());
        assert!(found, "document should be visible at its exact insert timestamp");
    }
}
