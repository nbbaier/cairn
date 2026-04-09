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
}
