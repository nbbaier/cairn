use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::schema;

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
}

#[cfg(test)]
mod tests {
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
}
