use std::path::Path;
use std::sync::Mutex;

use crate::error::{Error, Result};

pub struct Database {
    conn: Mutex<rusqlite::Connection>,
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
        };
        db.init()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.init()?;
        Ok(db)
    }

    fn init(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL")?;
        Ok(())
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
}
