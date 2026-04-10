pub mod error;
mod sql;

pub use cairndb_core::{Document, QueryResult};
pub use error::{Error, Result};

use std::path::Path;

use serde_json::Value;

pub struct Database {
    inner: cairndb_core::Database,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: cairndb_core::Database::open(path)?,
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        Ok(Self {
            inner: cairndb_core::Database::open_in_memory()?,
        })
    }

    pub fn create_table(&self, name: &str) -> Result<()> {
        Ok(self.inner.create_table(name)?)
    }

    pub fn insert(&self, table: &str, data: Value) -> Result<Document> {
        Ok(self.inner.insert(table, data)?)
    }

    pub fn update(&self, table: &str, id: &str, patch: Value) -> Result<Document> {
        Ok(self.inner.update(table, id, patch)?)
    }

    pub fn delete(&self, table: &str, id: &str) -> Result<()> {
        Ok(self.inner.delete(table, id)?)
    }

    pub fn erase(&self, table: &str, id: &str) -> Result<()> {
        Ok(self.inner.erase(table, id)?)
    }

    pub fn get(&self, table: &str, id: &str) -> Result<Document> {
        Ok(self.inner.get(table, id)?)
    }

    pub fn query(&self, table: &str) -> Result<QueryResult> {
        Ok(self.inner.query(table)?)
    }

    pub fn query_all(&self, table: &str) -> Result<QueryResult> {
        Ok(self.inner.query_all(table)?)
    }

    pub fn query_at(&self, table: &str, timestamp_iso: &str) -> Result<QueryResult> {
        Ok(self.inner.query_at(table, timestamp_iso)?)
    }

    pub fn query_between(
        &self,
        table: &str,
        from_iso: &str,
        to_iso: &str,
    ) -> Result<QueryResult> {
        Ok(self.inner.query_between(table, from_iso, to_iso)?)
    }

    pub fn sql(&self, query: &str) -> Result<QueryResult> {
        sql::execute(self, query)
    }
}
