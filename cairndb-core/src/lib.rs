pub mod db;
pub mod document;
pub mod error;
pub(crate) mod schema;

pub use db::Database;
pub use document::{Document, QueryResult};
pub use error::{Error, Result};
