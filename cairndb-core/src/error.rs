#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("invalid path: {0}")]
    InvalidPath(String),
}

pub type Result<T> = std::result::Result<T, Error>;
