#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Core(#[from] cairndb_core::Error),

    #[error("{0}")]
    Parse(#[from] cairndb_parser::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
