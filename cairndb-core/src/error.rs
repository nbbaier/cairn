/// All errors that can be returned by cairndb-core.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wraps a SQLite error from rusqlite.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// Wraps a JSON serialization/deserialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// The provided file path is invalid or not UTF-8.
    #[error("invalid path: {0}")]
    InvalidPath(String),

    /// A timestamp value could not be parsed or converted.
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    /// The requested table does not exist.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// The requested document does not exist.
    #[error("document not found: {0}")]
    DocumentNotFound(String),

    /// A table name failed validation (empty, starts with a digit, contains special characters).
    #[error("invalid table name: {0}")]
    InvalidTableName(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_variants_constructible() {
        // Verify each variant can be constructed and pattern-matched
        let sqlite_err = Error::Sqlite(rusqlite::Error::QueryReturnedNoRows);
        assert!(matches!(sqlite_err, Error::Sqlite(_)));

        let json_err: Error = serde_json::from_str::<serde_json::Value>("invalid json")
            .unwrap_err()
            .into();
        assert!(matches!(json_err, Error::Json(_)));

        let inv_path = Error::InvalidPath("bad/path".to_string());
        assert!(matches!(inv_path, Error::InvalidPath(_)));

        let inv_ts = Error::InvalidTimestamp("not a timestamp".to_string());
        assert!(matches!(inv_ts, Error::InvalidTimestamp(_)));

        let tbl_nf = Error::TableNotFound("events".to_string());
        assert!(matches!(tbl_nf, Error::TableNotFound(_)));

        let doc_nf = Error::DocumentNotFound("abc-123".to_string());
        assert!(matches!(doc_nf, Error::DocumentNotFound(_)));
    }

    #[test]
    fn display_messages_non_empty() {
        let errors: Vec<Error> = vec![
            Error::Sqlite(rusqlite::Error::QueryReturnedNoRows),
            serde_json::from_str::<serde_json::Value>("!").unwrap_err().into(),
            Error::InvalidPath("x".to_string()),
            Error::InvalidTimestamp("x".to_string()),
            Error::TableNotFound("x".to_string()),
            Error::DocumentNotFound("x".to_string()),
        ];
        for e in &errors {
            assert!(!e.to_string().is_empty(), "Error message was empty for: {e:?}");
        }
    }

    #[test]
    fn from_rusqlite_error() {
        let rusqlite_err = rusqlite::Error::QueryReturnedNoRows;
        let err: Error = rusqlite_err.into();
        assert!(matches!(err, Error::Sqlite(_)));
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("!!!").unwrap_err();
        let err: Error = json_err.into();
        assert!(matches!(err, Error::Json(_)));
    }
}
