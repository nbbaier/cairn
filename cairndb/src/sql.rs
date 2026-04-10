use cairndb_core::QueryResult;

use crate::{Database, Result};

pub(crate) fn execute(db: &Database, query: &str) -> Result<QueryResult> {
    let stmt = cairndb_parser::parse(query)?;
    match stmt {
        cairndb_parser::Statement::CreateTable { table } => {
            db.create_table(&table)?;
            Ok(QueryResult::default())
        }
        _ => Err(cairndb_parser::Error::Unsupported(
            "statement type not yet implemented in dispatch".to_string(),
        )
        .into()),
    }
}
