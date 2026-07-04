use cairndb_core::QueryResult;

use crate::{Database, Result};

pub(crate) fn execute(db: &Database, query: &str) -> Result<QueryResult> {
    let stmt = cairndb_parser::parse(query)?;
    match stmt {
        cairndb_parser::Statement::CreateTable { table } => {
            db.create_table(&table)?;
            Ok(QueryResult::default())
        }
        cairndb_parser::Statement::Select {
            table,
            filter,
            temporal,
        } => match (filter, temporal) {
            (None, None) => db.query(&table),
            (Some(cairndb_parser::Filter::ById(id)), None) => Ok(db.get(&table, &id)?.into()),
            (None, Some(cairndb_parser::TemporalClause::AsOf(ts))) => db.query_at(&table, &ts),
            (None, Some(cairndb_parser::TemporalClause::Between(from, to))) => {
                db.query_between(&table, &from, &to)
            }
            (None, Some(cairndb_parser::TemporalClause::All)) => db.query_all(&table),
            // The parser rejects this combination before it reaches dispatch
            // (see cairndb-parser::standard::parse_select); this is a
            // defensive fallback rather than a panic, per the repo's move
            // away from `unreachable!` in reachable code paths.
            (Some(_), Some(_)) => Err(cairndb_parser::Error::Unsupported(
                "WHERE _id combined with FOR SYSTEM_TIME is not supported".to_string(),
            )
            .into()),
        },
        _ => Err(cairndb_parser::Error::Unsupported(
            "statement type not yet implemented in dispatch".to_string(),
        )
        .into()),
    }
}
