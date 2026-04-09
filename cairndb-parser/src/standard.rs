use sqlparser::ast;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::{Error, Result};
use crate::ir::Statement;

pub(crate) fn parse_standard(sql: &str) -> Result<Statement> {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| Error::Parse(e.to_string()))?;

    if statements.len() != 1 {
        return Err(Error::Parse(format!(
            "expected exactly one statement, got {}",
            statements.len()
        )));
    }

    let stmt = statements.into_iter().next().unwrap();
    match stmt {
        ast::Statement::CreateTable(ct) => parse_create_table(ct),
        _ => Err(Error::Unsupported(format!(
            "statement type not yet supported: {}",
            stmt
        ))),
    }
}

fn parse_create_table(ct: ast::CreateTable) -> Result<Statement> {
    if ct.query.is_some() {
        return Err(Error::Unsupported(
            "CREATE TABLE ... AS SELECT is not supported".to_string(),
        ));
    }

    if ct.or_replace {
        return Err(Error::Unsupported(
            "CREATE OR REPLACE TABLE is not supported".to_string(),
        ));
    }

    if ct.temporary {
        return Err(Error::Unsupported(
            "CREATE TEMPORARY TABLE is not supported".to_string(),
        ));
    }

    if ct.external {
        return Err(Error::Unsupported(
            "CREATE EXTERNAL TABLE is not supported".to_string(),
        ));
    }

    if ct.like.is_some() {
        return Err(Error::Unsupported(
            "CREATE TABLE ... LIKE is not supported".to_string(),
        ));
    }

    if ct.clone.is_some() {
        return Err(Error::Unsupported(
            "CREATE TABLE ... CLONE is not supported".to_string(),
        ));
    }

    if !ct.columns.is_empty() {
        return Err(Error::Unsupported(
            "column definitions in CREATE TABLE are not supported (cairndb is schemaless)"
                .to_string(),
        ));
    }

    if !ct.constraints.is_empty() {
        return Err(Error::Unsupported(
            "table constraints in CREATE TABLE are not supported".to_string(),
        ));
    }

    let table = ct.name.to_string();

    Ok(Statement::CreateTable { table })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_basic() {
        let stmt = parse_standard("CREATE TABLE events").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn create_table_case_insensitive() {
        let stmt = parse_standard("create table Events").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "Events".to_string()
            }
        );
    }

    #[test]
    fn create_table_with_columns_rejected() {
        let err = parse_standard("CREATE TABLE events (id TEXT, name TEXT)").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("column definitions"));
    }

    #[test]
    fn create_table_with_semicolon() {
        let stmt = parse_standard("CREATE TABLE events;").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn invalid_sql_returns_parse_error() {
        let err = parse_standard("NOT VALID SQL").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
    }

    #[test]
    fn multiple_statements_rejected() {
        let err =
            parse_standard("CREATE TABLE a; CREATE TABLE b").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("exactly one statement"));
    }

    #[test]
    fn unsupported_statement_type() {
        let err = parse_standard("DROP TABLE events").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
    }

    #[test]
    fn create_table_as_select_rejected() {
        let err = parse_standard("CREATE TABLE dst AS SELECT * FROM src").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("AS SELECT"));
    }

    #[test]
    fn create_temp_table_rejected() {
        let err = parse_standard("CREATE TEMPORARY TABLE t").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("TEMPORARY"));
    }
}
