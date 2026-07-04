use sqlparser::ast;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::error::{Error, Result};
use crate::ir::{Filter, Statement, TemporalClause};

pub(crate) fn parse_standard(sql: &str, temporal: Option<TemporalClause>) -> Result<Statement> {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| Error::Parse(e.to_string()))?;

    if statements.len() != 1 {
        return Err(Error::Parse(format!(
            "expected exactly one statement, got {}",
            statements.len()
        )));
    }

    let stmt = statements.into_iter().next().unwrap();
    match stmt {
        ast::Statement::CreateTable(ct) => {
            if temporal.is_some() {
                return Err(Error::Parse(
                    "FOR SYSTEM_TIME is only valid on SELECT".to_string(),
                ));
            }
            parse_create_table(ct)
        }
        ast::Statement::Query(query) => parse_select(query, temporal),
        _ => {
            if temporal.is_some() {
                return Err(Error::Parse(
                    "FOR SYSTEM_TIME is only valid on SELECT".to_string(),
                ));
            }
            Err(Error::Unsupported(format!(
                "statement type not yet supported: {}",
                stmt
            )))
        }
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

    let parts = &ct.name.0;
    if parts.len() != 1 {
        return Err(Error::Unsupported(
            "qualified table names are not supported".to_string(),
        ));
    }
    let ident = parts[0]
        .as_ident()
        .ok_or_else(|| Error::Parse("expected a simple identifier for table name".to_string()))?;
    let table = ident.value.clone();

    Ok(Statement::CreateTable { table })
}

fn parse_select(query: Box<ast::Query>, temporal: Option<TemporalClause>) -> Result<Statement> {
    if query.with.is_some() {
        return Err(Error::Unsupported(
            "WITH (common table expressions) is not supported".to_string(),
        ));
    }

    if query.order_by.is_some() {
        return Err(Error::Unsupported("ORDER BY is not supported".to_string()));
    }

    if query.limit.is_some() {
        return Err(Error::Unsupported("LIMIT is not supported".to_string()));
    }

    let select = match *query.body {
        ast::SetExpr::Select(select) => select,
        _ => {
            return Err(Error::Unsupported(
                "set operations (UNION/EXCEPT/INTERSECT) are not supported".to_string(),
            ));
        }
    };

    if select.distinct.is_some() {
        return Err(Error::Unsupported("DISTINCT is not supported".to_string()));
    }

    if select.projection.len() != 1 || !matches!(select.projection[0], ast::SelectItem::Wildcard(_))
    {
        return Err(Error::Unsupported(
            "projections are not supported (only SELECT * is supported)".to_string(),
        ));
    }

    if select.from.len() != 1 {
        return Err(Error::Unsupported(
            "joins are not supported (only a single table is supported)".to_string(),
        ));
    }
    let table_with_joins = &select.from[0];
    if !table_with_joins.joins.is_empty() {
        return Err(Error::Unsupported("joins are not supported".to_string()));
    }

    let table = match &table_with_joins.relation {
        ast::TableFactor::Table { name, alias, .. } => {
            if alias.is_some() {
                return Err(Error::Unsupported(
                    "table aliases are not supported".to_string(),
                ));
            }
            let parts = &name.0;
            if parts.len() != 1 {
                return Err(Error::Unsupported(
                    "qualified table names are not supported".to_string(),
                ));
            }
            let ident = parts[0].as_ident().ok_or_else(|| {
                Error::Parse("expected a simple identifier for table name".to_string())
            })?;
            ident.value.clone()
        }
        _ => {
            return Err(Error::Unsupported(
                "only plain table references are supported in FROM".to_string(),
            ));
        }
    };

    let group_by_present = match &select.group_by {
        ast::GroupByExpr::All(_) => true,
        ast::GroupByExpr::Expressions(exprs, modifiers) => {
            !exprs.is_empty() || !modifiers.is_empty()
        }
    };
    if group_by_present {
        return Err(Error::Unsupported("GROUP BY is not supported".to_string()));
    }

    if select.having.is_some() {
        return Err(Error::Unsupported("HAVING is not supported".to_string()));
    }

    let filter = match &select.selection {
        None => None,
        Some(expr) => Some(parse_id_filter(expr)?),
    };

    if filter.is_some() && temporal.is_some() {
        return Err(Error::Unsupported(
            "WHERE _id combined with FOR SYSTEM_TIME is not supported".to_string(),
        ));
    }

    Ok(Statement::Select {
        table,
        filter,
        temporal,
    })
}

/// Accepts only `_id = '<string>'`, matching decision #16/#18's minimal
/// WHERE support. Anything else is rejected with a specific message so
/// users know exactly what's missing (v0.2 will add general expressions).
fn parse_id_filter(expr: &ast::Expr) -> Result<Filter> {
    if let ast::Expr::BinaryOp { left, op, right } = expr {
        if matches!(op, ast::BinaryOperator::Eq) {
            if let ast::Expr::Identifier(ident) = left.as_ref() {
                if ident.value == "_id" {
                    if let ast::Expr::Value(v) = right.as_ref() {
                        if let ast::Value::SingleQuotedString(s) = &v.value {
                            return Ok(Filter::ById(s.clone()));
                        }
                    }
                }
            }
        }
    }

    Err(Error::Unsupported(
        "arbitrary WHERE clauses are not supported (only WHERE _id = '<id>')".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_basic() {
        let stmt = parse_standard("CREATE TABLE events", None).unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn create_table_case_insensitive() {
        let stmt = parse_standard("create table Events", None).unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "Events".to_string()
            }
        );
    }

    #[test]
    fn create_table_with_columns_rejected() {
        let err = parse_standard("CREATE TABLE events (id TEXT, name TEXT)", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("column definitions"));
    }

    #[test]
    fn create_table_with_semicolon() {
        let stmt = parse_standard("CREATE TABLE events;", None).unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn invalid_sql_returns_parse_error() {
        let err = parse_standard("NOT VALID SQL", None).unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
    }

    #[test]
    fn multiple_statements_rejected() {
        let err = parse_standard("CREATE TABLE a; CREATE TABLE b", None).unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("exactly one statement"));
    }

    #[test]
    fn unsupported_statement_type() {
        let err = parse_standard("DROP TABLE events", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
    }

    #[test]
    fn create_table_as_select_rejected() {
        let err = parse_standard("CREATE TABLE dst AS SELECT * FROM src", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("AS SELECT"));
    }

    #[test]
    fn create_temp_table_rejected() {
        let err = parse_standard("CREATE TEMPORARY TABLE t", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("TEMPORARY"));
    }

    #[test]
    fn create_table_quoted_identifier() {
        let stmt = parse_standard(r#"CREATE TABLE "events""#, None).unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn create_table_qualified_name_rejected() {
        let err = parse_standard("CREATE TABLE schema1.events", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("qualified table names"));
    }

    // ------------------------------------------------------------------
    // SELECT
    // ------------------------------------------------------------------

    #[test]
    fn select_star_basic() {
        let stmt = parse_standard("SELECT * FROM events", None).unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "events".to_string(),
                filter: None,
                temporal: None,
            }
        );
    }

    #[test]
    fn select_with_id_filter() {
        let stmt = parse_standard("SELECT * FROM events WHERE _id = 'abc'", None).unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "events".to_string(),
                filter: Some(Filter::ById("abc".to_string())),
                temporal: None,
            }
        );
    }

    #[test]
    fn select_with_temporal_clause() {
        let stmt = parse_standard("SELECT * FROM events", Some(TemporalClause::All)).unwrap();
        assert_eq!(
            stmt,
            Statement::Select {
                table: "events".to_string(),
                filter: None,
                temporal: Some(TemporalClause::All),
            }
        );
    }

    #[test]
    fn select_projections_rejected() {
        let err = parse_standard("SELECT a, b FROM events", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("projections"));
    }

    #[test]
    fn select_arbitrary_where_rejected() {
        let err = parse_standard("SELECT * FROM events WHERE x = 1", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("arbitrary WHERE"));
    }

    #[test]
    fn select_order_by_rejected() {
        let err = parse_standard("SELECT * FROM events ORDER BY _id", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("ORDER BY"));
    }

    #[test]
    fn select_joins_rejected() {
        let err = parse_standard(
            "SELECT * FROM events JOIN other ON events._id = other._id",
            None,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("joins"));
    }

    #[test]
    fn select_table_alias_rejected() {
        let err = parse_standard("SELECT * FROM events e", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("aliases"));
    }

    #[test]
    fn select_group_by_rejected() {
        let err = parse_standard("SELECT * FROM events GROUP BY _id", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("GROUP BY"));
    }

    #[test]
    fn select_limit_rejected() {
        let err = parse_standard("SELECT * FROM events LIMIT 1", None).unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("LIMIT"));
    }

    #[test]
    fn select_filter_and_temporal_combination_rejected() {
        let err = parse_standard(
            "SELECT * FROM events WHERE _id = 'abc'",
            Some(TemporalClause::All),
        )
        .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("FOR SYSTEM_TIME"));
    }

    #[test]
    fn create_table_with_temporal_clause_rejected() {
        let err = parse_standard("CREATE TABLE events", Some(TemporalClause::All)).unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("FOR SYSTEM_TIME"));
    }

    #[test]
    fn unsupported_statement_with_temporal_clause_rejected() {
        let err = parse_standard("DROP TABLE events", Some(TemporalClause::All)).unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("FOR SYSTEM_TIME"));
    }
}
