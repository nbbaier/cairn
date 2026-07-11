use crate::error::Result;
use crate::insert;
use crate::ir::Statement;
use crate::standard;
use crate::temporal;

pub fn parse(sql: &str) -> Result<Statement> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(crate::error::Error::Parse("empty input".to_string()));
    }

    // Detect statement type by first keyword. Custom parsers run before the
    // temporal pre-processor and sqlparser-rs fallback (decision #18); INSERT
    // is handled here, ERASE is still pending (#19).
    let first = trimmed.split_whitespace().next().unwrap_or("");
    if first.eq_ignore_ascii_case("INSERT") {
        return insert::parse_insert(trimmed);
    }

    // Everything else routes to sqlparser-rs, after stripping the
    // non-standard `FOR SYSTEM_TIME` clause sqlparser-rs cannot parse.
    let (stripped, temporal) = temporal::strip_system_time(trimmed)?;
    standard::parse_standard(&stripped, temporal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::Statement;

    #[test]
    fn parse_create_table() {
        let stmt = parse("CREATE TABLE events").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn parse_with_whitespace() {
        let stmt = parse("  CREATE TABLE events  ").unwrap();
        assert_eq!(
            stmt,
            Statement::CreateTable {
                table: "events".to_string()
            }
        );
    }

    #[test]
    fn parse_empty_input() {
        let err = parse("").unwrap_err();
        assert!(err.to_string().contains("empty input"));
    }

    #[test]
    fn parse_whitespace_only() {
        let err = parse("   ").unwrap_err();
        assert!(err.to_string().contains("empty input"));
    }

    #[test]
    fn parse_invalid_sql() {
        let err = parse("GIBBERISH NONSENSE").unwrap_err();
        assert!(matches!(err, crate::error::Error::Parse(_)));
    }

    #[test]
    fn parse_insert_routes_to_custom_parser() {
        let stmt = parse("INSERT INTO events (name) VALUES ('x')").unwrap();
        let mut data = serde_json::Map::new();
        data.insert("name".to_string(), serde_json::json!("x"));
        assert_eq!(
            stmt,
            Statement::Insert {
                table: "events".to_string(),
                data,
            }
        );
    }

    #[test]
    fn parse_document_literal_insert_returns_typed_ir() {
        let statement = parse("INSERT INTO events {name: 'deploy'}").unwrap();
        let mut data = serde_json::Map::new();
        data.insert("name".to_string(), serde_json::json!("deploy"));
        assert_eq!(
            statement,
            Statement::Insert {
                table: "events".to_string(),
                data,
            }
        );
    }

    #[test]
    fn parse_insert_lowercase() {
        let stmt = parse("insert into t (a) values (1)").unwrap();
        let mut data = serde_json::Map::new();
        data.insert("a".to_string(), serde_json::json!(1));
        assert_eq!(
            stmt,
            Statement::Insert {
                table: "t".to_string(),
                data,
            }
        );
    }

    #[test]
    fn parse_insert_with_leading_whitespace() {
        let stmt = parse("   INSERT INTO t (a) VALUES (1)   ").unwrap();
        assert!(matches!(stmt, Statement::Insert { .. }));
    }
}
