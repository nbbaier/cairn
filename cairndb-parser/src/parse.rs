use crate::error::Result;
use crate::ir::Statement;
use crate::standard;

pub fn parse(sql: &str) -> Result<Statement> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(crate::error::Error::Parse("empty input".to_string()));
    }

    // Detect statement type by first keyword.
    // Custom parsers (INSERT, ERASE) will be added here in future slices.
    // For now, everything routes to sqlparser-rs.
    standard::parse_standard(trimmed)
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
}
