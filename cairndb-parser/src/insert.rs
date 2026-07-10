use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::ir::Statement;

/// Parses the column/value form of INSERT:
/// `INSERT INTO <table> (<col>, ...) VALUES (<value>, ...)`.
///
/// The document-literal form (`INSERT INTO t {..}`) is slice #15; until it
/// lands, seeing `{` after the table name returns `Error::Unsupported`.
pub(crate) fn parse_insert(sql: &str) -> Result<Statement> {
    let mut s = Scanner::new(sql);

    s.expect_keyword("INSERT")?;
    s.expect_keyword("INTO")?;
    let table = s.identifier("table name")?;

    s.skip_whitespace();
    match s.peek() {
        Some('{') => {
            return Err(Error::Unsupported(
                "document literal INSERT is not yet implemented".to_string(),
            ));
        }
        Some('(') => {}
        _ => {
            if s.peek_word().eq_ignore_ascii_case("SELECT") {
                return Err(Error::Unsupported(
                    "INSERT ... SELECT is not supported".to_string(),
                ));
            }
            return Err(Error::Parse(
                "expected a column list after the table name".to_string(),
            ));
        }
    }

    let columns = parse_column_list(&mut s)?;

    s.skip_whitespace();
    if s.peek_word().eq_ignore_ascii_case("SELECT") {
        return Err(Error::Unsupported(
            "INSERT ... SELECT is not supported".to_string(),
        ));
    }
    s.expect_keyword("VALUES")?;
    let values = parse_value_list(&mut s)?;

    s.skip_whitespace();
    if s.peek() == Some(',') {
        return Err(Error::Unsupported(
            "multi-row VALUES is not supported".to_string(),
        ));
    }
    s.expect_end()?;

    if columns.len() != values.len() {
        return Err(Error::Parse(format!(
            "expected {} values, got {}",
            columns.len(),
            values.len()
        )));
    }

    let mut data = Map::new();
    for (column, value) in columns.into_iter().zip(values) {
        data.insert(column, value);
    }
    Ok(Statement::Insert { table, data })
}

fn parse_column_list(s: &mut Scanner) -> Result<Vec<String>> {
    s.skip_whitespace();
    if s.bump() != Some('(') {
        return Err(Error::Parse(
            "expected '(' to start the column list".to_string(),
        ));
    }
    s.skip_whitespace();
    if s.peek() == Some(')') {
        return Err(Error::Parse("empty column list".to_string()));
    }
    let mut columns = Vec::new();
    loop {
        columns.push(s.identifier("column name")?);
        s.skip_whitespace();
        match s.bump() {
            Some(',') => continue,
            Some(')') => return Ok(columns),
            _ => {
                return Err(Error::Parse(
                    "expected ',' or ')' in the column list".to_string(),
                ));
            }
        }
    }
}

fn parse_value_list(s: &mut Scanner) -> Result<Vec<Value>> {
    s.skip_whitespace();
    if s.bump() != Some('(') {
        return Err(Error::Parse(
            "expected '(' to start the VALUES list".to_string(),
        ));
    }
    s.skip_whitespace();
    if s.peek() == Some(')') {
        return Err(Error::Parse("empty VALUES list".to_string()));
    }
    let mut values = Vec::new();
    loop {
        values.push(parse_value(s)?);
        s.skip_whitespace();
        match s.bump() {
            Some(',') => continue,
            Some(')') => return Ok(values),
            _ => {
                return Err(Error::Parse(
                    "expected ',' or ')' in the VALUES list".to_string(),
                ));
            }
        }
    }
}

fn parse_value(s: &mut Scanner) -> Result<Value> {
    s.skip_whitespace();
    match s.peek() {
        None => Err(Error::Parse(
            "expected a value, found end of input".to_string(),
        )),
        Some('\'') => s.string_literal(),
        Some(c) if c == '-' || c.is_ascii_digit() => s.number_literal(),
        _ => {
            let word = s.peek_word();
            if word.eq_ignore_ascii_case("TRUE") {
                s.consume_word();
                Ok(Value::Bool(true))
            } else if word.eq_ignore_ascii_case("FALSE") {
                s.consume_word();
                Ok(Value::Bool(false))
            } else if word.eq_ignore_ascii_case("NULL") {
                s.consume_word();
                Ok(Value::Null)
            } else if word.is_empty() {
                Err(Error::Parse(format!(
                    "unsupported value starting at '{}'",
                    s.rest().chars().take(10).collect::<String>()
                )))
            } else {
                Err(Error::Parse(format!("unsupported value '{word}'")))
            }
        }
    }
}

/// Char-boundary-safe cursor over the SQL text. `pos` is a byte offset that
/// only ever advances by whole chars, so slicing at it never splits a char.
struct Scanner<'a> {
    src: &'a str,
    pos: usize,
}

impl<'a> Scanner<'a> {
    fn new(src: &'a str) -> Self {
        Scanner { src, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.src[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }

    /// The bare word (identifier chars) at the cursor, without consuming it.
    fn peek_word(&self) -> &'a str {
        let rest = self.rest();
        let end = rest
            .char_indices()
            .find(|&(_, c)| !(c.is_ascii_alphanumeric() || c == '_'))
            .map_or(rest.len(), |(i, _)| i);
        &rest[..end]
    }

    fn consume_word(&mut self) {
        self.pos += self.peek_word().len();
    }

    fn expect_keyword(&mut self, keyword: &str) -> Result<()> {
        self.skip_whitespace();
        let word = self.peek_word();
        if word.eq_ignore_ascii_case(keyword) {
            self.consume_word();
            Ok(())
        } else if word.is_empty() {
            Err(Error::Parse(format!(
                "expected {keyword}, found end of input"
            )))
        } else {
            Err(Error::Parse(format!("expected {keyword}, found '{word}'")))
        }
    }

    fn identifier(&mut self, what: &str) -> Result<String> {
        self.skip_whitespace();
        let word = self.peek_word();
        match word.chars().next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {
                self.consume_word();
                Ok(word.to_string())
            }
            _ => Err(Error::Parse(format!("expected {what}"))),
        }
    }

    /// Single-quoted string; `''` inside is an escaped quote.
    fn string_literal(&mut self) -> Result<Value> {
        self.bump(); // opening quote
        let mut out = String::new();
        loop {
            match self.bump() {
                None => return Err(Error::Parse("unterminated string literal".to_string())),
                Some('\'') => {
                    if self.peek() == Some('\'') {
                        self.bump();
                        out.push('\'');
                    } else {
                        return Ok(Value::String(out));
                    }
                }
                Some(c) => out.push(c),
            }
        }
    }

    /// Optional leading `-`, then digits and at most the chars of a decimal
    /// literal. Parses as i64 first, then f64.
    fn number_literal(&mut self) -> Result<Value> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.bump();
        }
        while self.peek().is_some_and(|c| c.is_ascii_digit() || c == '.') {
            self.bump();
        }
        let text = &self.src[start..self.pos];
        if let Ok(i) = text.parse::<i64>() {
            return Ok(Value::Number(i.into()));
        }
        if let Ok(f) = text.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return Ok(Value::Number(n));
            }
        }
        Err(Error::Parse(format!("invalid number literal '{text}'")))
    }

    /// Consumes an optional trailing `;` and asserts nothing else remains.
    fn expect_end(&mut self) -> Result<()> {
        self.skip_whitespace();
        if self.peek() == Some(';') {
            self.bump();
            self.skip_whitespace();
        }
        if self.pos == self.src.len() {
            Ok(())
        } else {
            Err(Error::Parse(format!(
                "unexpected trailing input '{}'",
                self.rest().trim()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data_of(stmt: Statement) -> (String, Map<String, Value>) {
        match stmt {
            Statement::Insert { table, data } => (table, data),
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn single_string_column() {
        let stmt = parse_insert("INSERT INTO events (name) VALUES ('deploy')").unwrap();
        let (table, data) = data_of(stmt);
        assert_eq!(table, "events");
        assert_eq!(data.len(), 1);
        assert_eq!(data["name"], json!("deploy"));
    }

    #[test]
    fn multi_column_preserves_order_and_types() {
        let stmt = parse_insert(
            "INSERT INTO events (name, status) VALUES ('deploy', 'pending')",
        )
        .unwrap();
        let (table, data) = data_of(stmt);
        assert_eq!(table, "events");
        let keys: Vec<&String> = data.keys().collect();
        assert_eq!(keys, ["name", "status"]);
        assert_eq!(data["name"], json!("deploy"));
        assert_eq!(data["status"], json!("pending"));
    }

    #[test]
    fn integer_value() {
        let (_, data) = data_of(parse_insert("INSERT INTO t (a) VALUES (42)").unwrap());
        assert_eq!(data["a"], json!(42));
        assert!(data["a"].is_i64());
    }

    #[test]
    fn float_value() {
        let (_, data) = data_of(parse_insert("INSERT INTO t (a) VALUES (3.25)").unwrap());
        assert_eq!(data["a"], json!(3.25));
        assert!(data["a"].is_f64());
    }

    #[test]
    fn negative_numbers() {
        let (_, data) =
            data_of(parse_insert("INSERT INTO t (a, b) VALUES (-7, -0.5)").unwrap());
        assert_eq!(data["a"], json!(-7));
        assert_eq!(data["b"], json!(-0.5));
    }

    #[test]
    fn negative_zero_integer() {
        let (_, data) = data_of(parse_insert("INSERT INTO t (a) VALUES (-0)").unwrap());
        assert_eq!(data["a"], json!(0));
    }

    #[test]
    fn boolean_values() {
        let (_, data) =
            data_of(parse_insert("INSERT INTO t (a, b) VALUES (true, FALSE)").unwrap());
        assert_eq!(data["a"], json!(true));
        assert_eq!(data["b"], json!(false));
    }

    #[test]
    fn null_value() {
        let (_, data) = data_of(parse_insert("INSERT INTO t (a) VALUES (NULL)").unwrap());
        assert_eq!(data["a"], Value::Null);
    }

    #[test]
    fn escaped_quote_in_string() {
        let (_, data) =
            data_of(parse_insert("INSERT INTO t (a) VALUES ('it''s fine')").unwrap());
        assert_eq!(data["a"], json!("it's fine"));
    }

    #[test]
    fn case_insensitive_keywords() {
        let stmt = parse_insert("insert into t (a) values (1)").unwrap();
        let (table, data) = data_of(stmt);
        assert_eq!(table, "t");
        assert_eq!(data["a"], json!(1));
    }

    #[test]
    fn trailing_semicolon_and_whitespace() {
        let stmt = parse_insert("  INSERT INTO t (a) VALUES (1) ;  ").unwrap();
        let (_, data) = data_of(stmt);
        assert_eq!(data["a"], json!(1));
    }

    #[test]
    fn no_space_before_parens() {
        let stmt = parse_insert("INSERT INTO t(a) VALUES(1)").unwrap();
        let (table, data) = data_of(stmt);
        assert_eq!(table, "t");
        assert_eq!(data["a"], json!(1));
    }

    #[test]
    fn count_mismatch_rejected() {
        let err = parse_insert("INSERT INTO t (a) VALUES (1, 2)").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(
            err.to_string().contains("expected 1 values, got 2"),
            "error was: {err}"
        );
    }

    #[test]
    fn empty_column_list_rejected() {
        let err = parse_insert("INSERT INTO t () VALUES (1)").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("empty column list"));
    }

    #[test]
    fn empty_values_list_rejected() {
        let err = parse_insert("INSERT INTO t (a) VALUES ()").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("empty VALUES list"));
    }

    #[test]
    fn multi_row_values_rejected() {
        let err = parse_insert("INSERT INTO t (a) VALUES (1), (2)").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("multi-row"), "error was: {err}");
    }

    #[test]
    fn insert_select_rejected() {
        let err = parse_insert("INSERT INTO t SELECT * FROM u").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("SELECT"), "error was: {err}");
    }

    #[test]
    fn insert_columns_then_select_rejected() {
        let err = parse_insert("INSERT INTO t (a) SELECT a FROM u").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(err.to_string().contains("SELECT"), "error was: {err}");
    }

    #[test]
    fn document_literal_rejected_until_slice_15() {
        let err = parse_insert("INSERT INTO t {name: 'x'}").unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)));
        assert!(
            err.to_string().contains("document literal"),
            "error was: {err}"
        );
    }

    #[test]
    fn missing_values_keyword_rejected() {
        let err = parse_insert("INSERT INTO t (a) (1)").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("VALUES"), "error was: {err}");
    }

    #[test]
    fn unterminated_string_rejected() {
        let err = parse_insert("INSERT INTO t (a) VALUES ('oops)").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("unterminated"));
    }

    #[test]
    fn trailing_garbage_rejected() {
        let err = parse_insert("INSERT INTO t (a) VALUES (1) garbage").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("trailing"), "error was: {err}");
    }

    #[test]
    fn missing_table_name_rejected() {
        let err = parse_insert("INSERT INTO (a) VALUES (1)").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("table name"), "error was: {err}");
    }

    #[test]
    fn bare_word_value_rejected() {
        let err = parse_insert("INSERT INTO t (a) VALUES (banana)").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("banana"), "error was: {err}");
    }

    #[test]
    fn multibyte_chars_in_string_are_safe() {
        let (_, data) =
            data_of(parse_insert("INSERT INTO t (a) VALUES ('héllo 🎉')").unwrap());
        assert_eq!(data["a"], json!("héllo 🎉"));
    }
}
