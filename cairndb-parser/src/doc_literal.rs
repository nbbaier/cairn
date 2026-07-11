use serde_json::{Map, Number, Value};

use crate::error::{Error, Result};

/// Parses one Endb-style document literal.
pub(crate) fn parse_document(input: &str) -> Result<Map<String, Value>> {
    let mut parser = Parser::new(input);
    parser.skip_whitespace();
    let document = parser.object()?;
    parser.skip_whitespace();
    if parser.peek() == Some(';') {
        parser.bump();
        parser.skip_whitespace();
    }
    if parser.peek() == Some(',') {
        let comma = parser.pos;
        parser.bump();
        parser.skip_whitespace();
        if parser.peek() == Some('{') {
            return Err(Error::Unsupported(
                "multiple document literals are not supported".to_string(),
            ));
        }
        parser.pos = comma;
    }
    if parser.peek().is_some() {
        return Err(parser.error("unexpected trailing input"));
    }
    Ok(document)
}

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.input[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }

    fn error(&self, message: &str) -> Error {
        Error::Parse(format!("{message} at byte {}", self.pos))
    }

    fn object(&mut self) -> Result<Map<String, Value>> {
        let start = self.pos;
        if self.bump() != Some('{') {
            return Err(self.error("expected '{'"));
        }
        self.skip_whitespace();
        let mut map = Map::new();
        if self.peek() == Some('}') {
            self.bump();
            return Ok(map);
        }
        loop {
            let key = self.key()?;
            self.skip_whitespace();
            if self.peek() != Some(':') {
                return Err(self.error("expected ':' after object key"));
            }
            self.bump();
            let value = self.value()?;
            map.insert(key, value);
            self.skip_whitespace();
            match self.peek() {
                Some('}') => {
                    self.bump();
                    return Ok(map);
                }
                Some(',') => {
                    self.bump();
                    self.skip_whitespace();
                    if self.peek() == Some('}') {
                        return Err(self.error("trailing comma in object"));
                    }
                }
                None => {
                    self.pos = self.input.len();
                    return Err(Error::Parse(format!("unclosed brace at byte {start}")));
                }
                _ => return Err(self.error("expected ',' or '}' in object")),
            }
        }
    }

    fn array(&mut self) -> Result<Vec<Value>> {
        let start = self.pos;
        self.bump();
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.peek() == Some(']') {
            self.bump();
            return Ok(values);
        }
        loop {
            values.push(self.value()?);
            self.skip_whitespace();
            match self.peek() {
                Some(']') => {
                    self.bump();
                    return Ok(values);
                }
                Some(',') => {
                    self.bump();
                    self.skip_whitespace();
                    if self.peek() == Some(']') {
                        return Err(self.error("trailing comma in array"));
                    }
                }
                None => {
                    return Err(Error::Parse(format!("unclosed bracket at byte {start}")));
                }
                _ => return Err(self.error("expected ',' or ']' in array")),
            }
        }
    }

    fn key(&mut self) -> Result<String> {
        self.skip_whitespace();
        if self.peek() == Some('\'') {
            return self.string();
        }
        let start = self.pos;
        let first = self
            .peek()
            .ok_or_else(|| self.error("expected object key"))?;
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(self.error("invalid object key"));
        }
        self.bump();
        while self
            .peek()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            self.bump();
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn value(&mut self) -> Result<Value> {
        self.skip_whitespace();
        match self.peek() {
            Some('{') => Ok(Value::Object(self.object()?)),
            Some('[') => Ok(Value::Array(self.array()?)),
            Some('\'') => Ok(Value::String(self.string()?)),
            Some('-' | '0'..='9') => self.number(),
            Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => self.keyword(),
            Some(_) => Err(self.error("expected value")),
            None => Err(self.error("expected value")),
        }
    }

    fn string(&mut self) -> Result<String> {
        let start = self.pos;
        self.bump();
        let mut output = String::new();
        loop {
            match self.bump() {
                Some('\'') if self.peek() == Some('\'') => {
                    self.bump();
                    output.push('\'');
                }
                Some('\'') => return Ok(output),
                Some(ch) => output.push(ch),
                None => {
                    return Err(Error::Parse(format!("unclosed quote at byte {start}")));
                }
            }
        }
    }

    fn number(&mut self) -> Result<Value> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.bump();
        }
        while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
            self.bump();
        }
        if self.peek() == Some('.') {
            self.bump();
            while self.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                self.bump();
            }
        }
        let text = &self.input[start..self.pos];
        if let Ok(value) = text.parse::<i64>() {
            return Ok(Value::Number(value.into()));
        }
        if let Ok(value) = text.parse::<f64>() {
            if let Some(number) = Number::from_f64(value) {
                return Ok(Value::Number(number));
            }
        }
        Err(Error::Parse(format!(
            "invalid number literal '{text}' at byte {start}"
        )))
    }

    fn keyword(&mut self) -> Result<Value> {
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            self.bump();
        }
        let word = &self.input[start..self.pos];
        if word.eq_ignore_ascii_case("true") {
            Ok(Value::Bool(true))
        } else if word.eq_ignore_ascii_case("false") {
            Ok(Value::Bool(false))
        } else if word.eq_ignore_ascii_case("null") {
            Ok(Value::Null)
        } else {
            Err(Error::Parse(format!(
                "bare value '{word}' is not supported at byte {start}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_scalars_nested_values_and_unicode() {
        let value = parse_document(
            "{s: 'it''s こんにちは', i: 1, f: -2.5, yes: TRUE, no: false, n: NULL, nested: {x: 2}, arr: [1, 'x', {ok: true}], empty: []}",
        )
        .unwrap();
        assert_eq!(
            Value::Object(value),
            json!({
                "s": "it's こんにちは", "i": 1, "f": -2.5, "yes": true,
                "no": false, "n": null, "nested": {"x": 2},
                "arr": [1, "x", {"ok": true}], "empty": []
            })
        );
    }

    #[test]
    fn accepts_empty_object_and_single_quoted_keys() {
        assert_eq!(parse_document("{}").unwrap(), Map::new());
        assert_eq!(
            parse_document("{'my key': 'v'};").unwrap()["my key"],
            json!("v")
        );
    }

    #[test]
    fn duplicate_keys_use_last_value() {
        assert_eq!(parse_document("{a: 1, a: 2}").unwrap()["a"], json!(2));
    }

    #[test]
    fn named_syntax_errors_include_position() {
        for input in [
            "{a: 1",
            "{a: [1",
            "{a: 'x",
            "{a: 1,}",
            "{a: [1,]}",
            "{a 1}",
            "{1: 2}",
            "{a: nope}",
            "{} garbage",
        ] {
            let error = parse_document(input).unwrap_err();
            assert!(matches!(error, Error::Parse(_)), "{input}: {error}");
            assert!(error.to_string().contains("byte"), "{input}: {error}");
        }
    }

    #[test]
    fn trailing_non_document_content_is_a_parse_error() {
        for input in ["{a: 1},", "{a: 1}, nope"] {
            let error = parse_document(input).unwrap_err();
            assert!(matches!(error, Error::Parse(_)), "{input}: {error}");
            assert!(error.to_string().contains("unexpected trailing input"));
        }
    }

    #[test]
    fn two_or_more_documents_are_unsupported() {
        for input in ["{a: 1}, {a: 2}", "{a: 1}, {a: 2}, {a: 3}"] {
            let error = parse_document(input).unwrap_err();
            assert!(matches!(error, Error::Unsupported(_)), "{input}: {error}");
            assert!(error.to_string().contains("multiple document literals"));
        }
    }

    #[test]
    fn errors_point_at_unexpected_delimiter() {
        let missing_colon = parse_document("{a 1}").unwrap_err();
        assert!(missing_colon.to_string().contains("byte 3"));

        let bad_object_delimiter = parse_document("{a: 1 x}").unwrap_err();
        assert!(bad_object_delimiter.to_string().contains("byte 6"));

        let bad_array_delimiter = parse_document("{a: [1 x]}").unwrap_err();
        assert!(bad_array_delimiter.to_string().contains("byte 7"));
    }
}
