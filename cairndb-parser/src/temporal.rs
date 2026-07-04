use crate::error::{Error, Result};
use crate::ir::TemporalClause;

/// Scans `sql` for a `FOR SYSTEM_TIME ...` clause (case-insensitive, outside
/// single-quoted string literals), removes it, and returns the remaining SQL
/// plus the extracted clause. sqlparser-rs cannot parse this non-standard
/// clause, so it must be stripped first (decision #18).
pub(crate) fn strip_system_time(sql: &str) -> Result<(String, Option<TemporalClause>)> {
    let chars: Vec<char> = sql.chars().collect();

    let Some((kw_start, kw_end)) = find_keyword(&chars) else {
        return Ok((sql.to_string(), None));
    };

    let (clause, clause_end) = parse_clause_body(&chars, kw_end)?;

    if find_keyword(&chars[..kw_start]).is_some() || find_keyword(&chars[clause_end..]).is_some() {
        return Err(Error::Parse(
            "FOR SYSTEM_TIME specified more than once".to_string(),
        ));
    }

    let mut result = String::with_capacity(chars.len());
    result.extend(&chars[..kw_start]);
    result.push(' ');
    result.extend(&chars[clause_end..]);

    Ok((result, Some(clause)))
}

// ---------------------------------------------------------------------------
// Minimal literal-aware scanner
// ---------------------------------------------------------------------------

fn is_word_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn is_word_start(chars: &[char], i: usize) -> bool {
    i == 0 || !is_word_char(chars[i - 1])
}

/// Finds the first `FOR SYSTEM_TIME` keyword span (case-insensitive, word
/// bounded, separated by any run of whitespace) outside single-quoted string
/// literals. Returns the byte-equivalent char-index span `[start, end)`
/// covering just the two keywords (not the clause body that follows).
fn find_keyword(chars: &[char]) -> Option<(usize, usize)> {
    let n = chars.len();
    let mut i = 0;
    let mut in_string = false;

    while i < n {
        let c = chars[i];

        if in_string {
            if c == '\'' {
                if i + 1 < n && chars[i + 1] == '\'' {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }

        if c == '\'' {
            in_string = true;
            i += 1;
            continue;
        }

        if is_word_start(chars, i) {
            if let Some(after_for) = try_match_word_ci(chars, i, "FOR") {
                let ws_start = after_for;
                let j = skip_ws(chars, after_for);
                if j > ws_start {
                    if let Some(after_st) = try_match_word_ci(chars, j, "SYSTEM_TIME") {
                        return Some((i, after_st));
                    }
                }
            }
        }

        i += 1;
    }

    None
}

/// Attempts to match `word` (case-insensitive) at `chars[i..]`, requiring a
/// word boundary immediately after. Returns the end index on success.
fn try_match_word_ci(chars: &[char], i: usize, word: &str) -> Option<usize> {
    let wchars: Vec<char> = word.chars().collect();
    let end = i + wchars.len();
    if end > chars.len() {
        return None;
    }
    for (k, wc) in wchars.iter().enumerate() {
        if !chars[i + k].eq_ignore_ascii_case(wc) {
            return None;
        }
    }
    if end < chars.len() && is_word_char(chars[end]) {
        return None;
    }
    Some(end)
}

fn skip_ws(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    i
}

/// Parses a single-quoted string literal starting at `chars[start]` (which
/// must be `'`), handling `''` as an escaped quote. Returns the literal's
/// content (unescaped) and the index just past the closing quote.
fn parse_string_literal(chars: &[char], start: usize) -> Result<(String, usize)> {
    let n = chars.len();
    if start >= n || chars[start] != '\'' {
        return Err(Error::Parse(
            "malformed FOR SYSTEM_TIME clause: expected a quoted timestamp literal".to_string(),
        ));
    }

    let mut i = start + 1;
    let mut content = String::new();
    loop {
        if i >= n {
            return Err(Error::Parse(
                "malformed FOR SYSTEM_TIME clause: unterminated string literal".to_string(),
            ));
        }
        let c = chars[i];
        if c == '\'' {
            if i + 1 < n && chars[i + 1] == '\'' {
                content.push('\'');
                i += 2;
                continue;
            }
            i += 1;
            break;
        }
        content.push(c);
        i += 1;
    }

    Ok((content, i))
}

/// Parses the clause body following `FOR SYSTEM_TIME`, i.e. one of
/// `ALL`, `AS OF '<ts>'`, or `BETWEEN '<ts1>' AND '<ts2>'`. Returns the
/// parsed clause and the index just past the end of the clause.
fn parse_clause_body(chars: &[char], from: usize) -> Result<(TemporalClause, usize)> {
    let i = skip_ws(chars, from);

    if let Some(end) = try_match_word_ci(chars, i, "ALL") {
        return Ok((TemporalClause::All, end));
    }

    if let Some(after_as) = try_match_word_ci(chars, i, "AS") {
        let j = skip_ws(chars, after_as);
        let after_of = try_match_word_ci(chars, j, "OF").ok_or_else(|| {
            Error::Parse("malformed FOR SYSTEM_TIME clause: expected AS OF '<ts>'".to_string())
        })?;
        let j2 = skip_ws(chars, after_of);
        let (ts, end) = parse_string_literal(chars, j2)?;
        return Ok((TemporalClause::AsOf(ts), end));
    }

    if let Some(after_between) = try_match_word_ci(chars, i, "BETWEEN") {
        let j = skip_ws(chars, after_between);
        let (ts1, end1) = parse_string_literal(chars, j)?;
        let j2 = skip_ws(chars, end1);
        let after_and = try_match_word_ci(chars, j2, "AND").ok_or_else(|| {
            Error::Parse(
                "malformed FOR SYSTEM_TIME clause: expected BETWEEN '<ts1>' AND '<ts2>'"
                    .to_string(),
            )
        })?;
        let j3 = skip_ws(chars, after_and);
        let (ts2, end2) = parse_string_literal(chars, j3)?;
        return Ok((TemporalClause::Between(ts1, ts2), end2));
    }

    Err(Error::Parse(
        "malformed FOR SYSTEM_TIME clause: expected ALL, AS OF '<ts>', or BETWEEN '<ts1>' AND '<ts2>'"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_clause_present() {
        let (sql, clause) = strip_system_time("SELECT * FROM t").unwrap();
        assert_eq!(sql, "SELECT * FROM t");
        assert_eq!(clause, None);
    }

    #[test]
    fn all_clause() {
        let (sql, clause) = strip_system_time("SELECT * FROM t FOR SYSTEM_TIME ALL").unwrap();
        assert_eq!(sql, "SELECT * FROM t  ");
        assert_eq!(clause, Some(TemporalClause::All));
    }

    #[test]
    fn as_of_clause() {
        let (sql, clause) =
            strip_system_time("SELECT * FROM t FOR SYSTEM_TIME AS OF '2024-01-01T00:00:00.000Z'")
                .unwrap();
        assert_eq!(sql, "SELECT * FROM t  ");
        assert_eq!(
            clause,
            Some(TemporalClause::AsOf("2024-01-01T00:00:00.000Z".to_string()))
        );
    }

    #[test]
    fn between_clause() {
        let (sql, clause) = strip_system_time(
            "SELECT * FROM t FOR SYSTEM_TIME BETWEEN '2024-01-01T00:00:00.000Z' AND '2024-12-31T00:00:00.000Z'",
        )
        .unwrap();
        assert_eq!(sql, "SELECT * FROM t  ");
        assert_eq!(
            clause,
            Some(TemporalClause::Between(
                "2024-01-01T00:00:00.000Z".to_string(),
                "2024-12-31T00:00:00.000Z".to_string()
            ))
        );
    }

    #[test]
    fn case_insensitive_and_extra_whitespace() {
        let (sql, clause) = strip_system_time("select * from t   for   system_time   all").unwrap();
        assert_eq!(sql, "select * from t    ");
        assert_eq!(clause, Some(TemporalClause::All));
    }

    #[test]
    fn escaped_quote_literal_does_not_confuse_scanner() {
        let (sql, clause) =
            strip_system_time("SELECT * FROM t WHERE _id = 'it''s a test' FOR SYSTEM_TIME ALL")
                .unwrap();
        assert_eq!(sql, "SELECT * FROM t WHERE _id = 'it''s a test'  ");
        assert_eq!(clause, Some(TemporalClause::All));
    }

    #[test]
    fn keyword_inside_string_literal_is_ignored() {
        let (sql, clause) =
            strip_system_time("SELECT * FROM t WHERE _id = 'FOR SYSTEM_TIME ALL'").unwrap();
        assert_eq!(sql, "SELECT * FROM t WHERE _id = 'FOR SYSTEM_TIME ALL'");
        assert_eq!(clause, None);
    }

    #[test]
    fn malformed_clause_is_parse_error() {
        let err = strip_system_time("SELECT * FROM t FOR SYSTEM_TIME AS OF banana").unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("malformed"));
    }

    #[test]
    fn duplicated_clause_is_parse_error() {
        let err = strip_system_time("SELECT * FROM t FOR SYSTEM_TIME ALL FOR SYSTEM_TIME ALL")
            .unwrap_err();
        assert!(matches!(err, Error::Parse(_)));
        assert!(err.to_string().contains("more than once"));
    }

    #[test]
    fn clause_on_create_table_is_extracted_by_stripper() {
        // The stripper itself is statement-agnostic; rejecting FOR SYSTEM_TIME
        // on non-SELECT statements happens in `standard::parse_standard`.
        let (sql, clause) = strip_system_time("CREATE TABLE t FOR SYSTEM_TIME ALL").unwrap();
        assert_eq!(sql, "CREATE TABLE t  ");
        assert_eq!(clause, Some(TemporalClause::All));
    }
}
