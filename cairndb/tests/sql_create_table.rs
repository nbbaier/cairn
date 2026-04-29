use cairndb::Database;

#[test]
fn sql_create_table_basic() {
    let db = Database::open_in_memory().unwrap();
    let result = db.sql("CREATE TABLE events").unwrap();
    assert!(result.is_empty());
    // Verify the table actually exists by inserting into it
    let doc = db.insert("events", serde_json::json!({"x": 1})).unwrap();
    assert_eq!(db.get("events", doc.id()).unwrap().id(), doc.id());
}

#[test]
fn sql_create_table_idempotent() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    let result = db.sql("CREATE TABLE events");
    assert!(result.is_ok());
}

#[test]
fn sql_create_table_case_insensitive_keywords() {
    let db = Database::open_in_memory().unwrap();
    let result = db.sql("create table my_table");
    assert!(result.is_ok());
}

#[test]
fn sql_create_table_with_semicolon() {
    let db = Database::open_in_memory().unwrap();
    let result = db.sql("CREATE TABLE events;");
    assert!(result.is_ok());
}

#[test]
fn sql_create_table_with_columns_rejected() {
    let db = Database::open_in_memory().unwrap();
    let err = db.sql("CREATE TABLE events (id TEXT)").unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("column definitions"), "error was: {msg}");
}

#[test]
fn sql_parse_error_on_invalid_input() {
    let db = Database::open_in_memory().unwrap();
    let err = db.sql("NOT VALID SQL").unwrap_err();
    assert!(matches!(err, cairndb::Error::Parse(_)));
}

#[test]
fn sql_parse_error_on_empty_input() {
    let db = Database::open_in_memory().unwrap();
    let err = db.sql("").unwrap_err();
    assert!(matches!(err, cairndb::Error::Parse(_)));
}

#[test]
fn sql_unsupported_statement_rejected() {
    let db = Database::open_in_memory().unwrap();
    let err = db.sql("DROP TABLE events").unwrap_err();
    assert!(matches!(err, cairndb::Error::Parse(_)));
}

#[test]
fn sql_create_table_then_query_empty() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    let result = db.query("events").unwrap();
    assert!(result.is_empty());
}

#[test]
fn sql_create_table_returns_empty_query_result() {
    let db = Database::open_in_memory().unwrap();
    let result = db.sql("CREATE TABLE events").unwrap();
    assert_eq!(result.len(), 0);
    assert!(result.is_empty());
}
