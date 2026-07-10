use cairndb::Database;
use serde_json::json;

#[test]
fn insert_returns_one_document_with_id() {
    let db = Database::open_in_memory().unwrap();

    let result = db
        .sql("INSERT INTO events (name) VALUES ('test')")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert!(!result.documents()[0].id().is_empty());
}

#[test]
fn insert_auto_creates_table_and_doc_is_queryable() {
    let db = Database::open_in_memory().unwrap();

    // No CREATE TABLE first — insert must auto-create it.
    let inserted = db
        .sql("INSERT INTO events (name) VALUES ('deploy')")
        .unwrap();
    let id = inserted.documents()[0].id().to_string();

    let all = db.query("events").unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all.documents()[0].id(), id);
    assert_eq!(all.documents()[0].get("name"), Some(&json!("deploy")));
}

#[test]
fn insert_multi_column_mixed_types() {
    let db = Database::open_in_memory().unwrap();

    let result = db
        .sql("INSERT INTO t (a, b, c) VALUES (1, 'x', true)")
        .unwrap();
    let doc = &result.documents()[0];
    assert_eq!(doc.get("a"), Some(&json!(1)));
    assert_eq!(doc.get("b"), Some(&json!("x")));
    assert_eq!(doc.get("c"), Some(&json!(true)));

    // Read back through a fresh query, not just the returned document.
    let read = db.query("t").unwrap();
    let doc = &read.documents()[0];
    assert!(doc.get("a").unwrap().is_i64());
    assert!(doc.get("b").unwrap().is_string());
    assert!(doc.get("c").unwrap().is_boolean());
}

#[test]
fn insert_null_value_maps_to_json_null() {
    let db = Database::open_in_memory().unwrap();

    let result = db.sql("INSERT INTO t (a) VALUES (NULL)").unwrap();
    assert_eq!(
        result.documents()[0].get("a"),
        Some(&serde_json::Value::Null)
    );
}

#[test]
fn insert_float_and_negative_values() {
    let db = Database::open_in_memory().unwrap();

    let result = db
        .sql("INSERT INTO t (a, b) VALUES (-7, 3.25)")
        .unwrap();
    let doc = &result.documents()[0];
    assert_eq!(doc.get("a"), Some(&json!(-7)));
    assert_eq!(doc.get("b"), Some(&json!(3.25)));
}

#[test]
fn insert_sql_matches_rust_api_insert() {
    let db = Database::open_in_memory().unwrap();

    let via_sql = db
        .sql("INSERT INTO t (name, count) VALUES ('x', 3)")
        .unwrap();
    let via_api = db.insert("t", json!({"name": "x", "count": 3})).unwrap();

    let sql_doc = &via_sql.documents()[0];
    assert_eq!(sql_doc.get("name"), via_api.get("name"));
    assert_eq!(sql_doc.get("count"), via_api.get("count"));
    assert_ne!(sql_doc.id(), via_api.id());
}

#[test]
fn insert_string_with_escaped_quote() {
    let db = Database::open_in_memory().unwrap();

    let result = db
        .sql("INSERT INTO t (a) VALUES ('it''s fine')")
        .unwrap();
    assert_eq!(result.documents()[0].get("a"), Some(&json!("it's fine")));
}

#[test]
fn insert_count_mismatch_is_error() {
    let db = Database::open_in_memory().unwrap();

    let err = db.sql("INSERT INTO t (a) VALUES (1, 2)").unwrap_err();
    assert!(matches!(err, cairndb::Error::Parse(_)));
    assert!(
        err.to_string().contains("expected 1 values, got 2"),
        "error was: {err}"
    );
}

#[test]
fn insert_multi_row_values_is_error() {
    let db = Database::open_in_memory().unwrap();

    let err = db.sql("INSERT INTO t (a) VALUES (1), (2)").unwrap_err();
    assert!(err.to_string().contains("multi-row"), "error was: {err}");
}

#[test]
fn insert_select_is_error() {
    let db = Database::open_in_memory().unwrap();

    let err = db.sql("INSERT INTO t SELECT * FROM u").unwrap_err();
    assert!(err.to_string().contains("SELECT"), "error was: {err}");
}

#[test]
fn insert_document_literal_is_unsupported_for_now() {
    let db = Database::open_in_memory().unwrap();

    let err = db.sql("INSERT INTO t {name: 'x'}").unwrap_err();
    assert!(
        err.to_string().contains("document literal"),
        "error was: {err}"
    );
}

#[test]
fn insert_invalid_table_name_is_parse_error() {
    let db = Database::open_in_memory().unwrap();

    let err = db.sql("INSERT INTO 123abc (a) VALUES (1)").unwrap_err();
    assert!(matches!(err, cairndb::Error::Parse(_)), "error was: {err}");
}
