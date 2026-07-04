use cairndb::Database;
use serde_json::json;

#[test]
fn select_star_returns_all_current_docs() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    db.insert("events", json!({"x": 1})).unwrap();
    db.insert("events", json!({"x": 2})).unwrap();

    let result = db.sql("SELECT * FROM events").unwrap();
    assert_eq!(result.len(), 2);
}

#[test]
fn select_with_id_filter_returns_exact_doc() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    let doc = db.insert("events", json!({"x": 1})).unwrap();
    db.insert("events", json!({"x": 2})).unwrap();

    let result = db
        .sql(&format!("SELECT * FROM events WHERE _id = '{}'", doc.id()))
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.documents()[0].id(), doc.id());
}

#[test]
fn select_with_unknown_id_returns_document_not_found() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();

    let err = db
        .sql("SELECT * FROM events WHERE _id = 'does-not-exist'")
        .unwrap_err();
    assert!(matches!(err, cairndb::Error::Core(_)));
    assert!(err.to_string().contains("document not found"));
}

#[test]
fn select_for_system_time_all_returns_all_versions() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    let doc = db.insert("events", json!({"x": 1})).unwrap();
    db.update("events", doc.id(), json!({"x": 2})).unwrap();

    let result = db.sql("SELECT * FROM events FOR SYSTEM_TIME ALL").unwrap();
    assert!(
        result.len() >= 2,
        "expected at least 2 versions, got {}",
        result.len()
    );
}

#[test]
fn select_as_of_before_insert_is_empty_and_at_insert_returns_doc() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    let doc = db.insert("events", json!({"x": 1})).unwrap();
    let ts = doc.system_time();

    let before = db
        .sql("SELECT * FROM events FOR SYSTEM_TIME AS OF '1970-01-01T00:00:00.000Z'")
        .unwrap();
    assert!(before.is_empty());

    let at = db
        .sql(&format!(
            "SELECT * FROM events FOR SYSTEM_TIME AS OF '{ts}'"
        ))
        .unwrap();
    assert_eq!(at.len(), 1);
    assert_eq!(at.documents()[0].id(), doc.id());
}

#[test]
fn select_between_spanning_insert_returns_doc_and_reversed_range_is_empty() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    let doc = db.insert("events", json!({"x": 1})).unwrap();

    let spanning = db
        .sql(
            "SELECT * FROM events FOR SYSTEM_TIME BETWEEN '1970-01-01T00:00:00.000Z' AND '2999-01-01T00:00:00.000Z'",
        )
        .unwrap();
    assert_eq!(spanning.len(), 1);
    assert_eq!(spanning.documents()[0].id(), doc.id());

    let reversed = db
        .sql(
            "SELECT * FROM events FOR SYSTEM_TIME BETWEEN '2999-01-01T00:00:00.000Z' AND '1970-01-01T00:00:00.000Z'",
        )
        .unwrap();
    assert!(reversed.is_empty());
}

#[test]
fn select_projections_rejected() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();

    let err = db.sql("SELECT a, b FROM events").unwrap_err();
    assert!(err.to_string().contains("projections"), "error was: {err}");
}

#[test]
fn select_arbitrary_where_rejected() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();

    let err = db.sql("SELECT * FROM events WHERE x = 1").unwrap_err();
    assert!(
        err.to_string().contains("arbitrary WHERE"),
        "error was: {err}"
    );
}

#[test]
fn select_order_by_rejected() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();

    let err = db.sql("SELECT * FROM events ORDER BY _id").unwrap_err();
    assert!(err.to_string().contains("ORDER BY"), "error was: {err}");
}

#[test]
fn select_filter_and_temporal_combination_rejected() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();

    let err = db
        .sql("SELECT * FROM events WHERE _id = 'x' FOR SYSTEM_TIME ALL")
        .unwrap_err();
    assert!(
        err.to_string().contains("FOR SYSTEM_TIME"),
        "error was: {err}"
    );
}

#[test]
fn select_malformed_temporal_clause_is_parse_error() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();

    let err = db
        .sql("SELECT * FROM events FOR SYSTEM_TIME AS OF banana")
        .unwrap_err();
    assert!(matches!(err, cairndb::Error::Parse(_)));
}

#[test]
fn select_case_insensitive_keywords() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    db.insert("events", json!({"x": 1})).unwrap();

    let result = db.sql("select * from events for system_time all").unwrap();
    assert!(!result.is_empty());
}

#[test]
fn select_string_literal_safety() {
    let db = Database::open_in_memory().unwrap();
    db.sql("CREATE TABLE events").unwrap();
    // Insert a doc whose data contains the literal text "FOR SYSTEM_TIME" to
    // make sure the stripper only looks at SQL text, not document data.
    let doc = db
        .insert("events", json!({"note": "FOR SYSTEM_TIME ALL"}))
        .unwrap();

    let found = db
        .sql(&format!("SELECT * FROM events WHERE _id = '{}'", doc.id()))
        .unwrap();
    assert_eq!(found.len(), 1);

    // A WHERE value containing the temporal keywords must not be mistaken
    // for an actual FOR SYSTEM_TIME clause (proves the stripper respects
    // quoted string literals).
    let err = db
        .sql("SELECT * FROM events WHERE _id = 'FOR SYSTEM_TIME ALL'")
        .unwrap_err();
    assert!(matches!(err, cairndb::Error::Core(_)));
    assert!(err.to_string().contains("document not found"));
}
