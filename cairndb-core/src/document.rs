use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A versioned JSON document stored in the database.
///
/// Contains the document's unique ID, its JSON data payload, the transaction ID
/// that created/last-modified it, and the epoch-millisecond timestamp of that
/// transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    id: String,
    data: Map<String, Value>,
    valid_from: i64,
    txn_id: i64,
}

impl Document {
    /// Creates a new `Document`.  For internal use only.
    #[allow(dead_code)]
    pub(crate) fn new(id: String, data: Map<String, Value>, valid_from: i64, txn_id: i64) -> Self {
        Self {
            id,
            data,
            valid_from,
            txn_id,
        }
    }

    /// Returns the document's unique ID (UUIDv7 in hyphenated format).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the document's creation/modification time as an ISO 8601 string
    /// (e.g. `"2024-04-09T12:34:56.789Z"`), derived from the internal epoch-ms
    /// timestamp.
    pub fn system_time(&self) -> String {
        epoch_ms_to_iso8601(self.valid_from)
    }

    /// Returns the document's JSON data payload as a JSON object map.
    pub fn data(&self) -> &Map<String, Value> {
        &self.data
    }

    /// Returns the value for `key` within the document's data object, or
    /// `None` if the key does not exist.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// Returns the ID of the transaction that created or last modified this
    /// document.
    pub fn txn_id(&self) -> i64 {
        self.txn_id
    }
}

// ---------------------------------------------------------------------------
// Timestamp conversion
// ---------------------------------------------------------------------------

/// Converts an epoch-millisecond timestamp to an ISO 8601 UTC string.
///
/// Example: `0` → `"1970-01-01T00:00:00.000Z"`
fn epoch_ms_to_iso8601(epoch_ms: i64) -> String {
    // Use Euclidean division so negative timestamps work correctly.
    let secs = epoch_ms.div_euclid(1000);
    let ms = epoch_ms.rem_euclid(1000) as u32;

    let sec_of_day = secs.rem_euclid(86400) as u32;
    let hour = sec_of_day / 3600;
    let min = (sec_of_day % 3600) / 60;
    let sec = sec_of_day % 60;

    let days = secs.div_euclid(86400);
    let (year, month, day) = civil_from_days(days);

    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{ms:03}Z",
    )
}

/// Converts days-since-Unix-epoch (1970-01-01) to a `(year, month, day)` triple.
///
/// Uses the algorithm by Howard Hinnant:
/// <https://howardhinnant.github.io/date_algorithms.html>
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // day of era [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // year of era [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month primitive [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

/// A collection of [`Document`]s returned from a database query.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    documents: Vec<Document>,
}

impl QueryResult {
    /// Creates a new `QueryResult`.  For internal use only.
    #[allow(dead_code)]
    pub(crate) fn new(documents: Vec<Document>) -> Self {
        Self { documents }
    }

    /// Returns the number of documents in this result.
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Returns `true` if there are no documents in this result.
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Returns a slice of all documents in this result.
    pub fn documents(&self) -> &[Document] {
        &self.documents
    }

    /// Consumes the `QueryResult` and returns the inner `Vec<Document>`.
    pub fn into_documents(self) -> Vec<Document> {
        self.documents
    }
}

impl IntoIterator for QueryResult {
    type Item = Document;
    type IntoIter = std::vec::IntoIter<Document>;

    fn into_iter(self) -> Self::IntoIter {
        self.documents.into_iter()
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = &'a Document;
    type IntoIter = std::slice::Iter<'a, Document>;

    fn into_iter(self) -> Self::IntoIter {
        self.documents.iter()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn make_doc(id: &str, data: Value, valid_from: i64, txn_id: i64) -> Document {
        let map = match data {
            Value::Object(m) => m,
            _ => panic!("test data must be a JSON object"),
        };
        Document::new(id.to_string(), map, valid_from, txn_id)
    }

    /// Validate that a string looks like an ISO 8601 UTC timestamp
    /// (`YYYY-MM-DDTHH:MM:SS.mmmZ`, 24 characters).
    fn assert_iso8601(s: &str) {
        assert_eq!(s.len(), 24, "Expected 24-char ISO 8601 string, got: {s:?}");
        let bytes = s.as_bytes();
        assert_eq!(bytes[4], b'-');
        assert_eq!(bytes[7], b'-');
        assert_eq!(bytes[10], b'T');
        assert_eq!(bytes[13], b':');
        assert_eq!(bytes[16], b':');
        assert_eq!(bytes[19], b'.');
        assert_eq!(bytes[23], b'Z');
        // Digits in expected positions
        for &i in &[0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 22] {
            assert!(
                bytes[i].is_ascii_digit(),
                "Expected digit at position {i} in {s:?}"
            );
        }
    }

    // ------------------------------------------------------------------
    // epoch_ms_to_iso8601 unit tests
    // ------------------------------------------------------------------

    #[test]
    fn epoch_zero_is_unix_epoch() {
        assert_eq!(epoch_ms_to_iso8601(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn epoch_one_second() {
        assert_eq!(epoch_ms_to_iso8601(1_000), "1970-01-01T00:00:01.000Z");
    }

    #[test]
    fn epoch_one_day() {
        assert_eq!(epoch_ms_to_iso8601(86_400_000), "1970-01-02T00:00:00.000Z");
    }

    #[test]
    fn epoch_with_milliseconds() {
        // 999 ms → .999
        assert_eq!(epoch_ms_to_iso8601(999), "1970-01-01T00:00:00.999Z");
    }

    #[test]
    fn epoch_known_date_2024() {
        // 2024-01-01T00:00:00.000Z = 1704067200000 ms since epoch
        assert_eq!(epoch_ms_to_iso8601(1_704_067_200_000), "2024-01-01T00:00:00.000Z");
    }

    // ------------------------------------------------------------------
    // Document accessor tests
    // ------------------------------------------------------------------

    #[test]
    fn document_id() {
        let doc = make_doc("test-id-123", json!({"x": 1}), 1_000, 42);
        assert_eq!(doc.id(), "test-id-123");
    }

    #[test]
    fn document_system_time_iso8601() {
        let doc = make_doc("id", json!({}), 1_704_067_200_000, 1);
        let ts = doc.system_time();
        assert_iso8601(&ts);
        assert_eq!(ts, "2024-01-01T00:00:00.000Z");
    }

    #[test]
    fn document_data_access() {
        let doc = make_doc("id", json!({"name": "Alice", "age": 30}), 0, 1);
        let data = doc.data();
        assert_eq!(data["name"], json!("Alice"));
        assert_eq!(data["age"], json!(30));
    }

    #[test]
    fn document_get_existing_key() {
        let doc = make_doc("id", json!({"city": "London"}), 0, 1);
        let val = doc.get("city");
        assert!(val.is_some());
        assert_eq!(val.unwrap(), &json!("London"));
    }

    #[test]
    fn document_get_missing_key() {
        let doc = make_doc("id", json!({"a": 1}), 0, 1);
        assert!(doc.get("b").is_none());
    }

    #[test]
    fn document_txn_id() {
        let doc = make_doc("id", json!({}), 0, 99);
        assert_eq!(doc.txn_id(), 99);
    }

    #[test]
    fn document_serde_roundtrip() {
        let original = make_doc(
            "round-trip-id",
            json!({"key": "value", "num": 42, "nested": {"a": true}}),
            1_704_067_200_000,
            7,
        );

        let serialized = serde_json::to_string(&original).expect("serialize failed");
        let deserialized: Document =
            serde_json::from_str(&serialized).expect("deserialize failed");

        assert_eq!(deserialized.id(), original.id());
        assert_eq!(deserialized.data(), original.data());
        assert_eq!(deserialized.valid_from, original.valid_from);
        assert_eq!(deserialized.txn_id(), original.txn_id());
    }

    // ------------------------------------------------------------------
    // QueryResult tests
    // ------------------------------------------------------------------

    #[test]
    fn query_result_empty() {
        let qr = QueryResult::new(vec![]);
        assert!(qr.is_empty());
        assert_eq!(qr.len(), 0);
    }

    #[test]
    fn query_result_accessors() {
        let docs = vec![
            make_doc("id1", json!({"x": 1}), 0, 1),
            make_doc("id2", json!({"x": 2}), 1, 2),
            make_doc("id3", json!({"x": 3}), 2, 3),
        ];
        let qr = QueryResult::new(docs);
        assert!(!qr.is_empty());
        assert_eq!(qr.len(), 3);
        assert_eq!(qr.documents().len(), 3);
    }

    #[test]
    fn query_result_into_iter() {
        let docs = vec![
            make_doc("a", json!({"n": 1}), 0, 1),
            make_doc("b", json!({"n": 2}), 1, 2),
        ];
        let qr = QueryResult::new(docs);
        let ids: Vec<String> = qr.into_iter().map(|d| d.id().to_string()).collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn query_result_ref_into_iter() {
        let docs = vec![
            make_doc("x", json!({"v": 10}), 0, 1),
            make_doc("y", json!({"v": 20}), 1, 2),
        ];
        let qr = QueryResult::new(docs);
        let ids: Vec<&str> = (&qr).into_iter().map(|d| d.id()).collect();
        assert_eq!(ids, vec!["x", "y"]);
        // qr is still usable after borrowing iteration
        assert_eq!(qr.len(), 2);
    }

    #[test]
    fn query_result_into_documents() {
        let docs = vec![make_doc("d1", json!({}), 0, 1)];
        let qr = QueryResult::new(docs);
        let v = qr.into_documents();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].id(), "d1");
    }
}
