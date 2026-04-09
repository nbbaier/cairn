# cairn

An embedded temporal document database built on SQLite.

## What is cairn?

Cairn is an in-process, schema-flexible document database with automatic versioning and time-travel queries. Every insert, update, and delete is recorded — nothing is destroyed unless you explicitly erase it. The storage engine is SQLite (via rusqlite), so your database is a single file on disk.

Think of it as what [Endatabas](https://www.endatabas.com/) would look like if it were embedded like SQLite instead of running as a server.

## Status

**v0.1 — storage layer only.** The core storage engine (`cairndb-core`) is implemented and tested. The temporal SQL parser (`cairndb-parser`) is not yet built — the current API is Rust-native method calls, not SQL strings.

## Features

- **Schema-last document storage** — tables are auto-created on first insert, no schema declaration needed
- **Automatic versioning** — updates and deletes preserve full history in append-only history tables
- **Time-travel queries** — query documents as they existed at any point in time
- **Range queries** — query all document versions active during a time range
- **Soft delete** — `delete` removes from current state but preserves history
- **Hard erase** — `erase` permanently removes all traces (for GDPR compliance), logged to an audit table
- **UUIDv7 document IDs** — time-sortable, globally unique
- **JSON Merge Patch updates** — partial updates via RFC 7396 (set a key to `null` to remove it)
- **Thread-safe** — `Database` is `Send + Sync`, safe to share via `Arc`
- **WAL mode** — enabled by default for concurrent read performance
- **In-memory or file-backed** — `open_in_memory()` for tests, `open(path)` for persistence

## Usage

```rust
use cairndb_core::{Database, Error};
use serde_json::json;

// Open a database (file-backed or in-memory)
let db = Database::open("my.db")?;
// let db = Database::open_in_memory()?;

// Insert a document — table is auto-created
let doc = db.insert("sensors", json!({
    "device_id": "thermometer-1",
    "reading": 72.4,
    "location": {"building": "A", "floor": 3}
}))?;
println!("Inserted {} at {}", doc.id(), doc.system_time());

// Update with JSON Merge Patch
let updated = db.update("sensors", doc.id(), json!({
    "reading": 73.1
}))?;
// "device_id" and "location" are preserved, "reading" is patched

// Query all current documents in a table
let results = db.query("sensors")?;
for doc in results.documents() {
    println!("{}: {:?}", doc.id(), doc.data());
}

// Get a single document by ID
let fetched = db.get("sensors", doc.id())?;

// Time-travel: query state at a specific timestamp
let snapshot = db.query_at("sensors", "2026-04-01T00:00:00.000Z")?;

// Range query: all versions active during a time window
let history = db.query_between(
    "sensors",
    "2026-04-01T00:00:00.000Z",
    "2026-04-09T00:00:00.000Z",
)?;

// Full history: every version of every document
let all = db.query_all("sensors")?;

// Soft delete (preserved in history with _op='DELETE')
db.delete("sensors", doc.id())?;

// Hard erase (permanently removed, logged to _erasure_log)
db.erase("sensors", doc.id())?;
```

## API Reference

### `Database`

| Method | Description |
|--------|-------------|
| `Database::open(path)` | Open or create a file-backed database |
| `Database::open_in_memory()` | Create an in-memory database |
| `create_table(name)` | Explicitly create a table (idempotent; tables are also auto-created on insert) |
| `insert(table, data)` | Insert a JSON object as a new document, returns `Document` |
| `update(table, id, patch)` | Apply a JSON Merge Patch (RFC 7396) to an existing document |
| `delete(table, id)` | Soft-delete a document (removed from current state, preserved in history) |
| `erase(table, id)` | Permanently erase a document from current state and all history |
| `get(table, id)` | Retrieve a single document by ID |
| `query(table)` | Return all current (non-deleted) documents in a table |
| `query_all(table)` | Return every version of every document (history + current) |
| `query_at(table, timestamp)` | Return documents as they existed at an ISO 8601 UTC timestamp |
| `query_between(table, from, to)` | Return all versions active during the half-open range `[from, to)` |

### `Document`

| Method | Description |
|--------|-------------|
| `id()` | UUIDv7 document ID |
| `data()` | JSON data as a `serde_json::Map` |
| `get(key)` | Get a value by key from the document data |
| `system_time()` | ISO 8601 UTC timestamp of creation/last modification |
| `txn_id()` | Transaction ID |

### `QueryResult`

| Method | Description |
|--------|-------------|
| `len()` | Number of documents |
| `is_empty()` | Whether the result is empty |
| `documents()` | Borrow the documents as a slice |
| `into_documents()` | Consume into a `Vec<Document>` |

Implements `IntoIterator` for both owned and borrowed iteration.

### Error types

`Error::Sqlite`, `Error::Json`, `Error::InvalidPath`, `Error::InvalidTimestamp`, `Error::TableNotFound`, `Error::DocumentNotFound`, `Error::InvalidTableName`

## Build and Test

```sh
cargo build
cargo test
```

## Project Structure

```
cairn/
├── Cargo.toml              # Workspace root
├── cairndb-core/           # Storage engine (rusqlite, serde_json, uuid)
│   └── src/
│       ├── lib.rs           # Public exports: Database, Document, QueryResult, Error
│       ├── db.rs            # Database struct and public API methods
│       ├── document.rs      # Document and QueryResult types
│       ├── error.rs         # Error enum
│       ├── schema.rs        # Table creation and schema management
│       ├── storage.rs       # Insert, update, delete, query implementations
│       └── versioning.rs    # History and temporal query logic
├── cairndb/                 # High-level crate (depends on cairndb-core)
├── cairndb-parser/          # Temporal SQL parser (not yet implemented)
└── spec.md                  # Project vision and design spec
```

## License

TBD
