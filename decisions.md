# Implementation Decisions

Recorded from the initial design interview (April 2026). These decisions guided the v0.1 storage layer implementation.

## 1. Foundation: Vanilla SQLite

**Decision:** Start on vanilla SQLite, migrate to Turso later.

**Rationale:** The v0.1 scope doesn't require Turso's CDC or MVCC. Starting on SQLite lets us validate the temporal SQL parser and query compiler -- the hard, novel parts -- without fighting Turso's beta instability. The versioning layer is designed behind an abstraction so swapping triggers for CDC later is a controlled migration, not a rewrite.

## 2. Build Order: Storage Layer First

**Decision:** Build the storage and versioning layer before the parser.

**Rationale:** The physical schema is well-defined and relatively straightforward. Once the storage layer exists, it can be exercised directly from Rust with raw SQL. This gives us a working "temporal SQLite" immediately, even if the interface is ugly. The parser is the harder, riskier piece -- building it second means we already have a proven compilation target.

## 3. Project Structure: Cargo Workspace

**Decision:** Cargo workspace from day one with three crates: `cairndb`, `cairndb-core`, `cairndb-parser`.

**Rationale:** `cairndb-core` and `cairndb-parser` have zero dependency on each other. Keeping them separate enforces that boundary. The parser will eventually be substantial code; isolating it means it can be tested, fuzzed, and benchmarked independently. The cost is minimal -- three `Cargo.toml` files and a `[workspace]` block.

## 4. Project Name: cairndb

**Decision:** Use `cairndb` as the project and crate name.

**Rationale:** `cairn` is taken on crates.io. `cairndb` reads as a single compound word (like `surrealdb`, `redb`) -- the dominant convention for Rust database crates. `cairn-db` with the hyphen implies a namespace relationship and produces the awkward `cairn_db` module path. The full crate family: `cairndb`, `cairndb-core`, `cairndb-parser`.

## 5. SQLite Binding: rusqlite with bundled

**Decision:** Use `rusqlite` with the `bundled` feature.

**Rationale:** It's the standard Rust SQLite binding. The `bundled` feature compiles SQLite from source so users don't need a system install. When migrating to Turso later, the swap happens at the `libsqlite3-sys` linking layer -- `rusqlite` continues to work as the Rust-side interface.

## 6. Document IDs: UUIDv7

**Decision:** UUIDv7 (RFC 9562), stored as TEXT in standard hyphenated format.

**Rationale:** Time-sortable IDs mean `ORDER BY _id` gives roughly chronological order for free. B-tree friendly -- sequential inserts don't fragment the index like UUIDv4. Globally unique without coordination, which matters if sync/replication is added later. TEXT storage is slightly wasteful vs 16-byte BLOB, but human-readable in SQLite tooling. For an embedded database aimed at developers, debuggability wins.

## 7. Timestamps: Integer Milliseconds Since Epoch

**Decision:** Integer milliseconds since Unix epoch for all internal timestamp columns.

**Rationale:** Every temporal query hits timestamp columns. Integer comparison is faster than string comparison, and this is the hot path on every query. 8 bytes vs ~24 bytes per timestamp, with 2-3 timestamp columns per row in both current and history tables. The public API still accepts and returns ISO 8601 strings -- conversion happens in the Rust layer, transparent to the user. Millisecond precision is sufficient for an embedded single-process database.

## 8. Versioning Mechanism: SQLite BEFORE Triggers

**Decision:** Use SQLite BEFORE triggers to capture before-images into history tables.

**Rationale:** Atomicity is guaranteed -- the trigger runs inside the same transaction as the mutation. Fewer round-trips between Rust and SQLite compared to a Rust-layer interceptor. The "fragile" concern from the spec applies to user-controlled schemas; here, cairndb owns the physical schema entirely. When migrating to Turso CDC, we just drop the triggers -- the history table schema stays identical.

## 9. Transaction Context: Regular System Table

**Decision:** Pass transaction IDs to triggers via a regular system table `_cairn_tx_context` (not a temp table as originally planned).

**Rationale:** The original plan called for a connection-scoped temp table, but SQLite prohibits main-schema triggers from referencing objects in the temp schema. The table is cleared before each write (`DELETE FROM _cairn_tx_context`) and populated with the current txn_id and timestamp. Safe because `Mutex` serializes all access -- no concurrent writers can corrupt its state.

**Note:** This was a deviation discovered during implementation. The interview originally decided on a temp table approach, but the SQLite constraint required the change.

## 10. Table Creation: Both Lazy and Explicit

**Decision:** Lazy auto-creation on first insert, with explicit `create_table()` also supported.

**Rationale:** The spec says "INSERT INTO events {...} auto-creates the table if it doesn't exist" -- zero ceremony is a core ergonomic feature. But explicit creation is also valuable for documentation, tooling, or future table-level configuration. The same `ensure_table()` function is called either way.

## 11. Error Handling: Single Enum with thiserror

**Decision:** Single `cairndb::Error` enum with `thiserror` derives.

**Rationale:** Standard pattern for Rust library crates. The error surface is small enough for v0.1 that one enum covers it. `thiserror` provides `Display`, `Error` trait impl, and `#[from]` for automatic conversion from `rusqlite::Error`, `serde_json::Error`, etc. Can be split into per-crate errors later as a backwards-compatible refactor.

## 12. Query Results: Custom Document Type

**Decision:** Custom `Document` struct wrapping a `serde_json::Map<String, Value>` with system metadata accessors.

**Rationale:** The database is document-oriented, so results should feel like documents. `Document` exposes `.id()`, `.system_time()`, `.data()` as first-class methods while hiding the physical schema (`_data`, `_id`, `_valid_from`). Implements `Serialize` for JSON output. `QueryResult` wraps `Vec<Document>` with iteration support.

## 13. Concurrency: Mutex-Wrapped Connection

**Decision:** Single `rusqlite::Connection` behind `std::sync::Mutex`. `Database` is `Send + Sync`.

**Rationale:** An embedded database that isn't `Send + Sync` is surprisingly annoying to use in real Rust applications. A single Mutex-wrapped connection is the simplest correct concurrent implementation. The serialization isn't a problem for the embedded use case. Upgradeable to a reader/writer pool later without changing the public API. WAL mode enabled by default.

## 14. Testing: In-Memory by Default

**Decision:** In-memory SQLite databases for unit tests, with a small number of on-disk integration tests.

**Rationale:** In-memory is fast, no cleanup needed, and sufficient for testing the storage schema, triggers, and query logic. A few on-disk tests verify persistence, WAL mode, and file handling.
