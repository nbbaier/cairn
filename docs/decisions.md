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

---

# Parser Milestone Decisions

Recorded from the parser design interview (April 2026). These decisions guide the v0.1 parser and query compiler implementation.

## 15. Target SQL Dialect: Endb SQL Compatibility

**Decision:** The long-term target dialect is the Endatabas SQL reference (https://docs.endatabas.com/sql/). Feature parity will be reached incrementally across multiple milestones.

**Rationale:** Endb's SQL dialect is purpose-built for a temporal document database -- exactly what cairndb is. It covers document literals, time-travel queries, period predicates, path navigation, schema introspection, and schemaless DML. Adopting it as the north star avoids inventing a bespoke dialect and gives users a well-documented reference.

## 16. Parser Scope: v0.1 Statement Set

**Decision:** The v0.1 parser supports the following statements, all mapping to the existing `cairndb-core` Rust API:

1. `INSERT INTO <table> (cols) VALUES (vals)` -- column/value form
2. `INSERT INTO <table> {key: val, ...}` -- document literal form
3. `SELECT * FROM <table>` -- current state
4. `SELECT * FROM <table> WHERE _id = <id>` -- single document
5. `SELECT * FROM <table> FOR SYSTEM_TIME AS OF <ts>` -- time travel
6. `SELECT * FROM <table> FOR SYSTEM_TIME BETWEEN <ts1> AND <ts2>` -- time range
7. `SELECT * FROM <table> FOR SYSTEM_TIME ALL` -- full history
8. `UPDATE <table> SET col = val, ... WHERE _id = <id>` -- merge patch
9. `DELETE FROM <table> WHERE _id = <id>` -- soft delete
10. `ERASE FROM <table> WHERE _id = <id>` -- permanent removal
11. `CREATE TABLE <table>` -- explicit table creation

Mutations are ID-addressed only (WHERE _id = ...). Arbitrary WHERE clauses on mutations are deferred to v0.2+.

**Rationale:** This set covers every method on the existing `Database` API. ID-addressed mutations match the core's design. Arbitrary WHERE on mutations requires either extending the core with filtered operations or query-then-loop in the facade -- both are v0.2 concerns.

## 17. Base Parser: sqlparser-rs as Dependency

**Decision:** Use `sqlparser-rs` as a dependency (not a fork) for parsing standard SQL constructs. The spec's original plan to fork Turso's parser is abandoned.

**Rationale:** `sqlparser-rs` is the dominant Rust SQL parser (~4.5k stars, actively maintained, used by DataFusion/GlueSQL). It handles the full SQLite dialect. Forking Turso's parser only made sense when building on Turso's engine, which is deferred (Decision #1). Using `sqlparser-rs` as a dependency (not fork) means we get upstream improvements for free.

## 18. Parsing Strategy: Hybrid Custom + sqlparser-rs

**Decision:** Custom hand-written parsers for INSERT (both forms) and ERASE. For SELECT, strip `FOR SYSTEM_TIME` clauses before parsing, then use `sqlparser-rs`. Use `sqlparser-rs` directly for UPDATE, DELETE, and CREATE TABLE.

The dispatch order is: try custom parsers first (INSERT, ERASE), fall back to `sqlparser-rs` for everything else.

**Rationale:** INSERT needs custom parsing for document literal syntax (`{key: val}`), which `sqlparser-rs` cannot handle. ERASE is a non-standard statement `sqlparser-rs` won't recognize. `FOR SYSTEM_TIME` is a non-standard clause on otherwise-standard SELECT statements -- stripping it before parsing lets `sqlparser-rs` handle the rest of the SELECT (expressions, WHERE, etc.), which positions well for v0.2+ when SELECT gains projections, JOINs, and arbitrary WHERE. UPDATE, DELETE, and CREATE TABLE are standard SQL that `sqlparser-rs` parses natively.

## 19. Document Literal Value Types: Medium Set

**Decision:** The document literal parser supports: strings (single-quoted), integers, floats, booleans (`true`/`false`), null, nested objects (`{key: val}`), and arrays (`[val, ...]`). Bare date/time literals (unquoted `2024-01-01`) are deferred.

**Rationale:** Nested objects and arrays are essential for a document database. Bare date literals create parsing ambiguity with arithmetic expressions (`2024-01-01` = `2024 - 1 - 1 = 2022`) -- the spec already flagged this as an open question. Users pass dates as quoted strings (`'2024-01-01'`) for now. This matches Endb's `TIMESTAMP '...'` / `DATE '...'` keyword-prefixed alternative.

## 20. Compilation Target: Intermediate Representation

**Decision:** The parser produces a typed IR (`Statement` enum) that the `cairndb` facade dispatches against the `cairndb-core` Rust API. The parser does NOT emit raw SQL strings.

**Rationale:** The core Rust API already handles trigger management, transaction context, timestamp formatting, and all storage complexity. Emitting raw SQL would duplicate that logic and bypass safety guarantees. A typed IR keeps `cairndb-parser` independent of `cairndb-core` (they have zero dependency on each other, per Decision #3) and makes the parser's output testable in isolation.

## 21. IR Design: Extensible Statement Enum

**Decision:** The IR uses a `Statement` enum with a `Filter` type for extensibility:

```rust
enum Statement {
    Insert { table: String, data: Map<String, Value> },
    Select { table: String, filter: Option<Filter>, temporal: Option<TemporalClause> },
    Update { table: String, filter: Filter, patch: Map<String, Value> },
    Delete { table: String, filter: Filter },
    Erase { table: String, filter: Filter },
    CreateTable { table: String },
}

enum Filter {
    ById(String),
}

enum TemporalClause {
    AsOf(String),
    Between { from: String, to: String },
    All,
}
```

Timestamps are raw strings (core layer validates). `Filter` is extensible for v0.2+ (`Where(Expr)` variant).

**Rationale:** The IR reflects SQL structure rather than core API method signatures -- that mapping is the facade's responsibility. Merging `SELECT *` and `SELECT WHERE _id = ?` into one `Select` variant with optional fields is honest to the SQL and easier to extend. `serde_json::Map<String, Value>` for data reuses the existing type used throughout `cairndb-core`.

## 22. SQL Dispatch: cairndb Facade Crate

**Decision:** SQL-to-core-API dispatch lives in the `cairndb` facade crate via a new `Database::sql()` method. This method parses SQL via `cairndb-parser`, pattern-matches on the `Statement` IR, and calls the appropriate `cairndb-core` method.

**Rationale:** `cairndb` is the only crate that depends on both parser and core. It's the public API users interact with. This keeps the two lower crates independent of each other, exactly as designed in Decision #3.

## 23. INSERT Syntax: Column/Value and Document Literal

**Decision:** Support both Endb-style column/value syntax and document literal syntax for INSERT:

```sql
-- Column/value (standard SQL, parsed by custom INSERT parser)
INSERT INTO events (name, status) VALUES ('deploy', 'pending')

-- Document literal (Endb-style, parsed by custom INSERT parser)
INSERT INTO events {name: 'deploy', status: 'pending'}
```

Both forms produce the same IR: `Statement::Insert { table, data }` with a `serde_json::Map`.

**Rationale:** Column/value is standard SQL and what `sqlparser-rs` would parse (though we use our custom parser for both INSERT forms for consistency). Document literal is Endb's signature syntax and more natural for a document database. Supporting both from v0.1 means neither form is a second-class citizen.
