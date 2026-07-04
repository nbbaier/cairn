# Cairn: An Embedded Temporal Document Database

Spec — April 2026

## Premise

[Endatabas](https://www.endatabas.com/) is a SQL document database with complete history, immutable storage, time-travel queries, and schema-flexible documents stored in Apache Arrow columnar format. Its core ideas are compelling, but it requires running a server, which is at odds with the use cases where its features would be most valuable: personal data stores, local-first apps, developer tools, PKM systems, edge applications.

This document describes an embedded equivalent — something that is to Endatabas what SQLite is to Postgres. The foundation is vanilla SQLite (via `rusqlite` with bundled), extended with temporal SQL semantics, automatic versioning via BEFORE triggers, and a schema-flexible document model. The long-term target SQL dialect is [Endb SQL](https://docs.endatabas.com/sql/), implemented incrementally. Migration to Turso is a future option when its CDC and MVCC features stabilize.

The project name is **cairndb**.

Related documents: 
- [decisions.md](decisions.md): design decisions with rationale
- [roadmap.md](roadmap.md): milestones, scope, and Endb SQL compatibility matrix



## Design Principles

1. **No server.** The database is an in-process library. A single file (or small set of files) on disk. No daemon, no Docker, no port binding.

2. **SQL is the interface.** Not a custom API. The query language is a superset of SQLite-compatible SQL with temporal and document extensions. Existing SQLite tooling should work for basic operations.

3. **Immutable by default.** All records are versioned. `UPDATE` and `DELETE` produce new versions; they do not destroy data. History is queryable. The only true deletion is an explicit `ERASE` (for GDPR/compliance).

4. **Schema-flexible.** Tables accept semi-structured documents. You do not need to declare columns before inserting data. The engine infers and tracks schema dynamically (schema-last).

5. **Time-travel is a first-class query primitive.** `FOR SYSTEM_TIME AS OF`, `BETWEEN`, `ALL`, and SQL:2011 period predicates are part of the SQL dialect, not bolted on via application-level workarounds.

6. **Row-oriented storage is enough.** Columnar storage is theoretically desirable for analytical queries over historical data, but no viable embedded columnar option exists today. Row-oriented SQLite tables with proper indexing are the storage model. Columnar is a future optimization, not a design dependency.

7. **Endb SQL compatibility.** The long-term target dialect is the [Endatabas SQL reference](https://docs.endatabas.com/sql/). This covers data manipulation, queries, temporal queries, document literals, path navigation, schema introspection, views, and assertions. Feature parity is reached incrementally — each milestone adds a slice of the dialect.



## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   User SQL Query                    │
│  "SELECT * FROM events FOR SYSTEM_TIME AS OF ..."   │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│          Temporal SQL Parser (cairndb-parser)        │
│                                                     │
│  Hybrid parsing strategy:                           │
│  - Custom parser: INSERT (col/val + doc literal),   │
│    ERASE                                            │
│  - sqlparser-rs: SELECT, UPDATE, DELETE,            │
│    CREATE TABLE                                     │
│  - Pre-processing: strip FOR SYSTEM_TIME before     │
│    passing SELECT to sqlparser-rs                   │
│                                                     │
│  Output: Statement IR (not raw SQL)                 │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│           SQL Dispatch (cairndb facade)             │
│                                                     │
│  Database::sql() maps Statement IR to               │
│  cairndb-core Rust API calls:                       │
│                                                     │
│  - Insert → db.insert(table, data)                  │
│  - Select + AsOf → db.query_at(table, ts)           │
│  - Select + All → db.query_all(table)               │
│  - Update → db.update(table, id, patch)             │
│  - Delete → db.delete(table, id)                    │
│  - Erase → db.erase(table, id)                      │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│          Storage Layer (cairndb-core)               │
│                                                     │
│  - Vanilla SQLite via rusqlite (bundled)            │
│  - BEFORE triggers for automatic versioning         │
│  - Mutex<Connection>, Send + Sync, WAL mode         │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│            Storage Layer (on disk)                  │
│                                                     │
│  ┌───────────────┐  ┌────────────────────────┐      │
│  │ Current-state │  │ History / version log  │      │
│  │ tables        │  │ (append-only)          │      │
│  └───────────────┘  └────────────────────────┘      │
│                                                     │
│  ┌───────────────┐  ┌────────────────────────┐      │
│  │ Schema        │  │ Transaction log /      │      │
│  │ registry      │  │ erasure log            │      │
│  └───────────────┘  └────────────────────────┘      │
└─────────────────────────────────────────────────────┘
```



## Storage Model

### Current-state tables

Each logical table `T` has a corresponding physical table `_T_current` that holds the latest version of each document. This is what queries without temporal qualifiers read from — optimized for the common case.

```sql
-- Physical schema for a logical table "events"
CREATE TABLE _events_current (
    _id        TEXT    PRIMARY KEY,  -- UUIDv7, system-assigned
    _data      JSONB   NOT NULL,    -- the document itself
    _valid_from INTEGER NOT NULL,   -- epoch milliseconds (system time start)
    _txn_id    INTEGER NOT NULL     -- transaction that created this version
);
```

### History tables

Each logical table also has a `_T_history` table that stores all prior versions. This is append-only.

```sql
CREATE TABLE _events_history (
    _id        TEXT    NOT NULL,
    _data      JSONB   NOT NULL,
    _valid_from INTEGER NOT NULL,   -- epoch ms: when this version became active
    _valid_to  INTEGER NOT NULL,    -- epoch ms: when this version was superseded
    _txn_id    INTEGER NOT NULL,
    _op        TEXT    NOT NULL     -- 'UPDATE', 'DELETE'
);

CREATE INDEX _events_history_idx
    ON _events_history (_id, _valid_from, _valid_to);
```

### Versioning via BEFORE triggers

**Implemented.** SQLite BEFORE UPDATE and BEFORE DELETE triggers on `_T_current` capture before-images into `_T_history`. The triggers read `(txn_id, timestamp)` from the `_cairn_tx_context` system table, which the Rust layer populates at the start of each write transaction and clears after commit.

This approach was chosen over Turso CDC (the original plan) because v0.1 targets vanilla SQLite. The trigger-based mechanism is atomic (runs inside the same transaction as the mutation) and cairndb owns the physical schema entirely, avoiding the fragility concern with user-controlled triggers. When migrating to Turso CDC, the triggers are dropped — the history table schema stays identical.

### Transaction log

A global transaction metadata table tracks transaction boundaries for consistent time-travel:

```sql
CREATE TABLE _transactions (
    txn_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,     -- epoch milliseconds
    metadata  TEXT                   -- optional: user-supplied context
);
```

This enables queries like "show me the database as it was at transaction 42" in addition to timestamp-based time travel.



## Schema-Last Document Model

### How it works

Tables in cairn do not require `CREATE TABLE` with predefined columns. Instead:

- `INSERT INTO events {...}` auto-creates the table if it doesn't exist.
- Each row is stored as a JSONB document in the `_data` column.
- The engine maintains a **schema registry** that tracks observed keys, their inferred types, and first/last seen timestamps.

```sql
CREATE TABLE _schema_registry (
    _table       TEXT NOT NULL,
    _key_path    TEXT NOT NULL,       -- e.g. "address.city" for nested keys
    _inferred_type TEXT NOT NULL,     -- INTEGER, TEXT, REAL, BOOLEAN, ARRAY, OBJECT
    _first_seen  TEXT NOT NULL,
    _last_seen   TEXT NOT NULL,
    _nullable    BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (_table, _key_path)
);
```

### Document literals in SQL

Following Endatabas, cairndb supports document literal syntax for INSERT (v0.1):

```sql
-- Endatabas-style document INSERT
INSERT INTO stores {
    brand: 'Alonzo Analog Synthesizers',
    addresses: [
        {city: 'New Jersey', country: 'United States'},
        {city: 'Göttingen', country: 'Germany'}
    ]
};
```

The parser compiles this to an `Insert` IR node with `data` as a `serde_json::Map`, which the facade dispatches to `db.insert("stores", data)`. Supported value types in v0.1: strings (single-quoted), integers, floats, booleans, null, nested objects, arrays. Bare date/time literals are deferred.

### Path navigation

Endatabas supports `..` for recursive path descent into nested documents:

```sql
SELECT addresses..opened FROM stores;
```

This compiles to something like:

```sql
SELECT json_extract(_data, '$.addresses[*].opened')
FROM _stores_current;
```

SQLite's JSON support doesn't have recursive descent natively, so the compiler would need to handle this — possibly by expanding known paths from the schema registry at query planning time.



## Temporal Query Dispatch

In v0.1, temporal queries are dispatched via the Statement IR to the existing `cairndb-core` Rust API. The core layer handles the underlying SQL (history/current UNION queries, timestamp comparisons, etc.) internally.

### AS OF (time travel)

```sql
SELECT * FROM events FOR SYSTEM_TIME AS OF '2025-06-15T00:00:00.000Z';
-- Parser produces: Select { table: "events", temporal: Some(AsOf("2025-06-15T00:00:00.000Z")) }
-- Facade calls: db.query_at("events", "2025-06-15T00:00:00.000Z")
```

### ALL (full history)

```sql
SELECT * FROM events FOR SYSTEM_TIME ALL;
-- Parser produces: Select { table: "events", temporal: Some(All) }
-- Facade calls: db.query_all("events")
```

### BETWEEN

```sql
SELECT * FROM events FOR SYSTEM_TIME BETWEEN '2025-01-01T00:00:00.000Z' AND '2025-12-31T00:00:00.000Z';
-- Parser produces: Select { table: "events", temporal: Some(Between { from: ..., to: ... }) }
-- Facade calls: db.query_between("events", from, to)
```

### Default behavior (no temporal qualifier)

```sql
SELECT * FROM events;
-- Parser produces: Select { table: "events", temporal: None }
-- Facade calls: db.query("events")
```

Queries without `FOR SYSTEM_TIME` read only from `_T_current`. This is the fast path — no history scan, no UNION, just a normal indexed read. This matches Endatabas's design: "queries default to as-of-now, which is the thing you want 97% of the time."

### Period predicates (v0.2+)

SQL:2011 period predicates (`CONTAINS`, `OVERLAPS`, `PRECEDES`, `SUCCEEDS`, `IMMEDIATELY PRECEDES`, `IMMEDIATELY SUCCEEDS`) are deferred to v0.2+. They follow the closed-open period model as documented in [Endb's Time Queries](https://docs.endatabas.com/sql/time_queries).



## ERASE (Compliance Deletion)

`ERASE` is the one operation that truly destroys data. It removes a record from both current and history tables — as if it never existed. This is for GDPR right-to-be-forgotten and similar compliance requirements.

```sql
-- User writes:
ERASE FROM users WHERE _id = 'user-123';

-- Compiler emits:
DELETE FROM _users_current WHERE _id = 'user-123';
DELETE FROM _users_history WHERE _id = 'user-123';
-- Optionally: log the erasure event (without the erased data) for audit
INSERT INTO _erasure_log (_table, _id, _erased_at)
VALUES ('users', 'user-123', '2026-04-04T12:00:00Z');
```



## UPDATE and DELETE Semantics

In cairn, `UPDATE` and `DELETE` are non-destructive by default. They create new versions.

### UPDATE

```sql
-- User writes:
UPDATE events SET status = 'completed' WHERE _id = 'evt-1';

-- What actually happens:
-- 1. BEFORE UPDATE trigger copies the old row to _events_history with _op='UPDATE'
-- 2. The _data column in _events_current is patched via json_patch (RFC 7396):
UPDATE _events_current
SET _data = json_patch(_data, jsonb('{"status":"completed"}')),
    _valid_from = <epoch_ms>,
    _txn_id = <txn_id>
WHERE _id = 'evt-1';
```

The parser collects SET assignments (`status = 'completed'`) and assembles them into a JSON merge patch object. Multiple assignments are supported: `SET a = 1, b = 2` becomes `{"a": 1, "b": 2}`.

### DELETE

```sql
-- User writes:
DELETE FROM events WHERE _id = 'evt-1';

-- What actually happens:
-- 1. BEFORE DELETE trigger copies the old row to _events_history with _op='DELETE'
-- 2. The row is removed from _events_current
DELETE FROM _events_current WHERE _id = 'evt-1';
-- The row is NOT gone — it still exists in _events_history
```



## Columnar Storage: Deferred

### Why it's tempting

Historical data in a temporal database tends to be read-heavy and append-only — the ideal workload for columnar storage. Scanning months of versioned rows to compute aggregates is exactly where column-oriented layouts shine. Endatabas uses Apache Arrow columnar format for this reason.

### Why it's not worth pursuing now

The only existing SQLite columnar extension is Stanchion — a Zig-based project that launched in early 2024 and has seen no activity since. It's alpha, lacks DELETE support, is written in a different language than Turso, and is effectively abandoned. It is not a viable dependency.

Building a Rust-native columnar virtual table (using `arrow-rs` and SQLite's virtual table interface) would be the right long-term approach, but it's a project unto itself — months of work for an optimization that may not matter at the scales where an embedded temporal database is useful.

The realistic use cases for this project — personal data stores, local-first apps, developer tools, PKM systems — involve datasets in the thousands to low millions of rows. Row-oriented SQLite B-tree tables with proper indexes on `(_id, _valid_from, _valid_to)` will handle this fine. If someone needs to scan billions of historical rows, they should be using DuckDB or ClickHouse, not an embedded temporal store.

### Future options if it becomes necessary

If analytical performance over large history tables becomes a real bottleneck (not a theoretical one), there are two viable escape hatches that don't require building a columnar engine:

1. **Parquet export.** Add a `EXPORT HISTORY <table> TO '<path>.parquet'` command that dumps history tables to Parquet files via the `parquet` Rust crate. Users can then query those files with DuckDB, Polars, or any Arrow-compatible tool. This keeps the core engine simple and leans on mature analytical tools for the heavy lifting.

2. **Materialized aggregates.** For common temporal queries (e.g., "how many events per day over the last year"), pre-compute and cache the results in regular tables, updated incrementally via CDC. This avoids full history scans entirely.



## Parser Implementation

### Approach: hybrid custom + sqlparser-rs

The parser (`cairndb-parser` crate) uses a hybrid strategy:

- **Custom hand-written parsers** for statements that `sqlparser-rs` cannot handle: INSERT (both column/value and document literal `{key: val}` forms) and ERASE (non-standard statement).
- **`sqlparser-rs`** (as a dependency, not a fork) for standard SQL: UPDATE, DELETE, CREATE TABLE. It handles the full SQLite dialect.
- **Pre-processing** for SELECT: the `FOR SYSTEM_TIME` clause is stripped from the SQL string before passing to `sqlparser-rs`. The temporal clause is parsed separately and attached to the IR.

Dispatch order: try custom parsers first (INSERT, ERASE), fall back to `sqlparser-rs` for everything else.

### v0.1 statement set

| Statement | Parser | Maps to |
|---|---|---|
| `INSERT INTO t (cols) VALUES (vals)` | Custom | `db.insert(t, data)` |
| `INSERT INTO t {key: val, ...}` | Custom | `db.insert(t, data)` |
| `SELECT * FROM t` | sqlparser-rs | `db.query(t)` |
| `SELECT * FROM t WHERE _id = ?` | sqlparser-rs | `db.get(t, id)` |
| `SELECT * FROM t FOR SYSTEM_TIME AS OF ts` | Pre-process + sqlparser-rs | `db.query_at(t, ts)` |
| `SELECT * FROM t FOR SYSTEM_TIME BETWEEN t1 AND t2` | Pre-process + sqlparser-rs | `db.query_between(t, t1, t2)` |
| `SELECT * FROM t FOR SYSTEM_TIME ALL` | Pre-process + sqlparser-rs | `db.query_all(t)` |
| `UPDATE t SET col=val,... WHERE _id = ?` | sqlparser-rs | `db.update(t, id, patch)` |
| `DELETE FROM t WHERE _id = ?` | sqlparser-rs | `db.delete(t, id)` |
| `ERASE FROM t WHERE _id = ?` | Custom | `db.erase(t, id)` |
| `CREATE TABLE t` | sqlparser-rs | `db.create_table(t)` |

All mutations are ID-addressed only (`WHERE _id = ?`) in v0.1. Arbitrary WHERE clauses are deferred to v0.2+.

### Document literal syntax

The custom INSERT parser supports Endb-style document literals:

```sql
INSERT INTO stores {
    brand: 'Alonzo Analog Synthesizers',
    addresses: [
        {city: 'New Jersey', country: 'United States'},
        {city: 'Göttingen', country: 'Germany'}
    ]
};
```

Supported value types in v0.1: strings (single-quoted), integers, floats, booleans (`true`/`false`), `null`, nested objects, and arrays. Bare date/time literals (unquoted `2024-01-01`) are deferred — users pass dates as quoted strings.

### Compilation target: Statement IR

The parser produces a typed intermediate representation (IR), not raw SQL. The `cairndb` facade crate dispatches the IR against `cairndb-core`'s Rust API.

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
    ById(String),   // v0.1: only ID-addressed
    // Where(Expr),  // v0.2+: arbitrary WHERE clauses
}

enum TemporalClause {
    AsOf(String),
    Between { from: String, to: String },
    All,
}
```

This design keeps `cairndb-parser` independent of `cairndb-core` (zero dependency between them). The `Filter` enum is extensible for v0.2+. Timestamps are raw strings — the core layer validates them.

### SQL dispatch

The `cairndb` facade crate gains a `Database::sql()` method:

```rust
impl Database {
    pub fn sql(&self, query: &str) -> Result<QueryResult> {
        let stmt = cairndb_parser::parse(query)?;
        match stmt { /* dispatch to core API */ }
    }
}
```

This is the only place that depends on both `cairndb-parser` and `cairndb-core`.



## Open Questions

### Resolved

1. ~~**CDC granularity.**~~ **Resolved:** Using BEFORE triggers on vanilla SQLite instead of Turso CDC. See Decision #8.

2. ~~**Document ID generation.**~~ **Resolved:** UUIDv7, stored as TEXT. See Decision #6.

3. ~~**Turso stability.**~~ **Resolved:** Start on vanilla SQLite, migrate to Turso later. See Decision #1.

4. ~~**Query result format.**~~ **Resolved:** Custom `Document` type for the Rust API. See Decision #12.

### Open

1. **JSONB performance.** Storing all document data as JSONB means every field access is a json_extract call. For hot-path queries on known fields, this could be slow. Should the engine opportunistically promote frequently-queried JSON keys to real columns (partial materialization)? This is effectively what Endatabas's adaptive indexing does.

2. **Bare date literals.** Endatabas allows `2025-06-15` as a date literal without quotes. SQLite parses this as `2025 - 6 - 15 = 2004`. Deferred from v0.1 parser scope. Possible resolutions: require quotes, require a keyword prefix (`DATE '2025-06-15'` — which is standard SQL), or use context-sensitive parsing (risky).

3. **History compaction.** In a long-lived database, history tables will grow without bound. Should there be a `COMPACT` command that merges old versions into summary records? What's the retention policy model? This needs design work.



## Prior Art and Influences

- **Endatabas** — the direct inspiration and target SQL dialect. Immutable SQL document database with full history. Server-based, Common Lisp, Apache Arrow. [endatabas.com](https://www.endatabas.com), [docs.endatabas.com/sql](https://docs.endatabas.com/sql/)
- **XTDB** — immutable bitemporal database with Postgres-compatible SQL. Server-based, Clojure/Java, Arrow. [xtdb.com](https://xtdb.com)
- **Dolt** — version-controlled SQL database (Git for data). Server-based, Go. [dolthub.com](https://www.dolthub.com)
- **ImmuDB** — immutable ledger database, embeddable, PostgreSQL dialect subset. Go. [immudb.io](https://immudb.io)
- **SirixDB** — embeddable temporal evolutionary database. Java. Low activity.
- **SQLite temporal tables** — community patterns using triggers and history tables on vanilla SQLite. cairndb's v0.1 versioning mechanism is a refined version of this approach.
- **sqlparser-rs** — the Rust SQL parser used by cairndb-parser for standard SQL constructs. [github.com/sqlparser-rs/sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs)
- **Stanchion** — columnar storage extension for SQLite. Zig. Alpha, unmaintained since early 2024. Not viable as a dependency. [github.com/dgllghr/stanchion](https://github.com/dgllghr/stanchion)
- **Turso Database** — Rust reimplementation of SQLite with CDC, MVCC, async I/O. Beta. Future migration target. [github.com/tursodatabase/turso](https://github.com/tursodatabase/turso)
