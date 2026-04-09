# Embedded Temporal Document Database on Turso

Exploratory Spec — April 2026

## Premise

[Endatabas](https://www.endatabas.com/) is a SQL document database with complete history, immutable storage, time-travel queries, and schema-flexible documents stored in Apache Arrow columnar format. Its core ideas are compelling, but it requires running a server, which is at odds with the use cases where its features would be most valuable: personal data stores, local-first apps, developer tools, PKM systems, edge applications.

This document explores what it would look like to build an embedded equivalent — something that is to Endatabas what SQLite is to Postgres. The proposed foundation is Turso (the Rust-based SQLite-compatible database), extended with temporal SQL semantics, automatic versioning via CDC, and a schema-flexible document model.

The working name for this project is **cairn** (placeholder).



## Design Principles

1. **No server.** The database is an in-process library. A single file (or small set of files) on disk. No daemon, no Docker, no port binding.

2. **SQL is the interface.** Not a custom API. The query language is a superset of SQLite-compatible SQL with temporal and document extensions. Existing SQLite tooling should work for basic operations.

3. **Immutable by default.** All records are versioned. `UPDATE` and `DELETE` produce new versions; they do not destroy data. History is queryable. The only true deletion is an explicit `ERASE` (for GDPR/compliance).

4. **Schema-flexible.** Tables accept semi-structured documents. You do not need to declare columns before inserting data. The engine infers and tracks schema dynamically (schema-last).

5. **Time-travel is a first-class query primitive.** `FOR SYSTEM_TIME AS OF`, `BETWEEN`, `ALL`, and SQL:2011 period predicates are part of the SQL dialect, not bolted on via application-level workarounds.

6. **Row-oriented storage is enough.** Columnar storage is theoretically desirable for analytical queries over historical data, but no viable embedded columnar option exists today. Row-oriented Turso/SQLite tables with proper indexing are the storage model. Columnar is a future optimization, not a design dependency.



## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   User SQL Query                    │
│  "SELECT * FROM events FOR SYSTEM_TIME AS OF ..."   │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│               Temporal SQL Parser                   │
│                                                     │
│  Extended SQL dialect:                              │
│  - FOR SYSTEM_TIME {AS OF | BETWEEN | ALL | FROM}   │
│  - Document literals: {key: value, ...}             │
│  - Path navigation: column..nested_key              │
│  - Period predicates: CONTAINS, OVERLAPS, etc.      │
│  - ERASE statement                                  │
│                                                     │
│  Fork of Turso's parser crate                       │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│              Query Compiler / Rewriter              │
│                                                     │
│  Compiles temporal SQL → standard SQL against       │
│  the versioned storage schema.                      │
│                                                     │
│  - AS OF → WHERE _valid_from <= ? AND _valid_to > ? │
│  - Document paths → json_extract() calls            │
│  - Schema-last INSERTs → JSONB column writes        │
│  - Period predicates → range comparisons            │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│                  Turso Engine                       │
│                                                     │
│  - SQLite-compatible storage (single file)          │
│  - CDC stream for automatic version capture         │
│  - MVCC (BEGIN CONCURRENT) for concurrent access    │
│  - Async I/O (io_uring on Linux)                    │
│  - Encryption at rest (optional)                    │
│  - WASM compilation target                          │
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
    _id          TEXT PRIMARY KEY,   -- system-assigned document ID
    _data        JSONB NOT NULL,     -- the document itself
    _valid_from  TEXT NOT NULL,      -- ISO 8601 timestamp (system time start)
    _txn_id      INTEGER NOT NULL    -- transaction that created this version
);
```

### History tables

Each logical table also has a `_T_history` table that stores all prior versions. This is append-only.

```sql
CREATE TABLE _events_history (
    _id          TEXT NOT NULL,
    _data        JSONB NOT NULL,
    _valid_from  TEXT NOT NULL,      -- when this version became active
    _valid_to    TEXT NOT NULL,      -- when this version was superseded
    _txn_id      INTEGER NOT NULL,
    _op          TEXT NOT NULL       -- 'INSERT', 'UPDATE', 'DELETE'
);

CREATE INDEX _events_history_time
    ON _events_history (_id, _valid_from, _valid_to);
```

### Why CDC matters here

Rather than using SQLite triggers (fragile, hard to maintain, no access to transaction context), cairn would use Turso's CDC stream to populate history tables. When a write hits `_events_current`:

1. The CDC listener captures the before-image of the affected row(s).
2. The before-image is appended to `_events_history` with `_valid_to` set to the current transaction timestamp.
3. The new row in `_events_current` gets `_valid_from` set to the current transaction timestamp.

This keeps the versioning logic out of user-space triggers and inside the engine, where it belongs.

### Transaction log

A global transaction metadata table tracks transaction boundaries for consistent time-travel:

```sql
CREATE TABLE _transactions (
    _txn_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    _timestamp   TEXT NOT NULL,      -- wall-clock time of commit
    _metadata    JSONB               -- optional: user-supplied context
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

Following Endatabas, cairn's parser would support document literal syntax:

```sql
-- Endatabas-style document INSERT
INSERT INTO stores {
    brand: "Alonzo's Analog Synthesizers",
    addresses: [
        {city: "New Jersey", country: "United States", opened: 1929-09-01},
        {city: "Göttingen", country: "Germany", opened: 1928-09-01}
    ]
};
```

The parser compiles this to:

```sql
INSERT INTO _stores_current (_id, _data, _valid_from, _txn_id)
VALUES (
    'uuid-...',
    jsonb('{"brand":"Alonzo''s Analog Synthesizers","addresses":[...]}'),
    '2026-04-04T12:00:00Z',
    123
);
```

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



## Temporal Query Compilation

The core of the system is the query compiler that rewrites temporal SQL into standard SQL against the versioned storage schema.

### AS OF (time travel)

```sql
-- User writes:
SELECT * FROM events FOR SYSTEM_TIME AS OF '2025-06-15T00:00:00Z';

-- Compiler emits:
SELECT _id, _data FROM _events_history
WHERE _valid_from <= '2025-06-15T00:00:00Z'
  AND _valid_to > '2025-06-15T00:00:00Z'
UNION ALL
SELECT _id, _data FROM _events_current
WHERE _valid_from <= '2025-06-15T00:00:00Z';
```

The UNION is necessary because rows that are still current won't appear in the history table.

### ALL (full history)

```sql
-- User writes:
SELECT * FROM events FOR SYSTEM_TIME ALL;

-- Compiler emits:
SELECT _id, _data, _valid_from, _valid_to FROM _events_history
UNION ALL
SELECT _id, _data, _valid_from, NULL FROM _events_current;
```

### BETWEEN

```sql
-- User writes:
SELECT * FROM events
    FOR SYSTEM_TIME BETWEEN '2025-01-01' AND '2025-12-31';

-- Compiler emits:
SELECT _id, _data FROM _events_history
WHERE _valid_from <= '2025-12-31T23:59:59Z'
  AND _valid_to > '2025-01-01T00:00:00Z'
UNION ALL
SELECT _id, _data FROM _events_current
WHERE _valid_from <= '2025-12-31T23:59:59Z';
```

### Period predicates

SQL:2011 period predicates compile to range comparisons:

```sql
-- CONTAINS: period A contains period B
-- A.start <= B.start AND A.end >= B.end

-- OVERLAPS: any overlap between A and B
-- A.start < B.end AND A.end > B.start

-- PRECEDES: A ends before B starts
-- A.end <= B.start

-- IMMEDIATELY PRECEDES: A ends exactly when B starts
-- A.end = B.start
```

### Default behavior (no temporal qualifier)

Queries without `FOR SYSTEM_TIME` read only from `_T_current`. This is the fast path — no history scan, no UNION, just a normal indexed read against the current-state table. This matches Endatabas's design: "queries default to as-of-now, which is the thing you want 97% of the time."



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
UPDATE events SET {status: "completed"} WHERE _id = 'evt-1';

-- What actually happens:
-- 1. CDC captures the current row as a before-image → _events_history
-- 2. The _data column in _events_current is patched:
UPDATE _events_current
SET _data = json_patch(_data, '{"status":"completed"}'),
    _valid_from = '2026-04-04T12:00:00Z',
    _txn_id = 124
WHERE _id = 'evt-1';
```

### DELETE

```sql
-- User writes:
DELETE FROM events WHERE _id = 'evt-1';

-- What actually happens:
-- 1. CDC captures the current row → _events_history with _op = 'DELETE'
-- 2. The row is removed from _events_current
DELETE FROM _events_current WHERE _id = 'evt-1';
-- The row is NOT gone — it still exists in _events_history
```



## Columnar Storage: Deferred

### Why it's tempting

Historical data in a temporal database tends to be read-heavy and append-only — the ideal workload for columnar storage. Scanning months of versioned rows to compute aggregates is exactly where column-oriented layouts shine. Endatabas uses Apache Arrow columnar format for this reason.

### Why it's not worth pursuing now

The only existing SQLite columnar extension is Stanchion — a Zig-based project that launched in early 2024 and has seen no activity since. It's alpha, lacks DELETE support, is written in a different language than Turso, and is effectively abandoned. It is not a viable dependency.

Building a Rust-native columnar virtual table (using `arrow-rs` and Turso's virtual table interface) would be the right long-term approach, but it's a project unto itself — months of work for an optimization that may not matter at the scales where an embedded temporal database is useful.

The realistic use cases for this project — personal data stores, local-first apps, developer tools, PKM systems — involve datasets in the thousands to low millions of rows. Row-oriented SQLite/Turso B-tree tables with proper indexes on `(_id, _valid_from, _valid_to)` will handle this fine. If someone needs to scan billions of historical rows, they should be using DuckDB or ClickHouse, not an embedded temporal store.

### Future options if it becomes necessary

If analytical performance over large history tables becomes a real bottleneck (not a theoretical one), there are two viable escape hatches that don't require building a columnar engine:

1. **Parquet export.** Add a `EXPORT HISTORY <table> TO '<path>.parquet'` command that dumps history tables to Parquet files via the `parquet` Rust crate. Users can then query those files with DuckDB, Polars, or any Arrow-compatible tool. This keeps the core engine simple and leans on mature analytical tools for the heavy lifting.

2. **Materialized aggregates.** For common temporal queries (e.g., "how many events per day over the last year"), pre-compute and cache the results in regular tables, updated incrementally via CDC. This avoids full history scans entirely.



## Parser Implementation

### Approach: fork Turso's parser

Turso has a `parser/` crate in its repo. Forking this crate and extending it is the most direct path. The extensions needed:

1. **`FOR SYSTEM_TIME` clause** on `SELECT` statements, supporting `AS OF <expr>`, `BETWEEN <expr> AND <expr>`, `FROM <expr> TO <expr>`, and `ALL`.

2. **Document literal syntax** — `{key: value, ...}` as an expression type, and `[...]` for arrays within documents. This requires new token types and expression grammar rules.

3. **`ERASE` statement** — a new top-level statement type, syntactically similar to `DELETE` but with different semantics.

4. **Path navigation operator** — `..` for recursive descent into nested document fields.

5. **Period predicate operators** — `CONTAINS`, `OVERLAPS`, `PRECEDES`, `SUCCEEDS`, `IMMEDIATELY PRECEDES`, `IMMEDIATELY SUCCEEDS` as infix operators on period expressions.

6. **Bare date/time literals** — Endatabas supports unquoted `2025-06-15` and `2025-06-15T00:00:00` as date and timestamp literals. This may conflict with SQLite's interpretation of `2025-06-15` as an arithmetic expression (2025 minus 6 minus 15 = 2004). Needs careful grammar design — possibly requiring a `DATE` or `TIMESTAMP` keyword prefix for disambiguation, or using context-sensitive parsing.

### Compilation target

The parser produces an AST that the query compiler walks to emit standard Turso-compatible SQL. The compiler is a tree transformation, not a string-manipulation pass. This keeps it robust against injection and edge cases.



## Embedding API

### Rust (primary)

```rust
use endb_lite::{Database, QueryResult};

let db = Database::open("mydata.db")?;

// Schema-last insert — table auto-created
db.execute(r#"
    INSERT INTO sensors {
        device_id: "thermometer-1",
        reading: 72.4,
        unit: "fahrenheit",
        location: {building: "A", floor: 3}
    }
"#)?;

// Current-state query (fast path)
let results: QueryResult = db.query(
    "SELECT * FROM sensors WHERE device_id = ?",
    &["thermometer-1"]
)?;

// Time-travel query
let historical: QueryResult = db.query(
    "SELECT * FROM sensors FOR SYSTEM_TIME AS OF ?",
    &["2025-06-15T00:00:00Z"]
)?;

// Full history
let all_versions: QueryResult = db.query(
    "SELECT *, system_time FROM sensors FOR SYSTEM_TIME ALL",
    &[]
)?;
```

### JavaScript / TypeScript (via WASM or native bindings)

```typescript
import { open } from 'cairn';

const db = await open('mydata.db');

await db.execute(`
    INSERT INTO sensors {
        device_id: "thermometer-1",
        reading: 72.4,
        unit: "fahrenheit"
    }
`);

const current = await db.query('SELECT * FROM sensors');
const historical = await db.query(
    'SELECT * FROM sensors FOR SYSTEM_TIME AS OF ?',
    ['2025-06-15T00:00:00Z']
);
```

### Python (via PyO3 or CFFI)

```python
import endb_lite

db = endb_lite.connect("mydata.db")
db.execute("""
    INSERT INTO sensors {
        device_id: "thermometer-1",
        reading: 72.4
    }
""")

rows = db.query("SELECT * FROM sensors FOR SYSTEM_TIME ALL")
```



## Scope and Non-Goals

### In scope (v0.1)

- Temporal SQL parser (FOR SYSTEM_TIME AS OF, BETWEEN, ALL, FROM...TO)
- Automatic versioning via CDC (history tables, transaction log)
- Schema-last document storage (JSONB-backed)
- ERASE for compliance deletion
- Non-destructive UPDATE and DELETE
- Period predicates (CONTAINS, OVERLAPS, PRECEDES, SUCCEEDS)
- Rust library with C API for FFI
- Single-file database, no server

### In scope (v0.2+)

- Document literal syntax in SQL ({key: value})
- Path navigation (.. operator)
- JavaScript/WASM bindings
- Python bindings
- Schema registry with type inference
- Parquet export for history tables (escape hatch to external analytical tools)
- Bi-temporal support (valid time + system time)
- History compaction / retention policies

### Non-goals

- Client-server mode (use Turso Cloud or regular Turso for that)
- Replication / sync (orthogonal problem; could layer on later)
- Distributed transactions
- Full HTAP (leave heavy analytics to DuckDB/ClickHouse)
- Replacing SQLite for general-purpose use (this is a specialized tool)
- Adaptive indexing (Endatabas's most ambitious planned feature — out of scope)



## Open Questions

1. **CDC granularity.** Does Turso's CDC expose row-level before/after images, or only statement-level change notifications? The former is needed for clean versioning. If CDC doesn't provide before-images, we may need to fall back to a trigger-based approach or a WAL-scanning strategy.

2. **JSONB performance.** Storing all document data as JSONB means every field access is a json_extract call. For hot-path queries on known fields, this could be slow. Should the engine opportunistically promote frequently-queried JSON keys to real columns (partial materialization)? This is effectively what Endatabas's adaptive indexing does.

3. **Bare date literals.** Endatabas allows `2025-06-15` as a date literal without quotes. SQLite (and Turso) parse this as `2025 - 6 - 15 = 2004`. Possible resolutions: require quotes, require a keyword prefix (`DATE '2025-06-15'` — which is standard SQL), or use context-sensitive parsing (risky).

4. **Document ID generation.** Should `_id` be a UUID (globally unique, no coordination needed) or an autoincrementing integer (simpler, smaller, faster joins)? UUIDs are better for the general case, but they make history table indexes larger.

5. **History compaction.** In a long-lived database, history tables will grow without bound. Should there be a `COMPACT` command that merges old versions into summary records? What's the retention policy model? This needs design work.

6. **Turso stability.** Turso Database is in beta. Building on it means inheriting its instability. Is the right move to start on vanilla SQLite (stable, boring) and migrate to Turso when it matures? Or is the CDC/MVCC value worth the risk now?

7. **Query result format.** Should queries return documents (JSON objects) or flat rows? Endatabas returns documents by default. SQLite tooling expects flat rows. Probably need both: documents for the programmatic API, flat rows for CLI/tooling compatibility.



## Prior Art and Influences

- **Endatabas** — the direct inspiration. Immutable SQL document database with full history. Server-based, Common Lisp, Apache Arrow. [endatabas.com](https://www.endatabas.com)
- **XTDB** — immutable bitemporal database with Postgres-compatible SQL. Server-based, Clojure/Java, Arrow. [xtdb.com](https://xtdb.com)
- **Dolt** — version-controlled SQL database (Git for data). Server-based, Go. [dolthub.com](https://www.dolthub.com)
- **ImmuDB** — immutable ledger database, embeddable, PostgreSQL dialect subset. Go. [immudb.io](https://immudb.io)
- **SirixDB** — embeddable temporal evolutionary database. Java. Low activity.
- **SQLite temporal tables** — community patterns using triggers and history tables on vanilla SQLite. Fragile but proven.
- **Stanchion** — columnar storage extension for SQLite. Zig. Alpha, unmaintained since early 2024. Demonstrated the concept of columnar virtual tables in SQLite but not viable as a dependency. [github.com/dgllghr/stanchion](https://github.com/dgllghr/stanchion)
- **Turso Database** — Rust reimplementation of SQLite with CDC, MVCC, async I/O. Beta. [github.com/tursodatabase/turso](https://github.com/tursodatabase/turso)
