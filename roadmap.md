# cairndb Roadmap

Long-term target: full [Endb SQL](https://docs.endatabas.com/sql/) compatibility in an embedded, single-file, no-server package.

### Non-goals

- Client-server mode
- Replication / sync (orthogonal problem; could layer on later)
- Distributed transactions
- Full HTAP (leave heavy analytics to DuckDB/ClickHouse)
- Replacing SQLite for general-purpose use (this is a specialized tool)
- Adaptive indexing

---

## v0.1a — Storage Layer (cairndb-core) ✅

**Status: Complete** — 183 passing tests.

The foundational storage engine, exercised directly via Rust API.

- Vanilla SQLite via `rusqlite` (bundled)
- Schema-last document storage (JSONB-backed)
- UUIDv7 document IDs, integer millisecond timestamps
- Automatic versioning via BEFORE triggers (history tables)
- CRUD: `insert`, `update` (JSON merge patch), `delete` (soft), `erase` (permanent)
- Time-travel queries: `query` (current), `query_at` (AS OF), `query_between`, `query_all`
- Transaction log with monotonic IDs
- Erasure log for GDPR audit
- `Database` struct: `Send + Sync`, `Mutex<Connection>`, WAL mode
- `Document` / `QueryResult` types with system metadata accessors
- Single `Error` enum with `thiserror`

**Crate:** `cairndb-core`
**PRD:** [#1](https://github.com/nbbaier/cairn/issues/1) (closed)

---

## v0.1b — SQL Parser & Dispatch (cairndb-parser + cairndb facade)

**Status: Next milestone**

Adds a SQL interface on top of the storage layer. Users can write SQL instead of calling Rust methods directly.

### Parser (cairndb-parser)
- Hybrid parsing: custom parsers for INSERT/ERASE, `sqlparser-rs` for the rest
- `FOR SYSTEM_TIME` clause stripped before passing SELECT to `sqlparser-rs`
- Document literal INSERT: `INSERT INTO t {key: 'val', nested: {a: 1}, arr: [1, 2]}`
- Column/value INSERT: `INSERT INTO t (col1, col2) VALUES (v1, v2)`
- Statement IR output (not raw SQL): `Statement`, `Filter`, `TemporalClause` enums

### Supported Statements
| Statement | Maps to |
|---|---|
| `INSERT INTO t (cols) VALUES (vals)` | `db.insert()` |
| `INSERT INTO t {key: val, ...}` | `db.insert()` |
| `SELECT * FROM t` | `db.query()` |
| `SELECT * FROM t WHERE _id = ?` | `db.get()` |
| `SELECT * FROM t FOR SYSTEM_TIME AS OF ts` | `db.query_at()` |
| `SELECT * FROM t FOR SYSTEM_TIME BETWEEN t1 AND t2` | `db.query_between()` |
| `SELECT * FROM t FOR SYSTEM_TIME ALL` | `db.query_all()` |
| `UPDATE t SET col=val,... WHERE _id = ?` | `db.update()` |
| `DELETE FROM t WHERE _id = ?` | `db.delete()` |
| `ERASE FROM t WHERE _id = ?` | `db.erase()` |
| `CREATE TABLE t` | `db.create_table()` |

### Facade (cairndb)
- `Database::sql(&self, query: &str) -> Result<QueryResult>` method
- Pattern-matches on `Statement` IR, dispatches to `cairndb-core`

### Constraints
- All mutations are ID-addressed only (`WHERE _id = ?`)
- Document literal values: strings, integers, floats, booleans, null, nested objects, arrays
- No bare date/time literals, no arbitrary WHERE, no projections

**Crates:** `cairndb-parser`, `cairndb`

---

## v0.2 — Query Expressiveness

**Status: Future**

Unlocks the SQL features users expect from a real database.

### Core query features
- Arbitrary `WHERE` clauses (comparison operators, AND/OR/NOT, IS NULL)
- Column projections (`SELECT col1, col2 FROM t`)
- `ORDER BY`, `LIMIT` / `OFFSET`
- Table/column aliases (`AS`)
- `BETWEEN` operator (value, not temporal)

### Mutation improvements
- Arbitrary `WHERE` on UPDATE, DELETE, ERASE (not just `_id`)
- `DELETE FROM t` (all rows)
- Multi-row INSERT: `VALUES (...), (...)`

### Time query additions
- `FOR SYSTEM_TIME FROM ... TO ...`
- `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME`

---

## v0.3 — Document & Path Features

**Status: Future**

The features that make cairndb feel like a document database at the SQL layer.

### Document literals everywhere
- Bare date/time literals (`2024-01-01`, `2024-01-01T00:00:00Z`)
- Multi-document INSERT: `INSERT INTO t {doc1}, {doc2}`
- `OBJECTS` lists
- Spread operator (`...`)

### Path navigation
- Dot notation (`b.a`), deep scan (`..a`)
- Bracket notation: named (`['key']`), indexed (`[0]`), wildcard (`[*]`)
- `path_set`, `path_replace`, `path_insert`, `path_remove`, `path_extract`

### DML extensions
- `UPDATE ... UNSET/REMOVE`
- `UPDATE ... PATCH {key: val}`
- `UPDATE ... SET $.path = val`
- `INSERT ... ON CONFLICT ... DO UPDATE/NOTHING`

---

## v0.4 — Advanced SQL

**Status: Future**

Standard SQL features needed for non-trivial queries.

### Joins & subqueries
- `JOIN` (INNER, LEFT, CROSS), `USING`
- `IN` / `NOT IN`, `EXISTS`, `ANY` / `ALL`
- Subqueries in WHERE

### Aggregation
- `GROUP BY` / `HAVING`
- Aggregate functions: MIN, MAX, SUM, AVG, COUNT, ARRAY_AGG, GROUP_CONCAT
- `DISTINCT` / `ALL`

### Set operations
- `UNION` / `UNION ALL`, `INTERSECT`, `EXCEPT`

### CTEs
- `WITH` (common table expressions for queries and DML)
- `WITH RECURSIVE`

### Other
- `VALUES` lists
- `LATERAL` subqueries
- `LIKE`, `GLOB`, `REGEXP`
- `MATCH` / `@>` / `<@`

---

## v0.5 — Functions, Types & Temporal Predicates

**Status: Future**

Fills in the function library and completes temporal query support.

### Functions
- String: LENGTH, TRIM, LOWER, UPPER, REPLACE, SUBSTR, INSTR, POSITION, etc.
- Math: ROUND, ABS, SQRT, LOG, SIN, COS, FLOOR, CEILING, POWER, etc.
- Object: OBJECT_KEYS, OBJECT_VALUES, OBJECT_ENTRIES, OBJECT_FROM_ENTRIES, PATCH
- Date/Time: STRFTIME, UNIXEPOCH, JULIANDAY, EXTRACT, PERIOD
- Table-valued: UNNEST (with ORDINALITY), GENERATE_SERIES
- Utility: CAST, TYPEOF, IIF, COALESCE, NULLIF, UUID
- Crypto: SHA1, HEX, UNHEX, BASE64, RANDOMBLOB, ZEROBLOB

### Data types
- PERIOD, INTERVAL, TIME, BLOB
- Type widening/coercion

### Period predicates
- CONTAINS, OVERLAPS
- PRECEDES, SUCCEEDS
- IMMEDIATELY PRECEDES, IMMEDIATELY SUCCEEDS

---

## v0.6 — Schema, Views, Assertions & Vectors

**Status: Future**

Database-level features for introspection, reuse, and constraints.

### Schema introspection
- `information_schema.tables`
- `information_schema.columns`
- `information_schema.views`
- `information_schema.check_constraints`

### Views
- `CREATE VIEW ... AS SELECT ...`
- `DROP VIEW`

### Assertions
- `CREATE ASSERTION ... CHECK (...)`
- `DROP ASSERTION`

### Vector operations
- Operators: `<->` (L2), `<=>` (cosine), `<#>` (inner product)
- Functions: L2_DISTANCE, COSINE_DISTANCE, INNER_PRODUCT

---

## Future (unversioned)

Items that don't have a milestone yet but are in scope long-term.

- `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` (repeatable reads)
- `BEGIN` / `COMMIT` / `ROLLBACK` (explicit transactions)
- Row literals (`{table.*}`)
- Computed fields in object literals
- Bi-temporal support (valid time + system time)
- History compaction / retention policies
- Parquet export for history tables
- Schema registry with active type inference
- JavaScript/WASM bindings
- Python bindings (PyO3)
- C API for FFI
- Migration path to Turso (swap triggers for CDC)

---

## Endb SQL Compatibility Matrix

Status key: **done** = implemented via Rust API | **v0.1b** = shipping in parser milestone | **v0.2+** = planned future milestone | **--** = not yet planned

Reference: [Endb SQL Reference](https://docs.endatabas.com/sql/)

### Data Manipulation ([ref](https://docs.endatabas.com/sql/data_manipulation))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| `INSERT INTO t (cols) VALUES (vals)` | Yes | Yes | v0.1b |
| `INSERT INTO t (cols) VALUES (...), (...)` (multi-row) | Yes | -- | v0.3 |
| `INSERT INTO t {key: val, ...}` (document literal) | Yes | Yes | v0.1b |
| `INSERT INTO t {doc1}, {doc2}` (multi-doc) | Yes | -- | v0.3 |
| `INSERT INTO t SELECT ...` | Yes | -- | v0.4 |
| `UPDATE t SET col = val WHERE ...` | Yes | `WHERE _id` only | v0.1b |
| `UPDATE t SET col1 = v1, col2 = v2` (multi-column) | Yes | Yes | v0.1b |
| `UPDATE t UNSET/REMOVE col` | Yes | -- | v0.3 |
| `UPDATE t PATCH {key: val}` | Yes | -- | v0.3 |
| `UPDATE t SET $.path = val` (path SET) | Yes | -- | v0.3 |
| `UPDATE t UNSET $.path` (path UNSET) | Yes | -- | v0.3 |
| `DELETE FROM t WHERE ...` | Yes | `WHERE _id` only | v0.1b |
| `DELETE FROM t` (all rows) | Yes | -- | v0.2 |
| `ERASE FROM t WHERE ...` | Yes | `WHERE _id` only | v0.1b |
| `INSERT ... ON CONFLICT ... DO UPDATE/NOTHING` | Yes | -- | v0.3 |
| `WITH` (CTEs for DML) | Yes | -- | v0.4 |

### Queries ([ref](https://docs.endatabas.com/sql/queries))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| `SELECT * FROM t` | Yes | Yes | v0.1b |
| `SELECT col1, col2 FROM t` (projections) | Yes | -- | v0.2 |
| `SELECT * FROM t WHERE _id = ?` | Yes | Yes | v0.1b |
| `SELECT * FROM t WHERE <expr>` (arbitrary) | Yes | -- | v0.2 |
| `DISTINCT` / `ALL` | Yes | -- | v0.4 |
| `AS` (table/column aliases) | Yes | -- | v0.2 |
| `JOIN` (INNER, LEFT, CROSS) | Yes | -- | v0.4 |
| `JOIN ... USING` | Yes | -- | v0.4 |
| `ORDER BY` | Yes | -- | v0.2 |
| `GROUP BY` / `HAVING` | Yes | -- | v0.4 |
| `LIMIT` / `OFFSET` | Yes | -- | v0.2 |
| `VALUES` lists | Yes | -- | v0.4 |
| `OBJECTS` lists | Yes | -- | v0.3 |
| `UNION` / `INTERSECT` / `EXCEPT` | Yes | -- | v0.4 |
| `WITH` / `WITH RECURSIVE` (CTEs) | Yes | -- | v0.4 |
| `LATERAL` subqueries | Yes | -- | v0.4 |
| `SAVEPOINT` / `ROLLBACK TO` / `RELEASE` | Yes | -- | Future |
| `BEGIN` / `COMMIT` / `ROLLBACK` (transactions) | Yes | -- | Future |

### Time Queries ([ref](https://docs.endatabas.com/sql/time_queries))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| `FOR SYSTEM_TIME AS OF` | Yes | Yes | v0.1b |
| `FOR SYSTEM_TIME ALL` | Yes | Yes | v0.1b |
| `FOR SYSTEM_TIME BETWEEN ... AND ...` | Yes | Yes | v0.1b |
| `FOR SYSTEM_TIME FROM ... TO ...` | Yes | -- | v0.2 |
| `SELECT *, system_time FROM t` | Yes | via `.system_time()` accessor | done |
| `CONTAINS` (period predicate) | Yes | -- | v0.5 |
| `OVERLAPS` (period predicate) | Yes | -- | v0.5 |
| `PRECEDES` / `SUCCEEDS` | Yes | -- | v0.5 |
| `IMMEDIATELY PRECEDES` / `IMMEDIATELY SUCCEEDS` | Yes | -- | v0.5 |
| `CURRENT_TIMESTAMP` / `CURRENT_DATE` / `CURRENT_TIME` | Yes | -- | v0.2 |

### Data Types ([ref](https://docs.endatabas.com/sql/data_types))

| Type | Endb | cairndb | Milestone |
|---|---|---|---|
| NULL | Yes | Yes (in doc literals + JSONB) | v0.1b |
| TEXT (strings) | Yes | Yes | v0.1b |
| BOOLEAN | Yes | Yes | v0.1b |
| INTEGER | Yes | Yes | v0.1b |
| FLOAT | Yes | Yes | v0.1b |
| TIMESTAMP (bare literal) | Yes | -- (quoted strings only) | v0.3 |
| DATE (bare literal) | Yes | -- (quoted strings only) | v0.3 |
| TIME | Yes | -- | v0.5 |
| PERIOD | Yes | -- | v0.5 |
| INTERVAL | Yes | -- | v0.5 |
| BLOB | Yes | -- | v0.5 |
| ARRAY (in doc literals) | Yes | Yes | v0.1b |
| OBJECT (doc literals) | Yes | Yes (INSERT only) | v0.1b |
| Row literals (`{table.*}`) | Yes | -- | Future |
| Spread operator (`...`) | Yes | -- | v0.3 |
| Computed fields | Yes | -- | Future |

### Operators ([ref](https://docs.endatabas.com/sql/operators))

| Operator | Endb | cairndb | Milestone |
|---|---|---|---|
| Comparison (`=`, `>`, `<`, `>=`, `<=`, `<>`) | Yes | -- | v0.2 |
| `BETWEEN ... AND ...` | Yes | -- | v0.2 |
| Boolean (`AND`, `OR`, `NOT`) | Yes | -- | v0.2 |
| `IS` / `IS NOT` / `IS NULL` | Yes | -- | v0.2 |
| Math (`+`, `-`, `*`, `/`, `%`) | Yes | -- | v0.5 |
| Bitwise (`&`, `\|`, `~`) | Yes | -- | v0.5 |
| `LIKE` / `NOT LIKE` | Yes | -- | v0.4 |
| `REGEXP` / `NOT REGEXP` | Yes | -- | v0.4 |
| `GLOB` / `NOT GLOB` | Yes | -- | v0.4 |
| `MATCH` / `@>` / `<@` | Yes | -- | v0.4 |
| `ANY` / `SOME` / `ALL` (subquery) | Yes | -- | v0.4 |
| `EXISTS` | Yes | -- | v0.4 |
| `IN` / `NOT IN` | Yes | -- | v0.4 |
| `\|\|` (concatenation) | Yes | -- | v0.5 |
| `<->` (L2 distance) | Yes | -- | v0.6 |
| `<=>` (cosine distance) | Yes | -- | v0.6 |
| `<#>` (inner product) | Yes | -- | v0.6 |

### Functions ([ref](https://docs.endatabas.com/sql/functions))

| Category | Endb | cairndb | Milestone |
|---|---|---|---|
| String (LENGTH, TRIM, LOWER, UPPER, REPLACE, SUBSTR, etc.) | Yes | -- | v0.5 |
| Object (OBJECT_KEYS, OBJECT_VALUES, OBJECT_ENTRIES, PATCH) | Yes | -- | v0.5 |
| Table-valued (UNNEST, GENERATE_SERIES) | Yes | -- | v0.5 |
| Math (ROUND, SIN, COS, ABS, SQRT, LOG, etc.) | Yes | -- | v0.5 |
| Date/Time (STRFTIME, UNIXEPOCH, JULIANDAY, EXTRACT, PERIOD) | Yes | -- | v0.5 |
| Aggregate (MIN, MAX, SUM, AVG, COUNT, ARRAY_AGG, GROUP_CONCAT) | Yes | -- | v0.4 |
| Utility (CAST, TYPEOF, IIF, COALESCE, NULLIF, UUID) | Yes | -- | v0.5 |
| Crypto/Encoding (SHA1, HEX, UNHEX, BASE64) | Yes | -- | v0.5 |
| Vector (L2_DISTANCE, COSINE_DISTANCE, INNER_PRODUCT) | Yes | -- | v0.6 |

### Path Navigation ([ref](https://docs.endatabas.com/sql/path_navigation))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| Dot notation (`b.a`) | Yes | -- | v0.3 |
| Deep scan (`..a`) | Yes | -- | v0.3 |
| Named child (`['key']`) | Yes | -- | v0.3 |
| Indexed child (`[0]`) | Yes | -- | v0.3 |
| Wildcard (`[*]`) | Yes | -- | v0.3 |
| `path_set`, `path_replace`, `path_insert` | Yes | -- | v0.3 |
| `path_remove`, `path_extract` | Yes | -- | v0.3 |

### Schema ([ref](https://docs.endatabas.com/sql/schema))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| `information_schema.tables` | Yes | -- | v0.6 |
| `information_schema.columns` | Yes | -- | v0.6 |
| `information_schema.views` | Yes | -- | v0.6 |
| `information_schema.check_constraints` | Yes | -- | v0.6 |
| `CREATE TABLE` (explicit) | N/A (schemaless) | Yes | v0.1b |

### Views ([ref](https://docs.endatabas.com/sql/views))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| `CREATE VIEW` | Yes | -- | v0.6 |
| `DROP VIEW` | Yes | -- | v0.6 |

### Assertions ([ref](https://docs.endatabas.com/sql/assertions))

| Feature | Endb | cairndb | Milestone |
|---|---|---|---|
| `CREATE ASSERTION ... CHECK (...)` | Yes | -- | v0.6 |
| `DROP ASSERTION` | Yes | -- | v0.6 |
