# Plan 005: Parse and dispatch SELECT with FOR SYSTEM_TIME end-to-end (v0.1b slice 2)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 29aac20..HEAD -- cairndb-parser/src cairndb/src cairndb-core/src/document.rs`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: MED
- **Depends on**: 001 (CI). Independent of 003/004 (different files, except a
  15-line addition to `cairndb-core/src/document.rs`).
- **Category**: direction
- **Planned at**: commit `29aac20`, 2026-07-01

## Why this matters

The roadmap's active milestone (v0.1b) is a SQL interface over the finished
storage engine, and the design is fully decided (decisions #15–#23 in
`decisions.md`). Today the whole SQL surface is `CREATE TABLE` — the parser
IR already defines `Select`/`TemporalClause`, but nothing parses or dispatches
them, so `db.sql("SELECT * FROM t")` fails. This plan ships the SELECT family:
`SELECT * FROM t`, `WHERE _id = '...'`, and all three `FOR SYSTEM_TIME` forms
(`AS OF`, `BETWEEN ... AND ...`, `ALL`) — the read half of the milestone, and
the first statement that exercises the "strip temporal clause, then
sqlparser-rs" hybrid strategy the rest of v0.1b builds on.

## Current state

- `cairndb-parser/src/ir.rs` — IR already defined (do not redesign):

```rust
// ir.rs:12-15, 32-42
    Select {
        table: String,
        filter: Option<Filter>,
        temporal: Option<TemporalClause>,
    },
...
pub enum Filter { ById(String) }

pub enum TemporalClause {
    AsOf(String),
    Between(String, String),
    All,
}
```

- `cairndb-parser/src/parse.rs:5-15` — entry point; trims, rejects empty,
  routes everything to `standard::parse_standard`. Comment says custom
  parsers land here later.
- `cairndb-parser/src/standard.rs:8-28` — parses via
  `Parser::parse_sql(&SQLiteDialect {}, sql)`, requires exactly one statement,
  matches `ast::Statement::CreateTable` only; everything else →
  `Error::Unsupported`. Follow `parse_create_table` (:30-92) as the style
  exemplar: reject each unsupported AST feature with a specific
  `Error::Unsupported` message before accepting.
- `cairndb-parser/src/error.rs` — two variants: `Parse(String)`,
  `Unsupported(String)`.
- `cairndb/src/sql.rs` (entire file, 17 lines):

```rust
pub(crate) fn execute(db: &Database, query: &str) -> Result<QueryResult> {
    let stmt = cairndb_parser::parse(query)?;
    match stmt {
        cairndb_parser::Statement::CreateTable { table } => {
            db.create_table(&table)?;
            Ok(QueryResult::default())
        }
        _ => Err(cairndb_parser::Error::Unsupported(
            "statement type not yet implemented in dispatch".to_string(),
        )
        .into()),
    }
}
```

- `cairndb/src/lib.rs` — facade `Database` wraps `cairndb_core::Database`
  with delegating methods: `query`, `get`, `query_at`, `query_between`,
  `query_all` (lines 48–71) — these are the dispatch targets.
- `cairndb-core/src/document.rs:152` — `QueryResult::new` is `pub(crate)`;
  the facade crate cannot construct a `QueryResult` from the single
  `Document` returned by `get`. `QueryResult` derives `Default` (used by
  sql.rs today).
- Core temporal semantics (dispatch must preserve, do not re-validate in the
  parser): timestamps are ISO 8601 strings validated by the CORE
  (`Error::InvalidTimestamp` for wrong format, exactly
  `YYYY-MM-DDTHH:MM:SS.mmmZ`); IR carries them as raw strings (decision #21).
- Existing integration-test exemplar: `cairndb/tests/sql_create_table.rs`
  (e.g. `sql_create_table_basic` opens `Database::open_in_memory()`, calls
  `db.sql(...)`, asserts via the Rust API).
- Statement grammar to support (from decisions #16/#18 and roadmap v0.1b):

```
SELECT * FROM <table> [FOR SYSTEM_TIME <temporal>] [WHERE _id = '<string>']
<temporal> := AS OF '<ts>' | BETWEEN '<ts1>' AND '<ts2>' | ALL
```

  Notes: Endb puts `FOR SYSTEM_TIME` after the table name (as in SQL:2011).
  Combination of `WHERE _id` **with** a temporal clause has no core API →
  reject as `Unsupported` for now.

## Commands you will need

| Purpose | Command                    | Expected on success |
|---------|----------------------------|---------------------|
| Tests   | `cargo test --workspace`   | all pass            |
| Parser only | `cargo test -p cairndb-parser` | all pass       |
| Lint    | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| Format  | `cargo fmt --check`        | exit 0              |

## Scope

**In scope**:
- `cairndb-parser/src/temporal.rs` (create — FOR SYSTEM_TIME stripper)
- `cairndb-parser/src/parse.rs` (route through the stripper)
- `cairndb-parser/src/standard.rs` (add `ast::Statement::Query` handling)
- `cairndb-parser/src/lib.rs` (export the new module if needed)
- `cairndb/src/sql.rs` (dispatch `Statement::Select`)
- `cairndb/tests/sql_select.rs` (create)
- `cairndb-core/src/document.rs` (ONLY: add `impl From<Document> for QueryResult`)

**Out of scope**:
- INSERT (document literal or column/value), UPDATE, DELETE, ERASE parsing —
  later v0.1b slices.
- Projections (`SELECT col1, col2`), arbitrary WHERE, ORDER BY, LIMIT — v0.2;
  reject each with a specific `Unsupported` message.
- Any change to `cairndb-core` storage/schema/versioning/db modules.
- Timestamp validation in the parser (core owns it — decision #21).
- `FOR SYSTEM_TIME FROM ... TO ...` (v0.2 per roadmap).

## Git workflow

- Branch: `feat/sql-select-dispatch`
- Commit per step; short imperative subjects (repo exemplar: `Add parser scaffold and CREATE TABLE end-to-end`).
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Allow the facade to build a one-document QueryResult

In `cairndb-core/src/document.rs`, next to `QueryResult::new`, add:

```rust
impl From<Document> for QueryResult {
    fn from(doc: Document) -> Self {
        Self { documents: vec![doc] }
    }
}
```

(Adjust to the actual private field name — check the struct at
`document.rs:145`.) Add a small unit test in document.rs's test module:
converting a Document yields `len() == 1`.

**Verify**: `cargo test -p cairndb-core` → all pass.

### Step 2: Implement the FOR SYSTEM_TIME stripper in the parser

Create `cairndb-parser/src/temporal.rs`:

```rust
/// Scans `sql` for a `FOR SYSTEM_TIME ...` clause (case-insensitive, outside
/// single-quoted string literals), removes it, and returns the remaining SQL
/// plus the extracted clause. sqlparser-rs cannot parse this non-standard
/// clause, so it must be stripped first (decision #18).
pub(crate) fn strip_system_time(sql: &str) -> Result<(String, Option<TemporalClause>)>
```

Implementation requirements:
- Tokenize minimally: walk the input tracking whether you are inside a
  `'...'` string literal (with `''` as the escaped quote, SQL-style). Only
  match the keywords `FOR SYSTEM_TIME` outside literals.
- After matching `FOR SYSTEM_TIME` (any case, any run of whitespace between
  the words), parse exactly one of:
  - `ALL` → `TemporalClause::All`
  - `AS OF '<ts>'` → `TemporalClause::AsOf(ts)` (ts = literal content, no
    validation)
  - `BETWEEN '<ts1>' AND '<ts2>'` → `TemporalClause::Between(ts1, ts2)`
  - anything else → `Error::Parse("malformed FOR SYSTEM_TIME clause: ...")`
- Remove the matched span from the SQL (replace with a single space) and
  return the rest untouched.
- If `FOR SYSTEM_TIME` appears twice → `Error::Parse`.
- If absent → `Ok((sql.to_string(), None))`.

Unit tests in the same file (see Test plan).

**Verify**: `cargo test -p cairndb-parser temporal` → all new tests pass.

### Step 3: Route parse() through the stripper and parse SELECT

1. In `parse.rs::parse`, after the empty check: call
   `temporal::strip_system_time(trimmed)?`, pass the stripped SQL to
   `standard::parse_standard(&stripped, temporal)` (add the parameter).
2. In `standard.rs::parse_standard`, add a match arm for
   `ast::Statement::Query(query)` → new `fn parse_select(query: Box<ast::Query>, temporal: Option<TemporalClause>) -> Result<Statement>`.
3. `parse_select` accepts ONLY this shape, rejecting everything else with a
   specific `Error::Unsupported` message (mirror `parse_create_table`'s
   guard-clause style at `standard.rs:30-92`):
   - body is a plain `Select` (no set ops, no `WITH`, no `ORDER BY`/`LIMIT`),
   - projection is exactly `[SelectItem::Wildcard(_)]` (else "projections are
     not supported"),
   - exactly one plain table in `from` (no joins, no alias), single-part
     identifier (reuse the identifier extraction pattern from
     `parse_create_table` at `standard.rs:80-89`),
   - no GROUP BY / HAVING / DISTINCT,
   - `selection`: `None` → filter `None`; or exactly
     `BinaryOp { left: Identifier("_id"), op: Eq, right: Value(SingleQuotedString(id)) }`
     → `Filter::ById(id)`; anything else → "arbitrary WHERE clauses are not
     supported (only WHERE _id = '<id>')".
   - If BOTH a filter and a temporal clause are present →
     `Error::Unsupported("WHERE _id combined with FOR SYSTEM_TIME is not supported")`.
4. `CreateTable` path: if `temporal` is `Some`, return
   `Error::Parse("FOR SYSTEM_TIME is only valid on SELECT")`. Same for any
   other statement type.

Note on sqlparser 0.55 AST shapes: field names above (`SetExpr::Select`,
`SelectItem::Wildcard`, `TableFactor::Table`, `Expr::BinaryOp`,
`Expr::Identifier`, `ast::Value::SingleQuotedString`) should be confirmed
against the vendored crate: `cargo doc -p sqlparser --no-deps` or read
`~/.cargo/registry/src/*/sqlparser-0.55.0/src/ast/query.rs`. If a shape
differs materially, adapt the pattern match — the acceptance rules above are
what's load-bearing.

**Verify**: `cargo test -p cairndb-parser` → all pass (existing 21 + new).

### Step 4: Dispatch Select in the facade

In `cairndb/src/sql.rs`, add the arm:

```rust
cairndb_parser::Statement::Select { table, filter, temporal } => {
    match (filter, temporal) {
        (None, None) => db.query(&table),
        (Some(cairndb_parser::Filter::ById(id)), None) => {
            Ok(db.get(&table, &id)?.into())
        }
        (None, Some(cairndb_parser::TemporalClause::AsOf(ts))) => db.query_at(&table, &ts),
        (None, Some(cairndb_parser::TemporalClause::Between(from, to))) => {
            db.query_between(&table, &from, &to)
        }
        (None, Some(cairndb_parser::TemporalClause::All)) => db.query_all(&table),
        (Some(_), Some(_)) => unreachable!("parser rejects filter+temporal"),
    }
}
```

(If the parser guarantees the last combination never arrives, prefer making
the parser's rejection the single source of truth and use a defensive
`Err(Unsupported(...))` rather than `unreachable!` — this repo is moving away
from panics; see plans/003.)

**Verify**: `cargo test --workspace` → all pass.
**Verify (smoke)**: `cargo test -p cairndb` → integration tests pass.

### Step 5: Integration tests

Create `cairndb/tests/sql_select.rs` modeled on
`cairndb/tests/sql_create_table.rs` (same open-in-memory + `db.sql(...)` +
assert-via-Rust-API structure). Cover at minimum:

1. `SELECT * FROM t` returns all current docs (insert 2 via `db.insert`, assert `len() == 2`).
2. `SELECT * FROM t WHERE _id = '<id>'` returns exactly that doc (compare `id()`).
3. `WHERE _id` with unknown id → `Error::Core(DocumentNotFound)` (assert on `to_string()` containing "document not found").
4. `SELECT * FROM t FOR SYSTEM_TIME ALL` after insert+update returns ≥ 2 versions.
5. `AS OF` with a timestamp before the first insert returns empty; with a
   current timestamp returns the doc. Get a usable timestamp from
   `doc.system_time()` (returns the ISO string the core accepts).
6. `BETWEEN` spanning the insert returns the doc; reversed range returns empty.
7. Unsupported shapes each produce an error mentioning the right feature:
   `SELECT a, b FROM t` (projections), `SELECT * FROM t WHERE x = 1`
   (arbitrary WHERE), `SELECT * FROM t ORDER BY _id` (ORDER BY),
   `SELECT * FROM t WHERE _id = 'x' FOR SYSTEM_TIME ALL` (combination).
8. Malformed temporal clause → `Error::Parse`: `SELECT * FROM t FOR SYSTEM_TIME AS OF banana`.
9. Case-insensitivity: `select * from t for system_time all` works.
10. String-literal safety: inserting a doc with a value containing
    `FOR SYSTEM_TIME` as text, then `SELECT * FROM t WHERE _id = '<that id>'`
    — and a WHERE value containing the words, e.g.
    `SELECT * FROM t WHERE _id = 'FOR SYSTEM_TIME ALL'` → DocumentNotFound,
    NOT a temporal parse (proves the stripper respects quotes).

Parser-level unit tests (in `temporal.rs` / `standard.rs` test modules): happy
paths for all three clause forms, escaped-quote literal (`'it''s'`), clause
duplicated → Parse error, clause on CREATE TABLE → Parse error.

**Verify**: `cargo test --workspace` → 0 failed; new test count ≥ 15 above the pre-plan baseline.

## Test plan

See Step 5 (the enumerated cases ARE the test plan). Structural patterns:
`cairndb/tests/sql_create_table.rs` for integration, `standard.rs:94-189` for
parser units. Gate: `cargo test --workspace` all green, clippy `-D warnings`
clean, `cargo fmt --check` clean.

## Done criteria

- [ ] All 5 SELECT forms in the roadmap's v0.1b table work via `db.sql(...)` (cases 1, 2, 4, 5, 6 above)
- [ ] `cargo test --workspace` exits 0 with ≥ 15 new tests
- [ ] `grep -n "Unsupported" cairndb-parser/src/standard.rs` shows specific messages for projections, arbitrary WHERE, joins, ORDER BY (not one generic message)
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` exit 0
- [ ] Only in-scope files modified (`git status`)
- [ ] `plans/README.md` status row updated

## STOP conditions

- sqlparser 0.55's `Query`/`Select` AST differs so much from the shapes in
  Step 3 that the WHERE `_id` extraction can't be written as a pattern match —
  report the actual AST shape rather than loosening acceptance rules.
- You find yourself wanting to add a variant to the IR enums — the IR is a
  recorded design decision (#21); report instead.
- Supporting the temporal clause requires touching `cairndb-core` beyond the
  Step 1 `From<Document>` impl.
- The stripper cannot cleanly handle a case in the test list (e.g. quoted
  literals) without a real tokenizer — report; do not ship a stripper that
  corrupts string literals.

## Maintenance notes

- The next v0.1b slices (INSERT document-literal parser, UPDATE/DELETE/ERASE)
  plug into the same two seams: `parse.rs` routing and `sql.rs` dispatch.
  Keep `parse_select`'s guard-clause style as the template.
- v0.2 replaces `Filter::ById`-only WHERE handling with expression support —
  the rejection messages added here are the breadcrumbs users will hit until
  then; keep them accurate.
- Reviewer: scrutinize the stripper's quote handling (test case 10) and the
  decision to reject filter+temporal combinations (core has no
  `get_at(table, id, ts)`; adding one is a future core change, deliberately
  not smuggled in here).
