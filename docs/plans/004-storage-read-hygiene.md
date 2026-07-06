# Plan 004: Deduplicate row materialization and cut per-query overhead in storage.rs

> **MIGRATED (2026-07-06)**: this plan now lives as GitHub issue
> [#25](https://github.com/nbbaier/cairn/issues/25), refreshed against commit
> `53d5785`. The issue is canonical; this file is retained for lineage only —
> do not execute from it.

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 29aac20..HEAD -- cairndb-core/src/storage.rs cairndb-core/src/schema.rs cairndb-core/src/db.rs`
> Plan 003 intentionally touches storage.rs first (it replaces `unreachable!`
> with `Error::CorruptedData`). If storage.rs shows `Error::CorruptedData`
> where the excerpts below show `unreachable!`, that is EXPECTED drift —
> preserve the CorruptedData behavior inside the new helpers. Any other
> mismatch is a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: LOW
- **Depends on**: 003 (recommended order; both touch storage.rs)
- **Category**: tech-debt / perf
- **Planned at**: commit `29aac20`, 2026-07-01

## Why this matters

`cairndb-core/src/storage.rs` repeats the same row→`Document` materialization
loop 7 times (2 shapes: current rows as 4-tuples, history rows as 6-tuples).
Any fix to deserialization must be applied in 7 places. Separately, every read
operation pays avoidable per-call costs: a `sqlite_master` existence query
that ignores the `known_tables` cache the `Database` already maintains, and a
fresh `prepare()` of an identical SQL string on every call. Finally, the
`_current` tables have no index on `_valid_from`, so `query_at`/`query_between`
scan the whole current table. None of these change behavior — this is a
consolidation + cheap-wins plan that also makes plan 005's growth of the query
layer land on clean ground.

## Current state

- `cairndb-core/src/storage.rs` (~1710 lines incl. tests):
  - Current-row materialization (4-tuple `_id, json(_data), _valid_from,
    _txn_id` → `Document::new`) appears in: `read_current_doc` (:51),
    `read_all_current` (:85), `query_at` current block (:434-460),
    `query_between` current block (:526-552).
  - History-row materialization (6-tuple adding `_valid_to, _op` →
    `Document::new_history`) appears in: `query_all` (:347-375), `query_at`
    history block (:403-431), `query_between` history block (:495-523).
  - Each block looks like (current-row shape shown):

```rust
// cairndb-core/src/storage.rs:441-459 (query_at, current rows)
        let raw: Vec<(String, String, i64, i64)> = stmt
            .query_map(rusqlite::params![ts], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (id, data_str, valid_from, txn_id) in raw {
            let data_val: Value = serde_json::from_str(&data_str)?;
            let map = match data_val {
                Value::Object(m) => m,
                _ => unreachable!("stored data is always a JSON object"),
            };
            docs.push(Document::new(id, map, valid_from, txn_id));
        }
```

  - `table_exists` (:28-36) runs `SELECT COUNT(*) FROM sqlite_master WHERE
    type='table' AND name=?1` and is called at the top of `update` (:185),
    `delete` (:226), `erase` (:264), `get` (:308), `query` (:322),
    `query_all` (:340), `query_at` (:395), `query_between` (:480).
  - All statements are built with `conn.prepare(&format!(...))` — no use of
    rusqlite's built-in `prepare_cached`.
- `cairndb-core/src/db.rs`:
  - `Database` holds `known_tables: Mutex<HashSet<String>>`, currently passed
    only to `create_table` (:60-64) and `insert` (:70-74). Read/mutation
    methods (`update`, `delete`, `erase`, `get`, `query`, `query_all`,
    `query_at`, `query_between`, lines 80-152) pass only `&conn`.
  - There is NO drop-table API anywhere in the workspace — once a table
    exists it exists for the life of the file, so a positive existence result
    can be cached indefinitely.
- `cairndb-core/src/schema.rs`:
  - `build_table_sql` (:87-137) creates `_{name}_current` (PRIMARY KEY `_id`
    only), `_{name}_history`, two triggers, and ONE index:

```sql
CREATE INDEX IF NOT EXISTS _{name}_history_idx
    ON _{name}_history (_id, _valid_from, _valid_to);
```

    There is no index on `_{name}_current (_valid_from)`.
  - `ensure_table` (:54-67) validates, checks the in-memory cache, and runs
    the DDL batch (all `IF NOT EXISTS`) on cache miss. Because the cache is
    per-process, an existing database file re-opened later will re-run the
    DDL batch once per table — so a NEW index added to `build_table_sql` is
    retroactively created for existing databases automatically.

## Commands you will need

| Purpose | Command                  | Expected on success  |
|---------|--------------------------|----------------------|
| Tests   | `cargo test --workspace` | all pass (≥214; exact count depends on whether 003 landed) |
| Lint    | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| Format  | `cargo fmt --check`      | exit 0               |

## Scope

**In scope**:
- `cairndb-core/src/storage.rs`
- `cairndb-core/src/schema.rs` (one index line in `build_table_sql`)
- `cairndb-core/src/db.rs` (thread the cache into read paths)

**Out of scope**:
- Public API signatures — `Database` methods keep their exact signatures.
- Pagination / LIMIT / OFFSET (roadmap v0.2 — do not add).
- Splitting storage.rs into submodules (deliberately deferred; see plans/README.md).
- The parser and facade crates.
- Removing the `json(_data)` → `serde_json::from_str` round-trip (JSONB
  columns require `json()` to read as text; investigated and deferred).

## Git workflow

- Branch: `refactor/storage-read-hygiene`
- Commit per step; short imperative subjects.
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Extract materialization helpers

In the "Private helpers" section of `storage.rs` (near `read_current_doc`), add:

```rust
/// Maps a `_T_current` row (`_id, json(_data), _valid_from, _txn_id`) inside `query_map`.
fn current_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, i64, i64)> { ... }

/// Maps a `_T_history` row (adds `_valid_to, _op`) inside `query_map`.
fn history_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(String, String, i64, i64, i64, String)> { ... }

/// Parses the JSON payload and builds a current-state Document.
fn materialize_current((id, data_str, valid_from, txn_id): (String, String, i64, i64)) -> Result<Document> { ... }

/// Parses the JSON payload and builds a history Document (with op/valid_to metadata).
fn materialize_history((id, data_str, valid_from, txn_id, valid_to, op): (String, String, i64, i64, i64, String)) -> Result<Document> { ... }
```

The two `materialize_*` bodies contain the single copy of the
JSON-parse + object-check logic (preserving `Error::CorruptedData` if plan 003
landed, else the current `unreachable!`). Then rewrite the 7 call sites to:

```rust
let mut stmt = conn.prepare(&sql)?;
let docs_part = stmt
    .query_map(params, current_row)?
    .collect::<rusqlite::Result<Vec<_>>>()?
    .into_iter()
    .map(materialize_current)
    .collect::<Result<Vec<_>>>()?;
```

`read_current_doc` keeps its `QueryReturnedNoRows → DocumentNotFound` mapping
but delegates parsing to `materialize_current`.

**Verify**: `grep -c "serde_json::from_str(&data_str)" cairndb-core/src/storage.rs` → ≤ 2 (only inside the two materialize helpers).
**Verify**: `cargo test --workspace` → all pass.

### Step 2: Use `prepare_cached` for all statements in storage.rs

Replace every `conn.prepare(&format!(...))` and `conn.query_row(&format!(...))`
in non-test storage.rs code with the `prepare_cached` equivalent
(`conn.prepare_cached(...)` returns a `CachedStatement` usable exactly like
`Statement`; for `query_row` call sites, switch to
`let mut stmt = conn.prepare_cached(&sql)?; stmt.query_row(params, ...)`).
rusqlite's default cache capacity (16) is fine for now — the cache key is the
SQL text, so per-table strings each get a slot; do not tune capacity in this
plan.

**Verify**: `grep -n "conn.prepare(" cairndb-core/src/storage.rs` → no matches in non-test code.
**Verify**: `cargo test --workspace` → all pass.

### Step 3: Let read paths use the `known_tables` cache

1. Change `table_exists` to a cache-aware check:

```rust
/// Returns true if the physical current table exists, consulting (and
/// populating) the per-process cache. Safe to cache positives: cairndb has
/// no DROP TABLE, so existence is permanent for the life of the file.
fn table_exists(conn: &Connection, cache: &mut HashSet<String>, table_name: &str) -> Result<bool> {
    if cache.contains(table_name) {
        return Ok(true);
    }
    // ... existing sqlite_master query ...
    if exists { cache.insert(table_name.to_string()); }
    Ok(exists)
}
```

2. Add `cache: &mut HashSet<String>` as the second parameter to the 8
   `pub(crate)` functions that call it (`update`, `delete`, `erase`, `get`,
   `query`, `query_all`, `query_at`, `query_between`) and pass it through.
3. In `db.rs`, update the corresponding `Database` methods (lines 80-152) to
   also lock `known_tables` and pass it — mirroring what `insert` (:70-74)
   already does. **Lock ordering**: always lock `conn` first, then
   `known_tables`, in every method (this is the existing order in `insert`;
   keep it uniform to prevent deadlocks).
4. Update the storage.rs test module call sites for the new signatures (tests
   construct a local `HashSet` already for `ensure_table` — reuse it).

**Verify**: `grep -c "sqlite_master" cairndb-core/src/storage.rs` → 1 (single query inside `table_exists`).
**Verify**: `cargo test --workspace` → all pass.

### Step 4: Add the `_valid_from` index on current tables

In `schema.rs` `build_table_sql`, after the existing history index, add:

```sql
CREATE INDEX IF NOT EXISTS _{name}_current_valid_from_idx
    ON _{name}_current (_valid_from);
```

(Existing databases pick this up automatically: `ensure_table` re-runs the
`IF NOT EXISTS` DDL batch on first use per process — see Current state.)

Add a test in `schema.rs`'s test module asserting the index exists after
`ensure_table`:
`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='_events_current_valid_from_idx'` → 1.
Model it on the existing schema tests that inspect `sqlite_master`.

**Verify**: `cargo test -p cairndb-core valid_from_idx` → 1 test passes (name the test to match).
**Verify**: `cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check` → all exit 0.

## Test plan

- New: index-existence test in `schema.rs` (Step 4).
- Everything else is behavior-preserving refactor guarded by the existing 183
  cairndb-core tests, which cover all 8 rewritten functions (CRUD + 4 query
  shapes) — they are the characterization suite for this plan.
- Full gate: `cargo test --workspace` → 0 failed, count ≥ pre-plan count + 1.

## Done criteria

- [ ] Materialization logic exists once per row shape (grep check from Step 1)
- [ ] No `conn.prepare(` in non-test storage.rs (Step 2 grep)
- [ ] Exactly one `sqlite_master` query in storage.rs (Step 3 grep)
- [ ] `_{name}_current_valid_from_idx` created by `build_table_sql` + test
- [ ] Public `Database` method signatures unchanged
- [ ] `cargo test --workspace`, clippy `-D warnings`, `fmt --check` all exit 0
- [ ] `plans/README.md` status row updated

## STOP conditions

- storage.rs has drifted beyond the expected plan-003 changes (excerpt mismatch).
- Any existing test changes its asserted behavior (this plan must not alter
  observable behavior — a failing assertion means the refactor changed
  semantics; report, don't adjust the test).
- `prepare_cached` fails on any statement (would indicate a rusqlite
  version/feature issue; report rather than silently reverting to `prepare`).
- Threading the cache forces a public API signature change.

## Maintenance notes

- Plan 005 (SQL SELECT dispatch) adds no new SQL to storage.rs, but v0.2
  (arbitrary WHERE) will — new query builders should reuse
  `current_row`/`materialize_current` and `prepare_cached` from day one.
- Reviewer: check lock ordering (`conn` before `known_tables`) is uniform
  across all `Database` methods after Step 3, and that no test's expected
  behavior was edited.
- Deferred: statement-string constants / SQL template registry (DEBT-03) —
  `prepare_cached` removes the perf cost; a registry adds indirection for
  little gain at current size.
