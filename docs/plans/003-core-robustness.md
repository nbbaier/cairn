# Plan 003: Remove panic paths from cairndb-core (mutex poisoning, corrupted-data unreachable!, stale annotations)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 29aac20..HEAD -- cairndb-core/src`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: LOW
- **Depends on**: 001 (CI baseline) recommended first
- **Category**: bug
- **Planned at**: commit `29aac20`, 2026-07-01

## Why this matters

`cairndb-core` promises a `Send + Sync` embedded database, but has two classes
of panic paths that turn recoverable situations into process-killing or
database-bricking failures:

1. Every public method locks the connection with `.lock().unwrap()`. If any
   thread ever panics while holding the lock, the mutex is poisoned and every
   subsequent call panics — the `Database` handle becomes permanently unusable
   for the whole process even though SQLite itself is fine.
2. Seven `unreachable!("stored data is always a JSON object")` sites panic if
   a stored `_data` value deserializes to a non-object. That invariant is held
   by cairndb's own writers, but the database is a plain SQLite file a user
   can open and edit with any tool — external corruption should surface as an
   error, not a panic in the library.

Also cleaned up here: three `#[allow(dead_code)] // used by storage module
(next milestone)` annotations on `versioning::begin_write/commit/rollback`,
which are in fact called ~12 times from `storage.rs` — the comments are stale
and misleading; and an unchecked `u128 → i64` cast in the clock helper.

## Current state

- `cairndb-core/src/db.rs` — public API. Struct fields (around line 10–15):
  `conn: Mutex<Connection>`, `known_tables: Mutex<HashSet<String>>`.
  Non-test lock sites: lines 50, 61–62, 71–72, 81, 90, 99, 107, 116, 127,
  139, 150 — all shaped like:

```rust
// cairndb-core/src/db.rs:70-74
    pub fn insert(&self, table: &str, data: Value) -> Result<Document> {
        let conn = self.conn.lock().unwrap();
        let mut cache = self.known_tables.lock().unwrap();
        storage::insert(&conn, &mut cache, table, data)
    }
```

  (Test-module lock sites at lines 181, 201, 227, 429, 460, 529, 559, … may
  stay as `.unwrap()` — tests panicking is fine.)

- `cairndb-core/src/storage.rs` — `unreachable!` at lines 79, 107, 371, 427,
  456, 519, 548, all in the same materialization shape:

```rust
// cairndb-core/src/storage.rs:76-80
    let data_val: Value = serde_json::from_str(&data_str)?;
    let map = match data_val {
        Value::Object(m) => m,
        _ => unreachable!("stored data is always a JSON object"),
    };
```

  Note: comments at `storage.rs:171` and `storage.rs:182` reference this
  unreachable behavior — update them in Step 2.

- `cairndb-core/src/error.rs` — single `Error` enum with thiserror, variants:
  `Sqlite`, `Json`, `InvalidPath`, `InvalidTimestamp`, `TableNotFound`,
  `DocumentNotFound`, `InvalidTableName`. Follow this exact style when adding
  a variant:

```rust
// cairndb-core/src/error.rs:28-30
    /// A table name failed validation (empty, starts with a digit, contains special characters).
    #[error("invalid table name: {0}")]
    InvalidTableName(String),
```

- `cairndb-core/src/versioning.rs` — stale annotations at lines 48, 80, 93:

```rust
// cairndb-core/src/versioning.rs:48-49
#[allow(dead_code)] // used by storage module (next milestone)
pub(crate) fn begin_write(conn: &Connection) -> Result<(i64, i64)> {
```

  These functions ARE used: `grep -n "versioning::" cairndb-core/src/storage.rs`
  shows ~12 call sites (lines 135, 153, 157, 194, 209, 213, 234, 245, 249, …).

- `cairndb-core/src/versioning.rs:103-109` — unchecked cast:

```rust
fn now_epoch_ms() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| Error::InvalidTimestamp(e.to_string()))
}
```

## Commands you will need

| Purpose | Command                  | Expected on success  |
|---------|--------------------------|----------------------|
| Tests   | `cargo test --workspace` | all pass (214 at planning time; this plan adds more) |
| Lint    | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| Format  | `cargo fmt --check`      | exit 0               |

## Scope

**In scope**:
- `cairndb-core/src/db.rs` (lock handling in non-test code + a small private helper)
- `cairndb-core/src/storage.rs` (replace `unreachable!` sites; adjust two comments; new tests)
- `cairndb-core/src/error.rs` (one new variant + test)
- `cairndb-core/src/versioning.rs` (remove 3 stale annotations/comments; bounds-check cast)

**Out of scope**:
- Switching to `parking_lot` — do not add dependencies.
- Any change to `schema.rs`, the parser crate, or the facade crate.
- Deduplicating the 7 materialization loops (that is plan 004 — here you edit
  the `unreachable!` line in place at each site, nothing more).
- Public API signatures — nothing in this plan may change any `pub fn` signature.

## Git workflow

- Branch: `fix/core-panic-paths`
- Commit per step; short imperative subjects (e.g. `Handle mutex poisoning in Database methods`).
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Recover from mutex poisoning instead of panicking

In `cairndb-core/src/db.rs`, add two private helper methods on `Database`:

```rust
/// Locks the connection, recovering from poisoning.
///
/// A poisoned mutex means another thread panicked while holding the lock.
/// SQLite's own transaction state is consistent (an interrupted transaction
/// rolls back), so recovering the guard is safe — refusing forever would
/// brick the Database for the whole process.
fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
    self.conn.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn tables(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
    self.known_tables.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
```

Replace every `self.conn.lock().unwrap()` / `self.known_tables.lock().unwrap()`
in **non-test** code (the 13 sites listed in Current state) with `self.conn()`
/ `self.tables()`. Leave the `#[cfg(test)]` module's direct `.lock().unwrap()`
calls alone.

Caution: `begin_write` leaves an open SQLite transaction if a panic happens
between `begin_write` and `commit`/`rollback`; rusqlite rolls back an
unfinished transaction when the statement/connection state allows, and the
next `begin_write` executes `BEGIN` — if a stale transaction were somehow
open, that `BEGIN` fails with a clear SQLite error rather than corrupting
data. No extra handling needed; just preserve current behavior otherwise.

**Verify**: `grep -n "lock().unwrap()" cairndb-core/src/db.rs` → matches only inside the `#[cfg(test)]` module (line numbers ≥ the `mod tests` line).
**Verify**: `cargo test --workspace` → all pass.

### Step 2: Replace `unreachable!` with a `CorruptedData` error

1. In `cairndb-core/src/error.rs`, add (matching the existing doc/attribute style):

```rust
    /// Stored document data violated a storage invariant (e.g. `_data` is not a JSON object).
    /// Indicates external modification or corruption of the database file.
    #[error("corrupted data: {0}")]
    CorruptedData(String),
```

2. In `cairndb-core/src/storage.rs`, at each of the 7 sites (lines 79, 107,
   371, 427, 456, 519, 548 at planning time), replace

```rust
    _ => unreachable!("stored data is always a JSON object"),
```

   with

```rust
    _ => {
        return Err(Error::CorruptedData(format!(
            "document '{id}' in table '{...}': _data is not a JSON object"
        )))
    }
```

   Use the id/table variables in scope at each site (names differ slightly per
   site — e.g. `doc_id` in `read_current_doc`). Two sites are inside
   `.map(...).collect::<Result<...>>()` closures (`read_all_current`) — a plain
   `Err(...)` expression works there instead of `return Err(...)`.

3. Update the two stale comments that reference the old panic behavior:
   `storage.rs:171` (doc comment mentioning "unreachable! panic during
   document materialization") and `storage.rs:182` — reword to say non-object
   `_data` now surfaces as `Error::CorruptedData`.

4. Update `error.rs` tests: add `Error::CorruptedData` to
   `all_variants_constructible` and `display_messages_non_empty`
   (see `cairndb-core/src/error.rs:39-76` for the pattern).

**Verify**: `grep -c "unreachable!" cairndb-core/src/storage.rs` → 0.
**Verify**: `cargo test --workspace` → all pass.

### Step 3: Add a corruption regression test

In the `#[cfg(test)]` module of `cairndb-core/src/storage.rs`, add a test
following the existing pattern there (tests build a raw `Connection` +
`HashSet` cache via `schema::ensure_table` — see the existing tests around
`storage.rs:915-925` for the setup shape):

- `corrupted_non_object_data_returns_error`: create table, insert a document
  via `storage::insert`, then corrupt it directly:
  `conn.execute("UPDATE _events_current SET _data = jsonb('[1,2,3]') WHERE _id = ?1", ...)`.
  **Note**: this UPDATE fires the BEFORE UPDATE trigger, which reads
  `_cairn_tx_context`; that table is empty outside a write transaction, so the
  trigger's subselects yield NULL and the history insert fails with a NOT NULL
  constraint error. To avoid fighting the trigger, corrupt via
  `conn.execute("DROP TRIGGER _events_before_update", [])?` first (tests may
  do this; production code may not). Then assert
  `matches!(storage::get(&conn, "events", doc.id()), Err(Error::CorruptedData(_)))`
  and the same for `storage::query(&conn, "events")`.

**Verify**: `cargo test -p cairndb-core corrupted` → 1 test passes.

### Step 4: Remove stale `dead_code` annotations and bounds-check the clock cast

1. In `cairndb-core/src/versioning.rs`, delete the three lines
   `#[allow(dead_code)] // used by storage module (next milestone)` at lines
   48, 80, 93 (attribute + trailing comment, nothing else).
2. In `now_epoch_ms` (`versioning.rs:104-109`), guard the cast:

```rust
fn now_epoch_ms() -> Result<i64> {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::InvalidTimestamp(e.to_string()))?;
    i64::try_from(d.as_millis())
        .map_err(|_| Error::InvalidTimestamp("system clock out of range".to_string()))
}
```

**Verify**: `grep -c "allow(dead_code)" cairndb-core/src/versioning.rs` → 0.
**Verify**: `cargo clippy --workspace --all-targets -- -D warnings` → exit 0 (proves the functions really are used — clippy would now flag them if not).
**Verify**: `cargo test --workspace` → all pass.

## Test plan

- New: `corrupted_non_object_data_returns_error` in `storage.rs` tests (Step 3).
- Updated: the two variant-coverage tests in `error.rs` (Step 2.4).
- Pattern to follow: existing `storage.rs` test module (raw `Connection` +
  `ensure_table` setup) and `error.rs::all_variants_constructible`.
- Full gate: `cargo test --workspace` → ≥ 215 tests, 0 failed.

## Done criteria

- [ ] `grep -rn "lock().unwrap()" cairndb-core/src/db.rs` matches only in the test module
- [ ] `grep -c "unreachable!" cairndb-core/src/storage.rs` → 0
- [ ] `grep -c "allow(dead_code)" cairndb-core/src/versioning.rs` → 0
- [ ] `Error::CorruptedData` exists with doc comment and is covered in `error.rs` tests
- [ ] `cargo test --workspace` exits 0; `cargo clippy --workspace --all-targets -- -D warnings` exits 0; `cargo fmt --check` exits 0
- [ ] No public API signature changed (`git diff` shows no `pub fn` line modified in a way that alters its signature)
- [ ] `plans/README.md` status row updated

## STOP conditions

- The lock-site line numbers or the 7 `unreachable!` sites don't match the
  excerpts (drift — plan 004 may have run first and moved them; report and ask
  which order applies).
- Removing an `#[allow(dead_code)]` produces a real dead-code warning (would
  mean the function is NOT used — contradicts this plan's premise).
- The corruption test can't be made to pass without touching production code
  beyond the specified changes.

## Maintenance notes

- Plan 004 dedupes the materialization loops into shared helpers; after it
  lands, the 7 `CorruptedData` branches collapse to ~2. If 004 runs first
  instead, this plan's Step 2 touches fewer sites — that's fine, adapt counts.
- Reviewer should scrutinize the poisoning-recovery comment in Step 1: the
  argument that recovery is safe rests on SQLite rolling back interrupted
  transactions; if a future change adds Rust-side state mutated under the
  lock, recovery-after-panic must be re-justified.
- Deferred deliberately: reserved-table-name checks (no physical collision is
  currently possible — user tables map to `_<name>_current/_history`) and
  identifier quoting (the `validate_table_name` allowlist already blocks
  injection); see `plans/README.md` rejected list.
