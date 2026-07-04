# AGENTS.md

## What this is

cairndb is an embedded temporal document database built on vanilla SQLite.
Every write is versioned automatically, so queries can ask "what did this
document look like at time T" without any application-level bookkeeping. The
workspace has three crates: `cairndb` (public facade), `cairndb-core`
(storage engine), `cairndb-parser` (SQL → IR). See `spec.md` for the product
vision, `roadmap.md` for milestones and the Endb compatibility matrix, and
`decisions.md` for the numbered design-decision log.

## Build / test / lint

| Purpose | Command |
|---------|---------|
| Tests   | `cargo test --workspace` |
| Format  | `cargo fmt --check` |
| Lint    | `cargo clippy --workspace --all-targets -- -D warnings` |

Always run `cargo test --workspace` (not per-crate) before declaring done —
cross-crate behavior lives in `cairndb/tests/`.

## Workspace layout & dependency rule

- `cairndb-core` — the storage engine: schema, versioning triggers, system
  tables, query execution.
- `cairndb-parser` — parses SQL text into a typed IR; no knowledge of
  storage.
- `cairndb` — the public facade (e.g. `Database::sql()`); the only crate
  that depends on both of the above.

Hard rule: `cairndb-core` and `cairndb-parser` must never depend on each
other. Only `cairndb` may depend on both (Decisions #3 and #22 in
`decisions.md`).

## Invariants you must not break

- Table names are validated by `validate_table_name` in
  `cairndb-core/src/schema.rs` (`^[a-zA-Z_][a-zA-Z0-9_]*$`). This validation
  is the *only* thing making the `format!`-interpolated table names in
  `storage.rs`/`schema.rs` SQL-safe — never bypass or weaken it.
- Timestamps are integer epoch-milliseconds internally; the public API
  speaks ISO 8601 `YYYY-MM-DDTHH:MM:SS.mmmZ` (24 chars, UTC only) — see
  Decision #7. Conversion happens only in the Rust layer.
- Physical schema is owned entirely by cairndb: user table `t` becomes
  `_t_current` + `_t_history` plus BEFORE triggers (see `spec.md`, Storage
  Model section); system tables are `_transactions`, `_schema_registry`,
  `_erasure_log`, and `_cairn_tx_context` (the latter per Decision #9).
- The parser emits a typed IR (`cairndb-parser/src/ir.rs::Statement`), never
  raw SQL (Decision #20).

## Testing conventions

- Unit tests live inline in `#[cfg(test)]` modules next to the code they
  test.
- Cross-crate / end-to-end tests live in `cairndb/tests/` (e.g.
  `sql_create_table.rs`).
- Use `Database::open_in_memory()` by default; reach for `tempfile` only
  when a test needs on-disk behavior (persistence, WAL, file handling) —
  Decision #14.

## Git conventions

- Branches: `feat/<slug>` or `fix/<slug>`.
- Commit subjects: short, imperative (e.g. "Add AGENTS.md with repo
  conventions").
- PRs merge to `main`.

## Where to record decisions

- New design decisions get a numbered entry appended to `decisions.md`.
- Milestone or scope changes go in `roadmap.md`.
- Don't duplicate roadmap or API reference content here — link to it.
