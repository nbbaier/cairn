# Coding standards

Read before writing or reviewing code. The hard invariants are blocking: a
change that violates one is wrong regardless of anything else. The rules after
them are judgement calls. Fmt, clippy (`-D warnings`), and tests are enforced
by CI (`.github/workflows/ci.yml`); run them locally before pushing, and don't
spend review on them.

## Hard invariants

- **Crate dependencies.** `cairndb-core` and `cairndb-parser` never depend on
  each other; only `cairndb` depends on both (Decisions #3 and #22 in
  `docs/decisions.md`).
- **Table-name validation.** Table names are validated by
  `validate_table_name` in `cairndb-core/src/schema.rs`
  (`^[a-zA-Z_][a-zA-Z0-9_]*$`). This validation is the *only* thing making the
  `format!`-interpolated table names in `storage.rs`/`schema.rs` SQL-safe, so
  never bypass or weaken it. Every path that turns user text into a table name
  (new parser, quoting, aliasing) must end in `validate_table_name`, applied
  *after* unquoting or normalising (Decision #25); check every entry point a
  change adds, not just the first.
- **Timestamps.** Internally, timestamps are integer epoch-milliseconds; the
  public API speaks ISO 8601 `YYYY-MM-DDTHH:MM:SS.mmmZ` (24 chars, UTC only),
  per Decision #7. Conversion happens only in the Rust layer.
- **Physical schema.** cairndb owns the physical schema entirely: user table
  `t` becomes `_t_current` + `_t_history` plus BEFORE triggers (see
  `docs/spec.md`, Storage Model section); system tables are `_transactions`,
  `_schema_registry`, `_erasure_log`, and `_cairn_tx_context` (the latter per
  Decision #9).
- **Typed IR.** The parser emits a typed IR
  (`cairndb-parser/src/ir.rs::Statement`), never raw SQL (Decision #20).

## Rules

- **No silent data loss.** Input that would be overwritten or dropped
  (duplicate keys or columns, extra statements, trailing tokens) is a parse
  error, never last-write-wins. If the diff accepts it, the PR needs a decision
  entry saying why.
- **Unsupported, not wrong.** Syntax outside the current milestone
  (`docs/roadmap.md`) is rejected with the crate's `Unsupported`-style error and
  a message naming the construct. Treat a fallback that partly executes it as a
  bug.
- **No panics on reachable paths.** Non-test code returns the crate's `Error`
  rather than `unwrap`/`expect`/`unreachable!`. The exception is an invariant
  the line itself proves, with a `// safe:` comment saying why (see
  `schema.rs` `validate_table_name`). Corrupt stored data is
  `Error::CorruptedData`, not a panic.
- **Errors stay per crate.** Each crate has its own `thiserror` enum and
  `Result<T>` alias in `error.rs`; the facade converts at the boundary. Flag
  new error types that bypass this, or `String` errors.
- **Matching tests for each layer.** Unit tests live inline in
  `#[cfg(test)]` modules next to the code they test; cross-crate and
  end-to-end tests live in `cairndb/tests/`. A new SQL statement needs parser
  unit tests in the parser module *and* an end-to-end test in
  `cairndb/tests/sql_<stmt>.rs` that goes through `Database::sql()`. An
  end-to-end test alone hides which layer broke.
- **In-memory databases in tests.** Use `Database::open_in_memory()` by
  default; use `tempfile` only when a test needs on-disk behaviour
  (persistence, WAL, file handling), per Decision #14.
- **Follow the pattern already there.** A new parser or dispatch arm should
  look like its siblings (`insert.rs`, `temporal.rs`, the arms in
  `cairndb/src/sql.rs`). Copied-and-diverged helpers (two scanners, two
  quote handlers) are a finding: name the existing one to reuse.
- **Docs move with behaviour.** A change to what `db.sql()` accepts updates
  the roadmap's Endb compatibility matrix if a row changes. Status prose
  ("next milestone", test counts) does not belong in docs; it lives in the
  issues.

## Review

### Before flagging scope creep

Read the PR's existing review threads, resolved ones included
(`gh api repos/nbbaier/cairn/pulls/<n>/comments`), and the decision log. Behaviour
that looks unrequested is often a fix for an earlier thread. PR #27's quoted
identifiers and duplicate-column rejection were both that (Decision #25).
Flag it only if no thread or decision explains it.

### Spec source

The spec for a change is the GitHub issue its PR closes (`Closes #N`), plus
the numbered entries in `docs/decisions.md` it touches. Any behaviour that
contradicts a decision needs a new or amended decision entry in the same PR.
