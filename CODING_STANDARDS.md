# Coding standards

Read during review, not implementation. `AGENTS.md` holds the hard invariants
(crate dependency rule, table-name validation, timestamp format, typed IR);
treat a violation of any of them as blocking. This file covers the judgement
calls those invariants don't settle. Fmt, clippy (`-D warnings`), and tests
are enforced by CI (`.github/workflows/ci.yml`), so don't spend review on them.

## Before flagging scope creep

Read the PR's existing review threads, resolved ones included
(`gh api repos/nbbaier/cairn/pulls/<n>/comments`), and the decision log. Behaviour
that looks unrequested is often a fix for an earlier thread. PR #27's quoted
identifiers and duplicate-column rejection were both that (Decision #25).
Flag it only if no thread or decision explains it.

## Spec source

The spec for a change is the GitHub issue its PR closes (`Closes #N`), plus
the numbered entries in `docs/decisions.md` it touches. Any behaviour that
contradicts a decision needs a new or amended decision entry in the same PR.

## Rules

- **Identifier boundary.** Any new path that turns user text into a table name
  (new parser, quoting, aliasing) must end in `validate_table_name`, applied
  *after* unquoting or normalising (Decision #25). Check every entry point the
  diff adds, not just the first.
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
- **Matching tests for each layer.** A new SQL statement needs parser unit tests
  in the parser module *and* an end-to-end test in `cairndb/tests/sql_<stmt>.rs`
  that goes through `Database::sql()`. An end-to-end test alone hides which
  layer broke.
- **Follow the pattern already there.** A new parser or dispatch arm should
  look like its siblings (`insert.rs`, `temporal.rs`, the arms in
  `cairndb/src/sql.rs`). Copied-and-diverged helpers (two scanners, two
  quote handlers) are a finding: name the existing one to reuse.
- **Docs move with behaviour.** A change to what `db.sql()` accepts updates
  the roadmap's Endb compatibility matrix if a row changes. Status prose
  ("next milestone", test counts) does not belong in docs; it lives in the
  issues.
