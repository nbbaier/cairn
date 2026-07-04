# Plan 001: Establish a CI verification baseline (GitHub Actions: fmt, clippy, test)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 29aac20..HEAD -- .github cairndb/src/lib.rs`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: dx
- **Planned at**: commit `29aac20`, 2026-07-01

## Why this matters

The repo has no CI at all — there is no `.github/` directory. All verification
(214 tests, clippy, formatting) is manual, so nothing guards `main` against a
commit that breaks the build, fails tests, or drifts formatting. Every other
plan in `plans/` relies on `cargo test --workspace` as its verification gate;
this plan makes that gate automatic and is therefore the prerequisite for the
riskier changes. It also fixes the one existing formatting drift so the fmt
check starts green.

## Current state

- No `.github/` directory exists in the repo root.
- `cargo test --workspace` passes: 214 tests (183 in `cairndb-core`, 21 in
  `cairndb-parser` + facade units, 10 integration in
  `cairndb/tests/sql_create_table.rs`).
- `cargo clippy --workspace --all-targets` produces zero warnings.
- `cargo fmt --check` currently **fails** with exactly one diff, in
  `cairndb/src/lib.rs` around lines 64–71 (the `query_between` signature is
  wrapped across lines where rustfmt wants it on one line):

```rust
// cairndb/src/lib.rs:64-71 (current, unformatted per rustfmt)
    pub fn query_between(
        &self,
        table: &str,
        from_iso: &str,
        to_iso: &str,
    ) -> Result<QueryResult> {
        Ok(self.inner.query_between(table, from_iso, to_iso)?)
    }
```

- This is a Cargo workspace (`Cargo.toml` at root, members: `cairndb`,
  `cairndb-core`, `cairndb-parser`, resolver = "2"). No rustfmt.toml or
  clippy.toml exist — defaults apply, which is intended.
- `Cargo.lock` is committed (correct for a workspace producing a usable
  library + future binary; keep it committed).

## Commands you will need

| Purpose    | Command                                                    | Expected on success |
|------------|------------------------------------------------------------|---------------------|
| Format fix | `cargo fmt`                                                | exit 0              |
| Format chk | `cargo fmt --check`                                        | exit 0, no output   |
| Lint       | `cargo clippy --workspace --all-targets -- -D warnings`    | exit 0              |
| Tests      | `cargo test --workspace`                                   | 214 passed, 0 failed|

## Scope

**In scope** (the only files you should modify/create):
- `.github/workflows/ci.yml` (create)
- `cairndb/src/lib.rs` (formatting only, via `cargo fmt` — no logic changes)

**Out of scope** (do NOT touch):
- Any other source file. If `cargo fmt` changes files besides
  `cairndb/src/lib.rs`, that is fine (it is mechanical), but inspect the diff
  and confirm it is whitespace/wrapping only.
- Adding `cargo audit`, code coverage, release workflows, or caching beyond
  what's specified — keep the first CI minimal.
- rustfmt.toml / clippy.toml — do not add config files; defaults are the
  convention here.

## Git workflow

- Branch: `feat/ci-baseline` (repo uses `feat/<slug>` — see `git log`:
  `feat/parser-scaffold-create-table`, `feat/cairndb-core`).
- Commit style: short imperative subject, e.g. `Add GitHub Actions CI workflow`
  (matches `Add parser scaffold and CREATE TABLE end-to-end`).
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Fix the existing formatting drift

Run `cargo fmt` at the repo root. Inspect `git diff` — expect only
whitespace/line-wrapping changes (the known one is `cairndb/src/lib.rs`
`query_between`). If any diff changes tokens other than whitespace/newlines,
STOP.

**Verify**: `cargo fmt --check` → exit 0, no output.
**Verify**: `cargo test --workspace` → 214 passed, 0 failed.

### Step 2: Create the CI workflow

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - name: Format
        run: cargo fmt --check
      - name: Clippy
        run: cargo clippy --workspace --all-targets -- -D warnings
      - name: Test
        run: cargo test --workspace
```

**Verify**: `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace` → all three exit 0 (this is exactly what CI will run).

### Step 3 (only if `act` or pushing is available): sanity-check the workflow file

If you cannot execute GitHub Actions locally, validate YAML syntax instead:

**Verify**: `python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"` → exit 0.

## Test plan

No new Rust tests. The deliverable *is* the test harness. Local verification
of the exact CI command sequence (Step 2's verify line) stands in for a CI run.

## Done criteria

- [ ] `.github/workflows/ci.yml` exists and is valid YAML
- [ ] `cargo fmt --check` exits 0
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` exits 0
- [ ] `cargo test --workspace` exits 0 with 214 passed
- [ ] `git status` shows no modified files outside the in-scope list (fmt-only whitespace changes elsewhere are acceptable if verified whitespace-only)
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:

- `cargo fmt` produces a diff that changes anything other than
  whitespace/line-wrapping.
- `cargo clippy -- -D warnings` fails (it passed at planning time — a failure
  means the codebase drifted; do not "fix" lint findings inside this plan).
- Any test fails after Step 1.

## Maintenance notes

- When plan 005 (SQL SELECT dispatch) or other feature work lands, CI will run
  the growing suite automatically; if suite time grows past ~5 minutes,
  revisit caching/parallelization then, not now.
- A reviewer should check that `-D warnings` is present on the clippy line —
  without it the lint step is decorative.
- `cargo audit` was deliberately deferred (needs a maintained action or binary
  install; low value while there are only 6 direct deps).
