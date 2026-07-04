# Plan 002: Add a CLAUDE.md so agents inherit repo conventions

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 29aac20..HEAD -- CLAUDE.md README.md roadmap.md decisions.md`
> If CLAUDE.md already exists, treat it as a STOP condition (reconcile, don't
> overwrite).

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none (001 recommended first so the commands section can point at CI)
- **Category**: dx
- **Planned at**: commit `29aac20`, 2026-07-01

## Why this matters

This repo is developed heavily with coding agents (see git history: scaffold
missions, scrutiny/validation runs, plans in `plans/`). There is no CLAUDE.md
or AGENTS.md, so every agent session re-derives the workspace layout, test
commands, invariants, and conventions — and risks violating non-obvious ones
(e.g. the parser crate must never depend on the core crate). A ~100-line
CLAUDE.md makes every future agent-driven task cheaper and safer.

## Current state

- Repo root contains `README.md` (public-facing usage), `spec.md` (vision),
  `decisions.md` (23 numbered design decisions), `roadmap.md` (milestones +
  Endb compatibility matrix). No `CLAUDE.md`, no `AGENTS.md`, no `.claude/`
  directory committed.
- Workspace: `cairndb` (facade, public API incl. `Database::sql()`),
  `cairndb-core` (storage engine), `cairndb-parser` (SQL → IR). Decision #3 in
  `decisions.md`: core and parser have **zero dependency on each other**; only
  the facade depends on both (Decision #22).
- Tests: inline `#[cfg(test)]` modules in `cairndb-core`/`cairndb-parser`
  sources; integration tests in `cairndb/tests/` (e.g. `sql_create_table.rs`).
  214 tests total.
- Key invariants (from source, verified at planning time):
  - Table names validated by `cairndb-core/src/schema.rs:21` `validate_table_name`
    (`^[a-zA-Z_][a-zA-Z0-9_]*$`); this validation is the ONLY thing making the
    `format!`-interpolated table names in `storage.rs`/`schema.rs` SQL-safe.
  - Timestamps are integer epoch-milliseconds internally; public API speaks
    ISO 8601 `YYYY-MM-DDTHH:MM:SS.mmmZ` (24 chars, UTC only) — Decision #7.
  - Physical schema: user table `t` → `_t_current` + `_t_history` + BEFORE
    triggers; system tables `_transactions`, `_schema_registry`,
    `_erasure_log`, `_cairn_tx_context` (Decision #9).
  - The parser emits a typed IR (`cairndb-parser/src/ir.rs` `Statement`), never
    raw SQL (Decision #20).

## Commands you will need

| Purpose | Command                  | Expected on success  |
|---------|--------------------------|----------------------|
| Tests   | `cargo test --workspace` | 214 passed, 0 failed |
| Format  | `cargo fmt --check`      | exit 0 (after plan 001) |
| Lint    | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |

## Scope

**In scope**:
- `CLAUDE.md` (create, repo root)

**Out of scope**:
- README.md, spec.md, decisions.md, roadmap.md — reference them, don't edit them.
- `.claude/settings.json` or hooks — configuration is the user's, not this plan's.

## Git workflow

- Branch: `feat/claude-md`
- Commit style: short imperative subject, e.g. `Add CLAUDE.md with repo conventions`
- Do NOT push or open a PR unless the operator instructed it.

## Steps

### Step 1: Write CLAUDE.md

Create `CLAUDE.md` at the repo root covering, in this order (keep it under
~120 lines; terse bullets, no marketing prose):

1. **What this is** — one paragraph: embedded temporal document database on
   SQLite; workspace of three crates; link to `spec.md`, `roadmap.md`,
   `decisions.md` for depth.
2. **Build / test / lint** — exact commands from the table above, plus
   "always run `cargo test --workspace` (not per-crate) before declaring done".
3. **Workspace layout & dependency rule** — the three crates, one line each,
   and the hard rule: `cairndb-core` and `cairndb-parser` must never depend on
   each other; only `cairndb` depends on both (decisions #3/#22).
4. **Invariants you must not break** — the four bullets from "Current state"
   above (table-name validation guards SQL interpolation; epoch-ms internal /
   ISO-8601 external timestamps; physical `_t_current`/`_t_history` + system
   table naming; parser emits IR, never SQL).
5. **Testing conventions** — unit tests inline in `#[cfg(test)]` modules next
   to the code; cross-crate/end-to-end tests in `cairndb/tests/`; in-memory DB
   (`Database::open_in_memory()`) by default, `tempfile` for on-disk tests
   (Decision #14).
6. **Git conventions** — branches `feat/<slug>` / `fix/<slug>`; short
   imperative commit subjects; PRs merge to `main`.
7. **Where to record decisions** — new design decisions get a numbered entry
   in `decisions.md`; milestone/scope changes go in `roadmap.md`.

Do not duplicate the roadmap or API reference — link to them.

**Verify**: `wc -l CLAUDE.md` → between 60 and 140 lines.
**Verify**: `grep -c "cargo test --workspace" CLAUDE.md` → at least 1.

### Step 2: Confirm nothing else changed

**Verify**: `git status --porcelain` → only `?? CLAUDE.md` (plus the plans
index edit) listed.

## Test plan

Not applicable (documentation only). The verification greps in Step 1 are the gate.

## Done criteria

- [ ] `CLAUDE.md` exists at repo root, 60–140 lines
- [ ] Contains: build/test commands, the crate-dependency rule, the four invariants, testing conventions, git conventions
- [ ] `cargo test --workspace` still passes (nothing else touched)
- [ ] `plans/README.md` status row updated

## STOP conditions

- `CLAUDE.md` or `AGENTS.md` already exists.
- You find a contradiction between `decisions.md` and the code while writing
  (document what you found in your report instead of guessing which is right).

## Maintenance notes

- When v0.1b ships (SQL dispatch beyond CREATE TABLE), update the "What this
  is" status line.
- Reviewer should check the invariants section against `decisions.md` — that
  section is the highest-value, highest-risk-of-wrong content in the file.
