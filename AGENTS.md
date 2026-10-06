# AGENTS.md

cairndb is an embedded temporal document database built on vanilla SQLite:
`cairndb` (public facade) over `cairndb-core` (storage engine) and
`cairndb-parser` (SQL → IR).

Before declaring done, run `cargo test --workspace` (not per-crate):
cross-crate behavior lives in `cairndb/tests/`.

## Read when relevant

- Writing or reviewing code: read `CODING_STANDARDS.md` first; its hard
  invariants are blocking.
- Adding or extending a SQL statement: follow `docs/adding-a-statement.md`.
- Branching, committing, or recording a decision or scope change: read `docs/agents/workflow.md`.
- Naming or reasoning about domain concepts: read `GLOSSARY.md`; for glossary changes or model/code/decision conflicts, follow `docs/agents/domain.md`.
- Product vision, milestones, or Endb compatibility: read `docs/spec.md` and `docs/roadmap.md`.
- Working with issues (GitHub via `gh`): read `docs/agents/issue-tracker.md`.
- Applying triage labels: read `docs/agents/triage-labels.md`.
