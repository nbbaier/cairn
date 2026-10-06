# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **[`GLOSSARY.md`](../../GLOSSARY.md)**: canonical domain terms for this single context.
- **[`docs/decisions.md`](../decisions.md)**: this repo's ADR log, one numbered `## N. Title` entry per decision. Read the entries that touch the area you're about to work in (`rg -n '^## ' docs/decisions.md` lists them). New decisions are appended there; this repo has no `docs/adr/` directory.

## File structure

Single-context repo (this repo):

```
/
├── GLOSSARY.md          (domain vocabulary)
└── docs/decisions.md    (ADR log)
```

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `GLOSSARY.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Maintain the model

When a term is resolved, capture it in `GLOSSARY.md` with a tight definition and
an `_Avoid_` list for misleading synonyms. Keep implementation details, feature
status, and design rationale in the spec, roadmap, or decision log rather than
the glossary.

Check definitions against code and concrete scenarios, especially version
boundaries, deletion versus erasure, and document data versus system metadata.
If code and the intended model disagree, report both and consult the relevant
decision; do not silently redefine a term to make the discrepancy disappear.
A glossary definition does not establish that a feature is implemented.

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts Decision #16 (v0.1 parser statement set), but worth reopening because…_
