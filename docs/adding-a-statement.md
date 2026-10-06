# Adding a SQL statement

Every statement `Database::sql()` accepts goes through the same five layers.
Work through them in order; each step names the file, the sibling to copy, and
the test that proves the step.

## 1. Route it: `cairndb-parser/src/parse.rs`

`parse()` picks a parser by the first keyword:

- **Custom parser** for syntax `sqlparser-rs` can't handle (Decision #18):
  INSERT today (`insert.rs`), ERASE next (`erase.rs`, not yet written; #19).
  Add an `eq_ignore_ascii_case` branch *before* the temporal stripper.
- **Everything else** falls through to `temporal::strip_system_time()` and then
  `standard::parse_standard()`. UPDATE and DELETE need no routing change.

## 2. Parse it

**Via sqlparser** (`standard.rs`): add an arm to the `match stmt` in
`parse_standard()` and a `parse_<stmt>()` function modelled on
`parse_select()`: reject every unsupported clause explicitly with
`Error::Unsupported("<clause> is not supported")`, then build the IR. Reject a
`temporal` clause on anything but SELECT, the way the existing arms do. For a
`WHERE _id = '<id>'` filter, reuse `parse_id_filter()`.

**Custom** (`insert.rs`, future `erase.rs`): copy `insert.rs`'s shape. Reuse its
`Scanner` instead of writing a second tokenizer; it is private to `insert.rs`,
so make it `pub(crate)` or move it to a shared module first. Register the new
module in `lib.rs` (`mod <stmt>;`, private).

Unit tests go in the same file's `#[cfg(test)] mod tests`: one per accepted
form, one per rejected clause.

**sqlparser AST shapes**: the crate's source is the reference, and the version
is pinned in `cairndb-parser/Cargo.toml`. Find its directory with:

```sh
cargo metadata --format-version 1 | jq -r '.packages[] | select(.name=="sqlparser") | .manifest_path'
```

The AST lives under `src/ast/` next to that `Cargo.toml` (`mod.rs` for
`Statement`/`Expr`, `query.rs` for SELECT, `dml.rs` for INSERT/DELETE).

## 3. Emit the IR: `cairndb-parser/src/ir.rs`

`Statement` already has variants for every v0.1 statement (`CreateTable`,
`Insert`, `Select`, `Update`, `Delete`, `Erase`), so most slices add none.
Return the variant; never raw SQL (Decision #20). If a new variant is truly
needed, it is a design change: add a `docs/decisions.md` entry.

## 4. Dispatch it: `cairndb/src/sql.rs`

Add a match arm in `execute()` above the `_ =>` fallback, calling the matching
`cairndb-core` method (`db.update`, `db.delete`, `db.erase`, …; see
`cairndb-core/src/db.rs`). Return a `QueryResult`: `.into()` from a
`Document`, `QueryResult::default()` for statements that return nothing.

## 5. Prove it end to end: `cairndb/tests/sql_<stmt>.rs`

New file, one per statement, going through `Database::sql()` on
`Database::open_in_memory()`. Copy `sql_insert.rs`. Cover the happy path, the
effect on history/time travel where relevant, and each `Unsupported` rejection
as seen from the facade.

## Done when

- `cargo test --workspace`, `cargo clippy --workspace --all-targets -- -D warnings`
  and `cargo fmt --check` all pass.
- The statement's row in the Endb SQL Compatibility Matrix (`docs/roadmap.md`)
  is updated.
- The PR body says `Closes #<issue>`.
