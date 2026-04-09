# User Testing

## Validation Surface

This is a pure Rust library crate with no UI, CLI, or API server. The only validation surface is the test suite.

- **Surface:** `cargo test --package cairndb-core`
- **Tool:** Command-line execution (run cargo test, check exit code and output)
- **No browser, HTTP, or interactive testing required**

## Validation Concurrency

- **Max concurrent validators:** 5
- **Rationale:** `cargo test` is lightweight (in-memory SQLite, no I/O beyond temp files). Each test run uses ~200 MB RAM. On 16 GB / 8 cores with ~6 GB baseline usage, 5 concurrent runs = ~1 GB additional, well within budget.

## Testing Approach

All assertions are verified by running specific test patterns via `cargo test`. Tests are written inline in each module (`#[cfg(test)]`). Integration tests may be in the `tests/` directory or in the `db` module.

Validators should:
1. Run `cargo test --package cairndb-core` and verify exit code 0
2. Check that test names matching assertion patterns exist and pass
3. For specific assertions, run targeted tests: `cargo test --package cairndb-core <pattern>`
