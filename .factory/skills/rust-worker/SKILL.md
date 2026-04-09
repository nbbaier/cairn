---
name: rust-worker
description: Implements Rust library modules using TDD (test-first) for cairndb-core
---

# Rust Worker

NOTE: Startup and cleanup are handled by `worker-base`. This skill defines the WORK PROCEDURE.

## When to Use This Skill

Use for any feature that implements Rust library code in cairndb-core: new modules, expanding existing modules, adding tests, fixing bugs.

## Required Skills

None.

## Work Procedure

1. **Read context.** Read `.factory/library/architecture.md` and the feature description. Understand what module(s) you're building, their dependencies, and expected behavior.

2. **Add dependencies if needed.** If the feature requires new crates (serde, serde_json, uuid), add them to `cairndb-core/Cargo.toml`. Run `cargo check` to verify they resolve.

3. **Create module file(s).** Create the `.rs` file(s) in `cairndb-core/src/`. Add `mod` declarations to `lib.rs`. For public types, add `pub use` re-exports.

4. **Write failing tests first (RED).** In the module's `#[cfg(test)] mod tests {}`, write tests that cover:
   - Happy path for each public function
   - Error cases (invalid input, not-found, etc.)
   - Edge cases from the feature's `expectedBehavior`
   
   Run `cargo test --package cairndb-core` — tests should FAIL (compile errors or assertion failures).

5. **Implement to make tests pass (GREEN).** Write the minimum implementation to make all tests pass. Follow the architectural patterns in AGENTS.md.

6. **Refactor.** Clean up code while keeping tests green. Add doc comments to public items.

7. **Run full verification:**
   - `cargo test --workspace` — all tests pass (including existing ones)
   - `cargo clippy --all-targets` — no warnings
   - `cargo check --workspace` — clean compilation

8. **Verify feature's expectedBehavior.** Go through each item in the feature's `expectedBehavior` list and confirm it's covered by a passing test. If any behavior lacks a test, add one.

9. **Update lib.rs exports.** Ensure `cairndb-core/src/lib.rs` re-exports all new public types. Ensure `cairndb/src/lib.rs` re-exports match (it does `pub use cairndb_core::*`).

## Example Handoff

```json
{
  "salientSummary": "Implemented the document module with Document struct (id/system_time/data/get/txn_id accessors) and QueryResult wrapper with IntoIterator. Added serde and serde_json deps. Ran `cargo test --workspace` (12 passing) and `cargo clippy` (clean).",
  "whatWasImplemented": "cairndb-core/src/document.rs: Document struct wrapping id (String), data (serde_json::Map), valid_from (i64), txn_id (i64). Public accessors: id(), system_time() (converts epoch ms to ISO 8601), data(), get(key), txn_id(). Serialize/Deserialize derives. QueryResult struct with len(), is_empty(), documents(), into_documents(), IntoIterator impl. Updated lib.rs with pub mod document and re-exports.",
  "whatWasLeftUndone": "",
  "verification": {
    "commandsRun": [
      { "command": "cargo test --workspace", "exitCode": 0, "observation": "12 tests passed, 0 failed" },
      { "command": "cargo clippy --all-targets", "exitCode": 0, "observation": "No warnings" }
    ],
    "interactiveChecks": []
  },
  "tests": {
    "added": [
      {
        "file": "cairndb-core/src/document.rs",
        "cases": [
          { "name": "document_id_format", "verifies": "id() returns valid UUIDv7 string" },
          { "name": "document_system_time_iso8601", "verifies": "system_time() returns ISO 8601 string" },
          { "name": "document_data_access", "verifies": "data() returns user JSON map" },
          { "name": "document_get_field", "verifies": "get() returns Some for existing, None for missing" },
          { "name": "document_serde_roundtrip", "verifies": "Serialize then Deserialize preserves all fields" },
          { "name": "query_result_accessors", "verifies": "len(), is_empty(), documents() work correctly" },
          { "name": "query_result_iteration", "verifies": "IntoIterator yields all documents" },
          { "name": "query_result_empty", "verifies": "empty result: is_empty()==true, len()==0" }
        ]
      }
    ]
  },
  "discoveredIssues": []
}
```

## When to Return to Orchestrator

- Feature depends on a module/type that doesn't exist yet and isn't part of this feature
- Cargo.toml dependency resolution fails
- Existing tests break and the cause is outside this feature's scope
- Architectural ambiguity: the feature description contradicts AGENTS.md or architecture.md
