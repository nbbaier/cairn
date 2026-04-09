# Environment

**What belongs here:** Required env vars, external dependencies, toolchain notes.
**What does NOT belong here:** Service ports/commands (use `.factory/services.yaml`).

---

## Toolchain

- Rust 1.94.1 (stable), Cargo 1.94.1
- Clippy available at `/opt/homebrew/bin/cargo-clippy`
- macOS (darwin 24.6.0), 16 GB RAM, 8 CPU cores

## Dependencies (cairndb-core)

- `rusqlite 0.34` with `bundled` feature — bundles SQLite 3.49.1
  - JSONB support (3.45.0+): `jsonb()`, `json()`, `json_extract()`, `json_patch()`
  - All JSON functions available by default with bundled feature
- `thiserror 2` — Error derive macros
- `serde 1` with `derive` — Serialize/Deserialize for Document
- `serde_json 1` — JSON types (Value, Map)
- `uuid 1` with `v7` feature — UUIDv7 generation via `Uuid::now_v7()`
- `tempfile 3` (dev-dependency) — temporary directories for file-based tests

## External Dependencies

None. This is a pure embedded library with no network, server, or external service dependencies.
