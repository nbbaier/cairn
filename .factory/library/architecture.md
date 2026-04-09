# Architecture — cairndb-core v0.1

## Overview

cairndb-core is the storage layer for an embedded temporal document database. It stores JSON documents as versioned JSONB in SQLite, automatically captures history on every mutation via triggers, and exposes time-travel queries through typed Rust methods.

## Crate Structure

```
cairndb (public facade) ──depends on──> cairndb-core (all logic)
cairndb-parser (empty for v0.1)
```

All implementation lives in `cairndb-core`. The `cairndb` crate re-exports its public API.

## Module Architecture

```
db.rs (Database) ── public facade, Mutex<Connection>, delegates to internal modules
  ├── schema.rs    ── DDL generation, triggers, indexes, table cache [pub(crate)]
  ├── versioning.rs ── transaction lifecycle, _cairn_tx_context context table [pub(crate)]
  ├── storage.rs   ── CRUD operations + query execution against physical tables [pub(crate)]
  ├── document.rs  ── Document/QueryResult types, JSON conversion [pub]
  └── error.rs     ── Error enum [pub]
```

**Public API surface:** `Database`, `Document`, `QueryResult`, `Error`, `Result`
**Internal modules:** `schema`, `versioning`, `storage` — all `pub(crate)`

## Data Flow

### Write Path (insert/update/delete)

1. `Database` method acquires `Mutex<Connection>` lock
2. `versioning::begin_write()` — clears and populates `_cairn_tx_context`, inserts into `_transactions`, returns (txn_id, timestamp)
3. `schema::ensure_table()` — creates physical tables if not exists (idempotent)
4. `storage::insert/update/delete()` — executes SQL against `_T_current`
5. SQLite BEFORE triggers fire — copy old row to `_T_history` (on UPDATE/DELETE)
6. `versioning::commit()` — commits SQL transaction, then clears `_cairn_tx_context`
7. Returns `Document` to caller

### Read Path (query/get)

1. `Database` method acquires `Mutex<Connection>` lock
2. `storage::query*()` — builds and executes SELECT SQL
3. Rows mapped to `Document` objects via `document::Document::from_row()`
4. Returns `QueryResult` wrapper

### Erase Path

1. Same as write path for transaction setup
2. `storage::erase()` — DELETE from both `_T_current` and `_T_history`, INSERT into `_erasure_log`
3. Commit transaction

## Physical Schema

Each logical table `T` maps to two physical SQLite tables:

**`_T_current`** (latest version of each document):

- `_id TEXT PRIMARY KEY` — UUIDv7, hyphenated
- `_data JSONB NOT NULL` — document content
- `_valid_from INTEGER NOT NULL` — epoch milliseconds
- `_txn_id INTEGER NOT NULL` — transaction ID

**`_T_history`** (all prior versions, append-only):

- `_id TEXT NOT NULL`
- `_data JSONB NOT NULL`
- `_valid_from INTEGER NOT NULL`
- `_valid_to INTEGER NOT NULL` — when superseded
- `_txn_id INTEGER NOT NULL`
- `_op TEXT NOT NULL` — 'UPDATE' or 'DELETE'
- Composite index: `(_id, _valid_from, _valid_to)`

**System tables** (created on DB init):

- `_transactions` — txn_id (AUTOINCREMENT), timestamp, metadata (JSONB)
- `_schema_registry` — table, key_path, inferred_type, first_seen, last_seen (DDL only in v0.1)
- `_erasure_log` — table, id, erased_at

## Versioning Mechanism

SQLite BEFORE triggers on `_T_current`:

- **BEFORE UPDATE**: copies old row to `_T_history` with `_op='UPDATE'`, `_valid_to` from `_cairn_tx_context`
- **BEFORE DELETE**: copies old row to `_T_history` with `_op='DELETE'`, `_valid_to` from `_cairn_tx_context`

Transaction context passed via a **regular system table** `_cairn_tx_context` (NOT a temp table — SQLite prohibits main-schema triggers from referencing temp tables):

```sql
CREATE TABLE IF NOT EXISTS _cairn_tx_context (txn_id INTEGER NOT NULL, timestamp INTEGER NOT NULL);
```

Protocol: `begin_write()` clears then populates the row. `commit()` clears after commit. `rollback()` — the SQL ROLLBACK undoes the INSERT automatically. Safe because Mutex serializes all access. See `.factory/library/schema-decisions.md` for full details.

## Concurrency Model

Single `rusqlite::Connection` behind `std::sync::Mutex`. `Database` is `Send + Sync`. One writer at a time; readers blocked during writes (Mutex, not RWLock). WAL mode enables concurrent readers at the SQLite level but the Mutex serializes all access from Rust.

## Key Design Decisions

- **Timestamps:** INTEGER milliseconds internally; ISO 8601 strings at public API boundary
- **UUIDv7:** `uuid::Uuid::now_v7()`, stored as TEXT in hyphenated format, time-sortable
- **Updates:** SQLite `json_patch()` (RFC 7396 Merge Patch) — setting key to null removes it
- **Table names:** Validated against `^[a-zA-Z_][a-zA-Z0-9_]*$` to prevent SQL injection (names are interpolated into DDL, not parameterizable)
- **Schema-last:** Tables auto-created on first insert. Explicit `create_table` also supported, idempotent.
- **Erase:** Idempotent — erasing non-existent doc returns Ok (GDPR compliance)
