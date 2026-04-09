# Schema Module — Design Decisions

## `_cairn_tx_context` is a Regular Table, Not a TEMP Table

**Why this matters for the `versioning-transactions` feature worker.**

### SQLite Restriction

SQLite prohibits triggers on `main`-schema tables from referencing objects in the
`temp` schema. The error message is:

```
trigger _T_before_update cannot reference objects in database temp
```

This means the `_cairn_tx_context` table, which the BEFORE UPDATE/DELETE triggers
read to get `(txn_id, timestamp)`, **must live in the `main` schema**, not as a
`TEMP TABLE`.

### What Was Implemented

`init_system_tables()` creates `_cairn_tx_context` as a permanent table in `main`:

```sql
CREATE TABLE IF NOT EXISTS _cairn_tx_context (
    txn_id    INTEGER NOT NULL,
    timestamp INTEGER NOT NULL
);
```

The triggers reference it without any schema prefix:
```sql
(SELECT timestamp FROM _cairn_tx_context)
(SELECT txn_id   FROM _cairn_tx_context)
```

### What the Versioning Worker Must Do

The `versioning-transactions` feature description says "creates TEMP TABLE
`_cairn_tx_context`". **Do NOT do this.** The table is already created by
`init_system_tables`. Instead:

**`begin_write(conn)`** should:
```sql
BEGIN;
DELETE FROM _cairn_tx_context;           -- clear any stale context
INSERT INTO _transactions (timestamp) VALUES (?);
INSERT INTO _cairn_tx_context VALUES (last_insert_rowid(), ?);
```

**`commit(conn)`** should:
```sql
COMMIT;
DELETE FROM _cairn_tx_context;           -- clean up after commit
```

**`rollback(conn)`** should:
```sql
ROLLBACK;                                -- transaction rollback undoes the INSERT
```

After ROLLBACK, `_cairn_tx_context` will be empty because the INSERT was rolled
back as part of the SQL transaction.

### Safety

Since `Database` serializes all access through a `Mutex<Connection>`, only one
writer is active at a time. The single-row `_cairn_tx_context` is safe; no
concurrent writers can corrupt its state.

### Validation Implication for VAL-TXN-004

VAL-TXN-004 says "`_cairn_tx_context` exists during write transaction and is
cleaned up after commit." With a regular table:

- **"Exists during"**: `SELECT COUNT(*) FROM _cairn_tx_context` returns 1 while
  inside `begin_write → commit/rollback` window.
- **"Cleaned up after"**: `SELECT COUNT(*) FROM _cairn_tx_context` returns 0 after
  commit/rollback.

The test should check row count, not table existence.
