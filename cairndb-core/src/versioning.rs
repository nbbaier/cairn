//! Transaction lifecycle management for cairndb write operations.
//!
//! This module manages SQL transaction boundaries and the `_cairn_tx_context`
//! system table which provides per-row versioning metadata to BEFORE UPDATE/DELETE
//! triggers on user tables.
//!
//! ## Protocol
//!
//! Every write operation follows this sequence:
//!
//! ```text
//! let (txn_id, ts) = versioning::begin_write(&conn)?;
//! // ... perform DML (INSERT/UPDATE/DELETE on user tables) ...
//! versioning::commit(&conn)?;          // or versioning::rollback(&conn)?;
//! ```
//!
//! ## Why `_cairn_tx_context` is a regular table
//!
//! SQLite prohibits triggers on `main`-schema tables from referencing objects in
//! the `temp` schema. Since the BEFORE UPDATE/DELETE triggers need to read
//! `(txn_id, timestamp)`, `_cairn_tx_context` lives in `main` and is managed as
//! a single-row table. Because `Database` serialises all access through a
//! `Mutex<Connection>`, there is at most one active writer, so a single-row table
//! is safe and correct.

use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Public(crate) API
// ---------------------------------------------------------------------------

/// Starts a write transaction and records transaction metadata.
///
/// Sequence:
/// 1. `BEGIN` — opens a SQL transaction.
/// 2. `DELETE FROM _cairn_tx_context` — clears any stale context row.
/// 3. `INSERT INTO _transactions (timestamp) VALUES (?)` — registers the txn.
/// 4. `INSERT INTO _cairn_tx_context VALUES (last_insert_rowid(), ?)` — makes
///    the context available to BEFORE UPDATE/DELETE triggers.
///
/// Returns `(txn_id, timestamp_ms)` where `txn_id` is the AUTOINCREMENT value
/// from `_transactions` and `timestamp_ms` is the current wall-clock time in
/// epoch milliseconds.
#[allow(dead_code)] // used by storage module (next milestone)
pub(crate) fn begin_write(conn: &Connection) -> Result<(i64, i64)> {
    let ts = now_epoch_ms()?;
    conn.execute_batch("BEGIN")?;
    conn.execute("DELETE FROM _cairn_tx_context", [])?;
    conn.execute(
        "INSERT INTO _transactions (timestamp) VALUES (?1)",
        rusqlite::params![ts],
    )?;
    let txn_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO _cairn_tx_context (txn_id, timestamp) VALUES (?1, ?2)",
        rusqlite::params![txn_id, ts],
    )?;
    Ok((txn_id, ts))
}

/// Commits the current write transaction and clears the transaction context.
///
/// Sequence:
/// 1. `COMMIT` — persists all DML performed since [`begin_write`].
/// 2. `DELETE FROM _cairn_tx_context` — removes the context row now that
///    no triggers need it.
#[allow(dead_code)] // used by storage module (next milestone)
pub(crate) fn commit(conn: &Connection) -> Result<()> {
    conn.execute_batch("COMMIT")?;
    conn.execute("DELETE FROM _cairn_tx_context", [])?;
    Ok(())
}

/// Rolls back the current write transaction.
///
/// `ROLLBACK` undoes all DML since [`begin_write`], including the
/// `INSERT INTO _transactions` and `INSERT INTO _cairn_tx_context` rows, so
/// no manual cleanup of `_cairn_tx_context` is required — it will be empty
/// after the rollback completes (restored to the pre-`BEGIN` state).
#[allow(dead_code)] // used by storage module (next milestone)
pub(crate) fn rollback(conn: &Connection) -> Result<()> {
    conn.execute_batch("ROLLBACK")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Returns the current wall-clock time as epoch milliseconds.
fn now_epoch_ms() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .map_err(|e| Error::InvalidTimestamp(e.to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rusqlite::Connection;

    use crate::schema::init_system_tables;

    use super::*;

    /// Opens an in-memory connection with system tables initialised.
    fn open_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_system_tables(&conn).unwrap();
        conn
    }

    /// Returns the number of rows in `_transactions`.
    fn txn_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM _transactions", [], |r| r.get(0))
            .unwrap()
    }

    /// Returns the number of rows in `_cairn_tx_context`.
    fn ctx_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM _cairn_tx_context", [], |r| r.get(0))
            .unwrap()
    }

    // ------------------------------------------------------------------
    // VAL-TXN-002: Timestamps are millisecond-precision epoch integers
    // ------------------------------------------------------------------

    #[test]
    fn timestamp_in_reasonable_range() {
        // post-2020-01-01 in epoch ms: 1_577_836_800_000
        // pre-2100-01-01  in epoch ms: 4_102_444_800_000
        let ts = now_epoch_ms().unwrap();
        assert!(
            ts > 1_577_836_800_000,
            "timestamp {ts} is before 2020-01-01"
        );
        assert!(
            ts < 4_102_444_800_000,
            "timestamp {ts} is after 2100-01-01"
        );
    }

    // ------------------------------------------------------------------
    // VAL-TXN-003: Transaction metadata recorded after commit
    // ------------------------------------------------------------------

    #[test]
    fn commit_records_transaction_in_transactions_table() {
        let conn = open_conn();
        assert_eq!(txn_count(&conn), 0);

        let (txn_id, _ts) = begin_write(&conn).unwrap();
        commit(&conn).unwrap();

        assert_eq!(txn_count(&conn), 1, "_transactions should have 1 row after commit");
        let (stored_id, stored_ts): (i64, i64) = conn
            .query_row(
                "SELECT txn_id, timestamp FROM _transactions WHERE txn_id = ?1",
                rusqlite::params![txn_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored_id, txn_id);
        assert!(stored_ts > 0, "timestamp must be positive");
    }

    // ------------------------------------------------------------------
    // VAL-TXN-004: _cairn_tx_context has a row during write, cleaned after commit
    // ------------------------------------------------------------------

    #[test]
    fn context_has_row_during_transaction_and_is_cleared_after_commit() {
        let conn = open_conn();

        begin_write(&conn).unwrap();
        // Inside the transaction: context row must exist
        assert_eq!(ctx_count(&conn), 1, "_cairn_tx_context should have 1 row during transaction");

        commit(&conn).unwrap();
        // After commit: context row must be gone
        assert_eq!(ctx_count(&conn), 0, "_cairn_tx_context should be empty after commit");
    }

    // ------------------------------------------------------------------
    // VAL-TXN-005: Rollback cleans up — no _transactions entry, context empty
    // ------------------------------------------------------------------

    #[test]
    fn rollback_leaves_no_transactions_entry_and_clears_context() {
        let conn = open_conn();
        assert_eq!(txn_count(&conn), 0);

        begin_write(&conn).unwrap();
        assert_eq!(ctx_count(&conn), 1, "context must exist during transaction");

        rollback(&conn).unwrap();

        assert_eq!(txn_count(&conn), 0, "_transactions should be empty after rollback");
        assert_eq!(ctx_count(&conn), 0, "_cairn_tx_context should be empty after rollback");
    }

    // ------------------------------------------------------------------
    // VAL-TXN-001: Transaction IDs are strictly monotonically increasing
    // ------------------------------------------------------------------

    #[test]
    fn txn_ids_are_monotonically_increasing() {
        let conn = open_conn();
        let mut prev_id: i64 = 0;

        for _ in 0..5 {
            let (txn_id, _) = begin_write(&conn).unwrap();
            commit(&conn).unwrap();
            assert!(
                txn_id > prev_id,
                "txn_id {txn_id} is not greater than previous {prev_id}"
            );
            prev_id = txn_id;
        }
    }

    // ------------------------------------------------------------------
    // VAL-TXN-006: N write operations produce N distinct transaction IDs
    // ------------------------------------------------------------------

    #[test]
    fn distinct_ids_across_transactions() {
        let conn = open_conn();
        let n = 10;
        let mut ids: HashSet<i64> = HashSet::new();

        for _ in 0..n {
            let (txn_id, _) = begin_write(&conn).unwrap();
            commit(&conn).unwrap();
            ids.insert(txn_id);
        }

        assert_eq!(
            ids.len(),
            n,
            "Expected {n} distinct txn_ids, got {}",
            ids.len()
        );
    }

    // ------------------------------------------------------------------
    // VAL-TXN-002: returned timestamp is within reasonable range
    // ------------------------------------------------------------------

    #[test]
    fn begin_write_returns_reasonable_timestamp() {
        let conn = open_conn();
        let (_txn_id, ts) = begin_write(&conn).unwrap();
        commit(&conn).unwrap();

        assert!(
            ts > 1_577_836_800_000,
            "timestamp {ts} before 2020-01-01"
        );
        assert!(
            ts < 4_102_444_800_000,
            "timestamp {ts} after 2100-01-01"
        );
    }

    // ------------------------------------------------------------------
    // begin_write returns correct txn_id matching _transactions
    // ------------------------------------------------------------------

    #[test]
    fn begin_write_returns_matching_txn_id() {
        let conn = open_conn();
        let (txn_id, ts) = begin_write(&conn).unwrap();
        commit(&conn).unwrap();

        let (stored_id, stored_ts): (i64, i64) = conn
            .query_row(
                "SELECT txn_id, timestamp FROM _transactions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored_id, txn_id, "returned txn_id must match _transactions row");
        assert_eq!(stored_ts, ts, "returned timestamp must match _transactions row");
    }

    // ------------------------------------------------------------------
    // Multiple sequential transactions: all persist correctly
    // ------------------------------------------------------------------

    #[test]
    fn sequential_transactions_all_persist() {
        let conn = open_conn();

        for i in 0..3_i64 {
            let (txn_id, _) = begin_write(&conn).unwrap();
            commit(&conn).unwrap();
            assert_eq!(txn_id, i + 1, "expected txn_id to be {}", i + 1);
        }

        assert_eq!(txn_count(&conn), 3, "expected 3 rows in _transactions");
    }
}
