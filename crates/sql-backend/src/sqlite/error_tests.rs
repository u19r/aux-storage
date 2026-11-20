use std::time::Duration;

use rusqlite::{Connection, ErrorCode, TransactionBehavior};
use storage_common::native_outcome::NativeCommitError;
use storage_types::{NativeCallPhase, NativeCode};

use super::error::*;

fn failure(code: i32) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None)
}

#[test]
fn given_busy_and_io_failures_when_commit_error_then_conflict_and_unproven() {
    match commit_error(&failure(rusqlite::ffi::SQLITE_BUSY)) {
        NativeCommitError::Conflict(cause) => {
            assert_eq!(cause.code, NativeCode::Sqlite(5));
            assert_eq!(cause.phase, NativeCallPhase::Commit);
        }
        other => panic!("busy COMMIT must be a conflict, got {other:?}"),
    }
    assert!(matches!(
        commit_error(&failure(rusqlite::ffi::SQLITE_IOERR)),
        NativeCommitError::Unproven { .. }
    ));
}

/// The premise in rollback-journal mode; in WAL a busy COMMIT after BEGIN
/// IMMEDIATE is not reachable, and the rollback proof covers both.
#[test]
fn given_reader_lock_when_commit_busy_then_rollback_leaves_row_absent() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("busy.db");
    let open = || {
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "journal_mode", "DELETE")
            .unwrap();
        connection.busy_timeout(Duration::ZERO).unwrap();
        connection
    };
    let mut writer = open();
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    let reader = open();
    reader.execute_batch("BEGIN").unwrap();
    let seen: i64 = reader
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(seen, 0);

    let transaction = writer
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    transaction.execute("INSERT INTO t VALUES (1)", []).unwrap();
    let error = transaction.execute_batch("COMMIT").unwrap_err();
    match &error {
        rusqlite::Error::SqliteFailure(failure, _) => {
            assert_eq!(failure.code, ErrorCode::DatabaseBusy);
        }
        other => panic!("expected a busy COMMIT, got {other:?}"),
    }
    transaction.rollback().unwrap();
    reader.execute_batch("COMMIT").unwrap();

    let fresh = open();
    let rows: i64 = fresh
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 0);
}
