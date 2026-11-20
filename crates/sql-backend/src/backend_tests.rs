use crate::SqlEngine;

#[test]
fn given_placeholder_boundaries_when_rendered_then_preserve_sql_syntax() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        for index in [0, 1, 9, 10, 20, 21, 32, 100, usize::MAX] {
            assert_eq!(engine.placeholder(index), format!("{prefix}{index}"));
        }
        for count in [0, 1, 4, 20, 21, 32] {
            let expected = (1..=count)
                .map(|index| format!("{prefix}{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            assert_eq!(engine.placeholders(count), expected);
        }
    }
}

#[test]
fn given_numeric_placeholder_boundaries_when_rendered_then_preserve_cast_and_slot() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        for index in [0, 1, 2, 4, 5, 20, 21, 32, usize::MAX] {
            assert_eq!(
                engine.numeric_placeholder(index),
                format!("CAST({prefix}{index} AS TEXT)::NUMERIC")
            );
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_supervised_attempt_when_caller_is_cancelled_then_settlement_continues() {
    let (started, ready) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (settled, completed) = tokio::sync::oneshot::channel();
    let caller = tokio::spawn(storage_common::native_supervision::supervise_attempt(
        storage_common::native_supervision::AttemptLabel {
            provider: "test",
            class: storage_types::DeadlineClass::Write,
        },
        None,
        move |_lifecycle| async move {
            started.send(()).unwrap();
            released.await.unwrap();
            settled.send(()).unwrap();
            Ok(())
        },
        "attempt stopped",
    ));
    ready.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), completed)
        .await
        .unwrap()
        .unwrap();
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_supervised_attempt_when_operation_panics_then_caller_receives_the_panic() {
    let caller = tokio::spawn(
        storage_common::native_supervision::supervise_attempt::<(), _>(
            storage_common::native_supervision::AttemptLabel {
                provider: "test",
                class: storage_types::DeadlineClass::Write,
            },
            None,
            |_lifecycle| async { panic!("operation panic") },
            "attempt stopped",
        ),
    );
    let panic = caller.await.unwrap_err();
    assert!(panic.is_panic());
    assert_eq!(
        *panic.into_panic().downcast::<&str>().unwrap(),
        "operation panic"
    );
}

fn missing_relation() -> storage_types::StorageError {
    storage_types::StorageError::from(storage_types::AttemptFailure::FreshValidation(
        storage_types::FreshCause::MissingRelation(storage_types::NativeCause {
            engine: storage_types::NativeEngine::Postgres,
            code: storage_types::NativeCode::sql_state("42P01"),
            phase: storage_types::NativeCallPhase::Statement,
        }),
    ))
}

#[test]
fn given_missing_relation_outside_candidates_then_failure_is_internal() {
    let error = crate::SqlReadBatchFailure {
        statement_index: 0,
        error: missing_relation(),
    }
    .outside_candidates();
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::InternalServerError { message }
            if message == "SQL system relation is missing"
    ));
}

#[test]
fn given_other_failure_outside_candidates_then_error_is_kept() {
    let error = crate::SqlReadBatchFailure {
        statement_index: 0,
        error: storage_types::StorageError::internal("x"),
    }
    .outside_candidates();
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::InternalServerError { message } if message == "x"
    ));
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
fn begin_deadline(expires_in: Option<std::time::Duration>) -> storage_types::AttemptDeadline {
    let now = tokio::time::Instant::now();
    storage_types::AttemptDeadline {
        class: storage_types::DeadlineClass::Write,
        expires_at: match expires_in {
            Some(left) => now + left,
            None => now - std::time::Duration::from_millis(1),
        },
        commit_grace: std::time::Duration::from_millis(100),
    }
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
fn begin_deadline_phase(
    error: &storage_types::StorageError,
) -> Option<storage_types::AttemptPhase> {
    match error.attempt_failure() {
        Some(storage_types::AttemptFailure::Unavailable(
            storage_types::NativeUnavailable::Deadline(phase),
        )) => Some(*phase),
        _ => None,
    }
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
#[tokio::test]
async fn given_busy_begin_when_deadline_passed_then_it_is_a_begin_expiry_otherwise_unchanged() {
    use crate::backend::local_begin_failure;
    let expired = begin_deadline(None);
    let open = begin_deadline(Some(std::time::Duration::from_secs(10)));
    let conflict = storage_types::StorageError::native_conflict(storage_types::NativeCause {
        engine: storage_types::NativeEngine::Sqlite,
        code: storage_types::NativeCode::Sqlite(5),
        phase: storage_types::NativeCallPhase::Begin,
    });
    assert_eq!(
        begin_deadline_phase(&local_begin_failure(conflict.clone(), &expired)),
        Some(storage_types::AttemptPhase::Begin)
    );
    assert_eq!(
        local_begin_failure(conflict, &open).attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert_eq!(
        local_begin_failure(storage_types::StorageError::internal("x"), &expired).attempt_kind(),
        None
    );
}

#[cfg(any(feature = "sqlite", feature = "turso"))]
#[tokio::test]
async fn given_deadline_when_busy_wait_is_derived_then_it_is_the_smaller_of_remaining_and_five_seconds()
 {
    use crate::{backend::local_busy_wait, constants::LOCAL_SQL_BUSY_TIMEOUT};
    let error = local_busy_wait(&begin_deadline(None)).unwrap_err();
    assert_eq!(
        begin_deadline_phase(&error),
        Some(storage_types::AttemptPhase::Begin)
    );
    assert_eq!(
        local_busy_wait(&begin_deadline(Some(std::time::Duration::from_secs(10)))).unwrap(),
        LOCAL_SQL_BUSY_TIMEOUT
    );
    let short = local_busy_wait(&begin_deadline(Some(std::time::Duration::from_secs(1)))).unwrap();
    assert!(short <= std::time::Duration::from_secs(1) && !short.is_zero());
}
