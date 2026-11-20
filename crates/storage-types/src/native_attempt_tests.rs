use crate::{
    AttemptFailure, AttemptKind, AttemptPhase, ErrorClass, NativeAttemptSettlement,
    NativeCallPhase, NativeCause, NativeCode, NativeEngine, NativeUnavailable, StorageEnum,
    StorageError, expired_in,
};

fn unavailable() -> StorageError {
    crate::AttemptFailure::Unavailable(NativeUnavailable::Deadline(AttemptPhase::Statement)).into()
}

#[test]
fn unavailable_error_is_a_known_rejection() {
    assert!(
        unavailable()
            .attempt_kind()
            .is_some_and(crate::AttemptKind::is_unavailable)
    );
    assert!(!unavailable().is_commit_unknown());
}

#[test]
fn settlement_keeps_unavailable_rejection() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&unavailable());
    let error = settlement.require_open().expect_err("attempt is rejected");
    assert!(matches!(
        error.as_ref(),
        StorageEnum::Attempt(crate::AttemptFailure::Unavailable(_))
    ));
}

#[test]
fn unavailable_outranks_conflict_and_yields_to_capacity_and_unknown() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject();
    settlement.reject_error(&unavailable());
    assert!(matches!(
        settlement.require_open().expect_err("rejected").as_ref(),
        StorageEnum::Attempt(crate::AttemptFailure::Unavailable(_))
    ));
    settlement.reject_error(&crate::AttemptFailure::CAPACITY.into());
    assert!(matches!(
        settlement.require_open().expect_err("rejected").as_ref(),
        StorageEnum::Attempt(crate::AttemptFailure::Capacity {
            kind: crate::CapacityKind::NativeLimit,
            ..
        })
    ));
}

#[test]
fn settlement_failure_replaces_operation_error_only_when_stronger() {
    let operation = StorageError::internal("operation failed");
    let preserved = operation.preserve_native_settlement(Err(unavailable()));
    assert!(matches!(
        preserved.as_ref(),
        StorageEnum::Attempt(crate::AttemptFailure::Unavailable(_))
    ));
    let conflict: StorageError = StorageEnum::Attempt(crate::AttemptFailure::Conflict(
        crate::ConflictCause::Logical("conflict"),
    ))
    .into();
    let preserved =
        StorageError::internal("operation failed").preserve_native_settlement(Err(conflict));
    assert!(matches!(
        preserved.as_ref(),
        StorageEnum::InternalServerError { .. }
    ));
}

#[test]
fn given_native_byte_rejection_when_settling_then_preserve_byte_cause() {
    let settlement = NativeAttemptSettlement::new(None);
    let error: StorageError = crate::AttemptFailure::TRANSACTION_BYTES.into();
    assert!(matches!(
        error.attempt_kind(),
        Some(crate::AttemptKind::Capacity | crate::AttemptKind::TransactionBytes)
    ));
    settlement.reject_error(&error);
    let settled = settlement.require_open().expect_err("attempt rejected");
    assert!(matches!(
        settled.as_ref(),
        StorageEnum::Attempt(crate::AttemptFailure::Capacity {
            kind: crate::CapacityKind::TransactionBytes,
            ..
        })
    ));
    settlement.reject_error(&crate::AttemptFailure::CAPACITY.into());
    assert!(matches!(
        settlement
            .require_open()
            .expect_err("byte cause preserved")
            .as_ref(),
        StorageEnum::Attempt(crate::AttemptFailure::Capacity {
            kind: crate::CapacityKind::TransactionBytes,
            ..
        })
    ));
    settlement.reject_error(&StorageError::commit_unknown("unknown"));
    assert!(
        settlement
            .require_open()
            .expect_err("unknown dominates")
            .is_commit_unknown()
    );
}

#[test]
fn given_permanent_native_failure_when_conflict_and_capacity_follow_then_original_cause_survives() {
    let settlement = NativeAttemptSettlement::new(None);
    let permanent = StorageError::internal("permanent physical failure");
    let _: crate::StorageResult<()> = settlement
        .begin_call()
        .unwrap()
        .complete(Err(permanent.clone()));
    settlement.reject_error(&crate::AttemptFailure::TRANSACTION_BYTES.into());
    settlement.reject_error(&unavailable());
    settlement.reject_error(&StorageError::internal("later failure"));
    let conflict: StorageError = StorageEnum::Attempt(crate::AttemptFailure::Conflict(
        crate::ConflictCause::Logical("callback conflict"),
    ))
    .into();
    let result = conflict.preserve_native_settlement(settlement.require_open());
    assert_eq!(result.to_string(), permanent.to_string());
    assert!(
        !result
            .attempt_kind()
            .is_some_and(|kind| kind != crate::AttemptKind::CommitUnknown)
    );
    settlement.reject_error(&StorageError::commit_unknown("unknown"));
    assert!(settlement.require_open().unwrap_err().is_commit_unknown());
}

#[test]
fn given_concurrent_native_failures_when_settled_then_permanent_failure_prevents_retry() {
    let settlement = NativeAttemptSettlement::new(None);
    std::thread::scope(|scope| {
        let settlement = &settlement;
        scope.spawn(move || {
            settlement.reject_error(&StorageError::internal("permanent physical failure"))
        });
        scope.spawn(move || settlement.reject_error(&crate::AttemptFailure::CAPACITY.into()));
        scope.spawn(move || settlement.reject_error(&unavailable()));
        scope.spawn(move || settlement.reject());
    });
    let error = settlement.require_open().unwrap_err();
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != crate::AttemptKind::CommitUnknown)
    );
    assert!(matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { .. }
    ));
    let control: StorageError = crate::AttemptFailure::FRESH_VALIDATION.into();
    assert_eq!(
        control
            .preserve_native_settlement(Err(error.clone()))
            .to_string(),
        error.to_string()
    );
}

fn postgres_conflict() -> StorageError {
    StorageError::native_conflict(NativeCause {
        engine: NativeEngine::Postgres,
        code: NativeCode::sql_state("40001"),
        phase: NativeCallPhase::Statement,
    })
}

fn is_abandoned(error: &StorageError) -> bool {
    error.is_attempt_abandoned()
}

#[test]
fn given_caught_native_conflict_when_required_then_same_conflict_with_cause() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&postgres_conflict());
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));
    assert!(error.to_string().contains("SQLSTATE 40001"), "{error}");
}

#[test]
fn given_abandoned_call_when_required_then_known_unavailable_not_conflict() {
    let settlement = NativeAttemptSettlement::new(None);
    drop(settlement.begin_call().unwrap());
    let error = settlement.require_open().expect_err("rejected");
    assert!(is_abandoned(&error), "{error}");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Unavailable));
    assert_eq!(error.class(), ErrorClass::Retryable);
    assert!(!error.is_commit_unknown());
}

#[test]
fn given_abandoned_error_and_conflict_when_merged_then_conflict_wins() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&StorageError::attempt_abandoned());
    settlement.reject_error(&StorageError::logical_conflict("c"));
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));

    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&StorageError::logical_conflict("c"));
    settlement.reject_error(&StorageError::attempt_abandoned());
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));
}

#[test]
fn given_abandoned_then_conflict_when_required_then_conflict() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject();
    let miss = StorageError::write_miss("x", crate::MissExpectation::Absent);
    settlement.reject_error(&miss);
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.to_string(), miss.to_string());
}

#[test]
fn given_conflict_then_abandoned_when_required_then_conflict() {
    let settlement = NativeAttemptSettlement::new(None);
    let miss = StorageError::write_miss("x", crate::MissExpectation::Absent);
    settlement.reject_error(&miss);
    settlement.reject();
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.to_string(), miss.to_string());
}

#[test]
fn given_connection_lost_then_begin_deadline_when_required_then_begin_deadline() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&AttemptFailure::Unavailable(NativeUnavailable::ConnectionLost).into());
    settlement.reject_error(&expired_in(AttemptPhase::Begin));
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.deadline_phase(), Some(AttemptPhase::Begin));
}

#[test]
fn given_statement_then_settle_deadline_when_required_then_statement_phase() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&expired_in(AttemptPhase::Statement));
    settlement.reject_error(&expired_in(AttemptPhase::Settle));
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.deadline_phase(), Some(AttemptPhase::Statement));
}

#[test]
fn given_connection_lost_only_when_required_then_connection_lost() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&AttemptFailure::Unavailable(NativeUnavailable::ConnectionLost).into());
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(
        error.to_string(),
        "native attempt unavailable: ConnectionLost"
    );
}

#[test]
fn given_caught_fresh_validation_when_required_then_fresh_validation() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&AttemptFailure::FRESH_VALIDATION.into());
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.attempt_kind(), Some(AttemptKind::FreshValidation));
}

#[test]
fn given_unknown_after_any_when_required_then_unknown_kept() {
    let settlement = NativeAttemptSettlement::new(None);
    settlement.reject_error(&postgres_conflict());
    settlement.reject_error(&AttemptFailure::CAPACITY.into());
    settlement.reject_error(&StorageError::commit_unknown("u"));
    settlement.reject_error(&StorageError::internal("later"));
    let error = settlement.require_open().expect_err("rejected");
    assert_eq!(error.to_string(), "commit outcome unknown: u");
}

#[test]
fn given_abandoned_settlement_when_preserving_then_operation_error_kept() {
    let abandoned = StorageError::attempt_abandoned();
    let preserved = StorageError::internal("op").preserve_native_settlement(Err(abandoned));
    assert_eq!(preserved.to_string(), "internal error: op");
}

/// One call on a settlement: `reject()` or `reject_error(..)`.
enum Call {
    Reject,
    Error(StorageError),
}

/// What `require_open()` must return.
enum Expected {
    Open,
    Abandoned,
    Conflict,
    Unavailable,
    Deadline(AttemptPhase),
    Capacity,
    TransactionBytes,
    Internal(&'static str),
    CommitUnknown,
}

fn deadline_in(phase: AttemptPhase) -> StorageError {
    crate::AttemptFailure::Unavailable(NativeUnavailable::Deadline(phase)).into()
}

fn infrastructure(kind: AttemptFailure) -> StorageError {
    kind.into()
}

#[test]
fn given_rejection_sequences_when_settlement_is_required_then_the_strongest_first_cause_is_reported()
 {
    use Call::{Error, Reject};
    let queue_full = || {
        infrastructure(crate::AttemptFailure::Unavailable(
            NativeUnavailable::WriterQueueFull,
        ))
    };
    let capacity = || infrastructure(crate::AttemptFailure::CAPACITY);
    let bytes = || infrastructure(crate::AttemptFailure::TRANSACTION_BYTES);
    let unknown = || StorageError::commit_unknown("u");
    let conflict = || {
        StorageError::from(StorageEnum::Attempt(crate::AttemptFailure::Conflict(
            crate::ConflictCause::Logical("c"),
        )))
    };
    let rows: Vec<(&str, Vec<Call>, Expected)> = vec![
        ("R", vec![Reject], Expected::Abandoned),
        (
            "queue full",
            vec![Error(queue_full())],
            Expected::Unavailable,
        ),
        (
            "queue full, begin deadline",
            vec![Error(queue_full()), Error(deadline_in(AttemptPhase::Begin))],
            Expected::Deadline(AttemptPhase::Begin),
        ),
        (
            "two deadlines keep the first",
            vec![
                Error(deadline_in(AttemptPhase::Begin)),
                Error(deadline_in(AttemptPhase::Settle)),
            ],
            Expected::Deadline(AttemptPhase::Begin),
        ),
        (
            "deadline then capacity",
            vec![
                Error(deadline_in(AttemptPhase::Statement)),
                Error(capacity()),
            ],
            Expected::Capacity,
        ),
        (
            "capacity then bytes",
            vec![Error(capacity()), Error(bytes())],
            Expected::TransactionBytes,
        ),
        (
            "internal then bytes",
            vec![Error(StorageError::internal("a")), Error(bytes())],
            Expected::Internal("a"),
        ),
        (
            "internal then internal",
            vec![
                Error(StorageError::internal("a")),
                Error(StorageError::internal("b")),
            ],
            Expected::Internal("a"),
        ),
        (
            "internal then unknown",
            vec![Error(StorageError::internal("a")), Error(unknown())],
            Expected::CommitUnknown,
        ),
        (
            "unknown then R then internal",
            vec![Error(unknown()), Reject, Error(StorageError::internal("a"))],
            Expected::CommitUnknown,
        ),
        (
            "conflict then R",
            vec![Error(conflict()), Reject],
            Expected::Conflict,
        ),
        ("no call", vec![], Expected::Open),
    ];
    for (name, calls, expected) in rows {
        let settlement = NativeAttemptSettlement::new(None);
        for call in calls {
            match call {
                Call::Reject => settlement.reject(),
                Call::Error(error) => settlement.reject_error(&error),
            }
        }
        let result = settlement.require_open();
        match (expected, result) {
            (Expected::Open, Ok(())) => {}
            (Expected::Open, Err(error)) => panic!("{name}: expected Ok, got {error}"),
            (_, Ok(())) => panic!("{name}: expected an error"),
            (Expected::Abandoned, Err(error)) => {
                assert!(is_abandoned(&error), "{name}: {error:?}");
            }
            (Expected::Conflict, Err(error)) => assert_eq!(
                error.attempt_kind(),
                Some(AttemptKind::Conflict),
                "{name}: {error:?}"
            ),
            (Expected::Unavailable, Err(error)) => assert_eq!(
                error.attempt_kind(),
                Some(AttemptKind::Unavailable),
                "{name}: {error:?}"
            ),
            (Expected::Deadline(phase), Err(error)) => assert!(
                matches!(
                    error.as_ref(),
                    StorageEnum::Attempt(crate::AttemptFailure::Unavailable(
                        NativeUnavailable::Deadline(found)
                    )) if *found == phase
                ),
                "{name}: {error:?}"
            ),
            (Expected::Capacity, Err(error)) => assert!(
                matches!(
                    error.as_ref(),
                    StorageEnum::Attempt(crate::AttemptFailure::Capacity {
                        kind: crate::CapacityKind::NativeLimit,
                        ..
                    })
                ),
                "{name}: {error:?}"
            ),
            (Expected::TransactionBytes, Err(error)) => assert!(
                matches!(
                    error.as_ref(),
                    StorageEnum::Attempt(crate::AttemptFailure::Capacity {
                        kind: crate::CapacityKind::TransactionBytes,
                        ..
                    })
                ),
                "{name}: {error:?}"
            ),
            (Expected::Internal(text), Err(error)) => assert!(
                matches!(
                    error.as_ref(),
                    StorageEnum::InternalServerError { message } if message == text
                ),
                "{name}: {error:?}"
            ),
            (Expected::CommitUnknown, Err(error)) => {
                assert!(error.is_commit_unknown(), "{name}: {error:?}");
            }
        }
    }
}
