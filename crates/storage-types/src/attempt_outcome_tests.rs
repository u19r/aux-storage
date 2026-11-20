use crate::{
    AttemptPhase, ErrorClass, InfrastructureContext, InfrastructureResultKind, NativeUnavailable,
    StorageEnum, StorageError, attempt_outcome::*, expired_in,
};

fn fdb_cause(code: i32, phase: NativeCallPhase) -> NativeCause {
    NativeCause {
        engine: NativeEngine::FoundationDb,
        code: NativeCode::Fdb(code),
        phase,
    }
}

#[test]
fn given_each_attempt_failure_when_kind_then_fixed_kind_and_label() {
    let rows: Vec<(AttemptFailure, AttemptKind, &str)> = vec![
        (
            AttemptFailure::Conflict(ConflictCause::Native(fdb_cause(
                1020,
                NativeCallPhase::Commit,
            ))),
            AttemptKind::Conflict,
            "conflict",
        ),
        (
            AttemptFailure::FRESH_VALIDATION,
            AttemptKind::FreshValidation,
            "fresh_validation",
        ),
        (AttemptFailure::CAPACITY, AttemptKind::Capacity, "capacity"),
        (
            AttemptFailure::TRANSACTION_BYTES,
            AttemptKind::TransactionBytes,
            "transaction_bytes",
        ),
        (
            AttemptFailure::Unavailable(NativeUnavailable::ConnectionLost),
            AttemptKind::Unavailable,
            "unavailable",
        ),
        (
            AttemptFailure::Unavailable(NativeUnavailable::Deadline(AttemptPhase::Begin)),
            AttemptKind::Deadline,
            "deadline",
        ),
        (
            AttemptFailure::CommitUnknown(UnknownCause {
                reason: "unknown",
                native: None,
            }),
            AttemptKind::CommitUnknown,
            "commit_unknown",
        ),
    ];
    for (failure, kind, label) in rows {
        assert_eq!(failure.kind(), kind, "{failure}");
        assert_eq!(failure.kind().as_str(), label, "{failure}");
    }
}

#[test]
fn given_attempt_failures_when_displayed_then_cause_is_named() {
    let rows: Vec<(StorageError, &str)> = vec![
        (
            StorageError::native_conflict(fdb_cause(1020, NativeCallPhase::Commit)),
            "native conflict: foundationdb code 1020 at Commit",
        ),
        (
            StorageError::native_conflict(NativeCause {
                engine: NativeEngine::Postgres,
                code: NativeCode::sql_state("40001"),
                phase: NativeCallPhase::Statement,
            }),
            "native conflict: postgres SQLSTATE 40001 at Statement",
        ),
        (
            StorageError::write_miss("ttl_sweep_checkpoint", MissExpectation::Revision),
            "physical write miss on ttl_sweep_checkpoint: expected Revision",
        ),
        (
            StorageError::logical_conflict("prepared TTL policy changed"),
            "logical conflict: prepared TTL policy changed",
        ),
        (
            StorageError::commit_unknown("SQLite attempt task stopped"),
            "commit outcome unknown: SQLite attempt task stopped",
        ),
        (
            StorageError::native_commit_unknown(
                "FoundationDB commit was not acknowledged",
                fdb_cause(1021, NativeCallPhase::Commit),
            ),
            "commit outcome unknown: FoundationDB commit was not acknowledged (foundationdb code \
             1021 at Commit)",
        ),
        (
            expired_in(AttemptPhase::Settle),
            "native attempt unavailable: Deadline(Settle)",
        ),
    ];
    for (error, text) in rows {
        assert_eq!(error.to_string(), text);
    }
}

#[test]
fn given_sql_state_text_when_parsed_then_five_bytes_or_none() {
    assert_eq!(
        NativeCode::sql_state("40001"),
        NativeCode::SqlState(*b"40001")
    );
    assert_eq!(NativeCode::sql_state("4000"), NativeCode::None);
    assert_eq!(NativeCode::sql_state(""), NativeCode::None);
}

#[test]
fn given_each_storage_variant_when_classified_then_class_table_holds() {
    let retryable: Vec<StorageError> = vec![
        StorageError::native_conflict(fdb_cause(1020, NativeCallPhase::Commit)),
        AttemptFailure::FRESH_VALIDATION.into(),
        AttemptFailure::Unavailable(NativeUnavailable::ConnectionLost).into(),
        StorageError::attempt_abandoned(),
        expired_in(AttemptPhase::Statement),
        StorageEnum::TransactionInProgress {
            message: String::new(),
        }
        .into(),
        StorageEnum::ProvisionedThroughputExceeded {
            message: String::new(),
        }
        .into(),
        StorageEnum::Throttled {
            message: String::new(),
        }
        .into(),
        StorageError::service_unavailable(1),
        StorageEnum::LimitExceeded {
            message: String::new(),
        }
        .into(),
        StorageEnum::RequestLimitExceeded.into(),
    ];
    let capacity: Vec<StorageError> = vec![
        AttemptFailure::CAPACITY.into(),
        AttemptFailure::TRANSACTION_BYTES.into(),
    ];
    let client: Vec<StorageError> = vec![
        StorageEnum::ConditionalCheckFailed.into(),
        StorageError::table_not_found("t"),
        StorageError::table_already_exists("t"),
        StorageError::invalid_or_missing_key(),
        StorageError::raw_validation("v"),
        StorageError::idempotent_parameter_mismatch(),
        StorageEnum::TransactionCanceled { reasons: vec![] }.into(),
        StorageEnum::MissingAuthenticationToken.into(),
        StorageEnum::HistoryExpired.into(),
        StorageEnum::TrimmedDataAccess.into(),
    ];
    let internal: Vec<StorageError> = vec![
        StorageError::internal("x"),
        StorageError::guard_conflict("g"),
        StorageError::unsupported("u"),
        StorageEnum::HistoryUnavailable.into(),
        InfrastructureResultKind::Unavailable {
            context: InfrastructureContext::AttemptSettlement,
        }
        .into(),
    ];
    for error in &retryable {
        assert_eq!(error.class(), ErrorClass::Retryable, "{error}");
    }
    for error in &capacity {
        assert_eq!(error.class(), ErrorClass::Capacity, "{error}");
    }
    for error in &client {
        assert_eq!(error.class(), ErrorClass::Client, "{error}");
    }
    for error in &internal {
        assert_eq!(error.class(), ErrorClass::Internal, "{error}");
    }
    assert_eq!(
        StorageError::commit_unknown("u").class(),
        ErrorClass::CommitUnknown
    );
}

#[test]
fn given_internal_and_infrastructure_errors_when_displayed_then_cause_shown() {
    assert_eq!(
        StorageError::internal("job store bootstrap failed").to_string(),
        "internal error: job store bootstrap failed"
    );
    let unavailable: StorageError = InfrastructureResultKind::Unavailable {
        context: InfrastructureContext::AttemptSettlement,
    }
    .into();
    assert_eq!(
        unavailable.to_string(),
        "Required protected state is unavailable (AttemptSettlement)."
    );
    assert_eq!(
        StorageError::guard_conflict("g").to_string(),
        "Durable guard conflict: g"
    );
}

#[test]
fn given_deadline_error_when_deadline_phase_then_phase() {
    assert_eq!(
        expired_in(AttemptPhase::Begin).deadline_phase(),
        Some(AttemptPhase::Begin)
    );
    assert_eq!(StorageError::commit_unknown("x").deadline_phase(), None);
}
