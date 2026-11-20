use crate::{
    AttemptFailure, AttributeMap, AttributeValue, CancellationCode, CancellationReason, ErrorClass,
    InfrastructureContext, InfrastructureResultKind, ItemKeyError, NativeUnavailable, StorageEnum,
    StorageError, StorageValidationKind,
    context::{ErrorContext as _, WrappedError as _},
    dynamodb_table_not_found_message,
    item_key::ItemKeyEnum,
};

#[test]
fn storage_validation_helper_uses_canonical_key_validation_message() {
    let error = StorageError::invalid_or_missing_key();

    assert!(
        matches!(error.base_error(), Some(StorageEnum::Validation { message, kind: StorageValidationKind::InvalidOrMissingKey })
                if message == StorageValidationKind::InvalidOrMissingKey.message()),
        "unexpected error: {error:?}"
    );
}

#[test]
fn storage_table_helpers_preserve_resource_name_and_dynamodb_message() {
    let already_exists = StorageError::table_already_exists("Orders");
    let not_found = StorageError::table_not_found("Orders");

    assert!(
        matches!(already_exists.base_error(), Some(StorageEnum::TableAlreadyExists { name }) if name == "Orders"),
        "unexpected error: {already_exists:?}"
    );
    assert!(
        matches!(not_found.base_error(), Some(StorageEnum::TableNotFound { name, message })
                if name == "Orders" && message == &dynamodb_table_not_found_message("Orders")),
        "unexpected error: {not_found:?}"
    );
}

#[test]
fn storage_cursor_helpers_map_to_internal_resource_errors() {
    let not_found = StorageError::cursor_not_found("cursor-a");
    let exists = StorageError::cursor_already_exists("cursor-a");

    assert!(
        matches!(not_found.base_error(), Some(StorageEnum::ResourceNotFound {
                resource_type: "cursor",
                resource_id,
            }) if resource_id == "cursor-a"),
        "unexpected error: {not_found:?}"
    );
    assert!(
        matches!(exists.base_error(), Some(StorageEnum::ResourceExists {
                resource_type: "cursor",
                resource_id,
            }) if resource_id == "cursor-a"),
        "unexpected error: {exists:?}"
    );
}

#[test]
fn storage_error_context_preserves_underlying_error_and_context_stack() {
    let result: Result<(), StorageError> = Err(StorageError::internal("write failed"));
    let error = result
        .context("commit item")
        .expect_err("context should wrap error");

    let (inner, contexts) = error.recursive_context(Vec::new());

    assert!(
        matches!(
            inner,
            StorageEnum::InternalServerError { message } if message == "write failed"
        ),
        "unexpected inner error: {inner:?}"
    );
    assert_eq!(contexts, vec!["commit item".to_string()]);
    assert!(format!("{error:?}").contains("Context:"));
}

#[test]
fn storage_guard_and_unsupported_helpers_preserve_internal_messages() {
    let guard = StorageError::guard_conflict("revision mismatch");
    let unsupported = StorageError::unsupported("not implemented");

    assert!(
        matches!(guard.base_error(), Some(StorageEnum::GuardConflict { message })
                if message == "revision mismatch"),
        "unexpected error: {guard:?}"
    );
    assert!(
        matches!(unsupported.base_error(), Some(StorageEnum::Unsupported { message })
                if message == "not implemented"),
        "unexpected error: {unsupported:?}"
    );
}

#[test]
fn transaction_canceled_errors_are_not_retryable_writes() {
    let error: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![CancellationReason::from_parts(
            CancellationCode::TransactionConflict,
            None,
            None,
        )],
    }
    .into();

    assert!(!error.class().is_retryable());
}

#[test]
fn conditional_write_contention_requires_only_conditional_cancellation_reasons() {
    let direct: StorageError = StorageEnum::ConditionalCheckFailed.into();
    assert!(direct.is_conditional_write_contention());

    let conditional: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            CancellationReason::none(),
            CancellationReason::from_parts(
                CancellationCode::ConditionalCheckFailed,
                Some("condition did not match".into()),
                None,
            ),
        ],
    }
    .into();
    assert!(conditional.is_conditional_write_contention());

    let mixed: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            CancellationReason::conditional_check_failed(None),
            CancellationReason::from_parts(CancellationCode::TransactionConflict, None, None),
        ],
    }
    .into();
    assert!(!mixed.is_conditional_write_contention());
}

/// One value per `StorageEnum` variant (35) with the label
/// `StorageError::label` must give it.
fn one_error_per_variant_with_label() -> Vec<(StorageError, &'static str)> {
    let variants: Vec<(StorageEnum, &'static str)> = vec![
        (StorageEnum::Database(std::fmt::Error), "database"),
        (
            StorageEnum::AwsSerialization("x".into()),
            "aws_serialization",
        ),
        (
            StorageEnum::Serialization(
                serde_json::from_str::<u8>("x")
                    .expect_err("invalid json")
                    .into(),
            ),
            "serialization",
        ),
        (
            StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding {
                context: InfrastructureContext::RecordEncoding,
            }),
            "internal_server_error",
        ),
        (
            StorageEnum::Attempt(AttemptFailure::FRESH_VALIDATION),
            "fresh_validation",
        ),
        (
            StorageEnum::ResourceNotFound {
                resource_type: "cursor",
                resource_id: "x".into(),
            },
            "resource_not_found",
        ),
        (
            StorageEnum::ResourceExists {
                resource_type: "cursor",
                resource_id: "x".into(),
            },
            "resource_exists",
        ),
        (
            StorageEnum::IndexNotFound {
                index_name: "i".into(),
                table_name: "t".into(),
            },
            "index_not_found",
        ),
        (
            StorageEnum::KeyValidation(ItemKeyError::from(ItemKeyEnum::Validation("x".into()))),
            "key_validation",
        ),
        (
            StorageEnum::InternalServerError {
                message: "x".into(),
            },
            "internal_server_error",
        ),
        (StorageEnum::HistoryUnavailable, "history_unavailable"),
        (StorageEnum::HistoryExpired, "history_expired"),
        (StorageEnum::TrimmedDataAccess, "trimmed_data_access"),
        (
            StorageEnum::ConditionalCheckFailed,
            "conditional_check_failed",
        ),
        (
            StorageEnum::ConditionalCheckFailedWithItem {
                item: AttributeMap::new(),
            },
            "conditional_check_failed",
        ),
        (
            StorageEnum::GuardConflict {
                message: "x".into(),
            },
            "guard_conflict",
        ),
        (
            StorageEnum::Unsupported {
                message: "x".into(),
            },
            "unsupported",
        ),
        (
            StorageEnum::TableAlreadyExists { name: "t".into() },
            "table_exists",
        ),
        (
            StorageEnum::TableNotFound {
                name: "t".into(),
                message: "x".into(),
            },
            "table_not_found",
        ),
        (
            StorageEnum::Validation {
                kind: StorageValidationKind::Message,
                message: "x".into(),
            },
            "validation",
        ),
        (
            StorageEnum::RawValidation {
                message: "x".into(),
            },
            "validation",
        ),
        (
            StorageEnum::DeletionProtectionEnabled {
                table_name: "t".into(),
                message: "x".into(),
            },
            "validation",
        ),
        (
            StorageEnum::TransactionInProgress {
                message: "x".into(),
            },
            "transaction_in_progress",
        ),
        (
            StorageEnum::IdempotentParameterMismatch,
            "idempotent_parameter_mismatch",
        ),
        (
            StorageEnum::IdempotencyReplay { response: vec![1] },
            "idempotency_replay",
        ),
        (
            StorageEnum::TransactionCanceled {
                reasons: vec![CancellationReason::none()],
            },
            "transaction_canceled",
        ),
        (
            StorageEnum::ProvisionedThroughputExceeded {
                message: "x".into(),
            },
            "provisioned_throughput",
        ),
        (
            StorageEnum::Throttled {
                message: "x".into(),
            },
            "throttled",
        ),
        (
            StorageEnum::ServiceUnavailable {
                message: "x".into(),
                retry_after_seconds: 3,
            },
            "service_unavailable",
        ),
        (
            StorageEnum::LimitExceeded {
                message: "x".into(),
            },
            "limit_exceeded",
        ),
        (
            StorageEnum::Authentication {
                message: "x".into(),
            },
            "authentication",
        ),
        (
            StorageEnum::AccessDenied {
                message: "x".into(),
            },
            "access_denied",
        ),
        (StorageEnum::RequestLimitExceeded, "request_limit_exceeded"),
        (
            StorageEnum::MissingAuthenticationToken,
            "missing_authentication_token",
        ),
        (
            StorageEnum::AwsService {
                code: None,
                message: "x".into(),
            },
            "aws_service",
        ),
    ];
    variants
        .into_iter()
        .map(|(variant, label)| (StorageError::from(variant), label))
        .collect()
}

#[test]
fn given_every_variant_when_labelled_then_fixed_snake_case_label() {
    let mut cases = one_error_per_variant_with_label();
    assert_eq!(cases.len(), 35, "one value per StorageEnum variant");
    cases.extend([
        (StorageError::logical_conflict("test conflict"), "conflict"),
        (
            StorageError::from(AttemptFailure::FRESH_VALIDATION),
            "fresh_validation",
        ),
        (StorageError::from(AttemptFailure::CAPACITY), "capacity"),
        (
            StorageError::from(AttemptFailure::TRANSACTION_BYTES),
            "transaction_bytes",
        ),
        (
            StorageError::from(AttemptFailure::Unavailable(
                NativeUnavailable::ConnectionLost,
            )),
            "unavailable",
        ),
        (
            crate::expired_in(crate::AttemptPhase::Statement),
            "deadline",
        ),
        (StorageError::commit_unknown("lost"), "commit_unknown"),
    ]);

    for (error, expected) in cases {
        let label = error.label();
        assert_eq!(label, expected, "label of {error:?}");
        assert!(
            !label.is_empty() && label.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
            "label {label:?} is not snake_case"
        );
    }

    assert_eq!(ErrorClass::Retryable.as_str(), "retryable");
    assert_eq!(ErrorClass::Capacity.as_str(), "capacity");
    assert_eq!(ErrorClass::CommitUnknown.as_str(), "commit_unknown");
    assert_eq!(ErrorClass::Client.as_str(), "client");
    assert_eq!(ErrorClass::Internal.as_str(), "internal");
}

#[test]
fn given_typed_reasons_when_index_queried_then_condition_failure_by_code() {
    let item = AttributeMap::from_iter([("pk".to_string(), AttributeValue::S("a".into()))]);
    let error: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![
            CancellationReason::none(),
            CancellationReason::conditional_check_failed(Some(item)),
            CancellationReason::from_parts(CancellationCode::TransactionConflict, None, None),
        ],
    }
    .into();

    assert!(error.is_canceled_condition_check_failed(1));
    assert!(!error.is_canceled_condition_check_failed(0));
    assert!(!error.is_canceled_condition_check_failed(2));
    assert!(!error.is_canceled_condition_check_failed(3));
    assert_eq!(error.cancellation_reasons().map(<[_]>::len), Some(3));
}

#[test]
fn given_other_error_when_reasons_queried_then_none() {
    let direct: StorageError = StorageEnum::ConditionalCheckFailed.into();
    assert!(direct.cancellation_reasons().is_none());
    assert!(!direct.is_canceled_condition_check_failed(0));
    assert!(direct.is_conditional_write_contention());

    let no_failure: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![CancellationReason::none()],
    }
    .into();
    assert!(!no_failure.is_conditional_write_contention());
}
