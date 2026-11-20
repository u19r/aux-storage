use storage_types::{
    AttemptFailure, AttemptPhase, NativeUnavailable, StorageEnum, StorageError, expired_in,
};

use super::aws_query::*;

const INTERNAL: AwsQueryInternal = AwsQueryInternal {
    error_type: "InternalError",
    message: "fixed",
};

#[test]
fn given_each_class_when_answered_for_aws_query_then_503_only_when_known_not_committed() {
    let admission = StorageError::service_unavailable(7);
    let not_committed = [
        (StorageError::logical_conflict("test conflict"), 1),
        (StorageError::from(AttemptFailure::FRESH_VALIDATION), 1),
        (
            StorageError::from(AttemptFailure::Unavailable(
                NativeUnavailable::ConnectionLost,
            )),
            1,
        ),
        (expired_in(AttemptPhase::Statement), 1),
        (StorageError::from(AttemptFailure::CAPACITY), 1),
        (StorageError::from(AttemptFailure::TRANSACTION_BYTES), 1),
        (
            StorageError::from(StorageEnum::Throttled {
                message: "x".into(),
            }),
            1,
        ),
        (
            StorageError::from(StorageEnum::TransactionInProgress {
                message: "x".into(),
            }),
            1,
        ),
        (admission, 7),
    ];
    for (error, retry_after) in not_committed {
        let label = error.label();
        let answer = AwsQueryAnswer::for_storage(&error, INTERNAL);

        assert_eq!(answer.status_code, 503, "{label}");
        assert_eq!(
            answer.error_type, AWS_QUERY_SERVICE_UNAVAILABLE_ERROR_TYPE,
            "{label}"
        );
        assert_eq!(answer.error_type, "ServiceUnavailable", "{label}");
        assert_eq!(
            answer.message, AWS_QUERY_SERVICE_UNAVAILABLE_MESSAGE,
            "{label}"
        );
        assert_eq!(answer.retry_after_seconds, Some(retry_after), "{label}");
    }

    let answered_500 = [
        StorageError::commit_unknown("lost"),
        StorageError::internal("secret detail"),
        StorageError::table_not_found("t"),
    ];
    for error in answered_500 {
        let label = error.label();
        let answer = AwsQueryAnswer::for_storage(&error, INTERNAL);

        assert_eq!(answer.status_code, 500, "{label}");
        assert_eq!(answer.error_type, "InternalError", "{label}");
        assert_eq!(answer.message, "fixed", "{label}");
        assert_eq!(answer.retry_after_seconds, None, "{label}");
    }
}

#[test]
fn given_answer_when_converted_to_http_then_retry_after_header_only_on_503() {
    let retryable =
        AwsQueryAnswer::for_storage(&StorageError::service_unavailable(7), INTERNAL).into_http();
    let unknown =
        AwsQueryAnswer::for_storage(&StorageError::commit_unknown("lost"), INTERNAL).into_http();

    assert_eq!(retryable.status_code, 503);
    assert!(
        retryable
            .response_headers
            .iter()
            .any(|(name, value)| name == "Retry-After" && value == "7"),
        "{:?}",
        retryable.response_headers
    );
    assert_eq!(unknown.status_code, 500);
    assert!(
        !unknown
            .response_headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("retry-after")),
        "{:?}",
        unknown.response_headers
    );
}
