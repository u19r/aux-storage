use super::cancellation_reason::*;
use crate::{
    AttributeMap, AttributeValue, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE, StorageEnum,
    StorageError,
};

const ALL_CODES: [(CancellationCode, &str); 7] = [
    (CancellationCode::None, "None"),
    (
        CancellationCode::ConditionalCheckFailed,
        "ConditionalCheckFailed",
    ),
    (
        CancellationCode::ItemCollectionSizeLimitExceeded,
        "ItemCollectionSizeLimitExceeded",
    ),
    (CancellationCode::TransactionConflict, "TransactionConflict"),
    (
        CancellationCode::ProvisionedThroughputExceeded,
        "ProvisionedThroughputExceeded",
    ),
    (CancellationCode::ThrottlingError, "ThrottlingError"),
    (CancellationCode::ValidationError, "ValidationError"),
];

fn item(value: &str) -> AttributeMap {
    AttributeMap::from_iter([("pk".to_string(), AttributeValue::S(value.to_string()))])
}

#[test]
fn given_each_code_when_written_and_parsed_then_same_code() {
    for (code, text) in ALL_CODES {
        assert_eq!(code.as_str(), text);
        assert_eq!(CancellationCode::parse(code.as_str()), Some(code));
    }
}

#[test]
fn given_non_aws_code_text_when_parsed_then_none() {
    for text in ["ConditionalCheckFailed\tx", "conflict", "", "none"] {
        assert_eq!(CancellationCode::parse(text), None, "text {text:?}");
    }
}

#[test]
fn given_absent_message_when_built_from_parts_then_code_default() {
    let message = |code| CancellationReason::from_parts(code, None, None).message;

    assert_eq!(
        message(CancellationCode::ConditionalCheckFailed).as_deref(),
        Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE)
    );
    assert_eq!(
        message(CancellationCode::ValidationError).as_deref(),
        Some("")
    );
    assert_eq!(message(CancellationCode::None), None);
    assert_eq!(message(CancellationCode::TransactionConflict), None);
    assert_eq!(message(CancellationCode::ThrottlingError), None);
}

#[test]
fn given_message_when_built_from_parts_then_kept() {
    let reason = CancellationReason::from_parts(
        CancellationCode::ConditionalCheckFailed,
        Some("x".to_string()),
        Some(item("a")),
    );

    assert_eq!(reason.message.as_deref(), Some("x"));
    assert_eq!(reason.item, Some(item("a")));
}

#[test]
fn given_constructors_when_built_then_wire_fields() {
    assert_eq!(
        CancellationReason::none(),
        CancellationReason {
            code: CancellationCode::None,
            message: None,
            item: None
        }
    );
    assert_eq!(
        CancellationReason::conditional_check_failed(None)
            .message
            .as_deref(),
        Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE)
    );
    assert_eq!(
        CancellationReason::validation_error("m"),
        CancellationReason {
            code: CancellationCode::ValidationError,
            message: Some("m".to_string()),
            item: None,
        }
    );
    for (code, _) in ALL_CODES {
        let reason = CancellationReason::from_parts(code, None, None);
        assert_eq!(
            reason.is_conditional_check_failed(),
            code == CancellationCode::ConditionalCheckFailed,
            "code {code:?}"
        );
    }
}

#[test]
fn given_reason_or_error_with_item_when_debug_formatted_then_item_values_are_redacted() {
    const SECRET: &str = "distinctive-secret-value-7f3a";
    let reason = CancellationReason::conditional_check_failed(Some(item(SECRET)));
    let reason_debug = format!("{reason:?}");
    assert!(
        !reason_debug.contains(SECRET),
        "reason debug leaked the item: {reason_debug}"
    );
    assert!(
        reason_debug.contains("Some(<1 attributes>)"),
        "unexpected debug: {reason_debug}"
    );

    let canceled: StorageError = StorageEnum::TransactionCanceled {
        reasons: vec![reason.clone()],
    }
    .into();
    assert!(!format!("{canceled:?}").contains(SECRET));

    let with_item: StorageError =
        StorageEnum::ConditionalCheckFailedWithItem { item: item(SECRET) }.into();
    assert!(!format!("{with_item:?}").contains(SECRET));

    assert_eq!(reason.item, Some(item(SECRET)));
}
