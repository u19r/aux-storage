use alloc_counter::AllocationGuard;

use crate::{
    AttributeMap, AttributeValue,
    request_expression_validation::{
        ExpressionKind, is_dynamodb_reserved_word, validate_expression_set,
        validate_expression_syntax,
    },
};

const FUNCTION_EXPRESSION: &str = "attribute_exists(a) AND begins_with(b, :p) AND size(c) > :n";

fn function_values() -> AttributeMap {
    AttributeMap::from_iter([
        (":p".to_owned(), AttributeValue::S("x".to_owned())),
        (":n".to_owned(), AttributeValue::N("1".to_owned())),
    ])
}

#[test]
fn given_valid_function_expression_when_syntax_checked_then_nothing_is_allocated() {
    let guard = AllocationGuard::start(
        module_path!(),
        "valid_function_expression_syntax",
        file!(),
        line!(),
        Some("on_demand_lexer"),
    );
    let result = validate_expression_syntax(FUNCTION_EXPRESSION, ExpressionKind::Condition, false);
    let report = guard.finish();
    let _ = alloc_counter::emit_report(&report);
    assert!(result.is_ok());
    assert_eq!(report.allocation_count, 0);
}

#[test]
fn given_valid_function_expression_when_validated_then_nothing_is_allocated() {
    let values = function_values();
    // First use builds the reserved-word set; only the steady state counts.
    validate_expression_set(
        [(Some(FUNCTION_EXPRESSION), ExpressionKind::Condition)],
        None,
        Some(&values),
        false,
    )
    .expect("warm-up");
    let guard = AllocationGuard::start(
        module_path!(),
        "valid_function_expression_set",
        file!(),
        line!(),
        Some("function_scan"),
    );
    let result = validate_expression_set(
        [(Some(FUNCTION_EXPRESSION), ExpressionKind::Condition)],
        None,
        Some(&values),
        false,
    );
    let report = guard.finish();
    let _ = alloc_counter::emit_report(&report);
    assert!(result.is_ok());
    assert_eq!(report.allocation_count, 0);
}

#[test]
fn given_reserved_words_when_looked_up_then_case_and_boundaries_are_preserved() {
    for word in ["ABORT", "comment", "uPdAtE", "ZONE"] {
        assert!(is_dynamodb_reserved_word(word), "missing {word}");
    }
    for word in ["", "comments", "tenant_comment", "zone2", "not_reserved"] {
        assert!(!is_dynamodb_reserved_word(word), "unexpected {word}");
    }
}
