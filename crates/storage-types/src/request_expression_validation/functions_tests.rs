use super::{
    ExpressionKind,
    functions::{
        attribute_type_literal_operand_error, contains_same_operand_error, function_argument_lists,
        validate_attribute_type_value_operands, validate_begins_with_value_operands,
    },
};
use crate::{AttributeMap, AttributeValue};

#[test]
fn given_function_calls_when_scanned_then_preserves_case_nesting_and_unicode() {
    for (expression, expected) in [
        ("tenant = :tenant", vec![]),
        ("ConTaInS(tags, :tag)", vec!["tags, :tag"]),
        ("contains(étiquette, :tag)", vec!["étiquette, :tag"]),
        (
            "contains(tags, nested(a, b)) AND contains(other, :v)",
            vec!["tags, nested(a, b)", "other, :v"],
        ),
        (
            "contains(tags, contains(other, :v))",
            vec!["tags, contains(other, :v)"],
        ),
        (
            "contains(tags, :v) AND contains(unfinished",
            vec!["tags, :v"],
        ),
        ("contains (tags, :v)", vec![]),
        ("mycontains(tags, :v)", vec!["tags, :v"]),
    ] {
        assert_eq!(
            function_argument_lists(expression, "contains").collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn given_function_operands_when_validated_then_preserves_errors() {
    let values = operand_values();
    assert!(
        contains_same_operand_error("contains(tags, tags)", ExpressionKind::Filter, None)
            .unwrap()
            .contains("first operand: [tags]")
    );
    assert!(
        attribute_type_literal_operand_error("attribute_type(payload, S)", ExpressionKind::Filter)
            .unwrap()
            .contains("attribute_type")
    );
    assert!(
        validate_attribute_type_value_operands(
            "attribute_type(payload, :kind)",
            ExpressionKind::Filter,
            Some(&values),
            false
        )
        .is_ok()
    );
    assert!(
        validate_begins_with_value_operands(
            "begins_with(tenant, :revision)",
            ExpressionKind::Filter,
            Some(&values),
            false
        )
        .unwrap_err()
        .contains("operand type: N")
    );
}

fn operand_values() -> AttributeMap {
    AttributeMap::from_iter([
        (":prefix".to_owned(), AttributeValue::S("tenant".to_owned())),
        (":kind".to_owned(), AttributeValue::S("M".to_owned())),
        (":revision".to_owned(), AttributeValue::N("1".to_owned())),
    ])
}
