use super::{ExpressionKind, validate_expression_set};
use crate::AttributeValue;

fn legacy_in_operand_lists(expression: &str) -> Vec<&str> {
    let mut lists = Vec::new();
    let mut offset = 0usize;
    let upper = expression.to_ascii_uppercase();
    while let Some(relative_index) = upper.get(offset..).and_then(|tail| tail.find(" IN ")) {
        let after_in = offset + relative_index + 4;
        let Some(open_relative) = expression.get(after_in..).and_then(|tail| tail.find('(')) else {
            break;
        };
        let open = after_in + open_relative;
        let mut depth = 1usize;
        let args_start = open + 1;
        let Some(tail) = expression.get(args_start..) else {
            break;
        };
        let mut found_close = false;
        for (relative_arg_index, ch) in tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let close = args_start + relative_arg_index;
                        if let Some(args) = expression.get(args_start..close) {
                            lists.push(args);
                        }
                        offset = close + 1;
                        found_close = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !found_close {
            break;
        }
    }
    lists
}

#[test]
fn given_in_operand_shapes_when_scanned_then_preserve_nested_unicode_and_malformed_results() {
    for expression in [
        "tenant_key = :v0",
        "tenant_key IN (:v0, :v1)",
        "étiquette in (:v0, function(:v1)) AND x IN (:v2)",
        "x IN (",
        "x IN",
        "x IN ((:v0)",
        "x IN (:v0))",
        "x IN (:v0) AND y in (:v1)",
        "x IN (:v0) AND y IN (",
    ] {
        let actual: Vec<_> = super::shape::in_operand_lists(expression).collect();
        assert_eq!(actual, legacy_in_operand_lists(expression), "{expression}");
    }
}

#[test]
fn given_in_operand_limits_when_validated_then_allow_one_hundred_and_reject_one_hundred_one() {
    for count in [1, 100, 101] {
        let values: crate::AttributeMap = (0..count)
            .map(|index| (format!(":v{index}"), AttributeValue::N(index.to_string())))
            .collect();
        let operands = values.keys().collect::<Vec<_>>().join(", ");
        let expression = format!("tenant_key iN ({operands})");
        let result = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Condition)],
            None,
            Some(&values),
            false,
        );
        if count <= 100 {
            result.unwrap();
        } else {
            assert_eq!(
                result.unwrap_err(),
                "Invalid ConditionExpression: The IN operator is provided with too many operands; \
                 number of operands: 101"
            );
        }
    }
}

#[test]
fn given_invalid_function_case_when_validated_then_preserve_function_name_error() {
    for name in ["ATTRIBUTE_EXISTS", "Attribute_exists", "aTtRiBuTe_ExIsTs"] {
        let expression = format!("{name} (tenant_key)");
        let error = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Condition)],
            None,
            None,
            false,
        )
        .unwrap_err();
        assert_eq!(
            error,
            format!("Invalid ConditionExpression: Invalid function name; function: {name}")
        );
    }
}

fn legacy_expression_function_names(expression: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut index = 0usize;
    while index < expression.len() {
        let Some((relative_start, _)) = expression.get(index..).and_then(|tail| {
            tail.char_indices()
                .find(|(_, ch)| ch.is_ascii_alphabetic() || *ch == '_')
        }) else {
            break;
        };
        let start = index + relative_start;
        let mut end = start;
        for (relative_index, ch) in expression.get(start..).unwrap_or_default().char_indices() {
            if relative_index == 0 {
                end = start + ch.len_utf8();
                continue;
            }
            if ch.is_ascii_alphanumeric() || ch == '_' {
                end = start + relative_index + ch.len_utf8();
            } else {
                break;
            }
        }
        let tail = expression.get(end..).unwrap_or_default().trim_start();
        if tail.starts_with('(')
            && let Some(name) = expression.get(start..end)
        {
            names.push(name);
        }
        index = end;
    }
    names
}

#[test]
fn given_function_name_shapes_when_scanned_then_preserve_borrowed_names() {
    for expression in [
        "field",
        "function (nested(value))",
        "étiquette.attr(x)",
        "ATTRIBUTE_EXISTS (tenant_key)",
        "_func\u{2003}(x)",
        "12call(x)",
    ] {
        assert_eq!(
            super::shape::expression_function_names(expression).collect::<Vec<_>>(),
            legacy_expression_function_names(expression)
        );
    }
}

#[test]
fn given_invalid_update_values_when_validated_then_preserve_payload_and_operand_errors() {
    for keyword in ["ADD", "DELETE"] {
        let values =
            crate::AttributeMap::from_iter([(":delta".to_owned(), AttributeValue::NS(Vec::new()))]);
        let expression = format!("{keyword} tally_key :delta");
        let error = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Update)],
            None,
            Some(&values),
            false,
        )
        .unwrap_err();
        assert_eq!(
            error,
            "One or more parameter values were invalid: An number set  may not be empty"
        );
        let values = crate::AttributeMap::from_iter([(
            ":delta".to_owned(),
            AttributeValue::S("text".to_owned()),
        )]);
        let error = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Update)],
            None,
            Some(&values),
            false,
        )
        .unwrap_err();
        assert!(
            error.contains(&format!("operator: {keyword}, operand type: S")),
            "{error}"
        );
    }
}

#[test]
fn given_reversed_between_bounds_when_validated_then_each_kind_reports_its_own_prefix() {
    for (kind, expression) in [
        (ExpressionKind::Condition, "sk BETWEEN :a AND :b"),
        (ExpressionKind::Filter, "sk BETWEEN :a AND :b"),
        (
            ExpressionKind::KeyCondition,
            "pk = :a AND sk BETWEEN :a AND :b",
        ),
    ] {
        let values = crate::AttributeMap::from_iter([
            (":a".to_owned(), AttributeValue::N("5".to_owned())),
            (":b".to_owned(), AttributeValue::N("4".to_owned())),
        ]);
        let error = validate_expression_set([(Some(expression), kind)], None, Some(&values), false)
            .unwrap_err();
        assert_eq!(
            error,
            format!(
                "Invalid {kind}: The BETWEEN operator requires upper bound to be greater than or \
                 equal to lower bound; lower bound operand: AttributeValue: {{N:5}}, upper bound \
                 operand: AttributeValue: {{N:4}}"
            )
        );
        let ordered = crate::AttributeMap::from_iter([
            (":a".to_owned(), AttributeValue::N("4".to_owned())),
            (":b".to_owned(), AttributeValue::N("4".to_owned())),
        ]);
        validate_expression_set([(Some(expression), kind)], None, Some(&ordered), false).unwrap();
    }
}
