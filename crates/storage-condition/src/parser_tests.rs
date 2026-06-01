use std::collections::HashMap;

use storage_types::AttributeValue;

use crate::{Condition, SizeComparison, evaluate_condition, parse_condition_expression};

// Parser tests
#[test]
fn parse_simple_equality_condition() {
    let condition_expr = "name = :value";

    let mut attribute_values = HashMap::new();
    attribute_values.insert(":value".to_string(), AttributeValue::S("John".to_string()));

    let result = parse_condition_expression(condition_expr, None, Some(&attribute_values));
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: "name".to_string(),
        value: AttributeValue::S("John".to_string()),
    };
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn parse_attribute_names_substitution() {
    let condition_expr = "#n = :value";

    let mut attribute_names = HashMap::new();
    attribute_names.insert("#n".to_string(), "name".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(":value".to_string(), AttributeValue::S("John".to_string()));

    let result = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    );
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: "name".to_string(),
        value: AttributeValue::S("John".to_string()),
    };
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn parse_comparison_operators() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":value".to_string(), AttributeValue::N("25".to_string()));

    // Test less than
    let result = parse_condition_expression("age < :value", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::LessThan { .. }));

    // Test less than or equal
    let result = parse_condition_expression("age <= :value", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::LessThanEqual { .. }));

    // Test greater than
    let result = parse_condition_expression("age > :value", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::GreaterThan { .. }));

    // Test greater than or equal
    let result = parse_condition_expression("age >= :value", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(
        result.unwrap(),
        Condition::GreaterThanEqual { .. }
    ));

    // Test equality
    let result = parse_condition_expression("age = :value", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::Equal { .. }));
}

#[test]
fn parse_between_condition() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":min".to_string(), AttributeValue::N("20".to_string()));
    attribute_values.insert(":max".to_string(), AttributeValue::N("30".to_string()));

    let result =
        parse_condition_expression("age BETWEEN :min AND :max", None, Some(&attribute_values));
    assert!(result.is_ok());

    let expected = Condition::Between {
        field: "age".to_string(),
        min: "20".to_string(),
        max: "30".to_string(),
    };
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn parse_between_condition_rejects_reversed_literal_bounds() {
    for (lower, upper, expected) in [
        (
            AttributeValue::N("5".to_string()),
            AttributeValue::N("4".to_string()),
            "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater \
             than or equal to lower bound; lower bound operand: AttributeValue: {N:5}, upper \
             bound operand: AttributeValue: {N:4}",
        ),
        (
            AttributeValue::S("z".to_string()),
            AttributeValue::S("a".to_string()),
            "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater \
             than or equal to lower bound; lower bound operand: AttributeValue: {S:z}, upper \
             bound operand: AttributeValue: {S:a}",
        ),
        (
            AttributeValue::B("eg==".to_string()),
            AttributeValue::B("YQ==".to_string()),
            "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be greater \
             than or equal to lower bound; lower bound operand: AttributeValue: {B:eg==}, upper \
             bound operand: AttributeValue: {B:YQ==}",
        ),
    ] {
        let mut attribute_values = HashMap::new();
        attribute_values.insert(":min".to_string(), lower);
        attribute_values.insert(":max".to_string(), upper);

        assert_eq!(
            parse_condition_expression("pk BETWEEN :min AND :max", None, Some(&attribute_values))
                .expect_err("reversed BETWEEN bounds should fail"),
            expected
        );
    }
}

#[test]
fn parse_in_condition() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":values".to_string(),
        AttributeValue::SS(vec!["John".to_string(), "Bob".to_string()]),
    );

    let result = parse_condition_expression("name IN (:values)", None, Some(&attribute_values));
    assert!(result.is_ok());

    let expected = Condition::In {
        field: "name".to_string(),
        values: vec![
            AttributeValue::S("John".to_string()),
            AttributeValue::S("Bob".to_string()),
        ],
    };
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn parse_function_conditions() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":prefix".to_string(), AttributeValue::S("Jo".to_string()));
    attribute_values.insert(
        ":substring".to_string(),
        AttributeValue::S("oh".to_string()),
    );
    attribute_values.insert(":size_val".to_string(), AttributeValue::N("4".to_string()));
    attribute_values.insert(":type".to_string(), AttributeValue::S("S".to_string()));

    // Test attribute_exists
    let result = parse_condition_expression("attribute_exists(name)", None, None);
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::Exists { .. }));

    // Test attribute_not_exists
    let result = parse_condition_expression("attribute_not_exists(deleted)", None, None);
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::NotExists { .. }));

    // Test begins_with
    let result =
        parse_condition_expression("begins_with(name, :prefix)", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::BeginsWith { .. }));

    // Test contains
    let result =
        parse_condition_expression("contains(name, :substring)", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::Contains { .. }));

    // Test size
    let result =
        parse_condition_expression("size(name) = :size_val", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::Size { .. }));

    let result =
        parse_condition_expression("size(name) > :size_val", None, Some(&attribute_values));
    assert!(matches!(
        result.unwrap(),
        Condition::SizeCompare {
            operator: SizeComparison::GreaterThan,
            ..
        }
    ));

    let result = parse_condition_expression(
        "size(name) BETWEEN :size_val AND :size_max",
        None,
        Some(&HashMap::from([
            (":size_val".to_string(), AttributeValue::N("1".to_string())),
            (":size_max".to_string(), AttributeValue::N("8".to_string())),
        ])),
    );
    assert!(matches!(result.unwrap(), Condition::And { .. }));

    let result =
        parse_condition_expression("attribute_type(name, :type)", None, Some(&attribute_values));
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::AttributeType { .. }));
}

#[test]
fn parse_literal_equality_condition() {
    let values = HashMap::from([(":value".to_string(), AttributeValue::S("same".to_string()))]);

    let condition = parse_condition_expression(":value = :value", None, Some(&values)).unwrap();

    assert_eq!(
        condition,
        Condition::ValueEqual {
            left: AttributeValue::S("same".to_string()),
            right: AttributeValue::S("same".to_string()),
        }
    );
}

#[test]
fn parse_function_document_paths_and_contains_same_operand_error() {
    let mut attribute_names = HashMap::new();
    attribute_names.insert("#m".to_string(), "map".to_string());
    attribute_names.insert("#c".to_string(), "child".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(":size".to_string(), AttributeValue::N("2".to_string()));
    attribute_values.insert(":value".to_string(), AttributeValue::S("x".to_string()));

    let result = parse_condition_expression(
        "size(#m.#c) = :size AND contains(#m.#c, :value)",
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .expect("nested function paths should parse");
    assert!(matches!(result, Condition::And { .. }));

    let error = parse_condition_expression("contains(name, name)", None, None)
        .expect_err("contains must reject identical path and operand");
    assert_eq!(
        error,
        "Invalid ConditionExpression: The first operand must be distinct from the remaining \
         operands for this operator or function; operator: contains, first operand: [name]"
    );
}

#[test]
fn parse_rejects_leading_underscore_attribute_name() {
    let result = parse_condition_expression("attribute_not_exists(_v)", None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unexpected character: _"));
}

#[test]
fn parse_accepts_leading_underscore_via_expression_attribute_name_alias() {
    let mut attribute_names = HashMap::new();
    attribute_names.insert("#v".to_string(), "_v".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(":v".to_string(), AttributeValue::N("1".to_string()));

    let result = parse_condition_expression(
        "attribute_not_exists(#v) OR #v = :v",
        Some(&attribute_names),
        Some(&attribute_values),
    );
    assert!(result.is_ok());
}

#[test]
fn parse_logical_operators() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":name_val".to_string(),
        AttributeValue::S("John".to_string()),
    );
    attribute_values.insert(":age_val".to_string(), AttributeValue::N("25".to_string()));

    // Test AND
    let result = parse_condition_expression(
        "name = :name_val AND age > :age_val",
        None,
        Some(&attribute_values),
    );
    assert!(result.is_ok());

    match result.unwrap() {
        Condition::And { conditions } => {
            assert_eq!(conditions.len(), 2);
            assert!(matches!(conditions[0], Condition::Equal { .. }));
            assert!(matches!(conditions[1], Condition::GreaterThan { .. }));
        }
        _ => panic!("Expected AND condition"),
    }

    // Test OR
    let result = parse_condition_expression(
        "name = :name_val OR age > :age_val",
        None,
        Some(&attribute_values),
    );
    assert!(result.is_ok());

    match result.unwrap() {
        Condition::Or { conditions } => {
            assert_eq!(conditions.len(), 2);
            assert!(matches!(conditions[0], Condition::Equal { .. }));
            assert!(matches!(conditions[1], Condition::GreaterThan { .. }));
        }
        _ => panic!("Expected OR condition"),
    }
}

#[test]
fn parse_parenthesized_expressions() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":name_val".to_string(),
        AttributeValue::S("John".to_string()),
    );
    attribute_values.insert(":age_val".to_string(), AttributeValue::N("25".to_string()));
    attribute_values.insert(":active_val".to_string(), AttributeValue::BOOL(true));

    let condition_expr = "(name = :name_val AND age > :age_val) OR active = :active_val";
    let result = parse_condition_expression(condition_expr, None, Some(&attribute_values));
    assert!(result.is_ok());

    match result.unwrap() {
        Condition::Or { conditions } => {
            assert_eq!(conditions.len(), 2);
            assert!(matches!(conditions[0], Condition::And { .. }));
            assert!(matches!(conditions[1], Condition::Equal { .. }));
        }
        _ => panic!("Expected OR condition with parenthesized AND"),
    }
}

#[test]
fn parse_parenthesized_comparison_operands() {
    let mut attribute_names = HashMap::new();
    attribute_names.insert("#name".to_string(), "name".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":name_val".to_string(),
        AttributeValue::S("John".to_string()),
    );

    let condition = parse_condition_expression(
        "(#name) = (:name_val)",
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .expect("parenthesized comparison operands should parse");

    assert_eq!(
        condition,
        Condition::Equal {
            field: "name".to_string(),
            value: AttributeValue::S("John".to_string()),
        }
    );
}

#[test]
fn parse_vault_lock_condition_expression() {
    let condition_expr = "attribute_exists(#path) and attribute_exists(#key) and \
                          (attribute_not_exists(#identity) or #identity = :identity or #expires \
                          <= :now)";

    let mut attribute_names = HashMap::new();
    attribute_names.insert("#path".to_string(), "Path".to_string());
    attribute_names.insert("#key".to_string(), "Key".to_string());
    attribute_names.insert("#identity".to_string(), "Identity".to_string());
    attribute_names.insert("#expires".to_string(), "Expires".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":identity".to_string(),
        AttributeValue::B("YWQ1MjVkMjctNDI1Yy02NDJmLTEyNzYtYzUzNjIzMGY3ODY5".to_string()),
    );
    attribute_values.insert(
        ":now".to_string(),
        AttributeValue::N("1767618242388958663".to_string()),
    );

    let condition = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .expect("condition parse failed");

    let mut item = HashMap::new();
    item.insert("Path".to_string(), AttributeValue::S("vault/".to_string()));
    item.insert("Key".to_string(), AttributeValue::S("lock".to_string()));
    item.insert(
        "Identity".to_string(),
        AttributeValue::B("YWQ1MjVkMjctNDI1Yy02NDJmLTEyNzYtYzUzNjIzMGY3ODY5".to_string()),
    );
    item.insert(
        "Expires".to_string(),
        AttributeValue::N("1767616283868417977".to_string()),
    );
    item.insert(
        "Value".to_string(),
        AttributeValue::B("MWJlNTc1YjgtOWUzNy1kMzAxLTdlMWYtYWRjNzczYWY2YWJh".to_string()),
    );

    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn parse_vault_lock_condition_expression_missing_keys() {
    let condition_expr = "attribute_exists(#path) and attribute_exists(#key) and \
                          (attribute_not_exists(#identity) or #identity = :identity or #expires \
                          <= :now)";

    let mut attribute_names = HashMap::new();
    attribute_names.insert("#path".to_string(), "Path".to_string());
    attribute_names.insert("#key".to_string(), "Key".to_string());
    attribute_names.insert("#identity".to_string(), "Identity".to_string());
    attribute_names.insert("#expires".to_string(), "Expires".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":identity".to_string(),
        AttributeValue::B("YWQ1MjVkMjctNDI1Yy02NDJmLTEyNzYtYzUzNjIzMGY3ODY5".to_string()),
    );
    attribute_values.insert(
        ":now".to_string(),
        AttributeValue::N("1767618242388958663".to_string()),
    );

    let condition = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .expect("condition parse failed");

    let mut item = HashMap::new();
    item.insert(
        "Identity".to_string(),
        AttributeValue::B("YWQ1MjVkMjctNDI1Yy02NDJmLTEyNzYtYzUzNjIzMGY3ODY5".to_string()),
    );
    item.insert(
        "Expires".to_string(),
        AttributeValue::N("1767616283868417977".to_string()),
    );
    item.insert(
        "Value".to_string(),
        AttributeValue::B("MWJlNTc1YjgtOWUzNy1kMzAxLTdlMWYtYWRjNzczYWY2YWJh".to_string()),
    );

    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn parse_not_operator() {
    // Test NOT with exists
    let result = parse_condition_expression("NOT attribute_exists(name)", None, None);
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::Not { .. }));

    // Test NOT with not_exists (double negative)
    let result = parse_condition_expression("NOT attribute_not_exists(name)", None, None);
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::Not { .. }));
}

#[test]
fn parse_condition_precedence_matches_dynamodb() {
    let mut values = HashMap::new();
    values.insert(":x".to_string(), AttributeValue::S("x".to_string()));
    values.insert(":y".to_string(), AttributeValue::S("y".to_string()));
    values.insert(":bad".to_string(), AttributeValue::S("bad".to_string()));
    values.insert(":n3".to_string(), AttributeValue::N("3".to_string()));
    values.insert(":n7".to_string(), AttributeValue::N("7".to_string()));
    values.insert(":z".to_string(), AttributeValue::S("z".to_string()));

    let not_vs_or =
        parse_condition_expression("NOT a = :x OR b = :y", None, Some(&values)).unwrap();
    match not_vs_or {
        Condition::Or { conditions } => {
            assert!(matches!(conditions[0], Condition::Not { .. }));
            assert!(matches!(conditions[1], Condition::Equal { .. }));
        }
        other => panic!("expected NOT to bind inside OR, got {other:?}"),
    }

    let and_vs_or =
        parse_condition_expression("a = :x OR b = :y AND c = :bad", None, Some(&values)).unwrap();
    match and_vs_or {
        Condition::Or { conditions } => {
            assert!(matches!(conditions[0], Condition::Equal { .. }));
            assert!(matches!(conditions[1], Condition::And { .. }));
        }
        other => panic!("expected AND to bind inside OR, got {other:?}"),
    }

    let parenthesized =
        parse_condition_expression("(a = :x OR b = :y) AND c = :bad", None, Some(&values)).unwrap();
    match parenthesized {
        Condition::And { conditions } => {
            assert!(matches!(conditions[0], Condition::Or { .. }));
            assert!(matches!(conditions[1], Condition::Equal { .. }));
        }
        other => panic!("expected parentheses to override AND/OR precedence, got {other:?}"),
    }

    let between_internal_and =
        parse_condition_expression("n BETWEEN :n3 AND :n7 AND c = :z", None, Some(&values))
            .unwrap();
    match between_internal_and {
        Condition::And { conditions } => {
            assert!(matches!(conditions[0], Condition::Between { .. }));
            assert!(matches!(conditions[1], Condition::Equal { .. }));
        }
        other => panic!("expected BETWEEN to consume its own AND, got {other:?}"),
    }
}

#[test]
fn parse_string_literals() {
    // Test with double quotes
    let result = parse_condition_expression("name = \"John Doe\"", None, None);
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: "name".to_string(),
        value: AttributeValue::S("John Doe".to_string()),
    };
    assert_eq!(result.unwrap(), expected);

    // Test with single quotes
    let result = parse_condition_expression("name = 'Jane Smith'", None, None);
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: "name".to_string(),
        value: AttributeValue::S("Jane Smith".to_string()),
    };
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn parse_number_literals() {
    let result = parse_condition_expression("age = 25", None, None);
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: "age".to_string(),
        value: AttributeValue::N("25".to_string()),
    };
    assert_eq!(result.unwrap(), expected);

    // Test negative numbers
    let result = parse_condition_expression("balance = -100", None, None);
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: "balance".to_string(),
        value: AttributeValue::N("-100".to_string()),
    };
    assert_eq!(result.unwrap(), expected);
}

#[test]
fn parse_errors() {
    // Missing attribute value
    let result = parse_condition_expression("name = :missing", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("not found in ExpressionAttributeValues")
    );

    // Missing attribute name
    let result = parse_condition_expression("#missing = :value", None, None);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .contains("not found in ExpressionAttributeNames")
    );

    // Unterminated string
    let result = parse_condition_expression("name = \"unterminated", None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unterminated string literal"));

    // Invalid character
    let result = parse_condition_expression("name @ :value", None, None);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Unexpected character"));

    // Incomplete expression
    let result = parse_condition_expression("name =", None, None);
    assert!(result.is_err());

    // Missing closing parenthesis
    let result = parse_condition_expression("(name = :value", None, None);
    assert!(result.is_err());

    // Unsupported operator
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":value".to_string(), AttributeValue::S("test".to_string()));
    let result = parse_condition_expression("name <!> :value", None, Some(&attribute_values));
    assert!(result.is_err());
}

#[test]
fn parse_complex_expression() {
    let mut attribute_names = HashMap::new();
    attribute_names.insert("#n".to_string(), "name".to_string());
    attribute_names.insert("#a".to_string(), "age".to_string());
    attribute_names.insert("#s".to_string(), "status".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":name_val".to_string(),
        AttributeValue::S("John".to_string()),
    );
    attribute_values.insert(":min_age".to_string(), AttributeValue::N("18".to_string()));
    attribute_values.insert(":max_age".to_string(), AttributeValue::N("65".to_string()));
    attribute_values.insert(
        ":status_val".to_string(),
        AttributeValue::S("active".to_string()),
    );

    let condition_expr =
        "(#n = :name_val AND #a BETWEEN :min_age AND :max_age) OR #s = :status_val";
    let result = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    );

    assert!(result.is_ok());

    // Verify the structure - should be an OR with two conditions
    match result.unwrap() {
        Condition::Or { conditions } => {
            assert_eq!(conditions.len(), 2);

            // First condition should be an AND
            match &conditions[0] {
                Condition::And {
                    conditions: and_conditions,
                } => {
                    assert_eq!(and_conditions.len(), 2);
                    assert!(matches!(and_conditions[0], Condition::Equal { .. }));
                    assert!(matches!(and_conditions[1], Condition::Between { .. }));
                }
                _ => panic!("Expected AND condition"),
            }

            // Second condition should be an equality
            assert!(matches!(conditions[1], Condition::Equal { .. }));
        }
        _ => panic!("Expected OR condition"),
    }
}

#[test]
fn parse_case_insensitive_keywords() {
    let mut attribute_values = HashMap::new();
    attribute_values.insert(":val1".to_string(), AttributeValue::S("test1".to_string()));
    attribute_values.insert(":val2".to_string(), AttributeValue::S("test2".to_string()));

    // Test lowercase
    let result = parse_condition_expression(
        "name = :val1 and age = :val2",
        None,
        Some(&attribute_values),
    );
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::And { .. }));

    // Test uppercase
    let result = parse_condition_expression(
        "name = :val1 AND age = :val2",
        None,
        Some(&attribute_values),
    );
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::And { .. }));

    // Test mixed case
    let result = parse_condition_expression(
        "name = :val1 AnD age = :val2",
        None,
        Some(&attribute_values),
    );
    assert!(result.is_ok());
    assert!(matches!(result.unwrap(), Condition::And { .. }));
}

#[test]
fn parse_and_evaluate_integration() {
    // Create a test item
    let mut item = HashMap::new();
    item.insert(
        "name".to_string(),
        AttributeValue::S("John Doe".to_string()),
    );
    item.insert("age".to_string(), AttributeValue::N("25".to_string()));
    item.insert(
        "status".to_string(),
        AttributeValue::S("active".to_string()),
    );

    // Set up attribute names and values
    let mut attribute_names = HashMap::new();
    attribute_names.insert("#n".to_string(), "name".to_string());
    attribute_names.insert("#a".to_string(), "age".to_string());
    attribute_names.insert("#s".to_string(), "status".to_string());

    let mut attribute_values = HashMap::new();
    attribute_values.insert(
        ":name_val".to_string(),
        AttributeValue::S("John Doe".to_string()),
    );
    attribute_values.insert(":min_age".to_string(), AttributeValue::N("18".to_string()));
    attribute_values.insert(
        ":status_val".to_string(),
        AttributeValue::S("active".to_string()),
    );

    // Test case 1: Simple equality that should match
    let condition_expr = "#n = :name_val";
    let condition = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .unwrap();
    assert!(evaluate_condition(&item, &condition));

    // Test case 2: Greater than that should match
    let condition_expr = "#a > :min_age";
    let condition = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .unwrap();
    assert!(evaluate_condition(&item, &condition));

    // Test case 3: Complex AND condition that should match
    let condition_expr = "#n = :name_val AND #s = :status_val";
    let condition = parse_condition_expression(
        condition_expr,
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .unwrap();
    assert!(evaluate_condition(&item, &condition));

    // Test case 4: Function condition with contains
    let mut more_values = attribute_values.clone();
    more_values.insert(
        ":substring".to_string(),
        AttributeValue::S("John".to_string()),
    );

    let condition_expr = "contains(#n, :substring)";
    let condition =
        parse_condition_expression(condition_expr, Some(&attribute_names), Some(&more_values))
            .unwrap();
    assert!(evaluate_condition(&item, &condition));

    // Test case 5: Attribute exists
    let condition_expr = "attribute_exists(#n)";
    let condition =
        parse_condition_expression(condition_expr, Some(&attribute_names), None).unwrap();
    assert!(evaluate_condition(&item, &condition));

    // Test case 6: Condition that should NOT match
    more_values.insert(
        ":wrong_name".to_string(),
        AttributeValue::S("Jane Doe".to_string()),
    );
    let condition_expr = "#n = :wrong_name";
    let condition =
        parse_condition_expression(condition_expr, Some(&attribute_names), Some(&more_values))
            .unwrap();
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn not_equal_operator() {
    let expression = "id <> :value";
    let mut expression_attribute_values = HashMap::new();
    expression_attribute_values.insert(":value".to_string(), AttributeValue::S("test".to_string()));

    let result = parse_condition_expression(expression, None, Some(&expression_attribute_values));

    // Now this should succeed
    assert!(result.is_ok());

    let condition = result.unwrap();

    // Test with equal value - should be false
    let mut item_equal = HashMap::new();
    item_equal.insert("id".to_string(), AttributeValue::S("test".to_string()));
    assert!(!evaluate_condition(&item_equal, &condition));

    // Test with different value - should be true
    let mut item_different = HashMap::new();
    item_different.insert("id".to_string(), AttributeValue::S("different".to_string()));
    assert!(evaluate_condition(&item_different, &condition));

    // Test with missing field - should be true (missing != any value)
    let item_missing = HashMap::new();
    assert!(evaluate_condition(&item_missing, &condition));
}

#[test]
fn not_equal_functionality_with_workaround() {
    // This shows what NOT EQUAL should do when we implement it
    let mut item = HashMap::new();
    item.insert("id".to_string(), AttributeValue::S("different".to_string()));

    // Using equal to show expected opposite behavior
    let expression = "id = :value";
    let mut expression_attribute_values = HashMap::new();
    expression_attribute_values.insert(":value".to_string(), AttributeValue::S("test".to_string()));

    let condition =
        parse_condition_expression(expression, None, Some(&expression_attribute_values)).unwrap();

    // Equal should be false (different != test)
    let equal_result = evaluate_condition(&item, &condition);
    assert!(!equal_result);

    // When we implement NOT EQUAL, it should return true for this case
    // (different <> test should be true)
}
