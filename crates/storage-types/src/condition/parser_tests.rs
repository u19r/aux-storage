use std::collections::HashMap;

use crate::{
    AttributeValue, KeySchemaElement, KeyType,
    condition::{
        AttributePath, Condition, PathElement, SizeComparison, evaluate_condition,
        parse_condition_expression, validate_key_condition,
    },
};

/// An unaliased path from its text; `.` separates map member names.
fn attr_path(text: &str) -> AttributePath {
    let mut names = text.split('.');
    let mut path = AttributePath::top(names.next().unwrap_or_default().to_string());
    for name in names {
        path.push(PathElement::Name(name.to_string()));
    }
    path
}

fn parsed_equal_field(expression: &str, names: &[(&str, &str)]) -> AttributePath {
    let attribute_names = names
        .iter()
        .map(|(placeholder, name)| ((*placeholder).to_string(), (*name).to_string()))
        .collect::<HashMap<_, _>>();
    let attribute_values = HashMap::from([(":v".to_string(), AttributeValue::S("v".to_string()))]);
    match parse_condition_expression(expression, Some(&attribute_names), Some(&attribute_values))
        .expect("condition parses")
    {
        Condition::Equal { field, .. } => field,
        other => panic!("expected Equal, found {other:?}"),
    }
}

#[test]
fn given_alias_naming_a_dotted_attribute_when_parsing_then_path_is_one_name_element() {
    let field = parsed_equal_field("#a = :v", &[("#a", "a.b")]);

    assert_eq!(field.elements(), [PathElement::Name("a.b".to_string())]);
    assert_eq!(field.top_level(), Some("a.b"));
    assert_eq!(field.root(), "a.b");
    assert_eq!(field.to_string(), "a.b");
}

#[test]
fn given_alias_naming_a_bracketed_attribute_when_parsing_then_path_is_one_name_element() {
    let field = parsed_equal_field("#l = :v", &[("#l", "l[0]")]);

    assert_eq!(field.elements(), [PathElement::Name("l[0]".to_string())]);
    assert_eq!(field.top_level(), Some("l[0]"));
}

#[test]
fn given_unaliased_nested_path_with_alias_segment_when_parsing_then_each_segment_is_one_element() {
    let field = parsed_equal_field("a.b[2].#c = :v", &[("#c", "x.y")]);

    assert_eq!(
        field.elements(),
        [
            PathElement::Name("a".to_string()),
            PathElement::Name("b".to_string()),
            PathElement::Index(2),
            PathElement::Name("x.y".to_string()),
        ]
    );
    assert_eq!(field.root(), "a");
    assert_eq!(field.top_level(), None);
    assert!(field != *"a");
}

#[test]
fn given_dotted_alias_as_both_contains_operands_when_parsing_then_same_operand_error_names_it() {
    let attribute_names = HashMap::from([("#a".to_string(), "a.b".to_string())]);
    let error = parse_condition_expression("contains(#a, #a)", Some(&attribute_names), None)
        .expect_err("contains must reject identical path and operand");

    assert!(error.ends_with("first operand: [a.b]"), "{error}");
}

fn query_key_schema() -> Vec<KeySchemaElement> {
    vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ]
}

#[test]
fn key_condition_validation_accepts_partition_and_sort_key_shapes() {
    let values = HashMap::from([
        (":pk".to_string(), AttributeValue::S("acct".to_string())),
        (":lo".to_string(), AttributeValue::S("a".to_string())),
        (":hi".to_string(), AttributeValue::S("z".to_string())),
    ]);
    let schema = query_key_schema();

    for expression in [
        "pk = :pk",
        ":pk = pk AND :lo <= sk",
        "pk = :pk AND sk BETWEEN :lo AND :hi",
        "pk = :pk AND begins_with(sk, :lo)",
    ] {
        let condition = parse_condition_expression(expression, None, Some(&values))
            .unwrap_or_else(|error| panic!("{expression} should parse: {error}"));
        validate_key_condition(&condition, &schema)
            .unwrap_or_else(|error| panic!("{expression} should be a key condition: {error}"));
    }
}

#[test]
fn given_nested_paths_when_validating_key_condition_then_nested_attribute_error() {
    let values = HashMap::from([
        (":a".to_string(), AttributeValue::S("a".to_string())),
        (":b".to_string(), AttributeValue::S("b".to_string())),
    ]);
    let hash_only = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];

    for (expression, schema) in [
        ("pk.x = :a", &hash_only),
        ("pk = :a AND pk.x > :b", &hash_only),
        ("pk = :a AND pk.x < :b", &hash_only),
        ("pk = :a AND pk.x <= :b", &hash_only),
        ("pk = :a AND pk.x >= :b", &hash_only),
        ("pk = :a AND foo.bar BETWEEN :a AND :b", &hash_only),
        ("pk = :a AND sk.x > :b", &query_key_schema()),
    ] {
        let condition = parse_condition_expression(expression, None, Some(&values)).unwrap();
        assert_eq!(
            validate_key_condition(&condition, schema).unwrap_err(),
            "KeyConditionExpressions cannot have conditions on nested attributes",
            "{expression}"
        );
    }

    let condition =
        parse_condition_expression("pk = :a AND foo > :b", None, Some(&values)).unwrap();
    assert_eq!(
        validate_key_condition(&condition, &hash_only).unwrap_err(),
        "The number of query conditions (2) exceeds the number of key attributes defined in the \
         schema (1)"
    );
}

#[test]
fn key_condition_validation_rejects_filter_only_operators_but_parser_keeps_filter_or() {
    let values = HashMap::from([
        (":pk".to_string(), AttributeValue::S("acct".to_string())),
        (":sk".to_string(), AttributeValue::S("a".to_string())),
    ]);
    let schema = query_key_schema();

    for expression in [
        "pk = :pk AND sk <> :sk",
        "pk = :pk OR sk = :sk",
        "pk = :pk AND contains(sk, :sk)",
        "pk = :pk AND begins_with(pk, :sk)",
        "pk = :pk AND sk > :sk AND sk <= :sk",
    ] {
        let condition = parse_condition_expression(expression, None, Some(&values))
            .unwrap_or_else(|error| panic!("{expression} should parse: {error}"));
        assert!(
            validate_key_condition(&condition, &schema).is_err(),
            "{expression} must not be accepted as a key condition"
        );
    }

    let filter = parse_condition_expression("pk = :pk OR sk = :sk", None, Some(&values))
        .expect("the shared parser must retain OR for filter expressions");
    assert!(matches!(filter, Condition::Or { .. }));
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
        field: attr_path("name"),
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
fn given_owned_literals_and_alias_paths_when_parsing_then_values_and_names_remain_reusable() {
    let attribute_names = HashMap::from([
        ("#root".to_string(), "根".to_string()),
        ("#child".to_string(), "枝".to_string()),
    ]);
    let attribute_values = HashMap::from([
        (":left".to_string(), AttributeValue::S("left".to_string())),
        (":lower".to_string(), AttributeValue::N("2".to_string())),
        (":upper".to_string(), AttributeValue::N("8".to_string())),
        (":null".to_string(), AttributeValue::NULL(true)),
    ]);

    let condition = parse_condition_expression(
        "#root.#child[2].leaf = \"葉\\q\\n\" AND (:left) < #root.#child[2].leaf AND \
         #root.#child[2].leaf BETWEEN :lower AND :upper AND #root.#child[2].leaf = :null",
        Some(&attribute_names),
        Some(&attribute_values),
    )
    .expect("owned token and alias paths should parse");
    assert!(matches!(condition, Condition::And { .. }));
    assert_eq!(attribute_names["#root"], "根");
    assert_eq!(
        attribute_values[":left"],
        AttributeValue::S("left".to_string())
    );
    assert_eq!(attribute_values[":null"], AttributeValue::NULL(true));
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
fn parse_number_literals() {
    let result = parse_condition_expression("age = 25", None, None);
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: attr_path("age"),
        value: AttributeValue::N("25".to_string()),
    };
    assert_eq!(result.unwrap(), expected);

    // Test negative numbers
    let result = parse_condition_expression("balance = -100", None, None);
    assert!(result.is_ok());

    let expected = Condition::Equal {
        field: attr_path("balance"),
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
fn given_size_comparison_when_evaluated_then_operand_compares_as_a_dynamodb_number() {
    let item = HashMap::from([("name".to_string(), AttributeValue::S("abc".to_string()))]);
    let number = |value: &str| AttributeValue::N(value.to_string());
    let text = |value: &str| AttributeValue::S(value.to_string());
    for (expression, first, second, expected) in [
        ("size(name) = :s", text("a"), text("b"), false),
        ("size(name) <> :s", text("a"), text("b"), true),
        ("size(name) < :s", text("a"), text("b"), false),
        ("size(name) >= :s", text("a"), text("b"), false),
        ("size(name) BETWEEN :s AND :t", text("a"), text("b"), false),
        ("size(name) <> :s", number("3"), text("b"), false),
        ("size(name) < :s", number("3.5"), text("b"), true),
        ("size(name) > :s", number("-1"), text("b"), true),
        ("size(name) = :s", number("3.0"), text("b"), true),
        (
            "size(name) BETWEEN :s AND :t",
            number("2.5"),
            number("3.5"),
            true,
        ),
        (
            "size(name) BETWEEN :s AND :t",
            number("3.5"),
            number("4"),
            false,
        ),
    ] {
        let values = HashMap::from([(":s".to_string(), first), (":t".to_string(), second)]);
        let condition = parse_condition_expression(expression, None, Some(&values)).unwrap();
        assert_eq!(
            evaluate_condition(&item, &condition),
            expected,
            "{expression}"
        );
    }
}
