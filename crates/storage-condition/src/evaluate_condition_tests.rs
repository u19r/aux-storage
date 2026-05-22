use std::collections::HashMap;
#[cfg(test)]
use std::vec;

use storage_types::AttributeValue;

use crate::{Condition, SizeComparison, evaluate_condition};

fn create_test_item() -> HashMap<String, AttributeValue> {
    let mut item = HashMap::new();
    item.insert("name".to_string(), AttributeValue::S("John".to_string()));
    item.insert("age".to_string(), AttributeValue::N("25".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::B("YmluYXJ5X2RhdGE=".to_string()),
    );
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(vec!["rust".to_string(), "database".to_string()]),
    );
    item.insert(
        "scores".to_string(),
        AttributeValue::NS(vec!["100".to_string(), "95".to_string()]),
    );
    item.insert("active".to_string(), AttributeValue::BOOL(true));
    item.insert("deleted".to_string(), AttributeValue::NULL(true));
    // Add List type
    item.insert(
        "list_field".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("item1".to_string()),
            AttributeValue::N("42".to_string()),
            AttributeValue::BOOL(false),
        ]),
    );
    // Add Map type
    let mut map_value = HashMap::new();
    map_value.insert(
        "nested_name".to_string(),
        AttributeValue::S("nested_value".to_string()),
    );
    map_value.insert(
        "nested_count".to_string(),
        AttributeValue::N("10".to_string()),
    );
    item.insert("map_field".to_string(), AttributeValue::M(map_value));
    item
}

#[test]
fn exists_condition() {
    let item = create_test_item();

    let condition = Condition::Exists {
        field: "name".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Exists {
        field: "nonexistent".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn not_exists_condition() {
    let item = create_test_item();

    let condition = Condition::NotExists {
        field: "nonexistent".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::NotExists {
        field: "name".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn equal_condition() {
    let item = create_test_item();

    // String equality
    let condition = Condition::Equal {
        field: "name".to_string(),
        value: "John".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Equal {
        field: "name".to_string(),
        value: "Jane".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Number equality
    let condition = Condition::Equal {
        field: "age".to_string(),
        value: "25".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    // Binary equality
    let condition = Condition::Equal {
        field: "data".to_string(),
        value: "YmluYXJ5X2RhdGE=".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    // Boolean equality
    let condition = Condition::Equal {
        field: "active".to_string(),
        value: "true".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    // NULL equality
    let condition = Condition::Equal {
        field: "deleted".to_string(),
        value: "null".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn less_than_condition() {
    let item = create_test_item();

    // String comparison
    let condition = Condition::LessThan {
        field: "name".to_string(),
        value: "Zebra".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::LessThan {
        field: "name".to_string(),
        value: "Alice".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Number comparison
    let condition = Condition::LessThan {
        field: "age".to_string(),
        value: "30".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn less_than_equal_condition() {
    let item = create_test_item();

    let condition = Condition::LessThanEqual {
        field: "name".to_string(),
        value: "John".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::LessThanEqual {
        field: "name".to_string(),
        value: "Alice".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::LessThanEqual {
        field: "age".to_string(),
        value: "30".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn greater_than_condition() {
    let item = create_test_item();

    // String comparison
    let condition = Condition::GreaterThan {
        field: "name".to_string(),
        value: "Alice".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::GreaterThan {
        field: "name".to_string(),
        value: "Zebra".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Number comparison
    let condition = Condition::GreaterThan {
        field: "age".to_string(),
        value: "20".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn greater_than_equal_condition() {
    let item = create_test_item();

    let condition = Condition::GreaterThanEqual {
        field: "name".to_string(),
        value: "John".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::GreaterThanEqual {
        field: "name".to_string(),
        value: "Zebra".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::GreaterThanEqual {
        field: "age".to_string(),
        value: "25".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn between_condition() {
    let item = create_test_item();

    // String between
    let condition = Condition::Between {
        field: "name".to_string(),
        min: "Alice".to_string(),
        max: "Zebra".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Between {
        field: "name".to_string(),
        min: "Zebra".to_string(),
        max: "Zzz".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Number between
    let condition = Condition::Between {
        field: "age".to_string(),
        min: "20".to_string(),
        max: "30".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn in_condition() {
    let item = create_test_item();

    // String in
    let condition = Condition::In {
        field: "name".to_string(),
        values: vec!["John".to_string(), "Alice".to_string()],
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::In {
        field: "name".to_string(),
        values: vec!["Jane".to_string(), "Bob".to_string()],
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn contains_condition() {
    let item = create_test_item();

    // Test string contains
    let condition = Condition::Contains {
        field: "name".to_string(),
        value: AttributeValue::S("oh".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: "name".to_string(),
        value: AttributeValue::S("Jane".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test full string match
    let condition = Condition::Contains {
        field: "name".to_string(),
        value: AttributeValue::S("John".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    // Test binary data contains
    let condition = Condition::Contains {
        field: "data".to_string(),
        value: AttributeValue::B("YmluYXJ5".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: "data".to_string(),
        value: AttributeValue::B("ZGF0YQ==".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: "data".to_string(),
        value: AttributeValue::B("bWlzc2luZw==".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test string set contains exact elements.
    let condition = Condition::Contains {
        field: "tags".to_string(),
        value: AttributeValue::S("rust".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: "tags".to_string(),
        value: AttributeValue::S("data".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: "tags".to_string(),
        value: AttributeValue::S("python".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test with non-string/binary field (should return false)
    let condition = Condition::Contains {
        field: "age".to_string(),
        value: AttributeValue::N("2".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test with missing field
    let condition = Condition::Contains {
        field: "missing".to_string(),
        value: AttributeValue::S("value".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test edge case: empty value
    let condition = Condition::Contains {
        field: "name".to_string(),
        value: AttributeValue::S(String::new()),
    };
    assert!(evaluate_condition(&item, &condition)); // Empty string is contained in any string

    // Test case sensitivity
    let condition = Condition::Contains {
        field: "name".to_string(),
        value: AttributeValue::S("JOHN".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition)); // Case sensitive

    // Test list contains
    let condition = Condition::Contains {
        field: "list_field".to_string(),
        value: AttributeValue::S("item1".to_string()),
    };
    assert!(evaluate_condition(&item, &condition)); // List contains "item1"

    let condition = Condition::Contains {
        field: "list_field".to_string(),
        value: AttributeValue::N("42".to_string()),
    };
    assert!(evaluate_condition(&item, &condition)); // List contains "42"

    // Test map contains (in values)
    let condition = Condition::Contains {
        field: "map_field".to_string(),
        value: AttributeValue::S("nested_value".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition)); // Map value contains "nested_value"

    let condition = Condition::Contains {
        field: "map_field".to_string(),
        value: AttributeValue::N("10".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition)); // Map value contains "10"
}

#[test]
fn begins_with_condition() {
    let item = create_test_item();

    let condition = Condition::BeginsWith {
        field: "name".to_string(),
        prefix: AttributeValue::S("Jo".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::BeginsWith {
        field: "name".to_string(),
        prefix: AttributeValue::S("Jane".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test with binary data
    let condition = Condition::BeginsWith {
        field: "data".to_string(),
        prefix: AttributeValue::B("YmluYXJ5".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn size_condition() {
    let item = create_test_item();

    // String size
    let condition = Condition::Size {
        field: "name".to_string(),
        size: 4,
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Size {
        field: "name".to_string(),
        size: 5,
    };
    assert!(!evaluate_condition(&item, &condition));

    // String set size
    let condition = Condition::Size {
        field: "tags".to_string(),
        size: 2,
    };
    assert!(evaluate_condition(&item, &condition));

    // Number set size
    let condition = Condition::Size {
        field: "scores".to_string(),
        size: 2,
    };
    assert!(evaluate_condition(&item, &condition));

    // List size
    let condition = Condition::Size {
        field: "list_field".to_string(),
        size: 3,
    };
    assert!(evaluate_condition(&item, &condition));

    // Map size
    let condition = Condition::Size {
        field: "map_field".to_string(),
        size: 2,
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::SizeCompare {
        field: "name".to_string(),
        operator: SizeComparison::GreaterThan,
        size: 3,
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn size_matches_dynamodb_for_unicode_binary_sets_lists_and_maps() {
    let mut item = HashMap::new();
    item.insert("ascii".to_string(), AttributeValue::S("abcd".to_string()));
    item.insert("unicode".to_string(), AttributeValue::S("é𝄞".to_string()));
    item.insert(
        "binary".to_string(),
        AttributeValue::B("AQIDBAU=".to_string()),
    );
    item.insert(
        "strings".to_string(),
        AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
    );
    item.insert(
        "numbers".to_string(),
        AttributeValue::NS(vec!["1".to_string(), "2".to_string(), "3".to_string()]),
    );
    item.insert(
        "binaries".to_string(),
        AttributeValue::BS(vec![
            "AQI=".to_string(),
            "AwQ=".to_string(),
            "BQY=".to_string(),
        ]),
    );
    item.insert(
        "list".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("alpha".to_string()),
            AttributeValue::N("42".to_string()),
            AttributeValue::BOOL(true),
        ]),
    );
    item.insert(
        "map".to_string(),
        AttributeValue::M(HashMap::from([
            ("a".to_string(), AttributeValue::S("x".to_string())),
            ("b".to_string(), AttributeValue::N("2".to_string())),
            (
                "deep".to_string(),
                AttributeValue::M(HashMap::from([
                    ("one".to_string(), AttributeValue::S("1".to_string())),
                    ("two".to_string(), AttributeValue::S("2".to_string())),
                ])),
            ),
        ])),
    );

    for (field, size) in [
        ("ascii", 4),
        ("unicode", 3),
        ("binary", 5),
        ("strings", 2),
        ("numbers", 3),
        ("binaries", 3),
        ("list", 3),
        ("map", 3),
        ("map.deep", 2),
        ("map.deep.one", 1),
    ] {
        assert!(
            evaluate_condition(
                &item,
                &Condition::Size {
                    field: field.to_string(),
                    size
                }
            ),
            "{field} should have DynamoDB size {size}"
        );
        assert!(
            !evaluate_condition(
                &item,
                &Condition::Size {
                    field: field.to_string(),
                    size: size + 1
                }
            ),
            "{field} should not have DynamoDB size {}",
            size + 1
        );
    }
}

#[test]
fn contains_matches_dynamodb_exact_collection_element_semantics() {
    let mut item = HashMap::new();
    item.insert("string".to_string(), AttributeValue::S("abcd".to_string()));
    item.insert(
        "strings".to_string(),
        AttributeValue::SS(vec!["red".to_string(), "blue".to_string()]),
    );
    item.insert(
        "numbers".to_string(),
        AttributeValue::NS(vec!["1".to_string(), "2".to_string()]),
    );
    item.insert(
        "binaries".to_string(),
        AttributeValue::BS(vec!["AQI=".to_string(), "AwQ=".to_string()]),
    );
    item.insert(
        "list".to_string(),
        AttributeValue::L(vec![
            AttributeValue::S("alpha".to_string()),
            AttributeValue::N("42".to_string()),
        ]),
    );

    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: "string".to_string(),
            value: AttributeValue::S("bc".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: "strings".to_string(),
            value: AttributeValue::S("red".to_string()),
        }
    ));
    assert!(!evaluate_condition(
        &item,
        &Condition::Contains {
            field: "strings".to_string(),
            value: AttributeValue::S("re".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: "numbers".to_string(),
            value: AttributeValue::N("2".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: "binaries".to_string(),
            value: AttributeValue::B("AwQ=".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: "list".to_string(),
            value: AttributeValue::S("alpha".to_string()),
        }
    ));
    assert!(!evaluate_condition(
        &item,
        &Condition::Contains {
            field: "list".to_string(),
            value: AttributeValue::S("alp".to_string()),
        }
    ));
}

#[test]
fn attribute_type_matches_dynamodb_type_codes() {
    let mut item = HashMap::new();
    item.insert("string".to_string(), AttributeValue::S("x".to_string()));
    item.insert("number".to_string(), AttributeValue::N("1".to_string()));
    item.insert("binary".to_string(), AttributeValue::B("AQI=".to_string()));
    item.insert("bool".to_string(), AttributeValue::BOOL(true));
    item.insert("null".to_string(), AttributeValue::NULL(true));
    item.insert("list".to_string(), AttributeValue::L(vec![]));
    item.insert("map".to_string(), AttributeValue::M(HashMap::new()));

    for (field, attribute_type) in [
        ("string", "S"),
        ("number", "N"),
        ("binary", "B"),
        ("bool", "BOOL"),
        ("null", "NULL"),
        ("list", "L"),
        ("map", "M"),
    ] {
        assert!(evaluate_condition(
            &item,
            &Condition::AttributeType {
                field: field.to_string(),
                attribute_type: attribute_type.to_string(),
            }
        ));
    }
}

#[test]
fn evaluate_condition_precedence_matches_dynamodb() {
    let item = HashMap::from([
        ("a".to_string(), AttributeValue::S("x".to_string())),
        ("b".to_string(), AttributeValue::S("y".to_string())),
        ("c".to_string(), AttributeValue::S("z".to_string())),
        ("n".to_string(), AttributeValue::N("5".to_string())),
        ("tag".to_string(), AttributeValue::S("alpha".to_string())),
    ]);
    let values = HashMap::from([
        (":x".to_string(), AttributeValue::S("x".to_string())),
        (":y".to_string(), AttributeValue::S("y".to_string())),
        (":bad".to_string(), AttributeValue::S("bad".to_string())),
        (":n3".to_string(), AttributeValue::N("3".to_string())),
        (":n7".to_string(), AttributeValue::N("7".to_string())),
        (":z".to_string(), AttributeValue::S("z".to_string())),
        (":tag".to_string(), AttributeValue::S("alp".to_string())),
    ]);

    for expression in [
        "NOT a = :x OR b = :y",
        "a = :x OR b = :y AND c = :bad",
        "n BETWEEN :n3 AND :n7 AND c = :z",
        "NOT begins_with(tag, :tag) OR b = :y",
    ] {
        let condition = crate::parse_condition_expression(expression, None, Some(&values))
            .expect("precedence expression should parse");
        assert!(
            evaluate_condition(&item, &condition),
            "{expression} should match DynamoDB precedence"
        );
    }

    for expression in ["NOT (a = :x OR b = :y)", "(a = :x OR b = :y) AND c = :bad"] {
        let condition = crate::parse_condition_expression(expression, None, Some(&values))
            .expect("parenthesized precedence expression should parse");
        assert!(
            !evaluate_condition(&item, &condition),
            "{expression} should match DynamoDB precedence"
        );
    }
}

#[test]
fn and_condition() {
    let item = create_test_item();

    let condition = Condition::And {
        conditions: vec![
            Condition::Exists {
                field: "name".to_string(),
            },
            Condition::Equal {
                field: "active".to_string(),
                value: "true".to_string(),
            },
        ],
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::And {
        conditions: vec![
            Condition::Exists {
                field: "name".to_string(),
            },
            Condition::Equal {
                field: "active".to_string(),
                value: "false".to_string(),
            },
        ],
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn or_condition() {
    let item = create_test_item();

    let condition = Condition::Or {
        conditions: vec![
            Condition::NotExists {
                field: "name".to_string(),
            },
            Condition::Equal {
                field: "active".to_string(),
                value: "true".to_string(),
            },
        ],
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Or {
        conditions: vec![
            Condition::NotExists {
                field: "name".to_string(),
            },
            Condition::Equal {
                field: "active".to_string(),
                value: "false".to_string(),
            },
        ],
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn deeply_nested_and_or_conditions() {
    let item = create_test_item();

    // Complex nested condition: (exists name AND age > 20) OR (not exists deleted
    // AND active = true)
    let condition = Condition::Or {
        conditions: vec![
            Condition::And {
                conditions: vec![
                    Condition::Exists {
                        field: "name".to_string(),
                    },
                    Condition::GreaterThan {
                        field: "age".to_string(),
                        value: "20".to_string(),
                    },
                ],
            },
            Condition::And {
                conditions: vec![
                    Condition::NotExists {
                        field: "missing".to_string(),
                    },
                    Condition::Equal {
                        field: "active".to_string(),
                        value: "true".to_string(),
                    },
                ],
            },
        ],
    };
    assert!(evaluate_condition(&item, &condition));

    // Deeply nested: ((name exists AND age > 30) OR (tags size = 2)) AND active =
    // true
    let condition = Condition::And {
        conditions: vec![
            Condition::Or {
                conditions: vec![
                    Condition::And {
                        conditions: vec![
                            Condition::Exists {
                                field: "name".to_string(),
                            },
                            Condition::GreaterThan {
                                field: "age".to_string(),
                                value: "30".to_string(),
                            },
                        ],
                    },
                    Condition::Size {
                        field: "tags".to_string(),
                        size: 2,
                    },
                ],
            },
            Condition::Equal {
                field: "active".to_string(),
                value: "true".to_string(),
            },
        ],
    };
    assert!(evaluate_condition(&item, &condition));

    // Triple nested: (((name begins with "J" AND age between 20-30) OR tags size =
    // 3) AND active = true) OR deleted = null
    let condition = Condition::Or {
        conditions: vec![
            Condition::And {
                conditions: vec![
                    Condition::Or {
                        conditions: vec![
                            Condition::And {
                                conditions: vec![
                                    Condition::BeginsWith {
                                        field: "name".to_string(),
                                        prefix: AttributeValue::S("J".to_string()),
                                    },
                                    Condition::Between {
                                        field: "age".to_string(),
                                        min: "20".to_string(),
                                        max: "30".to_string(),
                                    },
                                ],
                            },
                            Condition::Size {
                                field: "tags".to_string(),
                                size: 3,
                            },
                        ],
                    },
                    Condition::Equal {
                        field: "active".to_string(),
                        value: "true".to_string(),
                    },
                ],
            },
            Condition::Equal {
                field: "deleted".to_string(),
                value: "null".to_string(),
            },
        ],
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn condition_with_missing_fields() {
    let item = HashMap::new();

    // All conditions should return false for missing fields except NotExists
    let condition = Condition::Exists {
        field: "missing".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::NotExists {
        field: "missing".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Equal {
        field: "missing".to_string(),
        value: "value".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::LessThan {
        field: "missing".to_string(),
        value: "value".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn list_map_attribute_types() {
    // Test with List type
    let mut item = HashMap::new();
    let list_value = AttributeValue::L(vec![
        AttributeValue::S("test".to_string()),
        AttributeValue::N("123".to_string()),
    ]);
    item.insert("list_attr".to_string(), list_value);

    // Test Map type
    let mut map_value = HashMap::new();
    map_value.insert("key1".to_string(), AttributeValue::S("value1".to_string()));
    map_value.insert("key2".to_string(), AttributeValue::N("456".to_string()));
    let map_attr = AttributeValue::M(map_value);
    item.insert("map_attr".to_string(), map_attr);

    // Test size conditions
    let condition = Condition::Size {
        field: "list_attr".to_string(),
        size: 2,
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Size {
        field: "map_attr".to_string(),
        size: 2,
    };
    assert!(evaluate_condition(&item, &condition));

    // Test contains conditions
    let condition = Condition::Contains {
        field: "list_attr".to_string(),
        value: AttributeValue::S("test".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: "map_attr".to_string(),
        value: AttributeValue::S("value1".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test exists conditions
    let condition = Condition::Exists {
        field: "list_attr".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Exists {
        field: "map_attr".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}
