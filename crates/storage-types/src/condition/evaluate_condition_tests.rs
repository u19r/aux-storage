use std::collections::HashMap;
#[cfg(test)]
use std::vec;

use crate::{
    AttributeMap, AttributeValue,
    condition::{AttributePath, Condition, PathElement, SizeComparison, evaluate_condition},
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
    item.insert(
        "map_field".to_string(),
        AttributeValue::M(AttributeMap::from(map_value)),
    );
    item
}

#[test]
fn less_than_equal_condition() {
    let item = create_test_item();

    let condition = Condition::LessThanEqual {
        field: attr_path("name"),
        value: "John".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::LessThanEqual {
        field: attr_path("name"),
        value: "Alice".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::LessThanEqual {
        field: attr_path("age"),
        value: "30".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn greater_than_condition() {
    let item = create_test_item();

    // String comparison
    let condition = Condition::GreaterThan {
        field: attr_path("name"),
        value: "Alice".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::GreaterThan {
        field: attr_path("name"),
        value: "Zebra".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Number comparison
    let condition = Condition::GreaterThan {
        field: attr_path("age"),
        value: "20".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn greater_than_equal_condition() {
    let item = create_test_item();

    let condition = Condition::GreaterThanEqual {
        field: attr_path("name"),
        value: "John".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::GreaterThanEqual {
        field: attr_path("name"),
        value: "Zebra".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::GreaterThanEqual {
        field: attr_path("age"),
        value: "25".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn between_condition() {
    let item = create_test_item();

    // String between
    let condition = Condition::Between {
        field: attr_path("name"),
        min: "Alice".to_string(),
        max: "Zebra".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Between {
        field: attr_path("name"),
        min: "Zebra".to_string(),
        max: "Zzz".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Number between
    let condition = Condition::Between {
        field: attr_path("age"),
        min: "20".to_string(),
        max: "30".to_string(),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn given_duplicate_sets_when_comparing_membership_then_existing_asymmetric_semantics_are_preserved()
{
    let item = HashMap::from([
        (
            "strings".to_string(),
            AttributeValue::SS(vec!["a".to_string(), "a".to_string()]),
        ),
        (
            "numbers".to_string(),
            AttributeValue::NS(vec!["1".to_string(), "1".to_string()]),
        ),
        (
            "binaries".to_string(),
            AttributeValue::BS(vec!["YQ==".to_string(), "YQ==".to_string()]),
        ),
    ]);

    for (field, same, different) in [
        (
            "strings",
            AttributeValue::SS(vec!["a".to_string(), "a".to_string()]),
            AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
        ),
        (
            "numbers",
            AttributeValue::NS(vec!["1".to_string(), "1".to_string()]),
            AttributeValue::NS(vec!["1".to_string(), "2".to_string()]),
        ),
        (
            "binaries",
            AttributeValue::BS(vec!["YQ==".to_string(), "YQ==".to_string()]),
            AttributeValue::BS(vec!["YQ==".to_string(), "Yg==".to_string()]),
        ),
    ] {
        assert!(evaluate_condition(
            &item,
            &Condition::Equal {
                field: attr_path(field),
                value: same,
            }
        ));
        assert!(evaluate_condition(
            &item,
            &Condition::Equal {
                field: attr_path(field),
                value: different,
            }
        ));
    }

    assert!(!evaluate_condition(
        &HashMap::new(),
        &Condition::ValueEqual {
            left: AttributeValue::SS(vec!["a".to_string(), "b".to_string()]),
            right: AttributeValue::SS(vec!["a".to_string(), "a".to_string()]),
        }
    ));
}

#[test]
fn literal_value_equality_condition() {
    let item = HashMap::new();

    let condition = Condition::ValueEqual {
        left: AttributeValue::S("same".to_string()),
        right: AttributeValue::S("same".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::ValueNotEqual {
        left: AttributeValue::S("same".to_string()),
        right: AttributeValue::S("other".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn contains_condition() {
    let item = create_test_item();

    // Test string contains
    let condition = Condition::Contains {
        field: attr_path("name"),
        value: AttributeValue::S("oh".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: attr_path("name"),
        value: AttributeValue::S("Jane".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test full string match
    let condition = Condition::Contains {
        field: attr_path("name"),
        value: AttributeValue::S("John".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    // Test binary data contains
    let condition = Condition::Contains {
        field: attr_path("data"),
        value: AttributeValue::B("YmluYXJ5".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: attr_path("data"),
        value: AttributeValue::B("ZGF0YQ==".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: attr_path("data"),
        value: AttributeValue::B("bWlzc2luZw==".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test string set contains exact elements.
    let condition = Condition::Contains {
        field: attr_path("tags"),
        value: AttributeValue::S("rust".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: attr_path("tags"),
        value: AttributeValue::S("data".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::Contains {
        field: attr_path("tags"),
        value: AttributeValue::S("python".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test with non-string/binary field (should return false)
    let condition = Condition::Contains {
        field: attr_path("age"),
        value: AttributeValue::N("2".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test with missing field
    let condition = Condition::Contains {
        field: attr_path("missing"),
        value: AttributeValue::S("value".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test edge case: empty value
    let condition = Condition::Contains {
        field: attr_path("name"),
        value: AttributeValue::S(String::new()),
    };
    assert!(evaluate_condition(&item, &condition)); // Empty string is contained in any string

    // Test case sensitivity
    let condition = Condition::Contains {
        field: attr_path("name"),
        value: AttributeValue::S("JOHN".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition)); // Case sensitive

    // Test list contains
    let condition = Condition::Contains {
        field: attr_path("list_field"),
        value: AttributeValue::S("item1".to_string()),
    };
    assert!(evaluate_condition(&item, &condition)); // List contains "item1"

    let condition = Condition::Contains {
        field: attr_path("list_field"),
        value: AttributeValue::N("42".to_string()),
    };
    assert!(evaluate_condition(&item, &condition)); // List contains "42"

    // Test map contains (in values)
    let condition = Condition::Contains {
        field: attr_path("map_field"),
        value: AttributeValue::S("nested_value".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition)); // Map value contains "nested_value"

    let condition = Condition::Contains {
        field: attr_path("map_field"),
        value: AttributeValue::N("10".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition)); // Map value contains "10"
}

#[test]
fn begins_with_condition() {
    let item = create_test_item();

    let condition = Condition::BeginsWith {
        field: attr_path("name"),
        prefix: AttributeValue::S("Jo".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::BeginsWith {
        field: attr_path("name"),
        prefix: AttributeValue::S("Jane".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // Test with binary data
    let condition = Condition::BeginsWith {
        field: attr_path("data"),
        prefix: AttributeValue::B("YmluYXJ5".to_string()),
    };
    assert!(evaluate_condition(&item, &condition));
}

#[test]
fn size_condition() {
    let item = create_test_item();

    // String size
    let condition = Condition::Size {
        field: attr_path("name"),
        size: Some("4".to_owned()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Size {
        field: attr_path("name"),
        size: Some("5".to_owned()),
    };
    assert!(!evaluate_condition(&item, &condition));

    // String set size
    let condition = Condition::Size {
        field: attr_path("tags"),
        size: Some("2".to_owned()),
    };
    assert!(evaluate_condition(&item, &condition));

    // Number set size
    let condition = Condition::Size {
        field: attr_path("scores"),
        size: Some("2".to_owned()),
    };
    assert!(evaluate_condition(&item, &condition));

    // List size
    let condition = Condition::Size {
        field: attr_path("list_field"),
        size: Some("3".to_owned()),
    };
    assert!(evaluate_condition(&item, &condition));

    // Map size
    let condition = Condition::Size {
        field: attr_path("map_field"),
        size: Some("2".to_owned()),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::SizeCompare {
        field: attr_path("name"),
        operator: SizeComparison::GreaterThan,
        size: Some("3".to_owned()),
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
        AttributeValue::M(AttributeMap::from(HashMap::from([
            ("a".to_string(), AttributeValue::S("x".to_string())),
            ("b".to_string(), AttributeValue::N("2".to_string())),
            (
                "deep".to_string(),
                AttributeValue::M(AttributeMap::from(HashMap::from([
                    ("one".to_string(), AttributeValue::S("1".to_string())),
                    ("two".to_string(), AttributeValue::S("2".to_string())),
                ]))),
            ),
        ]))),
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
                    field: attr_path(field),
                    size: Some(size.to_string())
                }
            ),
            "{field} should have DynamoDB size {size}"
        );
        assert!(
            !evaluate_condition(
                &item,
                &Condition::Size {
                    field: attr_path(field),
                    size: Some((size + 1).to_string())
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
            field: attr_path("string"),
            value: AttributeValue::S("bc".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: attr_path("strings"),
            value: AttributeValue::S("red".to_string()),
        }
    ));
    assert!(!evaluate_condition(
        &item,
        &Condition::Contains {
            field: attr_path("strings"),
            value: AttributeValue::S("re".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: attr_path("numbers"),
            value: AttributeValue::N("2".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: attr_path("binaries"),
            value: AttributeValue::B("AwQ=".to_string()),
        }
    ));
    assert!(evaluate_condition(
        &item,
        &Condition::Contains {
            field: attr_path("list"),
            value: AttributeValue::S("alpha".to_string()),
        }
    ));
    assert!(!evaluate_condition(
        &item,
        &Condition::Contains {
            field: attr_path("list"),
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
    item.insert("map".to_string(), AttributeValue::M(AttributeMap::new()));

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
                field: attr_path(field),
                attribute_type: attribute_type.to_string(),
            }
        ));
    }
}

#[test]
fn condition_with_missing_fields() {
    let item = HashMap::new();

    // All conditions should return false for missing fields except NotExists
    let condition = Condition::Exists {
        field: attr_path("missing"),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::NotExists {
        field: attr_path("missing"),
    };
    assert!(evaluate_condition(&item, &condition));

    let condition = Condition::Equal {
        field: attr_path("missing"),
        value: AttributeValue::S("value".to_string()),
    };
    assert!(!evaluate_condition(&item, &condition));

    let condition = Condition::LessThan {
        field: attr_path("missing"),
        value: "value".to_string(),
    };
    assert!(!evaluate_condition(&item, &condition));
}

#[test]
fn numeric_conditions_preserve_exact_order_and_equivalent_spellings() {
    for (lower, upper) in [
        ("9007199254740992", "9007199254740993"),
        (
            "12345678901234567890123456789012345677",
            "12345678901234567890123456789012345678",
        ),
        ("-9007199254740993", "-9007199254740992"),
        ("-1e-130", "0"),
        ("0", "1e-130"),
        ("1.09", "1.1"),
    ] {
        let item = HashMap::from([("n".to_string(), AttributeValue::N(upper.to_string()))]);
        for condition in [
            Condition::GreaterThan {
                field: attr_path("n"),
                value: lower.to_string(),
            },
            Condition::GreaterThanEqual {
                field: attr_path("n"),
                value: upper.to_string(),
            },
            Condition::NotEqual {
                field: attr_path("n"),
                value: AttributeValue::N(lower.to_string()),
            },
            Condition::Between {
                field: attr_path("n"),
                min: lower.to_string(),
                max: upper.to_string(),
            },
        ] {
            assert_numeric_condition_paths(&item, &condition, true);
        }
        for condition in [
            Condition::LessThan {
                field: attr_path("n"),
                value: lower.to_string(),
            },
            Condition::LessThanEqual {
                field: attr_path("n"),
                value: lower.to_string(),
            },
            Condition::Equal {
                field: attr_path("n"),
                value: AttributeValue::N(lower.to_string()),
            },
            Condition::Between {
                field: attr_path("n"),
                min: lower.to_string(),
                max: lower.to_string(),
            },
        ] {
            assert_numeric_condition_paths(&item, &condition, false);
        }
    }
    for (left, right) in [
        ("1", "1.00"),
        ("100", "1e2"),
        ("-0", "0"),
        ("-1.2", "-12e-1"),
    ] {
        let item = HashMap::from([("n".to_string(), AttributeValue::N(left.to_string()))]);
        assert_numeric_condition_paths(
            &item,
            &Condition::Equal {
                field: attr_path("n"),
                value: AttributeValue::N(right.to_string()),
            },
            true,
        );
    }
    for invalid in ["NaN", "Infinity", "", "1e126"] {
        let item = HashMap::from([("n".to_string(), AttributeValue::N(invalid.to_string()))]);
        assert!(!evaluate_condition(
            &item,
            &Condition::Equal {
                field: attr_path("n"),
                value: AttributeValue::N(invalid.to_string()),
            }
        ));
    }
}

fn assert_numeric_condition_paths(
    item: &HashMap<String, AttributeValue>,
    condition: &Condition,
    expected: bool,
) {
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "exact_numeric_condition",
        file!(),
        line!(),
        None,
    );
    let actual = evaluate_condition(item, condition);
    let allocations = guard.finish();
    assert_eq!(actual, expected, "{item:?} {condition:?}");
    assert_eq!(
        allocations.allocation_count, 0,
        "exact scalar comparisons retain inline digits"
    );
    let wire = crate::WireItem::try_from(item).unwrap();
    assert_eq!(
        crate::condition::evaluate_wire_condition(Some(&wire), condition).unwrap(),
        expected
    );
}

#[test]
fn given_dotted_alias_and_nested_map_when_evaluating_then_alias_reads_top_level_attribute() {
    let item = HashMap::from([
        ("a.b".to_string(), AttributeValue::S("top".to_string())),
        (
            "a".to_string(),
            AttributeValue::M(AttributeMap::from_iter([(
                "b".to_string(),
                AttributeValue::S("nested".to_string()),
            )])),
        ),
        ("l[0]".to_string(), AttributeValue::S("literal".to_string())),
        (
            "l".to_string(),
            AttributeValue::L(vec![AttributeValue::S("element".to_string())]),
        ),
    ]);
    let top = |name: &str| AttributePath::top(name.to_string());
    let equal = |field: AttributePath, value: &str| Condition::Equal {
        field,
        value: AttributeValue::S(value.to_string()),
    };

    assert!(evaluate_condition(&item, &equal(top("a.b"), "top")));
    assert!(!evaluate_condition(&item, &equal(top("a.b"), "nested")));
    assert!(evaluate_condition(
        &item,
        &equal(attr_path("a.b"), "nested")
    ));
    assert!(evaluate_condition(&item, &equal(top("l[0]"), "literal")));
    let mut indexed = top("l");
    indexed.push(PathElement::Index(0));
    assert!(evaluate_condition(&item, &equal(indexed, "element")));
    assert!(!evaluate_condition(
        &item,
        &Condition::Exists { field: top("a.c") }
    ));
}
