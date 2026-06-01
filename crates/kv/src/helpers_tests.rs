use std::collections::HashMap;

use storage_types::AttributeValue;

use crate::helpers::{parse_hash_begins_with_query, parse_hash_between_query};

#[test]
fn parse_hash_between_query_accepts_newline_around_and_tests() {
    let values = Some(HashMap::from([
        (":pk".to_string(), AttributeValue::S("pk".to_string())),
        (":lo".to_string(), AttributeValue::S("sk-001".to_string())),
        (":hi".to_string(), AttributeValue::S("sk-002".to_string())),
    ]));

    let (hash, start, end) =
        parse_hash_between_query("pk = :pk\nAND sk BETWEEN :lo AND :hi", &values)
            .expect("between query should parse with newline before AND");

    assert_eq!(hash, &AttributeValue::S("pk".to_string()));
    assert_eq!(start, &AttributeValue::S("sk-001".to_string()));
    assert_eq!(end, &AttributeValue::S("sk-002".to_string()));
}

#[test]
fn parse_hash_begins_with_query_accepts_tab_around_and_tests() {
    let values = Some(HashMap::from([
        (":pk".to_string(), AttributeValue::S("pk".to_string())),
        (":prefix".to_string(), AttributeValue::S("zz".to_string())),
    ]));

    let (hash, prefix) =
        parse_hash_begins_with_query("pk = :pk\tAND begins_with(sk, :prefix)", &values)
            .expect("begins_with query should parse")
            .expect("begins_with query should match");

    assert_eq!(hash, &AttributeValue::S("pk".to_string()));
    assert_eq!(prefix, &AttributeValue::S("zz".to_string()));
}
