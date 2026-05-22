use std::collections::HashMap;

use storage_types::{AttributeValue, KeySchemaElement, KeyType, Projection, ProjectionType};

use crate::projection::{apply_gsi_projection, apply_projection};

fn ks() -> Vec<KeySchemaElement> {
    vec![KeySchemaElement {
        attribute_name: "pk".into(),
        key_type: KeyType::Hash,
    }]
}

fn item() -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".into(), AttributeValue::S("v".into())),
        ("a".into(), AttributeValue::N("1".into())),
    ])
}

#[test]
fn all_default() {
    let out = apply_projection(&item(), None, &ks());
    assert_eq!(out.len(), 2);
}

#[test]
fn keys_only() {
    let p = Projection {
        projection_type: Some(ProjectionType::KeysOnly),
        non_key_attributes: None,
    };
    let out = apply_projection(&item(), Some(&p), &ks());
    assert_eq!(out.len(), 1);
}

#[test]
fn include_subset() {
    let p = Projection {
        projection_type: Some(ProjectionType::Include),
        non_key_attributes: Some(vec!["a".into()]),
    };
    let out = apply_projection(&item(), Some(&p), &ks());
    assert_eq!(out.len(), 1);
}

#[test]
fn gsi_keys_only_includes_base_and_index_keys() {
    let table_keys = vec![
        KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".into(),
            key_type: KeyType::Range,
        },
    ];
    let gsi_keys = vec![KeySchemaElement {
        attribute_name: "gsi_pk".into(),
        key_type: KeyType::Hash,
    }];
    let itm = HashMap::from([
        ("pk".into(), AttributeValue::S("p".into())),
        ("sk".into(), AttributeValue::S("s".into())),
        ("gsi_pk".into(), AttributeValue::S("gp".into())),
        ("a".into(), AttributeValue::N("1".into())),
    ]);
    let proj = Projection {
        projection_type: Some(ProjectionType::KeysOnly),
        non_key_attributes: None,
    };
    let out = apply_gsi_projection(&itm, Some(&proj), &table_keys, &gsi_keys);
    assert_eq!(out.len(), 3);
    assert!(out.contains_key("pk") && out.contains_key("sk") && out.contains_key("gsi_pk"));
}

#[test]
fn gsi_include_adds_requested_plus_keys() {
    let table_keys = vec![KeySchemaElement {
        attribute_name: "pk".into(),
        key_type: KeyType::Hash,
    }];
    let gsi_keys = vec![KeySchemaElement {
        attribute_name: "gsi_pk".into(),
        key_type: KeyType::Hash,
    }];
    let itm = HashMap::from([
        ("pk".into(), AttributeValue::S("p".into())),
        ("gsi_pk".into(), AttributeValue::S("gp".into())),
        ("included".into(), AttributeValue::S("v".into())),
        ("other".into(), AttributeValue::S("o".into())),
    ]);
    let proj = Projection {
        projection_type: Some(ProjectionType::Include),
        non_key_attributes: Some(vec!["included".into()]),
    };
    let out = apply_gsi_projection(&itm, Some(&proj), &table_keys, &gsi_keys);
    assert_eq!(out.len(), 3);
    assert!(out.contains_key("pk") && out.contains_key("gsi_pk") && out.contains_key("included"));
    assert!(!out.contains_key("other"));
}
