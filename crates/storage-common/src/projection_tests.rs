use std::collections::HashMap;

use storage_types::{AttributeValue, KeySchemaElement, KeyType, Projection, ProjectionType};

use crate::projection::apply_gsi_projection;

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

#[test]
fn given_all_or_omitted_projection_when_applied_then_the_complete_item_is_retained() {
    let (table_keys, gsi_keys, item) = projection_fixture();
    let omitted_projection_type = Projection {
        projection_type: None,
        non_key_attributes: None,
    };
    let all_projection = Projection {
        projection_type: Some(ProjectionType::All),
        non_key_attributes: None,
    };

    for projection in [None, Some(&omitted_projection_type), Some(&all_projection)] {
        let projected = apply_gsi_projection(&item, projection, &table_keys, &gsi_keys);
        assert_eq!(projected, item);
    }
    assert_eq!(
        apply_gsi_projection(&item, None, &table_keys, &gsi_keys),
        item
    );
}

#[test]
fn given_overlapping_keys_and_absent_include_fields_when_projected_then_only_distinct_present_attributes_are_retained()
 {
    let (table_keys, gsi_keys, item) = projection_fixture();
    let projection = Projection {
        projection_type: Some(ProjectionType::Include),
        non_key_attributes: Some(vec!["included".into(), "missing".into(), "pk".into()]),
    };

    let projected = apply_gsi_projection(&item, Some(&projection), &table_keys, &gsi_keys);

    assert_eq!(projected.len(), 4);
    assert!(projected.contains_key("pk"));
    assert!(projected.contains_key("shared"));
    assert!(projected.contains_key("gsi_pk"));
    assert!(projected.contains_key("included"));
    assert!(!projected.contains_key("missing"));
    assert!(!projected.contains_key("other"));
}

#[test]
fn given_overlapping_keys_when_keys_only_is_projected_then_each_key_is_retained_once() {
    let (table_keys, gsi_keys, item) = projection_fixture();
    let projection = Projection {
        projection_type: Some(ProjectionType::KeysOnly),
        non_key_attributes: None,
    };

    let projected = apply_gsi_projection(&item, Some(&projection), &table_keys, &gsi_keys);

    assert_eq!(projected.len(), 3);
    assert!(projected.contains_key("pk"));
    assert!(projected.contains_key("shared"));
    assert!(projected.contains_key("gsi_pk"));
}

fn projection_fixture() -> (
    Vec<KeySchemaElement>,
    Vec<KeySchemaElement>,
    HashMap<String, AttributeValue>,
) {
    let table_keys = vec![
        KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "shared".into(),
            key_type: KeyType::Range,
        },
    ];
    let gsi_keys = vec![
        KeySchemaElement {
            attribute_name: "gsi_pk".into(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "shared".into(),
            key_type: KeyType::Range,
        },
    ];
    let item = HashMap::from([
        ("pk".into(), AttributeValue::S("pk".into())),
        ("shared".into(), AttributeValue::S("shared".into())),
        ("gsi_pk".into(), AttributeValue::S("gsi".into())),
        ("included".into(), AttributeValue::S("included".into())),
        ("other".into(), AttributeValue::S("other".into())),
    ]);
    (table_keys, gsi_keys, item)
}
