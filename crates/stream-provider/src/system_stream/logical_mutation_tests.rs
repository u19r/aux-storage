use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use storage_types::{
    AttributeMap, AttributeValue, KeyAttributes, SystemStreamSourceTableName, TableName,
};

use super::logical_mutation::canonical_route_key;

#[derive(Serialize)]
struct ExpectedRouteEvidence {
    source_table_name: SystemStreamSourceTableName,
    key: BTreeMap<String, AttributeValue>,
}

fn route_fixture() -> (
    SystemStreamSourceTableName,
    Vec<storage_types::KeySchemaElement>,
    HashMap<String, AttributeValue>,
) {
    let table_name = TableName::new("orders");
    let source_table_name =
        SystemStreamSourceTableName::try_from(&table_name).expect("source table name");
    let key_schema = vec![
        storage_types::KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: storage_types::KeyType::Hash,
        },
        storage_types::KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: storage_types::KeyType::Range,
        },
    ];
    let item = HashMap::from([
        (
            "sk".to_string(),
            AttributeValue::S("quote\"and\\slash".to_string()),
        ),
        ("payload".to_string(), AttributeValue::B("AAH/".to_string())),
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
    ]);
    (source_table_name, key_schema, item)
}

fn canonical_key_for_test(
    attributes: &HashMap<String, AttributeValue>,
    key_schema: &[storage_types::KeySchemaElement],
) -> BTreeMap<String, AttributeValue> {
    key_schema
        .iter()
        .filter(|key| key.key_type == storage_types::KeyType::Hash)
        .filter_map(|key| {
            attributes
                .get(&key.attribute_name)
                .cloned()
                .map(|value| (key.attribute_name.clone(), value))
        })
        .collect()
}

#[test]
fn given_key_schema_when_canonical_route_then_borrowed_bytes_match_owned_encoding() {
    let (source_table_name, key_schema, item) = route_fixture();
    let actual_route =
        canonical_route_key(&source_table_name, &key_schema, &item).expect("canonical route");
    let expected_route = storage_types::storage_serde::to_bytes(&ExpectedRouteEvidence {
        source_table_name,
        key: canonical_key_for_test(&item, &key_schema),
    })
    .expect("owned canonical route");
    assert_eq!(actual_route, expected_route);
}

#[test]
fn given_partition_keys_when_routing_then_sort_key_is_ignored_and_table_is_retained() {
    let (_, schema, _) = route_fixture();
    for partition in [
        AttributeValue::S("tenant".into()),
        AttributeValue::N("123".into()),
        AttributeValue::B("AAH/".into()),
    ] {
        let route = |table: &str, sort: &str| {
            super::logical_mutation::DynamoMutationRoute::try_new(
                &TableName::new(table),
                &schema,
                &KeyAttributes::from_iter([
                    ("pk".into(), partition.clone()),
                    ("sk".into(), AttributeValue::S(sort.into())),
                ]),
            )
            .unwrap()
        };
        let first = route("orders", "first");
        let second = route("orders", "second");
        assert_eq!(first.routing_key(), second.routing_key());
        assert_ne!(first.routing_key(), route("other", "first").routing_key());
        let item: AttributeMap = HashMap::from([
            ("pk".into(), partition),
            ("sk".into(), AttributeValue::S("second".into())),
        ])
        .into();
        assert!(
            first
                .verify_item(storage_types::WireItem::try_from(item).unwrap())
                .is_err()
        );
    }
}

#[test]
fn given_equivalent_numeric_partition_keys_when_routing_then_canonical_bytes_match() {
    let (table, schema, _) = route_fixture();
    let routes = ["123", "123.0", "1.23E2"].map(|number| {
        canonical_route_key(
            &table,
            &schema,
            &KeyAttributes::from_iter([("pk".into(), AttributeValue::N(number.into()))]),
        )
        .unwrap()
    });
    assert_eq!(routes[0], routes[1]);
    assert_eq!(routes[0], routes[2]);
}

#[test]
fn given_equivalent_binary_partition_keys_when_routing_then_canonical_bytes_match() {
    let (table, schema, _) = route_fixture();
    let routes = ["AQ==", "AQ"].map(|binary| {
        canonical_route_key(
            &table,
            &schema,
            &KeyAttributes::from_iter([("pk".into(), AttributeValue::B(binary.into()))]),
        )
        .unwrap()
    });
    assert_eq!(routes[0], routes[1]);
}
