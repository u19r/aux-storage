use std::collections::HashMap;

use storage_types::{
    AttributeValue, DurableAbsenceProof, DurableBatchPointReadProof,
    DurableBatchPointReadProofEntry, DurablePointReadProof, KeyAttributes, KeysAndAttributes,
    TableName, TableNamespace,
};

use crate::{
    database_manager::read_ops::{
        RoutedBatchGetTable, RoutedBatchGetTarget, RoutedBatchProofTarget,
        insert_routed_batch_get_table, normalize_unprocessed_keys_for_shared_table,
        parse_stream_page_token, remap_routed_batch_proof,
    },
    namespace_routing::NamespaceRequestRewriter,
};

fn empty_keys_and_attributes() -> KeysAndAttributes {
    KeysAndAttributes {
        keys: Vec::new().into(),
        attributes_to_get: None,
        consistent_read: None,
        projection_expression: None,
        expression_attribute_names: None,
    }
}

#[test]
fn routed_batch_get_groups_distinct_dedicated_tables_by_connection() {
    let mut isolated = Vec::new();
    let mut grouped = HashMap::new();

    for suffix in ["a", "b"] {
        insert_routed_batch_get_table(
            &mut isolated,
            &mut grouped,
            &Some("TOTAL".to_string()),
            RoutedBatchGetTable {
                connection_id: "tenant-store".to_string(),
                physical_table: TableName::new(&format!("physical-{suffix}")),
                target: RoutedBatchGetTarget {
                    logical_table: TableName::new(&format!("logical-{suffix}")),
                    shared_namespace: None,
                },
                keys_and_attributes: empty_keys_and_attributes(),
            },
        );
    }

    assert!(isolated.is_empty());
    let request = grouped.get("tenant-store").expect("provider group");
    assert_eq!(request.request.request_items.len(), 2);
    assert_eq!(request.targets.len(), 2);
    assert_eq!(
        request.request.return_consumed_capacity.as_deref(),
        Some("TOTAL")
    );
    assert_eq!(
        request.targets[&TableName::new("physical-a")].logical_table,
        TableName::new("logical-a")
    );
}

#[test]
fn routed_batch_get_keeps_shared_and_default_tables_isolated() {
    let mut isolated = Vec::new();
    let mut grouped = HashMap::new();
    let namespace = TableNamespace::from_seed("shared");

    for (connection_id, namespace) in [
        ("tenant-store", Some(namespace)),
        ("default", None),
    ] {
        insert_routed_batch_get_table(
            &mut isolated,
            &mut grouped,
            &None,
            RoutedBatchGetTable {
                connection_id: connection_id.to_string(),
                physical_table: TableName::new(&format!("physical-{connection_id}")),
                target: RoutedBatchGetTarget {
                    logical_table: TableName::new(&format!("logical-{connection_id}")),
                    shared_namespace: namespace,
                },
                keys_and_attributes: empty_keys_and_attributes(),
            },
        );
    }

    assert_eq!(isolated.len(), 2);
    assert!(grouped.is_empty());
}

#[test]
fn stream_page_tokens_must_be_valid_stream_item_hex_ids() {
    let absent = parse_stream_page_token(None).expect("missing token is allowed");
    assert!(absent.is_none());

    let error = parse_stream_page_token(Some("not-hex")).expect_err("invalid token fails");
    assert!(format!("{error}").contains("DynamoDB could not process your request"));
}

#[test]
fn shared_table_unprocessed_keys_are_normalized_back_to_logical_keys() {
    let rewriter = NamespaceRequestRewriter::new();
    let namespace = TableNamespace::from_seed("tenant-a");
    let mut key = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
    ]));
    rewriter
        .rewrite_key_for_shared_table(&namespace, &mut key)
        .expect("rewrite key");
    let mut keys = KeysAndAttributes {
        keys: vec![key].into(),
        attributes_to_get: None,
        consistent_read: Some(true),
        projection_expression: None,
        expression_attribute_names: None,
    };

    normalize_unprocessed_keys_for_shared_table(&rewriter, &namespace, &mut keys)
        .expect("normalize keys");

    assert_eq!(
        keys.keys[0].get("pk"),
        Some(&AttributeValue::S("USER#1".to_string()))
    );
    assert_eq!(
        keys.keys[0].get("sk"),
        Some(&AttributeValue::S("PROFILE".to_string()))
    );
}

#[test]
fn routed_batch_cache_proof_keeps_its_logical_namespace() {
    let rewriter = NamespaceRequestRewriter::new();
    let namespace = TableNamespace::from_seed("tenant-a");
    let logical_table = TableName::new("users");
    let mut key = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
    ]));
    rewriter
        .rewrite_key_for_shared_table(&namespace, &mut key)
        .expect("rewrite key");

    let remapped = remap_routed_batch_proof(
        DurableBatchPointReadProof {
            responses: HashMap::from([(
                TableName::new("__shared"),
                vec![DurableBatchPointReadProofEntry {
                    key,
                    proof: DurablePointReadProof::Absent {
                        proof: DurableAbsenceProof::new(vec![2]),
                    },
                }],
            )]),
            unprocessed_keys: HashMap::new(),
        },
        RoutedBatchProofTarget {
            logical_table: &logical_table,
            shared_namespace: Some(&namespace),
            request_rewriter: &rewriter,
        },
    )
    .expect("remap proof");

    assert_eq!(
        remapped.responses[&logical_table][0].key.get("pk"),
        Some(&AttributeValue::S("USER#1".to_string()))
    );
}
