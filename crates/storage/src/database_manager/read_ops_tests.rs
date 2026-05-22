use std::collections::HashMap;

use storage_cache::{PhysicalToLogicalReadTableMap, RoutedBatchGetTarget};
use storage_types::{
    AttributeValue, DurableAbsenceProof, DurableBatchPointReadProof,
    DurableBatchPointReadProofEntry, DurableItemRevision, DurablePointReadProof, KeyAttributes,
    KeysAndAttributes, TableName, TableNamespace, WireItem,
};

use crate::{
    database_manager::read_ops::{
        RoutedBatchProofRemap, normalize_unprocessed_keys_for_shared_table,
        parse_stream_page_token, remap_routed_batch_proof_entry,
        remap_routed_batch_proof_for_cache_warming,
    },
    namespace_routing::NamespaceRequestRewriter,
};

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
fn routed_batch_proof_entries_normalize_present_items_for_shared_table_reads() {
    let rewriter = NamespaceRequestRewriter::new();
    let namespace = TableNamespace::from_seed("tenant-a");
    let mut key = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
    ]));
    let mut item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
        (
            "email".to_string(),
            AttributeValue::S("a@example.com".to_string()),
        ),
    ]);
    rewriter
        .rewrite_key_for_shared_table(&namespace, &mut key)
        .expect("rewrite key");
    rewriter
        .rewrite_item_for_shared_table(&namespace, &mut item)
        .expect("rewrite item");

    let remapped = remap_routed_batch_proof_entry(
        DurableBatchPointReadProofEntry {
            key,
            proof: DurablePointReadProof::Present {
                item: Box::new(WireItem::from_attribute_map(&item).expect("wire item")),
                revision: DurableItemRevision::new(vec![1]),
            },
        },
        Some(&namespace),
        &rewriter,
    )
    .expect("remap proof entry");

    assert_eq!(
        remapped.key.get("pk"),
        Some(&AttributeValue::S("USER#1".to_string()))
    );
    let DurablePointReadProof::Present { item, .. } = remapped.proof else {
        panic!("expected present proof");
    };
    assert_eq!(
        item.attribute_value("pk").expect("read pk"),
        Some(AttributeValue::S("USER#1".to_string()))
    );
    assert_eq!(
        item.attribute_value("email").expect("read email"),
        Some(AttributeValue::S("a@example.com".to_string()))
    );
}

#[test]
fn routed_batch_proofs_are_remapped_from_physical_tables_to_logical_tables_for_cache_warming() {
    let rewriter = NamespaceRequestRewriter::new();
    let namespace = TableNamespace::from_seed("tenant-a");
    let physical = TableName::new("__shared");
    let logical = TableName::new("users");
    let mut physical_to_logical = PhysicalToLogicalReadTableMap::default();
    physical_to_logical.insert(RoutedBatchGetTarget {
        connection_id: "replica-a".to_string(),
        physical_table: physical.clone(),
        logical_table: logical.clone(),
        shared_metadata: Some(namespace.clone()),
    });
    let mut key = KeyAttributes::from(HashMap::from([
        ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
        ("sk".to_string(), AttributeValue::S("PROFILE".to_string())),
    ]));
    rewriter
        .rewrite_key_for_shared_table(&namespace, &mut key)
        .expect("rewrite key");

    let remapped = remap_routed_batch_proof_for_cache_warming(
        DurableBatchPointReadProof {
            responses: HashMap::from([(
                physical.clone(),
                vec![DurableBatchPointReadProofEntry {
                    key,
                    proof: DurablePointReadProof::Absent {
                        proof: DurableAbsenceProof::new(vec![2]),
                    },
                }],
            )]),
            unprocessed_keys: HashMap::new(),
        },
        RoutedBatchProofRemap {
            connection_id: "replica-a",
            physical_to_logical: &physical_to_logical,
            request_rewriter: &rewriter,
        },
    )
    .expect("remap proof");

    assert!(remapped.responses.contains_key(&logical));
    assert!(!remapped.responses.contains_key(&physical));
    assert_eq!(
        remapped.responses[&logical][0].key.get("pk"),
        Some(&AttributeValue::S("USER#1".to_string()))
    );
}
