use std::collections::HashMap;

use storage_types::{
    AttributeValue, DurableAbsenceProof, DurableBatchPointReadProof,
    DurableBatchPointReadProofEntry, DurablePointReadProof, KeyAttributes, KeysAndAttributes,
    TableName, TableNamespace,
};

use crate::{
    database_manager::read_ops::{
        RoutedBatchProofTarget, normalize_unprocessed_keys_for_shared_table,
        parse_stream_page_token, remap_routed_batch_proof,
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
