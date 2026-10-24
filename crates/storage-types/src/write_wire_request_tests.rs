use std::collections::HashMap;

use crate::{
    AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest, EncodePutRequest,
    EncodeWriteRequest, PutRequest, TableName, TransactEncodeItem, TransactEncodePutRequest,
    TransactWriteItemsEncodeRequest, WireEntity, WireItem, WriteRequest, WriteRetryPolicy,
};

fn sample_wire_item() -> WireItem {
    let item = HashMap::from([
        ("pk".to_string(), AttributeValue::S("P#1".to_string())),
        ("sk".to_string(), AttributeValue::S("S#1".to_string())),
        (
            "entity_type".to_string(),
            AttributeValue::S("FIXTURE".to_string()),
        ),
    ]);
    WireItem::try_from(&item).unwrap()
}

fn transaction_with_25_four_kib_items() -> TransactWriteItemsEncodeRequest {
    let table_name = TableName::new("retry_alloc_table");
    let payload = vec![b'x'; 4 * 1024];
    TransactWriteItemsEncodeRequest::builder()
        .transact_items(
            (0..25)
                .map(|_| {
                    TransactEncodeItem::builder()
                        .put(
                            TransactEncodePutRequest::builder()
                                .table_name(table_name.clone())
                                .item(WireEntity::unindexed(WireItem::dynamo_json(
                                    payload.clone(),
                                )))
                                .build(),
                        )
                        .build()
                })
                .collect::<Vec<_>>(),
        )
        .build()
}

#[test]
fn owned_transaction_retry_handoff_avoids_payload_clones_tests() {
    let request = transaction_with_25_four_kib_items();
    let legacy_guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "owned_transaction_retry_handoff_avoids_payload_clones_tests",
        file!(),
        line!(),
        Some("legacy_clone"),
    );
    for _ in 0..3 {
        std::hint::black_box(request.clone());
    }
    let legacy = legacy_guard.finish();

    let owned_guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "owned_transaction_retry_handoff_avoids_payload_clones_tests",
        file!(),
        line!(),
        Some("owned_borrowed_attempts"),
    );
    let owned_request = request;
    for _ in 0..3 {
        std::hint::black_box(&owned_request);
    }
    let owned = owned_guard.finish();

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&owned);
    assert!(legacy.allocated_bytes >= 3 * 25 * 4 * 1024);
    assert_eq!(owned.allocation_count, 0);
    assert_eq!(owned.allocated_bytes, 0);
}

#[test]
fn write_retry_policy_always_has_an_attempt_tests() {
    let policy = WriteRetryPolicy::new(0, std::time::Duration::from_millis(1));
    assert_eq!(policy.max_attempts(), 1);
    assert_eq!(policy.delay(), std::time::Duration::from_millis(1));
}

#[test]
fn batch_write_item_encode_request_converts_put_item_tests() {
    let table = TableName::new("encode_batch_table");
    let request = BatchWriteItemEncodeRequest::builder()
        .request_items(HashMap::from([(
            table.clone(),
            vec![
                EncodeWriteRequest::builder()
                    .put_request(
                        EncodePutRequest::builder()
                            .item(WireEntity::unindexed(sample_wire_item()))
                            .build(),
                    )
                    .build(),
            ],
        )]))
        .build();

    let mapped = crate::BatchWriteItemRequest::try_from(request).unwrap();
    let writes = mapped.request_items.get(&table).unwrap();
    assert_eq!(writes.len(), 1);
    let put = writes[0].put_request.as_ref().unwrap();
    assert!(
        matches!(put.item.attribute_value("pk").unwrap(), Some(AttributeValue::S(v)) if v == "P#1")
    );
    assert!(
        matches!(put.item.attribute_value("sk").unwrap(), Some(AttributeValue::S(v)) if v == "S#1")
    );
}

#[test]
fn batch_write_item_request_converts_put_item_to_encode_tests() {
    let table = TableName::new("encode_batch_table");
    let request = BatchWriteItemRequest {
        request_items: crate::VecMap::from_iter([(
            table.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("P#1".to_string())),
                        ("sk".to_string(), AttributeValue::S("S#1".to_string())),
                        (
                            "entity_type".to_string(),
                            AttributeValue::S("FIXTURE".to_string()),
                        ),
                    ]))
                    .unwrap(),
                    indexers: None,
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let mapped = BatchWriteItemEncodeRequest::try_from(request).unwrap();
    let writes = mapped.request_items.get(&table).unwrap();
    assert_eq!(writes.len(), 1);
    let put = writes[0].put_request.as_ref().unwrap();
    assert_eq!(
        put.item.item().attribute_value("pk").unwrap(),
        Some(AttributeValue::S("P#1".to_string()))
    );
    assert_eq!(
        put.item.item().attribute_value("sk").unwrap(),
        Some(AttributeValue::S("S#1".to_string()))
    );
}

#[test]
fn transact_write_items_encode_request_converts_put_item_tests() {
    let table = TableName::new("encode_tx_table");
    let request = TransactWriteItemsEncodeRequest::builder()
        .transact_items(vec![
            TransactEncodeItem::builder()
                .put(
                    TransactEncodePutRequest::builder()
                        .table_name(table)
                        .item(WireEntity::unindexed(sample_wire_item()))
                        .condition_expression("attribute_not_exists(pk)")
                        .build(),
                )
                .build(),
        ])
        .client_request_token("tok-1")
        .build();

    let mapped = crate::TransactWriteItemsRequest::try_from(request).unwrap();
    assert_eq!(mapped.transact_items.len(), 1);
    let put = mapped.transact_items[0].put.as_ref().unwrap();
    assert!(
        matches!(put.item.attribute_value("pk").unwrap(), Some(AttributeValue::S(v)) if v == "P#1")
    );
    assert_eq!(
        put.condition_expression.as_deref(),
        Some("attribute_not_exists(pk)")
    );
}
