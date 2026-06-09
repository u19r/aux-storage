use std::collections::HashMap;

use crate::{
    AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest, EncodePutRequest,
    EncodeWriteRequest, PutRequest, TableName, TransactEncodeItem, TransactEncodePutRequest,
    TransactWriteItemsEncodeRequest, WireItem, WriteRequest,
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
    WireItem::from_attribute_map(&item).expect("wire item")
}

#[test]
fn batch_write_item_encode_request_converts_put_item_tests() {
    let table = TableName::new("encode_batch_table");
    let request = BatchWriteItemEncodeRequest::builder()
        .request_items(HashMap::from([(
            table.clone(),
            vec![
                EncodeWriteRequest::builder()
                    .put_request(EncodePutRequest::builder().item(sample_wire_item()).build())
                    .build(),
            ],
        )]))
        .build();

    let mapped = crate::BatchWriteItemRequest::try_from(request).expect("mapped request");
    let writes = mapped
        .request_items
        .get(&table)
        .expect("table write requests");
    assert_eq!(writes.len(), 1);
    let put = writes[0].put_request.as_ref().expect("put request");
    assert!(matches!(put.item.get("pk"), Some(AttributeValue::S(v)) if v == "P#1"));
    assert!(matches!(put.item.get("sk"), Some(AttributeValue::S(v)) if v == "S#1"));
}

#[test]
fn batch_write_item_request_converts_put_item_to_encode_tests() {
    let table = TableName::new("encode_batch_table");
    let request = BatchWriteItemRequest {
        request_items: HashMap::from([(
            table.clone(),
            vec![WriteRequest {
                put_request: Some(PutRequest {
                    item: HashMap::from([
                        ("pk".to_string(), AttributeValue::S("P#1".to_string())),
                        ("sk".to_string(), AttributeValue::S("S#1".to_string())),
                        (
                            "entity_type".to_string(),
                            AttributeValue::S("FIXTURE".to_string()),
                        ),
                    ]),
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            }],
        )]),
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
    };

    let mapped = BatchWriteItemEncodeRequest::try_from(request).expect("mapped request");
    let writes = mapped
        .request_items
        .get(&table)
        .expect("table write requests");
    assert_eq!(writes.len(), 1);
    let put = writes[0].put_request.as_ref().expect("put request");
    assert_eq!(
        put.item.attribute_value("pk").expect("pk lookup"),
        Some(AttributeValue::S("P#1".to_string()))
    );
    assert_eq!(
        put.item.attribute_value("sk").expect("sk lookup"),
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
                        .item(sample_wire_item())
                        .condition_expression("attribute_not_exists(pk)")
                        .build(),
                )
                .build(),
        ])
        .client_request_token("tok-1")
        .build();

    let mapped = crate::TransactWriteItemsRequest::try_from(request).expect("mapped request");
    assert_eq!(mapped.transact_items.len(), 1);
    let put = mapped.transact_items[0]
        .put
        .as_ref()
        .expect("mapped transact put");
    assert!(matches!(put.item.get("pk"), Some(AttributeValue::S(v)) if v == "P#1"));
    assert_eq!(
        put.condition_expression.as_deref(),
        Some("attribute_not_exists(pk)")
    );
}
