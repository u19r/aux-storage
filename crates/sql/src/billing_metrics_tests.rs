use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeValue, DeleteRequest, EncodePutRequest, EncodeWriteRequest, TableName,
    TransactConditionCheckRequest, TransactDeleteRequest, TransactEncodeItem,
    TransactEncodePutRequest, TransactPutRequest, TransactUpdateRequest, TransactWriteItem,
    WriteRequest,
};

use crate::billing_metrics::{
    WriteCostTally, attr_map_payload_bytes, record_read_cost, record_write_cost,
    serializable_payload_bytes,
};

#[test]
fn write_cost_tally_tracks_batch_puts_and_deletes() {
    let mut tally = WriteCostTally::default();
    tally.record_write_request(&WriteRequest {
        put_request: Some(storage_types::PutRequest {
            item: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]),
            aux_item_stream_ttl_hours: None,
        }),
        delete_request: None,
    });
    tally.record_write_request(&WriteRequest {
        put_request: None,
        delete_request: Some(DeleteRequest {
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))])
                .into(),
            aux_item_stream_ttl_hours: None,
        }),
    });

    assert_eq!(tally.put_ops, 1);
    assert_eq!(tally.delete_ops, 1);
    assert!(tally.put_bytes > 0);
    assert!(tally.delete_bytes > 0);
}

#[test]
fn write_cost_tally_tracks_encode_batch_puts_and_deletes() {
    let mut tally = WriteCostTally::default();
    tally.record_encode_write_request(&EncodeWriteRequest {
        put_request: Some(EncodePutRequest {
            item: storage_types::WireItem::from_attribute_map(&HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )]))
            .expect("wire item"),
            aux_item_stream_ttl_hours: None,
        }),
        delete_request: None,
    });
    tally.record_encode_write_request(&EncodeWriteRequest {
        put_request: None,
        delete_request: Some(DeleteRequest {
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))])
                .into(),
            aux_item_stream_ttl_hours: None,
        }),
    });

    assert_eq!(tally.put_ops, 1);
    assert_eq!(tally.delete_ops, 1);
    assert!(tally.put_bytes > 0);
    assert!(tally.delete_bytes > 0);
}

#[test]
fn write_cost_tally_tracks_transact_item_kinds() {
    let mut tally = WriteCostTally::default();
    let update_request = TransactUpdateRequest {
        table_name: TableName::new("tenant_t1"),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]).into(),
        update_expression: "SET #v = :v".to_string(),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#v".to_string(), "value".to_string())])),
        expression_attribute_values: Some(HashMap::from([(
            ":v".to_string(),
            AttributeValue::S("next".to_string()),
        )])),
        return_values_on_condition_check_failure: None,
        aux_item_stream_ttl_hours: None,
    };
    let check_request = TransactConditionCheckRequest {
        table_name: TableName::new("tenant_t1"),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#3".to_string()))]).into(),
        condition_expression: "attribute_exists(pk)".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values_on_condition_check_failure: None,
    };
    tally.record_transact_item(&TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: TableName::new("tenant_t1"),
            item: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        update: Some(update_request.clone()),
        delete: Some(TransactDeleteRequest {
            table_name: TableName::new("tenant_t1"),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))])
                .into(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        condition_check: Some(check_request.clone()),
    });

    assert_eq!(tally.put_ops, 1);
    assert_eq!(tally.update_ops, 1);
    assert_eq!(tally.delete_ops, 1);
    assert_eq!(tally.condition_check_ops, 1);
    assert_eq!(
        tally.update_bytes,
        serializable_payload_bytes(&update_request)
    );
    assert_eq!(
        tally.condition_check_bytes,
        serializable_payload_bytes(&check_request)
    );
}

#[test]
fn write_cost_tally_tracks_transact_encode_item_kinds() {
    let mut tally = WriteCostTally::default();
    let update_request = TransactUpdateRequest {
        table_name: TableName::new("tenant_t1"),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]).into(),
        update_expression: "SET #v = :v".to_string(),
        condition_expression: None,
        expression_attribute_names: Some(HashMap::from([("#v".to_string(), "value".to_string())])),
        expression_attribute_values: Some(HashMap::from([(
            ":v".to_string(),
            AttributeValue::S("next".to_string()),
        )])),
        return_values_on_condition_check_failure: None,
        aux_item_stream_ttl_hours: None,
    };
    let check_request = TransactConditionCheckRequest {
        table_name: TableName::new("tenant_t1"),
        key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#3".to_string()))]).into(),
        condition_expression: "attribute_exists(pk)".to_string(),
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values_on_condition_check_failure: None,
    };
    let put_item = storage_types::WireItem::from_attribute_map(&HashMap::from([(
        "pk".to_string(),
        AttributeValue::S("tenant#1".to_string()),
    )]))
    .expect("wire item");
    tally.record_transact_encode_item(&TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: TableName::new("tenant_t1"),
            item: put_item.clone(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        update: Some(update_request.clone()),
        delete: Some(TransactDeleteRequest {
            table_name: TableName::new("tenant_t1"),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))])
                .into(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        condition_check: Some(check_request.clone()),
    });

    assert_eq!(tally.put_ops, 1);
    assert_eq!(tally.put_bytes, put_item.payload_len() as u64);
    assert_eq!(tally.update_ops, 1);
    assert_eq!(tally.delete_ops, 1);
    assert_eq!(tally.condition_check_ops, 1);
    assert_eq!(
        tally.update_bytes,
        serializable_payload_bytes(&update_request)
    );
    assert_eq!(
        tally.condition_check_bytes,
        serializable_payload_bytes(&check_request)
    );
}

#[test]
fn subtract_removes_unprocessed_write_cost() {
    let mut requested = WriteCostTally::default();
    let mut unprocessed = WriteCostTally::default();
    let put_item = HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]);
    let delete_key = HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))]);

    requested.record_write_request(&WriteRequest {
        put_request: Some(storage_types::PutRequest {
            item: put_item.clone(),
            aux_item_stream_ttl_hours: None,
        }),
        delete_request: None,
    });
    requested.record_write_request(&WriteRequest {
        put_request: None,
        delete_request: Some(DeleteRequest {
            key: delete_key.clone().into(),
            aux_item_stream_ttl_hours: None,
        }),
    });
    unprocessed.record_write_request(&WriteRequest {
        put_request: None,
        delete_request: Some(DeleteRequest {
            key: delete_key.into(),
            aux_item_stream_ttl_hours: None,
        }),
    });

    let applied = requested.subtract(&unprocessed);
    assert_eq!(applied.put_ops, 1);
    assert_eq!(applied.delete_ops, 0);
    assert_eq!(applied.put_bytes, attr_map_payload_bytes(&put_item));
}

#[test]
fn sql_billing_metrics_skip_empty_cost_records() {
    let empty_guard = AllocationGuard::start(
        module_path!(),
        "sql_billing_metrics_skip_empty_cost_records",
        file!(),
        line!(),
        Some("empty_cost_records_1024"),
    );
    for _ in 0..1024 {
        record_read_cost("query", "item", 0, 0);
        record_write_cost("batch_write_item", "item", 0, 0);
    }
    let empty_report = empty_guard.finish();
    alloc_counter::emit_report(&empty_report);

    let non_empty_guard = AllocationGuard::start(
        module_path!(),
        "sql_billing_metrics_skip_empty_cost_records",
        file!(),
        line!(),
        Some("non_empty_cost_records_1024"),
    );
    for _ in 0..1024 {
        record_read_cost("query", "item", 1, 64);
        record_write_cost("batch_write_item", "item", 1, 64);
    }
    let non_empty_report = non_empty_guard.finish();
    alloc_counter::emit_report(&non_empty_report);

    assert_eq!(empty_report.allocation_count, 0);
    assert_eq!(empty_report.allocated_bytes, 0);
    assert!(non_empty_report.allocation_count > empty_report.allocation_count);
    assert!(non_empty_report.allocated_bytes > empty_report.allocated_bytes);
}
