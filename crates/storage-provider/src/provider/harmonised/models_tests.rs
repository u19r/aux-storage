use std::{collections::HashMap, sync::Arc};

use storage_types::{
    AttributeValue, DeleteItemRequest, KeyAttributes, PutItemRequest, TableName, UpdateItemRequest,
};

use super::models::{SingleUpdateOptions, TableDeleteInput, TablePutInput, TableUpdateInput};
use crate::{PhysicalWrite, TtlWriteContext};

fn table_name() -> TableName {
    TableName::new("orders")
}

fn table() -> Arc<storage_types::StoredTableInfo> {
    Arc::new(storage_types::StoredTableInfo {
        table_name: table_name(),
        table_status: storage_types::TableStatus::Active,
        created_at: storage_types::TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![storage_types::AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: storage_types::KeyAttributeType::S,
        }],
        key_schema: vec![storage_types::KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: storage_types::KeyType::Hash,
        }],
        max_indexers: storage_types::MaxIndexers::ZERO,
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration:
            storage_types::StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        default_item_stream_duration:
            storage_types::StreamRetentionDuration::DEFAULT_TABLE_STREAM_DURATION,
        deletion_protection_enabled: false,
    })
}

fn direct_context() -> TtlWriteContext {
    TtlWriteContext::direct(table_name()).expect("direct context")
}

fn item() -> HashMap<String, AttributeValue> {
    HashMap::from([("pk".to_string(), AttributeValue::S("tenant".to_string()))])
}

#[test]
fn given_put_write_when_for_single_put_then_foreground_context_applies() {
    let request = PutItemRequest::new(
        table_name(),
        storage_types::WireItem::try_from(&(item())).unwrap(),
    );
    let write =
        PhysicalWrite::try_new(0, table_name(), request, direct_context()).expect("physical write");

    let input = TablePutInput::for_single_put(write, table(), true, None);

    assert_eq!(input.table.table_name, table_name());
    assert!(input.stream.immediate_gsi_consistency);
    assert!(input.item_stream_ttl_hours.is_none());
}

#[test]
fn given_update_write_when_for_single_update_then_options_apply() {
    let request = UpdateItemRequest {
        table_name: table_name(),
        key: KeyAttributes::default(),
        indexers: None,
        update_expression: None,
        attribute_updates: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        expected: None,
        conditional_operator: None,
        return_values: None,
        return_consumed_capacity: None,
        return_item_collection_metrics: None,
        return_values_on_condition_check_failure: None,
        aux_item_stream_ttl_hours: None,
    };
    let write =
        PhysicalWrite::try_new(0, table_name(), request, direct_context()).expect("physical write");

    let input = TableUpdateInput::for_single_update(
        write,
        table(),
        SingleUpdateOptions {
            immediate_gsi_consistency: false,
            ttl_config: None,
        },
    );

    assert!(!input.context.stream.immediate_gsi_consistency);
    assert_eq!(input.context.index, 0);
}

#[test]
fn given_delete_write_when_for_single_delete_then_table_rides_along() {
    let request = DeleteItemRequest::new(table_name(), KeyAttributes::default());
    let write =
        PhysicalWrite::try_new(0, table_name(), request, direct_context()).expect("physical write");

    let input = TableDeleteInput::for_single_delete(write, table(), true, None);

    assert_eq!(input.table.table_name, table_name());
    assert!(input.stream.immediate_gsi_consistency);
}

#[test]
fn given_interleaved_queue_sends_when_grouped_then_first_seen_order_and_ordinals_survive() {
    use queue_provider::SendMessageRequest;

    use super::models::{QueueSend, QueueWriteInput};

    let sends = (0..10)
        .map(|ordinal| QueueSend {
            ordinal,
            request: SendMessageRequest {
                queue_url: format!("queue-{}", ordinal % 3),
                message_body: ordinal.to_string(),
                delay_seconds: None,
                message_attributes: None,
            },
        })
        .collect();
    let groups = QueueWriteInput::try_new(sends)
        .expect("ten sends fit the queue bound")
        .into_groups_by_queue();
    assert_eq!(groups.len(), 3);
    assert_eq!(groups[0].0, "queue-0");
    assert_eq!(groups[1].0, "queue-1");
    assert_eq!(groups[2].0, "queue-2");
    assert_eq!(
        groups[0]
            .1
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<Vec<_>>(),
        vec![0, 3, 6, 9]
    );
    assert_eq!(
        groups[1]
            .1
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<Vec<_>>(),
        vec![1, 4, 7]
    );
    assert_eq!(
        groups[2]
            .1
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<Vec<_>>(),
        vec![2, 5, 8]
    );
}
