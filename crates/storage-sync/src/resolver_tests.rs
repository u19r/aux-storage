use storage_types::{AttributeValue, DeleteItemRequest, KeyAttributes, PutItemRequest, TableName};

use crate::{SyncProposalId, SyncWriteProposalRequest, SyncWriteRequest};

#[test]
fn sync_write_request_names_supported_write_operations() {
    let put = SyncWriteRequest::PutItem(PutItemRequest {
        table_name: TableName::new("orders"),
        item: [("pk".to_string(), AttributeValue::S("order#1".to_string()))].into(),
        indexers: None,
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
    });
    let delete = SyncWriteRequest::DeleteItem(DeleteItemRequest {
        table_name: TableName::new("orders"),
        key: KeyAttributes::from([("pk".to_string(), AttributeValue::S("order#1".to_string()))]),
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
    });

    assert_eq!(put.operation_name(), "PutItem");
    assert_eq!(delete.operation_name(), "DeleteItem");
}

#[test]
fn sync_write_proposal_request_carries_operation_and_id() {
    let proposal_id = SyncProposalId::new("proposal-1").unwrap();
    let request = SyncWriteProposalRequest::new(
        proposal_id.clone(),
        SyncWriteRequest::DeleteItem(DeleteItemRequest {
            table_name: TableName::new("orders"),
            key: KeyAttributes::from([(
                "pk".to_string(),
                AttributeValue::S("order#1".to_string()),
            )]),
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
        }),
    );

    assert_eq!(request.proposal_id, proposal_id);
    assert_eq!(request.request.operation_name(), "DeleteItem");
}
