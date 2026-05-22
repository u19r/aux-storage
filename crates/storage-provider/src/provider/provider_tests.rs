use std::collections::HashMap;

use storage_types::{
    AttributeValue, TableName, TransactPutRequest, TransactWriteItem, TransactWriteItemsRequest,
};

#[test]
fn transact_write_request_preserves_client_request_token() {
    let request = TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: TableName::new("test_table"),
                item: {
                    let mut item = HashMap::new();
                    item.insert("id".to_string(), AttributeValue::S("test".to_string()));
                    item
                },
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            }),
            ..Default::default()
        }],
        client_request_token: Some("duplicate-token-123".to_string()),
        ..Default::default()
    };

    assert!(request.client_request_token.is_some());
    assert_eq!(request.client_request_token.unwrap(), "duplicate-token-123");
}

#[test]
fn transact_write_item_can_represent_invalid_multiple_operations_for_validation_tests() {
    let invalid_request = TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: TableName::new("test_table"),
                item: HashMap::new(),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            }),
            update: Some(storage_types::TransactUpdateRequest {
                table_name: TableName::new("test_table"),
                key: HashMap::new().into(),
                update_expression: "SET #a = :val".to_string(),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            }),
            ..Default::default()
        }],
        ..Default::default()
    };

    assert!(invalid_request.transact_items[0].put.is_some());
    assert!(invalid_request.transact_items[0].update.is_some());
}
