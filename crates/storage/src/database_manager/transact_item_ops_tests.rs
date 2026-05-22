use std::collections::HashMap;

use storage_types::{
    AttributeValue, KeyAttributes, TableName, TransactConditionCheckRequest, TransactDeleteRequest,
    TransactEncodeItem, TransactEncodePutRequest, TransactPutRequest, TransactUpdateRequest,
    TransactWriteItem, WireItem,
};

use crate::database_manager::transact_item_ops::*;

fn table(name: &str) -> TableName {
    TableName::new(name)
}

fn key() -> KeyAttributes {
    KeyAttributes::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))])
}

#[test]
fn transaction_table_name_uses_the_operation_present_on_the_item() {
    let put = TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: table("put_table"),
            item: HashMap::new(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    };
    let update = TransactWriteItem {
        update: Some(TransactUpdateRequest {
            table_name: table("update_table"),
            key: key(),
            update_expression: "SET value = :value".to_string(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    };
    let delete = TransactWriteItem {
        delete: Some(TransactDeleteRequest {
            table_name: table("delete_table"),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    };
    let check = TransactWriteItem {
        condition_check: Some(TransactConditionCheckRequest {
            table_name: table("check_table"),
            key: key(),
            condition_expression: "attribute_exists(pk)".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactWriteItem::default()
    };

    assert_eq!(
        transact_item_table_name(&put).expect("put").as_ref(),
        "put_table"
    );
    assert_eq!(
        transact_item_table_name(&update).expect("update").as_ref(),
        "update_table"
    );
    assert_eq!(
        transact_item_table_name(&delete).expect("delete").as_ref(),
        "delete_table"
    );
    assert_eq!(
        transact_item_table_name(&check).expect("check").as_ref(),
        "check_table"
    );
}

#[test]
fn setting_transaction_table_name_updates_every_present_operation() {
    let mut item = TransactWriteItem {
        put: Some(TransactPutRequest {
            table_name: table("old_put"),
            item: HashMap::new(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        update: Some(TransactUpdateRequest {
            table_name: table("old_update"),
            key: key(),
            update_expression: "SET value = :value".to_string(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        delete: Some(TransactDeleteRequest {
            table_name: table("old_delete"),
            key: key(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        condition_check: Some(TransactConditionCheckRequest {
            table_name: table("old_check"),
            key: key(),
            condition_expression: "attribute_exists(pk)".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
    };

    set_transact_item_table_name(&mut item, table("new_table"));

    assert_eq!(
        item.put.as_ref().expect("put").table_name.as_ref(),
        "new_table"
    );
    assert_eq!(
        item.update.as_ref().expect("update").table_name.as_ref(),
        "new_table"
    );
    assert_eq!(
        item.delete.as_ref().expect("delete").table_name.as_ref(),
        "new_table"
    );
    assert_eq!(
        item.condition_check
            .as_ref()
            .expect("check")
            .table_name
            .as_ref(),
        "new_table"
    );
}

#[test]
fn empty_transaction_item_is_rejected() {
    let error = transact_item_table_name(&TransactWriteItem::default())
        .expect_err("empty transaction item");

    assert!(error.to_string().contains("transaction item must contain"));
}

#[test]
fn encoded_transaction_helpers_use_and_update_wire_put_table_names() {
    let mut item = TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: table("wire_put"),
            item: WireItem::from_attribute_map(&HashMap::new()).expect("wire item"),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
        ..TransactEncodeItem::default()
    };

    assert_eq!(
        transact_encode_item_table_name(&item)
            .expect("wire put")
            .as_ref(),
        "wire_put"
    );

    set_transact_encode_item_table_name(&mut item, table("wire_new"));

    assert_eq!(
        item.put.as_ref().expect("put").table_name.as_ref(),
        "wire_new"
    );
}
