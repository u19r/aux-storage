use storage_types::{
    AttributeValue, KeyAttributes, TableName, TransactDeleteRequest, TransactUpdateRequest,
    TransactWriteItem, TransactWriteItemsRequest,
};

use crate::JobTerminalProjection;

fn request() -> TransactWriteItemsRequest {
    TransactWriteItemsRequest {
        transact_items: vec![TransactWriteItem {
            update: Some(TransactUpdateRequest {
                table_name: TableName::new("projection"),
                key: KeyAttributes::from_iter([(
                    "pk".to_string(),
                    AttributeValue::S("job".to_string()),
                )]),
                update_expression: "SET #status = :failed".to_string(),
                condition_expression: Some("#status = :running".to_string()),
                indexers: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        }],
        client_request_token: Some("terminal".to_string()),
        ..Default::default()
    }
}

#[test]
fn given_terminal_projection_when_a_conditional_update_is_supplied_then_it_is_accepted() {
    assert!(JobTerminalProjection::try_from(request()).is_ok());
}

#[test]
fn given_terminal_projection_when_execution_members_are_supplied_then_they_are_rejected() {
    let valid = request();
    let mut extra = valid.clone();
    extra.transact_items.push(valid.transact_items[0].clone());
    assert!(JobTerminalProjection::try_from(extra).is_err());
    let mut delete = valid.clone();
    let update = delete.transact_items[0].update.take().unwrap();
    delete.transact_items[0].delete = Some(TransactDeleteRequest {
        table_name: update.table_name,
        key: update.key,
        condition_expression: update.condition_expression,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values_on_condition_check_failure: None,
        aux_item_stream_ttl_hours: None,
    });
    assert!(JobTerminalProjection::try_from(delete).is_err());
    let mut put = valid.clone();
    let update = put.transact_items[0].update.take().unwrap();
    put.transact_items[0].put = Some(storage_types::TransactPutRequest {
        table_name: update.table_name,
        item: storage_types::WireItem::try_from(&storage_types::AttributeMap::from_iter([(
            "pk".to_string(),
            AttributeValue::S("job".to_string()),
        )]))
        .unwrap(),
        indexers: None,
        condition_expression: None,
        expression_attribute_names: None,
        expression_attribute_values: None,
        return_values_on_condition_check_failure: None,
        aux_item_stream_ttl_hours: None,
    });
    assert!(JobTerminalProjection::try_from(put).is_err());
    let mut unconditional = valid.clone();
    unconditional.transact_items[0]
        .update
        .as_mut()
        .unwrap()
        .condition_expression = None;
    assert!(JobTerminalProjection::try_from(unconditional).is_err());
    let mut unreceipted = valid;
    unreceipted.client_request_token = None;
    assert!(JobTerminalProjection::try_from(unreceipted).is_err());
}

#[test]
fn given_a_customer_alias_when_projection_is_prepared_then_existence_uses_a_distinct_alias() {
    let mut input = request();
    input.transact_items[0]
        .update
        .as_mut()
        .unwrap()
        .expression_attribute_names = Some(storage_types::VecMap::from_iter([(
        "#job_projection_key".to_string(),
        "status".to_string(),
    )]));
    let prepared = JobTerminalProjection::try_from(input)
        .unwrap()
        .into_request();
    let update = prepared.transact_items[0].update.as_ref().unwrap();
    assert_eq!(
        update.condition_expression.as_deref(),
        Some("(#status = :running) AND attribute_exists(#job_projection_key_)")
    );
    let names = update.expression_attribute_names.as_ref().unwrap();
    assert_eq!(
        names.get("#job_projection_key").map(String::as_str),
        Some("status")
    );
    assert_eq!(
        names.get("#job_projection_key_").map(String::as_str),
        Some("pk")
    );
}
