use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, AttributeValueRef, CreateTableRequest, ExprNameRef,
    ExprValueRef, KeyAttributeType, KeyRef, KeySchemaElement, KeyType, ReturnValuesOldNewUpdated,
    ScalarValueRef, StorageEnum, TableName, context::WrappedError as _,
};

use crate::{DatabaseManager, PutItemInput};

async fn create_hash_table(db: &DatabaseManager, table_name: &TableName) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    db.create_table(&request).await.expect("create table");
}

#[tokio::test]
async fn update_item_ref_converts_lightweight_expression_refs_and_returns_updated_attributes() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("write_ops_update_ref");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                ("count".to_string(), AttributeValue::N("1".to_string())),
            ]))
            .build(),
    )
    .await
    .expect("put item");

    let response = db
        .update_item_ref(
            table_name.clone(),
            KeyRef::new("pk", ScalarValueRef::S("item#1"), None, None),
            "SET #count = :next".to_string(),
            Some("attribute_exists(pk)".to_string()),
            Some(&[ExprNameRef::new("#count", "count")]),
            Some(&[ExprValueRef::new(":next", AttributeValueRef::N("2"))]),
            Some(ReturnValuesOldNewUpdated::UpdatedNew),
        )
        .await
        .expect("update item");

    let attributes = response.attributes.expect("updated attributes");
    assert_eq!(
        attributes.get("count"),
        Some(&AttributeValue::N("2".to_string()))
    );
    let stored = db
        .get_item_map(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read item")
        .expect("item exists");
    assert_eq!(
        stored.get("count"),
        Some(&AttributeValue::N("2".to_string()))
    );
}

#[tokio::test]
async fn update_item_ref_rejects_unused_expression_values_before_writing() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("write_ops_update_ref_validation");
    create_hash_table(&db, &table_name).await;

    let error = db
        .update_item_ref(
            table_name,
            KeyRef::new("pk", ScalarValueRef::S("item#1"), None, None),
            "SET #count = :next".to_string(),
            None,
            Some(&[ExprNameRef::new("#count", "count")]),
            Some(&[
                ExprValueRef::new(":next", AttributeValueRef::N("2")),
                ExprValueRef::new(":unused", AttributeValueRef::N("3")),
            ]),
            Some(ReturnValuesOldNewUpdated::UpdatedNew),
        )
        .await
        .expect_err("unused expression value fails");

    assert!(format!("{error}").contains(":unused"));
}

#[tokio::test]
async fn resolved_operation_rejects_a_write_for_another_table() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let resolved_table = TableName::new("resolved_operation_table");
    create_hash_table(&db, &resolved_table).await;
    let operation = db
        .resolve_storage_operation(resolved_table)
        .await
        .expect("resolve table");

    let error = db
        .put_item_with_resolved_operation(
            operation,
            PutItemInput::builder()
                .table_name(TableName::new("different_table"))
                .item(HashMap::from([(
                    "pk".to_string(),
                    AttributeValue::S("item#1".to_string()),
                )]))
                .build(),
        )
        .await
        .expect_err("resolved context must not bind to another table");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message }
            if message == "resolved storage operation does not match PutItem table"
    ));
}
