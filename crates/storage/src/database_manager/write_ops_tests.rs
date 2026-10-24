use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, AttributeValueRef, CreateTableRequest, ExprNameRef,
    ExprValueRef, KeyAttributeType, KeyRef, KeySchemaElement, KeyType, ReturnValuesOldNewUpdated,
    ScalarValueRef, TableName,
};

use crate::{DatabaseManager, DeleteItemInput, PutItemInput, UpdateItemInput};

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
        .get_item_decode::<_, storage_types::AttributeMap>(
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
async fn manager_put_item_persists_through_shared_owner() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("write_ops_gate_put");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([
                ("pk".to_string(), AttributeValue::S("item#1".to_string())),
                (
                    "payload".to_string(),
                    AttributeValue::S("value".to_string()),
                ),
            ]))
            .build(),
    )
    .await
    .expect("put succeeds after open gate");
    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read item")
        .expect("item exists");
    assert_eq!(
        stored.get("payload"),
        Some(&AttributeValue::S("value".to_string()))
    );
}

#[tokio::test]
async fn manager_update_item_persists_through_shared_owner() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("write_ops_gate_update");
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
    .expect("seed put");
    db.update_item(
        UpdateItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .update_expression("SET #count = :next".to_string())
            .expression_attribute_names(HashMap::from([(
                "#count".to_string(),
                "count".to_string(),
            )]))
            .expression_attribute_values(HashMap::from([(
                ":next".to_string(),
                AttributeValue::N("2".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("update succeeds after open gate");
    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
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
async fn manager_delete_item_persists_through_shared_owner() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("write_ops_gate_delete");
    create_hash_table(&db, &table_name).await;
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("seed put");
    db.delete_item(
        DeleteItemInput::builder()
            .table_name(table_name.clone())
            .key(HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("item#1".to_string()),
            )]))
            .build(),
    )
    .await
    .expect("delete succeeds after open gate");
    let stored = db
        .get_item_decode::<_, storage_types::AttributeMap>(
            table_name,
            HashMap::from([("pk".to_string(), AttributeValue::S("item#1".to_string()))]),
        )
        .await
        .expect("read item");
    assert!(stored.is_none());
}
