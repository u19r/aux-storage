//! With replication off the SQL schema has no regional relation (REG-13
//! `schema_statements(.., false)`), so every operation must succeed without
//! naming one.

use std::time::Duration;

use serde_json::json;
use storage_common::{TtlSweepPageCommand, TtlSweepPageInput};
use storage_types::{QueryRequest, UpdateItemRequest};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, put_value},
    regional_write_tests::{delete, delete_table, put},
    table_history_retention_tests::{change_duration, cleanup_page},
    test_support::TestStore,
};

#[tokio::test]
async fn given_sqlite_schema_without_regional_relations_when_operations_run_then_all_succeed() {
    let directory = tempfile::tempdir().unwrap();
    let store =
        TestStore::open_sqlite_regional_off(&directory.path().join("regional-off.db")).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    change_duration(&store, &table, 24).await.unwrap();
    store
        .operations
        .update_time_to_live(
            serde_json::from_value(json!({
                "TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    put(&store, &table, "a").await.unwrap();
    put_value(&store, &table, "b", "one").await;
    store
        .operations
        .update_item(
            UpdateItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": "b"}},
                "UpdateExpression": "SET #v = :v",
                "ExpressionAttributeNames": {"#v": "value"},
                "ExpressionAttributeValues": {":v": {"S": "two"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    delete(&store, &table, "a").await.unwrap();
    delete(&store, &table, "absent").await.unwrap();
    let found = store
        .operations
        .query(
            QueryRequest::try_from(json!({
                "TableName": table.as_ref(), "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "b"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(found).unwrap()["Count"], 1);

    // One TTL expiration page: an expired item is deleted.
    store
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Item": {"pk": {"S": "expired"}, "ttl": {"N": "1"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let policy = store
        .maintenance
        .read_ttl_sweep_policy(&table)
        .await
        .unwrap()
        .unwrap();
    let now = store.now();
    let command = TtlSweepPageCommand::try_new(
        policy,
        TtlSweepPageInput::try_new("ttl", now, 10, None).unwrap(),
        now,
    )
    .unwrap();
    assert_eq!(
        store
            .maintenance
            .execute_ttl_sweep_page(command)
            .await
            .unwrap()
            .deleted_items,
        1
    );

    // One retention page: history older than the policy is retired.
    store.wait_until_changes_are_readable().await;
    store.advance_time(Duration::from_secs(49 * 3600));
    assert!(cleanup_page(&store, 10).await.unwrap().rows_deleted > 0);

    delete_table(&store, &table).await.unwrap();
    store.finish().await;
}
