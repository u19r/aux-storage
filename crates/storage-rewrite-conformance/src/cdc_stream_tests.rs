//! Public snapshot streams preserve committed values and rejected-write
//! atomicity.

use std::collections::BTreeSet;

use serde_json::json;
use storage_types::{CreateTableRequest, PutItemRequest, StreamRecord, TableName};
use stream_provider::SystemStreamPageRequest;

use super::{
    image_retention_tests::table_records,
    test_support::{TestStore, hash_table_request, unique_table_name},
};

pub(super) async fn system_records(
    store: &TestStore,
    limit: u32,
) -> anyhow::Result<Vec<StreamRecord>> {
    let scope = store.stream_operations.system_stream_scope().await?;
    let mut checkpoint = None;
    let mut records = Vec::new();
    for _ in 0..16 {
        let page = store
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(
                scope.clone(),
                checkpoint,
                limit,
            )?)
            .await?;
        records.extend_from_slice(page.records());
        if page.terminal() {
            return Ok(records);
        }
        checkpoint = Some(page.issued_checkpoint().token().clone());
    }
    anyhow::bail!("snapshot fixture exceeded bounded pagination")
}

async fn create_table(store: &TestStore) -> anyhow::Result<TableName> {
    let table = unique_table_name();
    create_named_table(store, table.clone()).await?;
    Ok(table)
}

pub(super) async fn create_named_table(store: &TestStore, table: TableName) -> anyhow::Result<()> {
    let mut request: CreateTableRequest = hash_table_request(table);
    request.stream_specification = Some(storage_types::StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
    });
    store.operations.create_table(request).await?;
    Ok(())
}

async fn put(store: &TestStore, table: &TableName, key: &str, value: &str) -> anyhow::Result<()> {
    store
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": key}, "value": {"S": value}}
            }))
            .unwrap(),
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn given_stream_enabled_table_when_item_is_put_then_cdc_exposes_the_exact_new_image()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let result = async {
        let table = create_table(&store).await?;
        anyhow::ensure!(system_records(&store, 10).await?.is_empty());
        put(&store, &table, "customer-1", "active").await?;
        let records = system_records(&store, 10).await?;
        anyhow::ensure!(records.len() == 1);
        anyhow::ensure!(records[0].source_table_name.as_ref() == Some(&table));
        anyhow::ensure!(
            serde_json::to_value(&records[0].item)?
                == json!({"pk": {"S": "customer-1"}, "value": {"S": "active"}})
        );
        anyhow::ensure!(
            serde_json::to_value(&table_records(&store, &table).await?[0].item)?
                == serde_json::to_value(&records[0].item)?
        );
        Ok(())
    }
    .await;
    store.finish().await;
    result
}

#[tokio::test]
async fn given_existing_item_when_conditional_put_fails_then_no_cdc_record_is_added()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let result = async {
        let table = create_table(&store).await?;
        put(&store, &table, "customer-1", "existing").await?;
        let before = system_records(&store, 10).await?;
        let error = store.operations.put_item(PutItemRequest::try_from(json!({
            "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "replacement"}},
            "ConditionExpression": "attribute_not_exists(pk)"
        })).unwrap()).await.expect_err("existing key must reject condition");
        anyhow::ensure!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::ConditionalCheckFailed
        ));
        let after = system_records(&store, 10).await?;
        anyhow::ensure!(serde_json::to_value(before)? == serde_json::to_value(after)?);
        let current = store
            .operations
            .get_item(
                storage_types::GetItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await?;
        anyhow::ensure!(
            serde_json::to_value(current)?["Item"]["value"] == json!({"S": "existing"})
        );
        Ok(())
    }
    .await;
    store.finish().await;
    result
}

#[tokio::test]
async fn given_three_committed_items_when_cdc_is_paged_then_every_change_appears_once()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let result = async {
        let table = create_table(&store).await?;
        for key in ["A", "B", "C"] {
            put(&store, &table, key, key).await?;
        }
        let paged = system_records(&store, 1).await?;
        let complete = system_records(&store, 10).await?;
        anyhow::ensure!(paged.len() == 3);
        anyhow::ensure!(serde_json::to_value(&paged)? == serde_json::to_value(complete)?);
        anyhow::ensure!(
            paged
                .iter()
                .map(|row| &row.sequence_number)
                .collect::<BTreeSet<_>>()
                .len()
                == 3
        );
        Ok(())
    }
    .await;
    store.finish().await;
    result
}

#[tokio::test]
async fn given_two_stream_enabled_tables_when_writes_interleave_then_each_table_stream_pages_only_its_own_changes()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let result = async {
        let first = create_table(&store).await?;
        let second = create_table(&store).await?;
        for (table, value) in [
            (&first, "A1"),
            (&second, "B1"),
            (&first, "A2"),
            (&second, "B2"),
        ] {
            put(&store, table, value, value).await?;
        }
        for (table, values) in [(&first, ["A1", "A2"]), (&second, ["B1", "B2"])] {
            let records = table_records(&store, table).await?;
            anyhow::ensure!(records.len() == 2);
            for (record, expected) in records.iter().zip(values) {
                anyhow::ensure!(
                    serde_json::to_value(&record.item)?["value"] == json!({"S": expected})
                );
            }
        }
        anyhow::ensure!(system_records(&store, 1).await?.len() == 4);
        Ok(())
    }
    .await;
    store.finish().await;
    result
}

#[tokio::test]
async fn given_large_item_when_put_then_cdc_snapshot_has_exact_logical_attributes()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let result = async {
        let table = create_table(&store).await?;
        let value = "wide".repeat(40_000);
        put(&store, &table, "large", &value).await?;
        let records = system_records(&store, 10).await?;
        anyhow::ensure!(records.len() == 1);
        anyhow::ensure!(serde_json::to_value(&records[0].item)?["value"] == json!({"S": value}));
        Ok(())
    }
    .await;
    store.finish().await;
    result
}

/// The page read runs in a read-only native attempt, so it must use snapshot
/// reads and never request conflict protection, which only write attempts
/// allow.
#[tokio::test]
async fn system_stream_page_reads_history_in_a_read_only_attempt() {
    let store = TestStore::new().await;
    let table = create_table(&store).await.unwrap();
    put(&store, &table, "customer-1", "active").await.unwrap();
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let request =
        stream_provider::SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap();
    let first = store
        .stream_operations
        .get_system_stream_page(request)
        .await
        .unwrap();
    assert!(!first.records().is_empty());
    let resumed = stream_provider::SystemStreamPageRequest::try_new(
        scope,
        Some(first.issued_checkpoint().token().clone()),
        10,
    )
    .unwrap();
    let second = store
        .stream_operations
        .get_system_stream_page(resumed)
        .await
        .unwrap();
    assert!(second.records().is_empty());
    store.finish().await;
}
