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
    // Postgres publishes a committed entry once the snapshot xmin passes it;
    // a concurrent test's open transaction can hold that back.
    store.wait_until_changes_are_readable().await;
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

pub(super) async fn system_records_with_count(
    store: &TestStore,
    limit: u32,
    expected: usize,
) -> anyhow::Result<Vec<StreamRecord>> {
    let scope = store.stream_operations.system_stream_scope().await?;
    let lanes = store
        .stream_operations
        .system_stream_lineage_catalog(scope.clone())
        .await?
        .len();
    let mut checkpoint = None;
    let mut records = Vec::new();
    // Each page consumes a catalog entry or a known record before the empty
    // tail.
    for _ in 0..lanes + expected {
        let page = store
            .stream_operations
            .get_system_stream_page(SystemStreamPageRequest::try_new(
                scope.clone(),
                checkpoint.clone(),
                limit,
            )?)
            .await?;
        anyhow::ensure!(page.source_scope() == &scope && page.records().len() <= limit as usize);
        if !page.terminal() {
            anyhow::ensure!(Some(page.issued_checkpoint().token()) != checkpoint.as_ref());
        }
        records.extend_from_slice(page.records());
        checkpoint = Some(page.issued_checkpoint().token().clone());
        if page.terminal() || records.len() >= expected {
            break;
        }
    }
    anyhow::ensure!(records.len() == expected);
    let tail = store
        .stream_operations
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope, checkpoint, 100)?)
        .await?;
    anyhow::ensure!(tail.records().is_empty() && tail.terminal());
    Ok(records)
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
    store.wait_until_changes_are_readable().await;
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
        let paged = system_records_with_count(&store, 1, 3).await?;
        let complete = system_records(&store, 10).await?;
        anyhow::ensure!(paged.len() == 3);
        for (index, record) in paged.iter().enumerate() {
            let position = record.system_position.as_ref().unwrap();
            for later in &paged[index + 1..] {
                let later = later.system_position.as_ref().unwrap();
                if position.lineage_id() == later.lineage_id() {
                    anyhow::ensure!(position.offset() < later.offset());
                }
            }
        }
        // Page limits may change the order between independent lineages.
        let mut paged_records: Vec<_> = paged.iter().collect();
        let mut complete_records: Vec<_> = complete.iter().collect();
        paged_records.sort_by(|left, right| left.system_position.cmp(&right.system_position));
        complete_records.sort_by(|left, right| left.system_position.cmp(&right.system_position));
        anyhow::ensure!(
            serde_json::to_value(paged_records)? == serde_json::to_value(complete_records)?
        );
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
            let observed = records
                .iter()
                .map(|record| {
                    serde_json::to_value(&record.item).map(|image| image["value"].clone())
                })
                .collect::<Result<Vec<_>, _>>()?;
            for expected in values {
                anyhow::ensure!(observed.contains(&json!({"S": expected})));
            }
        }
        anyhow::ensure!(system_records_with_count(&store, 1, 4).await?.len() == 4);
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

#[tokio::test]
async fn given_fresh_provider_when_system_stream_is_opened_then_initial_roots_match_provider_policy()
 {
    let expected = match TestStore::selected_backend().as_str() {
        "postgres" | "foundationdb" => 4,
        _ => 1,
    };
    let store = TestStore::new_durable().await;
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let page = store
        .stream_operations
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope.clone(), None, 100).unwrap())
        .await
        .unwrap();
    assert_eq!(
        page.issued_checkpoint().checkpoint().frontier().len(),
        expected
    );
    let before = page.issued_checkpoint().checkpoint().clone();
    let store = store.reopen().await;
    assert_eq!(
        store.stream_operations.system_stream_scope().await.unwrap(),
        scope
    );
    let page = store
        .stream_operations
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope, None, 100).unwrap())
        .await
        .unwrap();
    let after = page.issued_checkpoint().checkpoint();
    assert_eq!(after.scope(), before.scope());
    assert_eq!(after.frontier().len(), before.frontier().len());
    for (previous, current) in before.frontier().iter().zip(after.frontier()) {
        assert_eq!(current.shard_id(), previous.shard_id());
        assert_eq!(current.lineage_id(), previous.lineage_id());
        assert!(current.offset() >= previous.offset());
    }
    store.finish().await;
}
