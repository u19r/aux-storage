//! Disabled writes stay unpublished; enabling publishes only subsequent
//! mutations.

use serde_json::json;
use storage_types::PutItemRequest;

use super::{
    image_retention_tests::table_records,
    test_support::{TestStore, hash_table_request, unique_table_name},
};

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[derive(Default)]
struct NativeWrites {
    queries: std::sync::atomic::AtomicU64,
    writes: std::sync::atomic::AtomicU64,
    history_writes: std::sync::atomic::AtomicU64,
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl sql_backend::SqlFaultProbe for NativeWrites {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        use std::sync::atomic::Ordering::Relaxed;
        if call.operation == sql_backend::SqlFaultOperation::Execute {
            self.writes.fetch_add(1, Relaxed);
            if call.sql.contains("sys_stream_entries")
                || call.sql.contains("sys_dynamo_stream")
                || call.sql.contains("sys_system_stream")
                || call.sql.contains("sys_table_stream")
            {
                self.history_writes.fetch_add(1, Relaxed);
            }
        } else {
            self.queries.fetch_add(1, Relaxed);
        }
        None
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
#[derive(Default)]
struct RouteReads(std::sync::atomic::AtomicUsize);

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for RouteReads {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }
    fn observe_access(&self, access: kv_backend::KvAccess<'_>) {
        if matches!(
            access.kind,
            kv_backend::KvAccessKind::Read | kv_backend::KvAccessKind::ReadRange
        ) && (access.key == b"\x7fs/s" || access.key.starts_with(b"\x7fs/l/"))
        {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[tokio::test]
async fn given_disabled_table_when_overwriting_items_then_no_mutation_history_is_written()
-> anyhow::Result<()> {
    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    let counts = std::sync::Arc::new(NativeWrites::default());
    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    let store = TestStore::new_sql_with_fault_probe(counts.clone()).await;
    #[cfg(not(any(feature = "sqlite", feature = "turso", feature = "postgres")))]
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        for count in [&counts.queries, &counts.writes, &counts.history_writes] {
            count.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        for value in 0..100 {
            store.operations.put_item(PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"N": value.to_string()}}
            })).map_err(anyhow::Error::msg)?).await?;
        }
        let system = super::cdc_stream_tests::system_records(&store, 100).await?;
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        eprintln!(
            "native_calls: queries={} writes={} history_writes={}",
            counts.queries.load(std::sync::atomic::Ordering::Relaxed),
            counts.writes.load(std::sync::atomic::Ordering::Relaxed),
            counts
                .history_writes
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        anyhow::ensure!(
            counts
                .history_writes
                .load(std::sync::atomic::Ordering::Relaxed)
                == 0,
            "disabled write touched canonical history"
        );
        anyhow::ensure!(system.is_empty(), "disabled table emitted system records");
        anyhow::ensure!(
            store
                .operations
                .list_streams(serde_json::from_value(json!({"TableName": table}))?)
                .await?
                .streams
                .is_empty()
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

pub(super) async fn set_stream(
    store: &TestStore,
    table: &storage_types::TableName,
    enabled: bool,
) -> anyhow::Result<storage_types::UpdateTableResponse> {
    Ok(store.operations.update_table(serde_json::from_value(json!({
        "TableName": table, "StreamSpecification": {"StreamEnabled": enabled, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }))?).await?)
}

#[tokio::test]
async fn given_populated_disabled_table_when_enabled_then_only_real_mutations_publish_exact_images()
-> anyhow::Result<()> {
    let store = TestStore::new_durable().await;
    let table = unique_table_name();
    store
        .operations
        .create_table(hash_table_request(table.clone()))
        .await?;
    for key in ["before", "during", "removed"] {
        super::dynamodb_stream_history_tests::put_value(&store, &table, key, "old").await;
    }
    let response = set_stream(&store, &table, true).await?;
    anyhow::ensure!(
        response
            .table_description
            .stream_specification
            .unwrap()
            .stream_enabled
    );
    anyhow::ensure!(table_records(&store, &table).await?.is_empty());
    anyhow::ensure!(
        super::cdc_stream_tests::system_records(&store, 100)
            .await?
            .is_empty()
    );
    let arn = response.table_description.latest_stream_arn.unwrap();
    super::dynamodb_stream_history_tests::put_value(&store, &table, "during", "new").await;
    store
        .operations
        .delete_item(storage_types::DeleteItemRequest::new(
            table.clone(),
            [(
                "pk".into(),
                storage_types::AttributeValue::S("removed".into()),
            )],
        ))
        .await?;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "added", "new").await;
    let reopened = store.reopen().await;
    reopened.wait_until_changes_are_readable().await;
    let records = super::table_history_retention_tests::read_standard(&reopened, &arn).await?;
    anyhow::ensure!(records.len() == 3);
    let modified = records
        .iter()
        .find(|record| record.event_name == "MODIFY")
        .unwrap();
    let old = serde_json::from_value(json!({"pk": {"S": "during"}, "value": {"S": "old"}}))?;
    let new = serde_json::from_value(json!({"pk": {"S": "during"}, "value": {"S": "new"}}))?;
    anyhow::ensure!(modified.dynamodb.old_image == Some(old));
    anyhow::ensure!(modified.dynamodb.new_image == Some(new));
    let reads = super::image_retention_tests::canonical_history_reads(&reopened, &table).await?;
    let retained = (reopened.canonical_history_read)(reads.clone()).await?;
    set_stream(&reopened, &table, false).await?;
    super::dynamodb_stream_history_tests::put_value(&reopened, &table, "during", "disabled").await;
    let after = (reopened.canonical_history_read)(reads).await?;
    anyhow::ensure!(
        after
            .iter()
            .flat_map(|page| &page.entries)
            .map(|entry| entry.entry.encode())
            .collect::<Result<Vec<_>, _>>()?
            == retained
                .iter()
                .flat_map(|page| &page.entries)
                .map(|entry| entry.entry.encode())
                .collect::<Result<Vec<_>, _>>()?
    );
    set_stream(&reopened, &table, true).await?;
    super::dynamodb_stream_history_tests::put_value(&reopened, &table, "during", "reenabled").await;
    reopened.wait_until_changes_are_readable().await;
    anyhow::ensure!(
        super::cdc_stream_tests::system_records(&reopened, 100)
            .await?
            .len()
            == 4
    );
    reopened.finish().await;
    Ok(())
}

#[tokio::test]
async fn given_disabled_table_with_active_immediate_index_when_writing_then_index_changes_without_history()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store.operations.create_table(super::gsi_workflow_tests::table_request(table.as_ref())).await?;
        for group in ["old", "new"] { super::gsi_workflow_tests::put_group(&store, table.as_ref(), "one", group).await?; }
        let rows = store.operations.query(storage_types::QueryRequest::try_from(json!({
            "TableName": table, "IndexName": "by_group", "KeyConditionExpression": "gsi_pk = :group", "ExpressionAttributeValues": {":group": {"S": "new"}}
        })).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(rows.items.unwrap_or_default().len() == 1);
        anyhow::ensure!(super::cdc_stream_tests::system_records(&store, 100).await?.is_empty());
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_disabled_stream_when_mutating_through_each_public_verb_then_primary_results_have_no_history()
-> anyhow::Result<()> {
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    let route_reads = std::sync::Arc::new(RouteReads::default());
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    let store = TestStore::new_kv_with_fault_probe(route_reads.clone()).await;
    #[cfg(not(any(feature = "rocksdb", feature = "foundationdb")))]
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store.operations.create_table(hash_table_request(table.clone())).await?;
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        route_reads.0.store(0, std::sync::atomic::Ordering::Relaxed);
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "old").await;
        store.operations.update_item(storage_types::UpdateItemRequest::try_from(json!({
            "TableName": table, "Key": {"pk": {"S": "one"}}, "UpdateExpression": "SET #v = :v",
            "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": "new"}}
        })).map_err(anyhow::Error::msg)?).await?;
        store.operations.batch_write_item(storage_types::BatchWriteItemRequest::try_from(json!({
            "RequestItems": {table.as_ref(): [
                {"PutRequest": {"Item": {"pk": {"S": "two"}}}},
                {"DeleteRequest": {"Key": {"pk": {"S": "one"}}}}
            ]}
        })).map_err(anyhow::Error::msg)?).await?;
        store.operations.transact_write_items(storage_types::TransactWriteItemsRequest::try_from(json!({
            "TransactItems": [
                {"Put": {"TableName": table, "Item": {"pk": {"S": "three"}}}},
                {"Delete": {"TableName": table, "Key": {"pk": {"S": "two"}}}}
            ]
        })).map_err(anyhow::Error::msg)?).await?;
        store.operations.delete_item(storage_types::DeleteItemRequest::new(table.clone(), [("pk".into(), storage_types::AttributeValue::S("three".into()))])).await?;
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        anyhow::ensure!(route_reads.0.load(std::sync::atomic::Ordering::Relaxed) == 0,
            "stream-disabled single, batch or transaction mutation read scope or lineages");
        let scanned = store.operations.scan(storage_types::ScanRequest::try_from(json!({"TableName": table})).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(scanned.items.unwrap_or_default().is_empty());
        anyhow::ensure!(super::cdc_stream_tests::system_records(&store, 100).await?.is_empty());
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_disabled_stream_when_ttl_build_and_expiration_run_then_expiry_needs_no_history()
-> anyhow::Result<()> {
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    let route_reads = std::sync::Arc::new(RouteReads::default());
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    let store = TestStore::new_kv_with_fault_probe(route_reads.clone()).await;
    #[cfg(not(any(feature = "rocksdb", feature = "foundationdb")))]
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store.operations.create_table(hash_table_request(table.clone())).await?;
        let expired = (store.now().timestamp_millis() / 1000 - 60).to_string();
        store.operations.put_item(PutItemRequest::try_from(json!({"TableName": table, "Item": {"pk": {"S": "due"}, "ttl": {"N": expired}}})).map_err(anyhow::Error::msg)?).await?;
        store.operations.update_time_to_live(serde_json::from_value(json!({"TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}}))?).await?;
        super::gsi_lifecycle_tests::finish_backfill(&store, 8).await?;
        let request = storage_provider::maintenance::ExpirationRequest {
            destination: scheduled_bg_jobs::Namespace::try_new("stream-eligibility-expiration")?,
            job: [19; 16].into(), fence: std::num::NonZeroU64::MIN,
            requested_cutoff: store.now(), candidate_limit: std::num::NonZeroUsize::new(8).unwrap(),
        };
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        route_reads.0.store(0, std::sync::atomic::Ordering::Relaxed);
        for _ in 0..16 {
            if matches!(store.maintenance.advance_expiration(request.clone()).await?, storage_types::maintenance::StepResult::Complete) { break; }
        }
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        anyhow::ensure!(route_reads.0.load(std::sync::atomic::Ordering::Relaxed) == 0,
            "stream-disabled expiration read scope or lineages");
        let item = store.operations.get_item(storage_types::GetItemRequest::try_from(json!({"TableName": table, "Key": {"pk": {"S": "due"}}})).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(item.item.is_none(), "expiry did not remove the current item");
        anyhow::ensure!(super::cdc_stream_tests::system_records(&store, 100).await?.is_empty());
        // An enabled stream still needs no routes for an empty direct sweep.
        set_stream(&store, &table, true).await?;
        let policy = store.maintenance.read_ttl_sweep_policy(&table).await?.unwrap();
        let now = store.now();
        let input = storage_provider::TtlSweepPageInput::try_new("ttl", now, 100, None)?;
        let command = storage_provider::TtlSweepPageCommand::try_new(policy, input, now)?;
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        route_reads.0.store(0, std::sync::atomic::Ordering::Relaxed);
        let swept = store.maintenance.execute_ttl_sweep_page(command).await?;
        anyhow::ensure!(swept.deleted_items == 0);
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        anyhow::ensure!(route_reads.0.load(std::sync::atomic::Ordering::Relaxed) == 0,
            "empty direct expiration read scope or lineages");
        set_stream(&store, &table, false).await?;
        store.operations.update_time_to_live(serde_json::from_value(json!({"TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": false}}))?).await?;
        anyhow::ensure!(!store.operations.get_table_info(table).await?.requires_stream_entry());
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_disabled_enable_when_reenabled_then_only_subsequent_mutation_is_published()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "old").await;
        set_stream(&store, &table, true).await?;
        set_stream(&store, &table, false).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "current").await;
        set_stream(&store, &table, true).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "after").await;
        store.wait_until_changes_are_readable().await;
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .len()
                == 1
        );
        let arn = store
            .operations
            .describe_table(storage_types::DescribeTableRequest {
                table_name: table.clone(),
            })
            .await?
            .table
            .latest_stream_arn
            .unwrap();
        let records = super::table_history_retention_tests::read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1 && records[0].event_name == "MODIFY");
        anyhow::ensure!(
            records[0].dynamodb.old_image
                == Some(serde_json::from_value(
                    json!({"pk": {"S": "one"}, "value": {"S": "current"}})
                )?)
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_enabled_table_when_recreated_then_replacement_stays_disabled() -> anyhow::Result<()>
{
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "old").await;
        set_stream(&store, &table, true).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "old-event").await;
        store.wait_until_changes_are_readable().await;
        let old_reads =
            super::image_retention_tests::canonical_history_reads(&store, &table).await?;
        store
            .operations
            .delete_table(storage_types::DeleteTableRequest {
                table_name: table.clone(),
            })
            .await?;
        store.advance_time(std::time::Duration::from_secs(30));
        store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        anyhow::ensure!(
            !store
                .operations
                .get_table_info(table.clone())
                .await?
                .requires_stream_entry()
        );
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "replacement").await;
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .is_empty()
        );
        set_stream(&store, &table, true).await?;
        anyhow::ensure!(table_records(&store, &table).await?.is_empty());
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "new-event").await;
        store.wait_until_changes_are_readable().await;
        let new_reads =
            super::image_retention_tests::canonical_history_reads(&store, &table).await?;
        anyhow::ensure!(
            old_reads[0].lane != new_reads[0].lane,
            "recreated table reused the old incarnation"
        );
        anyhow::ensure!(table_records(&store, &table).await?.len() == 1);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_retained_event_when_disabled_writes_replace_its_item_then_predecessor_survives_until_expiry()
-> anyhow::Result<()> {
    use std::time::Duration;

    use super::table_history_retention_tests::{change_duration, cleanup_page};

    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        change_duration(&store, &table, 24).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "item", "anchor").await;
        set_stream(&store, &table, true).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "item", "event").await;
        store.wait_until_changes_are_readable().await;
        let reads = super::image_retention_tests::canonical_history_reads(&store, &table).await?;
        let retained = (store.canonical_history_read)(reads.clone()).await?;
        set_stream(&store, &table, false).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "item", "uncaptured").await;
        for _ in 0..8 {
            store.advance_time(Duration::from_millis(1));
            cleanup_page(&store, 100).await?;
        }
        anyhow::ensure!(
            (store.canonical_history_read)(reads.clone())
                .await?
                .iter()
                .flat_map(|page| &page.entries)
                .map(|entry| entry.entry.encode())
                .collect::<Result<Vec<_>, _>>()?
                == retained
                    .iter()
                    .flat_map(|page| &page.entries)
                    .map(|entry| entry.entry.encode())
                    .collect::<Result<Vec<_>, _>>()?,
            "retained event lost its predecessor"
        );
        store.advance_time(Duration::from_secs(25 * 3600));
        for _ in 0..8 {
            store.advance_time(Duration::from_millis(1));
            cleanup_page(&store, 100).await?;
        }
        anyhow::ensure!(
            super::table_history_retention_tests::history_count(&store, &reads).await? == 0,
            "expired canonical event leaked after disable"
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}
