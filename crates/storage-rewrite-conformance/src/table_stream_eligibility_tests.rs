//! Disabled tables do no history work; enable captures existing values first.

use std::time::Instant;

use serde_json::json;
use storage_types::{BoundTableInfo, PutItemRequest, TableHistoryId};
use stream_provider::StreamRowKind;

use super::test_support::{TestStore, hash_table_request, unique_table_name};

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
            if call.sql.contains("sys_dynamo_stream")
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
        let created = store
            .operations
            .create_table(hash_table_request(table.clone()))
            .await?;
        let history_id = TableHistoryId::from(
            *uuid::Uuid::parse_str(&created.table_description.table_id)?.as_bytes(),
        );
        let bound = BoundTableInfo {
            history_id,
            info: store.operations.get_table_info(table.clone()).await?,
        };
        let key = serde_json::from_value(json!({"pk": {"S": "one"}}))?;
        let address = stream_provider::dynamo_item_history_address(&bound, &key)?;
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        for count in [&counts.queries, &counts.writes, &counts.history_writes] {
            count.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        let started = Instant::now();
        for value in 0..100 {
            store.operations.put_item(PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"N": value.to_string()}}
            })).map_err(anyhow::Error::msg)?).await?;
        }
        let elapsed = started.elapsed();
        let system = super::cdc_stream_tests::system_records(&store, 100).await?;
        let history = store
            .stream_operations
            .read_forward(
                StreamRowKind::Item(address.physical_stream_name()?),
                None,
                1000,
            )
            .await?;
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        eprintln!(
            "native_calls: queries={} writes={} history_writes={}",
            counts.queries.load(std::sync::atomic::Ordering::Relaxed),
            counts.writes.load(std::sync::atomic::Ordering::Relaxed),
            counts
                .history_writes
                .load(std::sync::atomic::Ordering::Relaxed)
        );
        eprintln!(
            "eligibility: writes=100 elapsed_us={} system_rows={} item_rows={} item_bytes={}",
            elapsed.as_micros(),
            system.len(),
            history.items.len(),
            history
                .items
                .iter()
                .map(|row| row.data.len())
                .sum::<usize>()
        );
        anyhow::ensure!(
            system.is_empty(),
            "disabled table emitted {} system records",
            system.len()
        );
        anyhow::ensure!(
            history.items.is_empty(),
            "disabled table emitted item history"
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

pub(super) async fn finish_initialization(
    store: &TestStore,
    table: &storage_types::TableName,
) -> anyhow::Result<()> {
    for _ in 0..256 {
        let description = store
            .operations
            .describe_table(storage_types::DescribeTableRequest {
                table_name: table.clone(),
            })
            .await?
            .table;
        if description
            .stream_specification
            .as_ref()
            .is_some_and(|spec| spec.stream_enabled)
        {
            return Ok(());
        }
        store
            .stream_initialization_repository
            .advance_table_stream_initialization()
            .await?;
    }
    anyhow::bail!("stream initialization did not complete")
}

#[tokio::test]
async fn given_populated_disabled_table_when_enabling_during_writes_then_current_images_precede_publication()
-> anyhow::Result<()> {
    let store = TestStore::new_durable().await;
    let table = unique_table_name();
    let created = store
        .operations
        .create_table(hash_table_request(table.clone()))
        .await?;
    let history_id = TableHistoryId::from(
        *uuid::Uuid::parse_str(&created.table_description.table_id)?.as_bytes(),
    );
    for key in ["before", "during", "removed"] {
        super::dynamodb_stream_history_tests::put_value(&store, &table, key, "old").await;
    }
    for index in 0..40 {
        super::dynamodb_stream_history_tests::put_value(
            &store,
            &table,
            &format!("a{index:03}"),
            "filler",
        )
        .await;
    }
    let response = set_stream(&store, &table, true).await?;
    anyhow::ensure!(
        response.table_description.table_status == storage_types::TableStatus::Updating
    );
    anyhow::ensure!(
        !response
            .table_description
            .stream_specification
            .unwrap()
            .stream_enabled
    );
    let listed = store
        .operations
        .list_streams(serde_json::from_value(json!({"TableName": table}))?)
        .await?;
    anyhow::ensure!(
        listed.streams.is_empty(),
        "uninitialized stream was published"
    );
    store
        .stream_initialization_repository
        .advance_table_stream_initialization()
        .await?;
    anyhow::ensure!(
        !store
            .operations
            .describe_table(storage_types::DescribeTableRequest {
                table_name: table.clone()
            })
            .await?
            .table
            .stream_specification
            .unwrap()
            .stream_enabled,
        "first page published an incomplete stream"
    );
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
    finish_initialization(&reopened, &table).await?;
    let bound = BoundTableInfo {
        history_id,
        info: reopened.operations.get_table_info(table.clone()).await?,
    };
    let key = serde_json::from_value(json!({"pk": {"S": "before"}}))?;
    let address = stream_provider::dynamo_item_history_address(&bound, &key)?;
    let initial = reopened
        .stream_operations
        .read_forward(
            StreamRowKind::Item(address.physical_stream_name()?),
            None,
            100,
        )
        .await?;
    anyhow::ensure!(
        initial.items.len() == 1,
        "current item was not captured exactly once"
    );
    let records = super::cdc_stream_tests::system_records(&reopened, 100).await?;
    anyhow::ensure!(
        records.len() == 3,
        "initialization fabricated mutation history"
    );
    let arn = reopened
        .operations
        .describe_table(storage_types::DescribeTableRequest {
            table_name: table.clone(),
        })
        .await?
        .table
        .latest_stream_arn
        .unwrap();
    let shards = super::dynamodb_stream_history_tests::describe_shards(&reopened, &arn).await;
    let mut standard_records = Vec::new();
    for shard in shards {
        let token = super::dynamodb_stream_history_tests::iterator(
            &reopened,
            &arn,
            &shard.shard_id,
            "TRIM_HORIZON",
            None,
        )
        .await;
        standard_records.extend(
            reopened
                .operations
                .get_records(storage_types::GetRecordsRequest {
                    shard_iterator: token,
                    limit: Some(100),
                })
                .await?
                .records,
        );
    }
    let modified = standard_records
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    anyhow::ensure!(
        modified
            .iter()
            .any(|record| record["dynamodb"]["OldImage"]["value"]["S"] == "old"),
        "old image is missing"
    );
    set_stream(&reopened, &table, false).await?;
    super::dynamodb_stream_history_tests::put_value(&reopened, &table, "during", "disabled").await;
    anyhow::ensure!(
        super::cdc_stream_tests::system_records(&reopened, 100)
            .await?
            .len()
            == 3
    );
    set_stream(&reopened, &table, true).await?;
    finish_initialization(&reopened, &table).await?;
    super::dynamodb_stream_history_tests::put_value(&reopened, &table, "during", "reenabled").await;
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
    let store = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        store.operations.create_table(hash_table_request(table.clone())).await?;
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
        let scanned = store.operations.scan(storage_types::ScanRequest::try_from(json!({"TableName": table})).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(scanned.items.unwrap_or_default().is_empty());
        anyhow::ensure!(super::cdc_stream_tests::system_records(&store, 100).await?.is_empty());
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_internal_gsi_capture_when_build_finishes_then_public_stream_stays_off_and_history_stops()
-> anyhow::Result<()> {
    let store = TestStore::new_durable().await;
    let table = unique_table_name();
    let mut create = super::gsi_workflow_tests::table_request(table.as_ref());
    let indexes = create.global_secondary_indexes.take().unwrap();
    store.operations.create_table(create).await?;
    super::gsi_workflow_tests::put_group(&store, table.as_ref(), "base", "old").await?;
    store
        .operations
        .update_table(serde_json::from_value(json!({
            "TableName": table, "GlobalSecondaryIndexUpdates": [{"Create": indexes[0]}]
        }))?)
        .await?;
    let info = store.operations.get_table_info(table.clone()).await?;
    anyhow::ensure!(info.has_internal_stream_capture() && !info.has_enabled_stream());
    anyhow::ensure!(info.table_status == storage_types::TableStatus::Active);
    anyhow::ensure!(
        !store
            .stream_initialization_repository
            .advance_table_stream_initialization()
            .await?,
        "internal capture scheduled an unnecessary full item scan"
    );
    anyhow::ensure!(
        store
            .operations
            .list_streams(serde_json::from_value(json!({"TableName": table}))?)
            .await?
            .streams
            .is_empty()
    );
    store
        .gsi_build_repository
        .advance_gsi_build(super::gsi_workflow_tests::build_request(1))
        .await?;
    super::gsi_workflow_tests::put_group(&store, table.as_ref(), "behind", "new").await?;
    super::gsi_workflow_tests::put_group(&store, table.as_ref(), "base", "new").await?;
    let store = store.reopen().await;
    let outcome = async {
        anyhow::ensure!(
            store
                .operations
                .get_table_info(table.clone())
                .await?
                .has_internal_stream_capture()
        );
        super::gsi_lifecycle_tests::finish_backfill(&store, 1).await?;
        let info = store.operations.get_table_info(table.clone()).await?;
        anyhow::ensure!(
            !info.requires_stream_entry() && !info.has_enabled_stream(),
            "completed internal GSI build left mutation capture enabled"
        );
        anyhow::ensure!(
            super::gsi_workflow_tests::group_keys(&store, table.as_ref(), "new").await?
                == ["base", "behind"]
        );
        let before = super::cdc_stream_tests::system_records(&store, 100)
            .await?
            .len();
        anyhow::ensure!(before == 2, "build history did not survive restart");
        super::gsi_workflow_tests::put_group(&store, table.as_ref(), "base", "last").await?;
        anyhow::ensure!(
            super::gsi_workflow_tests::group_keys(&store, table.as_ref(), "last").await?
                == ["base"]
        );
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .len()
                == before
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_disabled_stream_when_ttl_build_and_expiration_run_then_expiry_needs_no_history()
-> anyhow::Result<()> {
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
        for _ in 0..16 {
            if matches!(store.expiration_repository.advance_expiration(request.clone()).await?, storage_types::maintenance::StepResult::Complete) { break; }
        }
        let item = store.operations.get_item(storage_types::GetItemRequest::try_from(json!({"TableName": table, "Key": {"pk": {"S": "due"}}})).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(item.item.is_none(), "expiry did not remove the current item");
        anyhow::ensure!(super::cdc_stream_tests::system_records(&store, 100).await?.is_empty());
        store.operations.update_time_to_live(serde_json::from_value(json!({"TableName": table, "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": false}}))?).await?;
        anyhow::ensure!(!store.operations.get_table_info(table).await?.requires_stream_entry());
        Ok(())
    }.await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_cancelled_enable_when_reenabled_then_current_value_is_captured_before_publication()
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
        finish_initialization(&store, &table).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "after").await;
        anyhow::ensure!(
            super::cdc_stream_tests::system_records(&store, 100)
                .await?
                .len()
                == 1
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_pending_enable_when_table_is_recreated_then_old_work_cannot_enable_replacement()
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
            store
                .stream_initialization_repository
                .advance_table_stream_initialization()
                .await?
        );
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
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_initialization_anchors_when_stream_is_disabled_then_unused_images_are_reclaimed()
-> anyhow::Result<()> {
    use std::time::Duration;

    use super::table_history_retention_tests::{change_duration, cleanup_page, history_count};

    for hours in [24, -1] {
        let store = TestStore::new().await;
        let outcome = async {
            let table = unique_table_name();
            let created = store
                .operations
                .create_table(hash_table_request(table.clone()))
                .await?;
            let bound = BoundTableInfo {
                history_id: TableHistoryId::from(
                    *uuid::Uuid::parse_str(&created.table_description.table_id)?.as_bytes(),
                ),
                info: store.operations.get_table_info(table.clone()).await?,
            };
            let key = serde_json::from_value(json!({"pk": {"S": "item"}}))?;
            let address = stream_provider::dynamo_item_history_address(&bound, &key)?;
            let stream = StreamRowKind::Item(address.physical_stream_name()?);
            change_duration(&store, &table, hours).await?;
            for version in ["first", "second", "third"] {
                super::dynamodb_stream_history_tests::put_value(&store, &table, "item", version)
                    .await;
                set_stream(&store, &table, true).await?;
                finish_initialization(&store, &table).await?;
                anyhow::ensure!(
                    history_count(&store, &stream).await? == 1,
                    "initialization did not capture one current image"
                );
                set_stream(&store, &table, false).await?;
                super::dynamodb_stream_history_tests::put_value(
                    &store,
                    &table,
                    "item",
                    "uncaptured",
                )
                .await;
                for _ in 0..8 {
                    store.advance_time(Duration::from_millis(1));
                    cleanup_page(&store, 100).await?;
                }
                anyhow::ensure!(
                    history_count(&store, &stream).await? == 0,
                    "disabled anchor leaked under retention {hours}"
                );
            }
            Ok(())
        }
        .await;
        store.finish().await;
        outcome?;
    }
    Ok(())
}

#[tokio::test]
async fn given_retained_event_when_disabled_writes_replace_its_item_then_predecessor_survives_until_expiry()
-> anyhow::Result<()> {
    use std::time::Duration;

    use super::table_history_retention_tests::{
        change_duration, cleanup_page, history_count, item_history,
    };

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
        finish_initialization(&store, &table).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "item", "event").await;
        let stream = item_history(&store, &table).await?;
        set_stream(&store, &table, false).await?;
        super::dynamodb_stream_history_tests::put_value(&store, &table, "item", "uncaptured").await;
        for _ in 0..8 {
            store.advance_time(Duration::from_millis(1));
            cleanup_page(&store, 100).await?;
        }
        anyhow::ensure!(
            history_count(&store, &stream).await? == 2,
            "retained event lost its predecessor"
        );
        store.advance_time(Duration::from_secs(25 * 3600));
        for _ in 0..8 {
            store.advance_time(Duration::from_millis(1));
            cleanup_page(&store, 100).await?;
        }
        anyhow::ensure!(
            history_count(&store, &stream).await? == 0,
            "expired event or predecessor leaked after disable"
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}
