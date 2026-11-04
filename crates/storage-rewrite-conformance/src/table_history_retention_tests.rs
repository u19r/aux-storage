//! Native table cleanup preserves live anchors and exact DynamoDB predecessors.

use std::time::Duration;

use serde_json::json;
use storage_provider::{
    StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimPageLimit,
    maintenance::StreamDurationTrimStats,
};
use storage_types::{
    DeleteItemRequest, GetRecordsRequest, ItemHistoryPointer, TableName, TimestampMillis,
    storage_serde,
};
use stream_provider::StreamRowKind;

use super::{
    dynamodb_stream_history_tests::{create_stream_table, describe_shards, iterator, put_value},
    image_retention_tests::{table_records, table_stream},
    test_support::TestStore,
};

pub(super) async fn change_duration(
    store: &TestStore,
    table: &TableName,
    hours: i64,
) -> anyhow::Result<()> {
    store
        .operations
        .update_table(serde_json::from_value(json!({
            "TableName": table, "AuxStreamDurationHours": hours
        }))?)
        .await
        .map_err(|error| anyhow::anyhow!("change duration: {:?}", error.as_ref()))?;
    Ok(())
}

pub(super) async fn cleanup_page(
    store: &TestStore,
    limit: usize,
) -> anyhow::Result<StreamDurationTrimStats> {
    let config = StreamDurationTrimConfig {
        stream_page_size: StreamTrimPageLimit::try_from(limit)?,
        ..Default::default()
    };
    StreamDurationTrimWorker::new(store.stream_retention_repository.as_ref(), config)
        .run_due_page(store.now(), store.now())
        .await
        .map_err(|error| anyhow::anyhow!("cleanup page: {:?}", error.as_ref()))
}

pub(super) async fn item_history(
    store: &TestStore,
    table: &TableName,
) -> anyhow::Result<StreamRowKind> {
    let lane = table_stream(store, table)
        .await
        .map_err(|error| anyhow::anyhow!("find table stream: {error:?}"))?;
    let rows = store
        .stream_operations
        .read_forward(lane, None, 1)
        .await
        .map_err(|error| anyhow::anyhow!("load table pointers: {:?}", error.as_ref()))?;
    let pointer: ItemHistoryPointer = storage_serde::from_bytes(&rows.items[0].data)?;
    Ok(StreamRowKind::Item(
        pointer.metadata().address().physical_stream_name()?,
    ))
}

pub(super) async fn history_count(
    store: &TestStore,
    stream: &StreamRowKind,
) -> anyhow::Result<usize> {
    Ok(store
        .stream_operations
        .read_forward(stream.clone(), None, 100)
        .await?
        .items
        .len())
}

async fn read_standard(
    store: &TestStore,
    arn: &str,
) -> anyhow::Result<Vec<storage_types::DynamoDbStreamsRecord>> {
    let shards = describe_shards(store, arn).await;
    let token = iterator(store, arn, &shards[0].shard_id, "TRIM_HORIZON", None).await;
    Ok(store
        .operations
        .get_records(GetRecordsRequest {
            shard_iterator: token,
            limit: Some(100),
        })
        .await?
        .records)
}

#[tokio::test]
async fn given_hashed_item_address_when_cleanup_retires_events_then_live_anchor_and_exact_predecessor_survive()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        let key = "k".repeat(1500);
        put_value(&store, &table, &key, "v1").await;
        let stream = item_history(&store, &table).await?;
        store.advance_time(Duration::from_secs(23 * 3600));
        put_value(&store, &table, &key, "v2").await;
        store.advance_time(Duration::from_secs(2 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        anyhow::ensure!(history_count(&store, &stream).await? == 2);
        let records = read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1);
        let record = serde_json::to_value(&records[0])?;
        anyhow::ensure!(record["dynamodb"]["OldImage"]["value"]["S"] == "v1");
        anyhow::ensure!(record["dynamodb"]["NewImage"]["value"]["S"] == "v2");
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 2);
        anyhow::ensure!(history_count(&store, &stream).await? == 1);
        put_value(&store, &table, &key, "v3").await;
        let records = read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1);
        let record = serde_json::to_value(&records[0])?;
        anyhow::ensure!(record["dynamodb"]["OldImage"]["value"]["S"] == "v2");
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_terminal_delete_when_cleanup_repeats_then_history_and_pointer_inventory_become_empty()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        put_value(&store, &table, "item", "v1").await;
        let stream = item_history(&store, &table).await?;
        let pointers = table_stream(&store, &table).await?;
        store
            .operations
            .delete_item(
                DeleteItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": "item"}}
                }))
                .unwrap(),
            )
            .await?;
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        anyhow::ensure!(history_count(&store, &stream).await? == 1);
        anyhow::ensure!(history_count(&store, &pointers).await? == 2);
        store.advance_time(Duration::from_millis(1));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 3);
        anyhow::ensure!(history_count(&store, &stream).await? == 0);
        anyhow::ensure!(history_count(&store, &pointers).await? == 0);
        anyhow::ensure!(table_records(&store, &table).await?.is_empty());
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_partial_cleanup_when_policy_becomes_forever_then_retired_work_finishes_without_resurrecting_events()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        for version in ["v1", "v2", "v3", "v4", "v5"] {
            put_value(&store, &table, "item", version).await;
        }
        let stream = item_history(&store, &table).await?;
        let pointers = table_stream(&store, &table).await?;
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        change_duration(&store, &table, -1).await?;
        anyhow::ensure!(
            table_records(&store, &table).await?.is_empty(),
            "Forever revived committed retired events"
        );
        let markers = store
            .stream_retention_repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
            .await?;
        anyhow::ensure!(markers.len() == 1, "pending retirement lost its schedule");
        for _ in 0..6 {
            store.advance_time(Duration::from_millis(1));
            cleanup_page(&store, 100).await?;
        }
        anyhow::ensure!(history_count(&store, &stream).await? == 1);
        anyhow::ensure!(history_count(&store, &pointers).await? == 0);
        anyhow::ensure!(
            store
                .stream_retention_repository
                .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
                .await?
                .is_empty()
        );
        put_value(&store, &table, "item", "v6").await;
        let records = read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1);
        anyhow::ensure!(
            serde_json::to_value(&records[0])?["dynamodb"]["OldImage"]["value"]["S"] == "v5"
        );
        store.advance_time(Duration::from_secs(365 * 24 * 3600));
        cleanup_page(&store, 100).await?;
        anyhow::ensure!(table_records(&store, &table).await?.len() == 1);
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_two_busy_tables_when_one_cleanup_wave_runs_then_both_histories_make_progress()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let mut histories = Vec::new();
        for _ in 0..2 {
            let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
            change_duration(&store, &table, 24).await?;
            for version in 0..40 {
                put_value(&store, &table, "item", &version.to_string()).await;
            }
            histories.push(item_history(&store, &table).await?);
        }
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 2);
        for history in histories {
            anyhow::ensure!(history_count(&store, &history).await? == 39);
        }
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_wide_histories_when_cleanup_exhausts_its_image_budget_then_waiting_tables_receive_the_next_turn()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let mut entropy = 0x3c79_ac49_2ba7_b653_u64;
        let value = (0..396_000)
            .map(|_| {
                entropy ^= entropy << 13;
                entropy ^= entropy >> 7;
                entropy ^= entropy << 17;
                char::from(33 + (entropy % 94) as u8)
            })
            .collect::<String>();
        let mut histories = Vec::new();
        for _ in 0..32 {
            let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
            change_duration(&store, &table, 24).await?;
            for _ in 0..5 {
                put_value(&store, &table, "item", &value).await;
            }
            let history = item_history(&store, &table).await?;
            let first = store
                .stream_operations
                .read_forward(history.clone(), None, 1)
                .await?;
            anyhow::ensure!(first.items[0].data.len() > 256 * 1024);
            histories.push((history, first.items[0].id));
        }
        store.advance_time(Duration::from_secs(25 * 3600));
        cleanup_page(&store, 100).await?;
        store.advance_time(Duration::from_secs(1));
        cleanup_page(&store, 100).await?;
        for (history, original) in histories {
            let first = store
                .stream_operations
                .read_forward(history, None, 1)
                .await?;
            anyhow::ensure!(
                first.items[0].id != original,
                "wide table received no cleanup turn"
            );
        }
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_one_row_cleanup_pages_when_store_reopens_then_durable_progress_finishes_and_preserves_current_items()
-> anyhow::Result<()> {
    let mut store = TestStore::new_durable().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    change_duration(&store, &table, 24).await?;
    for key in ["a", "b", "c"] {
        put_value(&store, &table, key, "live").await;
    }
    let pointers = table_stream(&store, &table).await?;
    store.advance_time(Duration::from_secs(25 * 3600));
    for remaining in [2, 1, 0] {
        anyhow::ensure!(cleanup_page(&store, 1).await?.rows_deleted == 1);
        anyhow::ensure!(history_count(&store, &pointers).await? == remaining);
        store = store.reopen().await;
        store.advance_time(Duration::from_millis(1));
    }
    anyhow::ensure!(table_records(&store, &table).await?.is_empty());
    for key in ["a", "b", "c"] {
        let response = store
            .operations
            .get_item(serde_json::from_value(json!({
                "TableName": table, "Key": {"pk": {"S": key}}
            }))?)
            .await?;
        anyhow::ensure!(
            response.item.is_some(),
            "cleanup removed a current primary item"
        );
    }
    store.finish().await;
    Ok(())
}
