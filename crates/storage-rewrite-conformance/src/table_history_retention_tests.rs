//! Canonical cleanup retires events while preserving exact images and current
//! items.

use std::time::Duration;

use serde_json::json;
use storage_provider::{
    StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimPageLimit,
    maintenance::StreamDurationTrimStats,
};
use storage_types::{DeleteItemRequest, GetRecordsRequest, TableName, TimestampMillis};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, describe_shards, iterator, put_value},
    image_retention_tests::{canonical_history_reads, table_records},
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
    StreamDurationTrimWorker::new(store.maintenance.as_ref(), config)
        .run_due_page(store.now(), store.now())
        .await
        .map_err(|error| anyhow::anyhow!("cleanup page: {:?}", error.as_ref()))
}

pub(super) async fn history_count(
    store: &TestStore,
    reads: &[stream_provider::TableHistoryIndexRead],
) -> anyhow::Result<usize> {
    let pages = (store.canonical_history_read)(reads.to_vec()).await?;
    anyhow::ensure!(
        pages.iter().all(|page| !page.more),
        "fixture exceeded its physical inspection bound"
    );
    Ok(pages.iter().map(|page| page.entries.len()).sum())
}

pub(super) async fn read_standard(
    store: &TestStore,
    arn: &str,
) -> anyhow::Result<Vec<storage_types::DynamoDbStreamsRecord>> {
    let shards = describe_shards(store, arn).await;
    let mut records = Vec::new();
    for shard in shards {
        let mut token = iterator(store, arn, &shard.shard_id, "TRIM_HORIZON", None).await;
        loop {
            let page = store
                .operations
                .get_records(GetRecordsRequest {
                    shard_iterator: token,
                    limit: Some(2),
                })
                .await?;
            let empty = page.records.is_empty();
            records.extend(page.records);
            if empty {
                break;
            }
            let Some(next) = page.next_shard_iterator else {
                break;
            };
            token = next;
        }
    }
    Ok(records)
}

#[tokio::test]
async fn given_wide_item_key_when_cleanup_retires_events_then_exact_images_and_current_item_survive()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        let key = "k".repeat(1500);
        put_value(&store, &table, &key, "v1").await;
        store.wait_until_changes_are_readable().await;
        let stream = canonical_history_reads(&store, &table).await?;
        store.advance_time(Duration::from_secs(23 * 3600));
        put_value(&store, &table, &key, "v2").await;
        store.wait_until_changes_are_readable().await;
        store.advance_time(Duration::from_secs(2 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        anyhow::ensure!(history_count(&store, &stream).await? == 1);
        let records = read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1);
        let record = serde_json::to_value(&records[0])?;
        anyhow::ensure!(record["dynamodb"]["OldImage"]["value"]["S"] == "v1");
        anyhow::ensure!(record["dynamodb"]["NewImage"]["value"]["S"] == "v2");
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        anyhow::ensure!(history_count(&store, &stream).await? == 0);
        put_value(&store, &table, &key, "v3").await;
        store.wait_until_changes_are_readable().await;
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
async fn given_terminal_delete_when_cleanup_repeats_then_canonical_inventory_becomes_empty()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        put_value(&store, &table, "item", "v1").await;
        store.wait_until_changes_are_readable().await;
        let stream = canonical_history_reads(&store, &table).await?;
        store
            .operations
            .delete_item(
                DeleteItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": "item"}}
                }))
                .unwrap(),
            )
            .await?;
        store.wait_until_changes_are_readable().await;
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 2);
        anyhow::ensure!(history_count(&store, &stream).await? == 0);
        store.advance_time(Duration::from_millis(1));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 0);
        anyhow::ensure!(history_count(&store, &stream).await? == 0);
        anyhow::ensure!(table_records(&store, &table).await?.is_empty());
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_partial_cleanup_when_policy_becomes_forever_then_unretired_suffix_survives()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        for version in ["v1", "v2", "v3", "v4", "v5"] {
            put_value(&store, &table, "item", version).await;
        }
        store.wait_until_changes_are_readable().await;
        let stream = canonical_history_reads(&store, &table).await?;
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 1).await?.rows_deleted == 1);
        change_duration(&store, &table, -1).await?;
        anyhow::ensure!(
            table_records(&store, &table).await?.len() == 4,
            "Forever did not preserve the unretired suffix"
        );
        let markers = store
            .maintenance
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
            .await?;
        anyhow::ensure!(markers.is_empty(), "Forever left a finite-policy schedule");
        for _ in 0..6 {
            store.advance_time(Duration::from_millis(1));
            cleanup_page(&store, 100).await?;
        }
        anyhow::ensure!(history_count(&store, &stream).await? == 4);
        anyhow::ensure!(
            store
                .maintenance
                .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
                .await?
                .is_empty()
        );
        put_value(&store, &table, "item", "v6").await;
        store.wait_until_changes_are_readable().await;
        let records = read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 5);
        anyhow::ensure!(
            serde_json::to_value(records.last().unwrap())?["dynamodb"]["OldImage"]["value"]["S"]
                == "v5"
        );
        store.advance_time(Duration::from_secs(365 * 24 * 3600));
        cleanup_page(&store, 100).await?;
        anyhow::ensure!(table_records(&store, &table).await?.len() == 5);
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
            store.wait_until_changes_are_readable().await;
            histories.push(canonical_history_reads(&store, &table).await?);
        }
        store.advance_time(Duration::from_secs(25 * 3600));
        anyhow::ensure!(cleanup_page(&store, 1).await?.rows_deleted == 2);
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
    let census = std::sync::Arc::new(super::native_census_test_support::NativeCensus::default());
    let store = TestStore::new_with_census(std::sync::Arc::clone(&census)).await;
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
            store.wait_until_changes_are_readable().await;
            let history = canonical_history_reads(&store, &table).await?;
            let pages = (store.canonical_history_read)(history.clone()).await?;
            let entry = pages.iter().flat_map(|page| &page.entries).next()
                .ok_or_else(|| anyhow::anyhow!("wide history fixture published no entries"))?;
            anyhow::ensure!(entry.entry.encode()?.len() > 256 * 1024);
            histories.push((history, entry.position.clone()));
        }
        store.advance_time(Duration::from_secs(25 * 3600));
        census.reset();
        let stats = cleanup_page(&store, 100).await?;
        eprintln!("table_retention_cost_report {}", json!({
            "native": census.snapshot().to_string(),
            "entry_blob_bytes": census.entry_blob_bytes(),
            "chunk_key_reads": census.chunk_key_reads(),
            "rows_deleted": stats.rows_deleted,
            "scope": "one cleanup page; 32 tables, five 396000-byte writes each; setup and inspection excluded",
        }));
        store.advance_time(Duration::from_secs(1));
        census.reset();
        let stats = cleanup_page(&store, 100).await?;
        eprintln!("table_retention_cost_report {}", json!({
            "native": census.snapshot().to_string(),
            "entry_blob_bytes": census.entry_blob_bytes(),
            "chunk_key_reads": census.chunk_key_reads(),
            "rows_deleted": stats.rows_deleted,
            "scope": "one cleanup page; 32 tables, five 396000-byte writes each; setup and inspection excluded",
        }));
        for (history, original) in histories {
            let pages = (store.canonical_history_read)(history).await?;
            anyhow::ensure!(pages.iter().flat_map(|page| &page.entries).all(|entry| entry.position != original), "wide table received no cleanup turn");
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
    // A cleanup read admits at least one row per lineage. Keep this one-row
    // restart fixture in one actual writer lineage on multi-root providers.
    let scope = store.stream_operations.system_stream_scope().await?;
    let catalog = stream_provider::SystemStreamLineageCatalog::try_new(
        scope.clone(),
        store
            .stream_operations
            .system_stream_lineage_catalog(scope)
            .await?,
    )?;
    let schema = super::test_support::hash_table_request(table.clone()).key_schema;
    let mut keys = Vec::new();
    let mut selected = None;
    for index in 0..512 {
        let key = format!("cleanup-key-{index}");
        let attributes = serde_json::from_value(json!({"pk": {"S": key}}))?;
        let route = stream_provider::DynamoMutationRoute::try_new(&table, &schema, &attributes)?;
        let lineage = catalog
            .writer_lineage(route.routing_key())?
            .lineage_id()
            .clone();
        if selected
            .as_ref()
            .is_none_or(|selected| selected == &lineage)
        {
            selected = Some(lineage);
            keys.push(key);
            if keys.len() == 3 {
                break;
            }
        }
    }
    anyhow::ensure!(
        keys.len() == 3,
        "fixture needs three keys in one writer lineage"
    );
    for key in &keys {
        put_value(&store, &table, key, "live").await;
    }
    store.wait_until_changes_are_readable().await;
    let reads = canonical_history_reads(&store, &table).await?;
    store.advance_time(Duration::from_secs(25 * 3600));
    for remaining in [2, 1, 0] {
        anyhow::ensure!(cleanup_page(&store, 1).await?.rows_deleted == 1);
        anyhow::ensure!(history_count(&store, &reads).await? == remaining);
        store = store.reopen().await;
        store.advance_time(Duration::from_millis(1));
    }
    anyhow::ensure!(table_records(&store, &table).await?.is_empty());
    for key in &keys {
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

#[tokio::test]
async fn given_regressing_table_timestamps_when_cleanup_reaches_unexpired_entry_then_causal_suffix_survives()
-> anyhow::Result<()> {
    let store = TestStore::new().await;
    let outcome = async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        let start = store.now().timestamp_millis();
        put_value(&store, &table, "item", "v1").await;
        store.set_time(TimestampMillis::from_timestamp(start + 23 * 3_600_000));
        put_value(&store, &table, "item", "v2").await;
        store.set_time(TimestampMillis::from_timestamp(start + 3_600_000));
        put_value(&store, &table, "item", "v3").await;
        store.wait_until_changes_are_readable().await;
        let reads = canonical_history_reads(&store, &table).await?;
        let before = (store.canonical_history_read)(reads.clone()).await?;
        let positions = before
            .iter()
            .flat_map(|page| &page.entries)
            .map(|entry| entry.position.clone())
            .collect::<Vec<_>>();
        anyhow::ensure!(positions.len() == 3);
        store.set_time(TimestampMillis::from_timestamp(start + 25 * 3_600_000));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 1);
        let after = (store.canonical_history_read)(reads.clone()).await?;
        let retained = after
            .iter()
            .flat_map(|page| &page.entries)
            .map(|entry| entry.position.clone())
            .collect::<Vec<_>>();
        anyhow::ensure!(retained == positions[1..]);
        let entries = after
            .iter()
            .flat_map(|page| &page.entries)
            .collect::<Vec<_>>();
        for (entry, (old, new)) in entries.iter().zip([("v1", "v2"), ("v2", "v3")]) {
            let old_image = entry.entry.old_image().unwrap().item.to_attribute_map()?;
            let new_image = entry.entry.new_image().unwrap().item.to_attribute_map()?;
            anyhow::ensure!(serde_json::to_value(old_image)?["value"]["S"] == old);
            anyhow::ensure!(serde_json::to_value(new_image)?["value"]["S"] == new);
        }
        anyhow::ensure!(
            positions[1]
                .offset()
                .compare_order(positions[2].offset())?
                .is_lt()
        );
        // Public visibility still excludes expired v3; physical GC must keep it
        // behind v2.
        let records = read_standard(&store, &arn).await?;
        anyhow::ensure!(records.len() == 1);
        let image = serde_json::to_value(&records[0])?;
        anyhow::ensure!(image["dynamodb"]["OldImage"]["value"]["S"] == "v1");
        anyhow::ensure!(image["dynamodb"]["NewImage"]["value"]["S"] == "v2");
        store.advance_time(Duration::from_millis(1));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 0);
        anyhow::ensure!(history_count(&store, &reads).await? == 2);
        let current = store
            .operations
            .get_item(serde_json::from_value(json!({
                "TableName": table, "Key": {"pk": {"S": "item"}}
            }))?)
            .await?;
        anyhow::ensure!(serde_json::to_value(current)?["Item"]["value"]["S"] == "v3");
        store.set_time(TimestampMillis::from_timestamp(start + 48 * 3_600_000));
        anyhow::ensure!(cleanup_page(&store, 100).await?.rows_deleted == 2);
        anyhow::ensure!(history_count(&store, &reads).await? == 0);
        anyhow::ensure!(read_standard(&store, &arn).await?.is_empty());
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}

#[tokio::test]
async fn given_unpaired_stream_table_when_cleanup_runs_then_no_regional_family_read()
-> anyhow::Result<()> {
    let census = crate::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(std::sync::Arc::clone(&census)).await;
    let outcome = async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        change_duration(&store, &table, 24).await?;
        for version in ["v1", "v2", "v3"] {
            put_value(&store, &table, "item", version).await;
        }
        store.wait_until_changes_are_readable().await;
        store.advance_time(Duration::from_secs(49 * 3600));
        census.reset();
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        census.record();
        let stats = cleanup_page(&store, 10).await?;
        let counts = census.snapshot();
        anyhow::ensure!(
            stats.rows_deleted > 0,
            "the window must observe real cleanup: {counts}"
        );
        for label in counts.touched_read.iter().chain(&counts.touched_written) {
            anyhow::ensure!(
                !label.contains("regional"),
                "unpaired cleanup touched {label}: {counts}"
            );
        }
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        anyhow::ensure!(
            census
                .recorded()
                .iter()
                .all(|statement| !(statement.contains("regional_markers")
                    || statement.contains("regional_progress"))),
            "unpaired cleanup ran a regional statement"
        );
        Ok(())
    }
    .await;
    store.finish().await;
    outcome
}
