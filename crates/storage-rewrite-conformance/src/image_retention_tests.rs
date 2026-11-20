//! Table history cursors through real provider operations.

use std::collections::HashMap;

use storage_types::{AttributeValue, StreamSpecification, StreamViewType, TableName};

use super::test_support::{TestStore, hash_table_request};

async fn publish_snapshots(fixture: &TestStore, label: &str) -> anyhow::Result<TableName> {
    let table = TableName::new("table_snapshot_lifetime");
    let mut request = hash_table_request(table.clone());
    request.aux_stream_duration_hours =
        Some(storage_types::StreamRetentionDuration::from_hours(24)?);
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    fixture.operations.create_table(request).await?;
    for generation in 0..3 {
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("item".to_owned())),
            (
                "payload".to_owned(),
                AttributeValue::S(format!("{label}/{generation}")),
            ),
        ]);
        fixture
            .operations
            .put_item(storage_types::PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&item)?,
            ))
            .await?;
    }
    fixture.wait_until_changes_are_readable().await;
    anyhow::ensure!(table_records(fixture, &table).await?.len() == 3);
    Ok(table)
}

#[tokio::test]
async fn given_verified_head_when_table_stream_is_idle_then_polls_keep_the_same_frontier()
-> anyhow::Result<()> {
    const POLLS: usize = 3;
    let census = super::native_census_test_support::NativeCensus::new();
    let fixture = TestStore::new_with_census(std::sync::Arc::clone(&census)).await;
    let outcome = async {
        let table = TableName::new("table_frontier_large_images");
        let create = hash_table_request(table.clone());
        let scope = fixture.stream_operations.system_stream_scope().await?;
        let entries = fixture.stream_operations.system_stream_lineage_catalog(scope.clone()).await?;
        let catalog = stream_provider::SystemStreamLineageCatalog::try_new(scope, entries)?;
        let mut keys = std::collections::BTreeMap::new();
        for ordinal in 0..10_000 {
            let key = storage_types::KeyAttributes::from([("pk".to_owned(), AttributeValue::S(format!("key-{ordinal}")))]);
            let route = stream_provider::DynamoMutationRoute::try_new(&table, &create.key_schema, &key)?;
            let lineage = catalog.writer_lineage(route.routing_key())?;
            keys.entry(lineage.lineage_id().as_str().to_owned()).or_insert(key);
            if keys.len() == catalog.entries().len() { break; }
        }
        anyhow::ensure!(keys.len() == catalog.entries().len());
        fixture.operations.create_table(create).await?;
        for generation in 0..2 {
            if generation == 1 {
                fixture.operations.update_table(serde_json::from_value(serde_json::json!({
                    "TableName": table,
                    "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
                }))?).await?;
            }
            for (ordinal, key) in keys.values().enumerate() {
                let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ ((ordinal as u64 + 1) << 32) ^ generation;
                let payload = (0..390_000).map(|_| {
                    state ^= state << 13; state ^= state >> 7; state ^= state << 17;
                    char::from(32 + state as u8 % 95)
                }).collect::<String>();
                let mut image = key.iter().map(|(name, value)| (name.to_owned(), value.clone())).collect::<HashMap<_, _>>();
                image.insert("payload".to_owned(), AttributeValue::S(payload));
                fixture.operations.put_item(storage_types::PutItemRequest::new(table.clone(), storage_types::WireItem::try_from(&image)?)).await?;
            }
        }
        fixture.wait_until_changes_are_readable().await;
        let initial = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        let mut initial = initial;
        let mut seen = initial.records().len();
        while !initial.records().is_empty() {
            initial = fixture.operations.get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(), initial.last_evaluated_key().map(str::to_owned), Some(10),
            )).await?;
            seen += initial.records().len();
        }
        anyhow::ensure!(seen == keys.len(), "each populated root has one canonical entry: {seen}");
        let frontier = initial
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("initial frontier is absent"))?;
        let checkpoint = initial
            .last_evaluated_key()
            .ok_or_else(|| anyhow::anyhow!("initial checkpoint is absent"))?;
        census.reset();
        for _ in 0..POLLS {
            let page = fixture
                .operations
                .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                    table.clone(),
                    Some(checkpoint.to_owned()),
                    Some(10),
                ))
                .await?;
            anyhow::ensure!(page.records().is_empty());
            let next: storage_types::TableStreamCheckpoint = page.last_evaluated_key()
                .ok_or_else(|| anyhow::anyhow!("idle read lost its checkpoint"))?.parse()?;
            let previous: storage_types::TableStreamCheckpoint = checkpoint.parse()?;
            anyhow::ensure!(next.history_id() == previous.history_id()
                && next.source_scope() == previous.source_scope());
            anyhow::ensure!(previous.positions().iter().all(|old|
                next.positions().iter().any(|new| new.shard_id() == old.shard_id()
                    && new.lineage_id() == old.lineage_id()
                    && new.offset().compare_order(old.offset()).is_ok_and(|order| !order.is_lt()))));
            if matches!(TestStore::selected_backend().as_str(), "sqlite" | "turso") {
                anyhow::ensure!(page.last_evaluated_key() == Some(checkpoint));
            }
            anyhow::ensure!(page.table_frontier() == Some(frontier));
        }
        anyhow::ensure!(census.snapshot().rows_written == 0);
        anyhow::ensure!(census.chunk_key_reads() == 0, "idle frontier hydrated canonical chunks");
        anyhow::ensure!(census.entry_blob_bytes() == 0, "idle frontier returned canonical SQL images");
        eprintln!(
            "table_stream_idle_poll_report {}",
            serde_json::json!({
                "polls": POLLS,
                "native": census.snapshot().to_string(),
                "entry_blob_bytes": census.entry_blob_bytes(),
                "chunk_key_reads": census.chunk_key_reads(),
                "roots": keys.len(),
                "scope": "public table-stream page at a verified head",
            })
        );
        let arn = fixture.operations.describe_table(storage_types::DescribeTableRequest { table_name: table.clone() }).await?
            .table.latest_stream_arn.ok_or_else(|| anyhow::anyhow!("missing stream ARN"))?;
        let lane = &frontier.lanes[0];
        let origin = storage_types::SystemStreamPosition::try_new(lane.shard_id.as_str(), lane.lineage_id.as_str(), "0")?;
        let shard = storage_types::TableHistoryLane::try_from((frontier.table_history_id, &frontier.source_scope, &origin))?.public_shard_id()?;
        let token = super::dynamodb_stream_history_tests::iterator(&fixture, &arn, &shard, "TRIM_HORIZON", None).await;
        let request = storage_types::GetRecordsRequest { shard_iterator: token, limit: Some(1) };
        let warm = fixture.operations.get_records(request.clone()).await?;
        anyhow::ensure!(warm.records.len() == 1);
        let expected = serde_json::to_value(&warm.records)?;
        census.reset();
        for _ in 0..POLLS {
            let page = fixture.operations.get_records(request.clone()).await?;
            anyhow::ensure!(serde_json::to_value(&page.records)? == expected);
            let details = &page.records[0].dynamodb;
            for image in [&details.old_image, &details.new_image] {
                anyhow::ensure!(matches!(image.as_ref().and_then(|image| image.get("payload")), Some(AttributeValue::S(value)) if value.len() == 390_000));
            }
        }
        anyhow::ensure!(census.snapshot().rows_written == 0);
        eprintln!("table_stream_single_shard_report {}", serde_json::json!({
            "polls": POLLS, "roots": keys.len(), "native": census.snapshot().to_string(),
            "entry_blob_bytes": census.entry_blob_bytes(), "chunk_key_reads": census.chunk_key_reads(),
            "scope": "public GetRecords one populated shard, same TRIM_HORIZON iterator",
        }));
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_trimmed_table_history_when_a_new_pointer_arrives_then_old_checkpoint_cannot_resume()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = publish_snapshots(&fixture, "original").await?;
        let before = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(before.records().len() == 3);
        let first = before
            .table_frontier()
            .and_then(|frontier| frontier.lanes.first().and_then(|lane| lane.oldest))
            .ok_or_else(|| anyhow::anyhow!("initial table frontier is empty"))?;
        let history_id = before
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("initial table frontier is absent"))?
            .table_history_id;

        fixture.advance_time(std::time::Duration::from_secs(25 * 3600));
        let mut deleted = 0;
        for _ in 0..4 {
            deleted += super::table_history_retention_tests::cleanup_page(&fixture, 10)
                .await?
                .rows_deleted;
            fixture.advance_time(std::time::Duration::from_secs(60));
        }
        anyhow::ensure!(deleted > 0);
        anyhow::ensure!(table_records(&fixture, &table).await?.is_empty());
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("fresh".to_owned())),
            (
                "payload".to_owned(),
                AttributeValue::S("after trim".to_owned()),
            ),
        ]);
        fixture
            .operations
            .put_item(storage_types::PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&item)?,
            ))
            .await?;
        fixture.wait_until_changes_are_readable().await;
        let after = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        let frontier = after
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("trimmed table frontier is absent"))?;
        anyhow::ensure!(frontier.table_history_id == history_id);
        anyhow::ensure!(after.records().len() == 1);
        let old_cursor = before.records()[0].cursor.clone();
        let error = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                old_cursor,
                Some(10),
            ))
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("trimmed cursor was accepted"))?;
        anyhow::ensure!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::HistoryUnavailable
        ));
        let lane = &frontier.lanes[0];
        let head = lane
            .head
            .ok_or_else(|| anyhow::anyhow!("new pointer omitted from frontier"))?;
        anyhow::ensure!(lane.oldest == Some(head) && first < head);
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_new_publication_between_table_pages_then_pagination_keeps_a_complete_prefix()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = publish_snapshots(&fixture, "original").await?;
        let first = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(2),
            ))
            .await?;
        anyhow::ensure!(!first.records().is_empty() && first.records().len() <= 2);
        let cursor = first
            .last_evaluated_key()
            .ok_or_else(|| anyhow::anyhow!("bounded first page omitted its cursor"))?;
        fixture.wait_until_changes_are_readable().await;
        let initial = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(initial.records().len() == 3);
        anyhow::ensure!(
            first
                .records()
                .iter()
                .map(|record| &record.sequence_number)
                .collect::<Vec<_>>()
                == initial.records()[..first.records().len()]
                    .iter()
                    .map(|record| &record.sequence_number)
                    .collect::<Vec<_>>()
        );
        let initial_frontier = first
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("first page omitted its frontier"))?;
        anyhow::ensure!(initial_frontier == initial.table_frontier().unwrap());

        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("fourth".to_owned())),
            (
                "payload".to_owned(),
                AttributeValue::S("after first page".to_owned()),
            ),
        ]);
        fixture
            .operations
            .put_item(storage_types::PutItemRequest::new(
                table.clone(),
                storage_types::WireItem::try_from(&item)?,
            ))
            .await?;
        fixture.wait_until_changes_are_readable().await;
        let complete = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(complete.records().len() == 4);
        let mut paged = first
            .records()
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        let mut checkpoint = cursor.to_owned();
        let mut reached_head = false;
        for _ in 0..8 {
            let page = fixture
                .operations
                .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                    table.clone(),
                    Some(checkpoint.clone()),
                    Some(2),
                ))
                .await?;
            anyhow::ensure!(page.records().len() <= 2);
            anyhow::ensure!(page.table_frontier() == complete.table_frontier());
            if page.records().is_empty() {
                reached_head = true;
                break;
            }
            paged.extend(
                page.records()
                    .iter()
                    .map(serde_json::to_string)
                    .collect::<Result<Vec<_>, _>>()?,
            );
            let next = page
                .last_evaluated_key()
                .ok_or_else(|| anyhow::anyhow!("page omitted its checkpoint"))?;
            anyhow::ensure!(
                next != checkpoint,
                "nonempty page did not advance exclusively"
            );
            checkpoint = next.to_owned();
        }
        anyhow::ensure!(reached_head);
        let mut expected = complete
            .records()
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?;
        let original_key = &initial.records()[0].keys;
        let original_sequences = initial
            .records()
            .iter()
            .map(|record| record.sequence_number.clone())
            .collect::<Vec<_>>();
        let observed_sequences = paged
            .iter()
            .map(|record| serde_json::from_str::<storage_types::StreamRecord>(record))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|record| &record.keys == original_key)
            .map(|record| record.sequence_number)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            observed_sequences == original_sequences,
            "same-key publication order changed across pages"
        );
        paged.sort();
        expected.sort();
        anyhow::ensure!(
            paged == expected,
            "publication escaped or duplicated a page"
        );
        let later_frontier = complete
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("complete page omitted frontier"))?;
        anyhow::ensure!(initial_frontier.table_history_id == later_frontier.table_history_id);
        anyhow::ensure!(
            initial_frontier
                .lanes
                .iter()
                .zip(&later_frontier.lanes)
                .any(|(before, after)| before.head < after.head)
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

pub(super) async fn table_records(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<Vec<storage_types::StreamRecord>> {
    let page = fixture
        .operations
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            table.clone(),
            None,
            Some(100),
        ))
        .await?;
    Ok(page.into_parts().0)
}

/// Capture canonical lane bounds before expiry so cleanup tests inspect
/// persisted entries.
pub(super) async fn canonical_history_reads(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<Vec<stream_provider::TableHistoryIndexRead>> {
    let page = fixture
        .operations
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            table.clone(),
            None,
            Some(1),
        ))
        .await?;
    let frontier = page
        .table_frontier()
        .ok_or_else(|| anyhow::anyhow!("missing table frontier"))?;
    frontier
        .lanes
        .iter()
        .filter(|lane| lane.head.is_some())
        .map(|lane| {
            let position = storage_types::SystemStreamPosition::try_new(
                lane.shard_id.as_str(),
                lane.lineage_id.as_str(),
                "0",
            )?;
            Ok(stream_provider::TableHistoryIndexRead {
                lane: storage_types::TableHistoryLane::try_from((
                    frontier.table_history_id,
                    &frontier.source_scope,
                    &position,
                ))?,
                exclusive_start: None,
                through: storage_types::TableStreamSequence::from(storage_types::StreamItemId::MAX),
                direction: stream_provider::ReadDirection::Forward,
                limit: std::num::NonZeroU32::new(100).unwrap(),
            })
        })
        .collect()
}
