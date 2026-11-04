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
    anyhow::ensure!(table_records(fixture, &table).await?.len() == 3);
    Ok(table)
}

#[tokio::test]
async fn given_verified_head_when_table_stream_is_idle_then_polls_keep_the_same_frontier()
-> anyhow::Result<()> {
    const POLLS: usize = 64;
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = publish_snapshots(&fixture, "idle_poll").await?;
        let initial = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                None,
                Some(10),
            ))
            .await?;
        let frontier = initial
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("initial frontier is absent"))?;
        let checkpoint = initial
            .last_evaluated_key()
            .ok_or_else(|| anyhow::anyhow!("initial checkpoint is absent"))?;
        let started = std::time::Instant::now();
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
            anyhow::ensure!(page.last_evaluated_key() == Some(checkpoint));
            anyhow::ensure!(page.table_frontier() == Some(frontier));
        }
        eprintln!(
            "table_stream_idle_poll_report {}",
            serde_json::json!({
                "polls": POLLS,
                "total_ns": started.elapsed().as_nanos(),
                "scope": "public table-stream page at a verified head",
            })
        );
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
        anyhow::ensure!(first.records().len() == 2);
        let cursor = first
            .last_evaluated_key()
            .ok_or_else(|| anyhow::anyhow!("bounded first page omitted its cursor"))?;
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
                == initial.records()[..2]
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
        let second = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table.clone(),
                Some(cursor.to_owned()),
                Some(2),
            ))
            .await?;
        anyhow::ensure!(second.records().len() == 2);
        anyhow::ensure!(second.last_evaluated_key().is_some());
        let complete = fixture
            .operations
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                table,
                None,
                Some(10),
            ))
            .await?;
        anyhow::ensure!(complete.records().len() == 4);
        let paged = first
            .records()
            .iter()
            .chain(second.records())
            .map(|record| &record.sequence_number)
            .collect::<Vec<_>>();
        let expected = complete
            .records()
            .iter()
            .map(|record| &record.sequence_number)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            paged == expected,
            "publication escaped or duplicated a page"
        );
        let later_frontier = second
            .table_frontier()
            .ok_or_else(|| anyhow::anyhow!("second page omitted its frontier"))?;
        anyhow::ensure!(later_frontier == complete.table_frontier().unwrap());
        anyhow::ensure!(
            initial_frontier.table_history_id == later_frontier.table_history_id
                && initial_frontier.lanes[0].head < later_frontier.lanes[0].head
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

pub(super) async fn table_stream(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<stream_provider::StreamRowKind> {
    let page = fixture
        .operations
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            table.clone(),
            None,
            Some(1),
        ))
        .await
        .map_err(|error| anyhow::anyhow!("table history page: {:?}", error.as_ref()))?;
    let frontier = page
        .table_frontier()
        .ok_or_else(|| anyhow::anyhow!("missing table frontier"))?;
    let lane = frontier
        .lanes
        .iter()
        .find(|lane| lane.head.is_some())
        .or_else(|| frontier.lanes.first())
        .ok_or_else(|| anyhow::anyhow!("missing table history lineage"))?;
    let position = storage_types::SystemStreamPosition::try_new(
        lane.shard_id.as_str(),
        lane.lineage_id.as_str(),
        "0",
    )?;
    Ok(stream_provider::StreamRowKind::Table(
        storage_types::TableHistoryLane::try_from((
            frontier.table_history_id,
            &frontier.source_scope,
            &position,
        ))?
        .physical_stream_name()?,
    ))
}
