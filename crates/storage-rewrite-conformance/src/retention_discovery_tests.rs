use anyhow::Context as _;
use storage_provider::{
    StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimDueMarker, StreamTrimPageLimit,
};
use storage_types::{StreamRetentionDuration, TimestampMillis};

use super::test_support::{TestStore, hash_table_request};

async fn seed_equal_deadlines(
    fixture: &TestStore,
    due: TimestampMillis,
) -> anyhow::Result<Vec<StreamTrimDueMarker>> {
    for name in ["equal-deadline-a", "equal-deadline-b", "equal-deadline-c"] {
        let mut request = hash_table_request(storage_types::TableName::new(name));
        request.aux_stream_duration_hours = Some(StreamRetentionDuration::from_hours(24).unwrap());
        fixture.operations.create_table(request).await?;
    }
    let markers = fixture
        .stream_retention_repository
        .list_due_stream_trim_markers(due, 10)
        .await?;
    anyhow::ensure!(markers.len() == 3);
    anyhow::ensure!(
        markers
            .iter()
            .all(|marker| marker.due_at == markers[0].due_at)
    );
    Ok(markers)
}

async fn verify_empty_page(
    fixture: &TestStore,
    due: TimestampMillis,
    expected: usize,
) -> anyhow::Result<()> {
    let stats = StreamDurationTrimWorker::new(
        fixture.stream_retention_repository.as_ref(),
        StreamDurationTrimConfig {
            marker_page_size: StreamTrimPageLimit::try_from(2usize)?,
            stream_page_size: StreamTrimPageLimit::try_from(1usize)?,
        },
    )
    .run_due_page(due, due)
    .await?;
    anyhow::ensure!(stats.due_markers_scanned == expected && stats.state_writes == expected);
    anyhow::ensure!(stats.marker_page_full == (expected == 2));
    anyhow::ensure!(
        stats.rows_deleted == 0 && stats.scopes_trimmed == 0 && stats.stale_markers == 0
    );
    Ok(())
}

#[tokio::test]
async fn given_replaced_table_policies_when_reopened_then_only_current_schedule_remains()
-> anyhow::Result<()> {
    let fixture = TestStore::new_durable().await;
    let outcome = async {
        let request = hash_table_request(storage_types::TableName::new("one-table-schedule"));
        fixture.operations.create_table(request.clone()).await?;
        let repository = &fixture.stream_retention_repository;
        let horizon = TimestampMillis::from_timestamp(i64::MAX);
        let original = repository
            .list_due_stream_trim_markers(horizon, 100)
            .await?;
        anyhow::ensure!(original.len() == 1);
        for duration in [24, 168, 24, -1, -1, 48, 48, -1, 72] {
            fixture
                .operations
                .update_table(serde_json::from_value(serde_json::json!({
                    "TableName": request.table_name, "AuxStreamDurationHours": duration
                }))?)
                .await?;
            let markers = repository
                .list_due_stream_trim_markers(horizon, 100)
                .await?;
            anyhow::ensure!(
                markers.len() == 1,
                "policy replacement accumulated schedules"
            );
            let state = repository
                .load_stream_trim_states(&[original[0].scope.clone()])
                .await?
                .pop()
                .flatten()
                .context("table state")?;
            anyhow::ensure!(state.policy_version > original[0].policy_version);
            anyhow::ensure!(markers.iter().all(|marker| state.marker_matches(marker)));
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = outcome {
        fixture.finish().await;
        return Err(error);
    }
    let fixture = fixture.reopen().await;
    let outcome = async {
        let markers = fixture
            .stream_retention_repository
            .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 100)
            .await?;
        anyhow::ensure!(markers.len() == 1, "reopen revived superseded schedules");
        let states = fixture
            .stream_retention_repository
            .load_stream_trim_states(&[markers[0].scope.clone()])
            .await?;
        anyhow::ensure!(
            states[0]
                .as_ref()
                .context("reopened state")?
                .marker_matches(&markers[0])
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_equal_deadlines_when_empty_work_page_continues_then_reopen_reaches_remaining_scope()
-> anyhow::Result<()> {
    let mut fixture = TestStore::new_durable().await;
    let due = fixture.now() + 25 * 60 * 60 * 1000;
    let markers = match seed_equal_deadlines(&fixture, due).await {
        Ok(markers) => markers,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    for (count, consumed) in [(2, 2), (1, 3)] {
        if let Err(error) = verify_empty_page(&fixture, due, count).await {
            fixture.finish().await;
            return Err(error);
        }
        fixture = fixture.reopen().await;
        let outcome = async {
            let repository = &fixture.stream_retention_repository;
            let remaining = repository.list_due_stream_trim_markers(due, 10).await?;
            anyhow::ensure!(
                remaining == markers[consumed..],
                "equal deadlines skipped or resurrected work after reopen"
            );
            let scopes = markers
                .iter()
                .map(|marker| marker.scope.clone())
                .collect::<Vec<_>>();
            let states = repository.load_stream_trim_states(&scopes).await?;
            for (index, state) in states.into_iter().enumerate() {
                let state = state.context("table scope keeps policy")?;
                anyhow::ensure!(state.policy_version == markers[index].policy_version);
                if index < consumed {
                    anyhow::ensure!(state.next_due_at == Some(due + 24 * 60 * 60 * 1000));
                } else {
                    anyhow::ensure!(state.marker_matches(&markers[index]));
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = outcome {
            fixture.finish().await;
            return Err(error);
        }
    }
    fixture.finish().await;
    Ok(())
}
