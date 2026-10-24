use anyhow::Context as _;
use storage_provider::{
    StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimDueMarker, StreamTrimPageLimit,
    StreamTrimState,
};
use storage_types::{StreamRetentionDuration, TimestampMillis};

use super::test_support::{TestStore, hash_table_request};

async fn seed_stale_markers(
    fixture: &TestStore,
    due: TimestampMillis,
) -> anyhow::Result<(Vec<StreamTrimDueMarker>, StreamTrimState)> {
    let mut request = hash_table_request(storage_types::TableName::new("bounded-stale-discovery"));
    request.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    fixture.operations.create_table(request.clone()).await?;
    // Policy changes are due immediately; place them after the initial due
    // marker.
    fixture.advance_time(std::time::Duration::from_secs(3600));
    for duration in [1, 1, 1, 1, 1, 1, 2] {
        fixture
            .operations
            .update_table(serde_json::from_value(serde_json::json!({
                "TableName": request.table_name, "AuxStreamDurationHours": duration
            }))?)
            .await?;
    }
    let markers = fixture
        .stream_retention_repository
        .as_ref()
        .list_due_stream_trim_markers(due, 100)
        .await?;
    anyhow::ensure!(
        markers.len() == 8,
        "expected seven stale markers and one current marker, got {}",
        markers.len()
    );
    let current = markers.last().context("current marker")?;
    let state = fixture
        .stream_retention_repository
        .as_ref()
        .load_stream_trim_states(std::slice::from_ref(&current.scope))
        .await?
        .pop()
        .flatten()
        .context("current state")?;
    anyhow::ensure!(state.policy_version == current.policy_version);
    anyhow::ensure!(
        markers[..7]
            .iter()
            .all(|marker| marker.policy_version < state.policy_version)
    );
    Ok((markers, state))
}

async fn verify_tick(
    fixture: &TestStore,
    due: TimestampMillis,
    remaining: &[StreamTrimDueMarker],
    original: &StreamTrimState,
) -> anyhow::Result<()> {
    let repository = fixture.stream_retention_repository.as_ref();
    let stats = StreamDurationTrimWorker::new(
        repository,
        StreamDurationTrimConfig {
            marker_page_size: StreamTrimPageLimit::try_from(2usize)?,
            stream_page_size: StreamTrimPageLimit::try_from(1usize)?,
        },
    )
    .run_due_page(due, due)
    .await?;
    anyhow::ensure!(
        stats.due_markers_scanned == 2 && stats.marker_page_full,
        "tick did not stop at the examined-marker bound"
    );
    anyhow::ensure!(
        stats.stale_markers == if remaining.len() > 2 { 2 } else { 1 },
        "stale markers must consume the page budget"
    );
    let after = repository.list_due_stream_trim_markers(due, 100).await?;
    anyhow::ensure!(
        after == remaining[2..],
        "tick skipped or recreated a marker"
    );
    let state = repository
        .load_stream_trim_states(std::slice::from_ref(&original.scope))
        .await?
        .pop()
        .flatten()
        .context("state survives cleanup")?;
    if remaining.len() > 2 {
        anyhow::ensure!(
            &state == original,
            "stale-only page changed current policy or progress"
        );
        anyhow::ensure!(
            stats.state_writes == 0 && stats.scopes_trimmed == 0 && stats.rows_deleted == 0
        );
    } else {
        anyhow::ensure!(
            state.policy_version == original.policy_version && state.next_due_at.is_none(),
            "current marker was not completed after stale pages"
        );
        anyhow::ensure!(
            stats.state_writes == 1 && stats.scopes_trimmed == 0 && stats.rows_deleted == 0
        );
    }
    Ok(())
}

async fn seed_equal_deadlines(
    fixture: &TestStore,
    due: TimestampMillis,
) -> anyhow::Result<Vec<StreamTrimDueMarker>> {
    use storage_provider::{StreamTrimScopeKind, maintenance::StreamRetentionWave};
    use storage_types::{AttributeValue, StreamSpecification, StreamViewType};
    let mut request = hash_table_request(storage_types::TableName::new("equal-deadline-items"));
    request.aux_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    request.aux_default_item_stream_duration_hours = Some(StreamRetentionDuration::FiniteHours(1));
    request.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    });
    fixture.operations.create_table(request.clone()).await?;
    for key in ["a", "b", "c"] {
        let mut put = storage_types::PutItemRequest::new(
            request.table_name.clone(),
            storage_types::WireItem::try_from(
                &(std::collections::HashMap::from([(
                    "pk".to_owned(),
                    AttributeValue::S(key.to_owned()),
                )])),
            )
            .unwrap(),
        );
        put.aux_item_stream_ttl_hours = Some(StreamRetentionDuration::FiniteHours(1));
        fixture.operations.put_item(put).await?;
    }
    let repository = fixture.stream_retention_repository.as_ref();
    let markers = repository.list_due_stream_trim_markers(due, 10).await?;
    anyhow::ensure!(markers.len() == 4);
    let (table, items): (Vec<_>, Vec<_>) = markers
        .into_iter()
        .partition(|marker| marker.scope.kind == StreamTrimScopeKind::Table);
    let page_limit = StreamTrimPageLimit::try_from(10usize)?;
    repository
        .trim_stream_marker_wave(StreamRetentionWave::try_new(table, due, due, page_limit)?)
        .await?;
    // Each item is the latest version of its key and must survive. The shared
    // wave reschedules all three scopes using the same explicit horizon.
    let stats = repository
        .trim_stream_marker_wave(StreamRetentionWave::try_new(items, due, due, page_limit)?)
        .await?;
    anyhow::ensure!(stats.rows_deleted == 0 && stats.state_writes == 3);
    let markers = repository
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
        .await?;
    anyhow::ensure!(markers.len() == 3);
    let bucket = markers[0].due_bucket;
    anyhow::ensure!(bucket > due && markers.iter().all(|marker| marker.due_bucket == bucket));
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
async fn given_stale_markers_exceed_page_when_reopening_each_tick_then_later_current_work_finishes()
-> anyhow::Result<()> {
    let mut fixture = TestStore::new_durable().await;
    let due = fixture.now() + 3 * 60 * 60 * 1000;
    let seeded = seed_stale_markers(&fixture, due).await;
    let (markers, state) = match seeded {
        Ok(seeded) => seeded,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    for offset in (0..8).step_by(2) {
        if let Err(error) = verify_tick(&fixture, due, &markers[offset..], &state).await {
            fixture.finish().await;
            return Err(error);
        }
        fixture = fixture.reopen().await;
        let outcome = async {
            let after = fixture
                .stream_retention_repository
                .list_due_stream_trim_markers(due, 100)
                .await?;
            anyhow::ensure!(
                after == markers[offset + 2..],
                "reopen lost bounded discovery progress"
            );
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

#[tokio::test]
async fn given_equal_deadlines_when_empty_work_page_continues_then_reopen_reaches_remaining_scope()
-> anyhow::Result<()> {
    let mut fixture = TestStore::new_durable().await;
    let due = fixture.now() + 2 * 60 * 60 * 1000;
    let markers = match seed_equal_deadlines(&fixture, due).await {
        Ok(markers) => markers,
        Err(error) => {
            fixture.finish().await;
            return Err(error);
        }
    };
    let due = markers[0].due_bucket;
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
                let state = state.context("retained latest-item scope keeps policy")?;
                anyhow::ensure!(state.policy_version == markers[index].policy_version);
                anyhow::ensure!(
                    state.next_due_at
                        == Some(if index < consumed {
                            markers[index].rescheduled_after(due, due)?.due_bucket
                        } else {
                            due
                        })
                );
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
