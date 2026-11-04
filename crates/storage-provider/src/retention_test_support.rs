//! Native fixtures seed missing policy state; one contract checks worker
//! cleanup and reopen.
use storage_types::{StorageResult, TimestampMillis};

use crate::{
    StorageProvider, StreamDurationTrimConfig, StreamDurationTrimWorker, StreamRetentionRepository,
    StreamTrimDueMarker, StreamTrimPageLimit,
};

#[cfg(test)]
pub(crate) fn frontier(policy_version: u64) -> crate::maintenance::TableHistoryTrimFrontier {
    let high = storage_types::SystemStreamCheckpoint::try_new(
        storage_types::SystemStreamScope::try_new("retention", 1).unwrap(),
        vec![storage_types::SystemStreamPosition::try_new("root", "root", "0").unwrap()],
    )
    .unwrap();
    crate::maintenance::TableHistoryTrimFrontier::initial(
        1_u32.into(),
        std::num::NonZeroU64::new(policy_version).unwrap(),
        &high,
    )
    .unwrap()
}

#[cfg(test)]
pub(crate) fn scope(table_name: &str) -> crate::StreamTrimScope {
    crate::StreamTrimScope::table(
        crate::table_retention_scope_id(1_u32.into()),
        storage_types::TableName::new(table_name),
    )
}

pub struct MissingPolicyMarker {
    marker: StreamTrimDueMarker,
    catalog: serde_json::Value,
}

impl MissingPolicyMarker {
    pub async fn reclaim(
        provider: &(impl StorageProvider + StreamRetentionRepository),
    ) -> StorageResult<Self> {
        let due = TimestampMillis::from_timestamp(i64::MAX);
        let markers = provider.list_due_stream_trim_markers(due, 2).await?;
        assert_eq!(
            markers.len(),
            1,
            "fixture must have exactly one orphaned marker"
        );
        let marker = markers.into_iter().next().expect("marker");
        assert!(
            provider
                .load_stream_trim_states(std::slice::from_ref(&marker.scope))
                .await?
                == vec![None]
        );
        let catalog =
            serde_json::to_value(provider.get_table_info(&marker.scope.table_name).await?)
                .expect("catalog JSON");
        let stats = StreamDurationTrimWorker::new(
            provider,
            StreamDurationTrimConfig {
                marker_page_size: StreamTrimPageLimit::try_from(1usize)?,
                stream_page_size: StreamTrimPageLimit::try_from(1usize)?,
            },
        )
        .run_due_page(due, TimestampMillis::now())
        .await?;
        assert_eq!(stats.due_markers_scanned, 1);
        assert_eq!(stats.missing_states, 1);
        assert_eq!(stats.state_writes, 0);
        assert_eq!(stats.rows_deleted, 0);
        let proof = Self { marker, catalog };
        proof.verify(provider).await?;
        Ok(proof)
    }

    pub async fn verify(
        &self,
        provider: &(impl StorageProvider + StreamRetentionRepository),
    ) -> StorageResult<()> {
        assert!(
            provider
                .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 2)
                .await?
                .is_empty(),
            "orphaned marker must remain removed after reopen"
        );
        assert!(
            provider
                .load_stream_trim_states(std::slice::from_ref(&self.marker.scope))
                .await?
                == vec![None],
            "cleanup must not recreate policy state"
        );
        assert_eq!(
            serde_json::to_value(
                provider
                    .get_table_info(&self.marker.scope.table_name)
                    .await?
            )
            .expect("catalog JSON"),
            self.catalog,
            "cleanup must preserve table metadata"
        );
        Ok(())
    }
}

/// A later deadline must not hide due work at a native discovery page boundary.
pub async fn qualify_due_marker_progress<P: crate::DynamoPort>(provider: &P)
where for<'native> P::Attempt<'native>: crate::maintenance::StreamRetentionPort {
    use crate::StreamRetentionRepository;
    let markers = create_deadline_tables(provider).await;
    let future_scope = markers[0].scope.clone();
    let due_scope = markers[1].scope.clone();
    let now = select_test_deadline();
    seed_marker_deadlines(
        provider,
        vec![
            (markers[0].clone(), now + 10_000),
            (markers[1].clone(), now - 1_000),
        ],
        now,
    )
    .await;
    let stats = run_one_marker_page(provider, now).await;
    assert_eq!(
        stats.state_writes, 1,
        "a future marker blocked due work at the page boundary"
    );
    let states = provider
        .load_stream_trim_states(&[future_scope, due_scope])
        .await
        .unwrap();
    assert_eq!(states[0].as_ref().unwrap().updated_at, now - 1);
    assert_eq!(states[1].as_ref().unwrap().updated_at, now);
    assert!(
        provider
            .list_due_stream_trim_markers(now, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

/// Repeated work on one table must yield to another table with the same due
/// time.
pub async fn qualify_pending_marker_progress<P: crate::DynamoPort>(provider: &P)
where for<'native> P::Attempt<'native>: crate::maintenance::StreamRetentionPort {
    use crate::{DynamoOperations, StreamRetentionRepository};
    let markers = create_deadline_tables(provider).await;
    for key in ["one", "two"] {
        provider
            .put_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": markers[0].scope.table_name, "Item": {"pk": {"S": key}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let now = select_test_deadline();
    seed_marker_deadlines(
        provider,
        markers
            .iter()
            .cloned()
            .map(|marker| (marker, now))
            .collect(),
        now,
    )
    .await;
    assert_eq!(run_one_marker_page(provider, now).await.state_writes, 1);
    let scopes = markers
        .iter()
        .map(|marker| marker.scope.clone())
        .collect::<Vec<_>>();
    let first = provider.load_stream_trim_states(&scopes).await.unwrap();
    assert!(
        first[0]
            .as_ref()
            .unwrap()
            .next_due_at
            .is_some_and(|due| due > now),
        "pending table did not yield this discovery window"
    );
    assert_eq!(first[1].as_ref().unwrap().updated_at, now - 1);
    assert_eq!(run_one_marker_page(provider, now).await.state_writes, 1);
    let states = provider.load_stream_trim_states(&scopes).await.unwrap();
    assert_eq!(
        states[1].as_ref().unwrap().updated_at,
        now,
        "pending work hid another due table"
    );
    for key in ["one", "two"] {
        let response = provider
            .get_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": markers[0].scope.table_name, "Key": {"pk": {"S": key}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            response.item.is_some(),
            "history cleanup removed a current item"
        );
    }
}

async fn create_deadline_tables<P: crate::DynamoPort>(provider: &P) -> Vec<StreamTrimDueMarker>
where for<'native> P::Attempt<'native>: crate::maintenance::StreamRetentionPort {
    use crate::{DynamoOperations, StreamRetentionRepository};
    for name in ["deadline-first", "deadline-second"] {
        provider
            .create_table(
                serde_json::from_value(serde_json::json!({
                    "TableName": name,
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}]
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let mut markers = provider
        .list_due_stream_trim_markers(TimestampMillis::from_timestamp(i64::MAX), 10)
        .await
        .unwrap();
    assert_eq!(markers.len(), 2);
    markers.sort_by(|left, right| left.scope.scope_id.cmp(&right.scope.scope_id));
    markers
}

fn select_test_deadline() -> TimestampMillis {
    TimestampMillis::from_timestamp(
        TimestampMillis::now()
            .timestamp_millis()
            .div_euclid(3_600_000)
            * 3_600_000
            + 4 * 86_400_000
            + 10_000,
    )
}

async fn run_one_marker_page<P: crate::DynamoPort>(
    provider: &P,
    now: TimestampMillis,
) -> crate::maintenance::StreamDurationTrimStats
where
    for<'native> P::Attempt<'native>: crate::maintenance::StreamRetentionPort,
{
    StreamDurationTrimWorker::new(
        provider,
        StreamDurationTrimConfig {
            marker_page_size: StreamTrimPageLimit::try_from(1usize).unwrap(),
            stream_page_size: StreamTrimPageLimit::try_from(1usize).unwrap(),
        },
    )
    .run_due_page(now, now)
    .await
    .unwrap()
}

async fn seed_marker_deadlines<P: crate::DynamoPort>(
    provider: &P,
    deadlines: Vec<(StreamTrimDueMarker, TimestampMillis)>,
    now: TimestampMillis,
) where
    for<'native> P::Attempt<'native>: crate::maintenance::StreamRetentionPort,
{
    use crate::maintenance::{
        StreamRetentionFinish, StreamRetentionFinishReason, StreamRetentionPort,
    };
    provider
        .with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let scopes = deadlines
                    .iter()
                    .map(|(marker, _)| marker.scope.clone())
                    .collect::<Vec<_>>();
                let states = StreamRetentionPort::load_stream_trim_states(attempt, &scopes).await?;
                let finishes = deadlines
                    .into_iter()
                    .zip(states)
                    .map(|((marker, deadline), state)| {
                        let expected_state = state.unwrap();
                        let mut state = expected_state.clone();
                        state.next_due_at = Some(deadline);
                        state.updated_at = now - 1;
                        let next_marker = state.scheduled_marker();
                        StreamRetentionFinish {
                            marker,
                            update: Some(crate::StreamTrimStateUpdate {
                                expected_state,
                                write: crate::StreamTrimStateWrite {
                                    state,
                                    next_marker,
                                    replaced_marker: None,
                                },
                            }),
                            reason: StreamRetentionFinishReason::Page { deleted_rows: 0 },
                        }
                    })
                    .collect();
                attempt.finish_stream_trim_markers(finishes).await
            })
        })
        .await
        .unwrap();
}
