//! Transaction I/O adapter for the existing deterministic retention models.

use storage_types::StorageResult;

use crate::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamRetentionRepository,
    StreamTrimScope, StreamTrimScopeBoundaries,
    maintenance::{StreamRetentionFinish, StreamRetentionTransaction},
};

pub(crate) trait ModelRetentionIo {
    fn trim_stream_pages(
        &self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> impl std::future::Future<Output = StorageResult<Vec<StreamDurationTrimPageResult>>> + Send;

    fn finish_stream_trim_markers(
        &self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send;
}

pub(crate) struct ModelTransaction<'a, T: ?Sized> {
    model: &'a T,
    rejected: bool,
}

impl<'a, T: ?Sized> ModelTransaction<'a, T> {
    pub(crate) fn new(model: &'a T) -> Self {
        Self {
            model,
            rejected: false,
        }
    }
}

impl<T: StreamRetentionRepository + ModelRetentionIo + ?Sized> StreamRetentionTransaction
    for ModelTransaction<'_, T>
{
    async fn load_stream_trim_observations(
        &mut self,
        markers: &[crate::StreamTrimDueMarker],
    ) -> StorageResult<Vec<crate::maintenance::StreamRetentionObservation>> {
        let scopes = markers
            .iter()
            .map(|marker| marker.scope.clone())
            .collect::<Vec<_>>();
        let states = self.model.load_stream_trim_states(&scopes).await?;
        let present = self
            .model
            .list_due_stream_trim_markers(
                storage_types::TimestampMillis::from_timestamp(i64::MAX),
                usize::MAX,
            )
            .await?;
        Ok(markers
            .iter()
            .zip(states)
            .map(|(marker, state)| (present.contains(marker), state).into())
            .collect())
    }

    async fn load_stream_trim_boundaries(
        &mut self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        self.model.load_stream_trim_boundaries(scopes).await
    }

    async fn trim_stream_pages(
        &mut self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
        self.model.trim_stream_pages(requests).await
    }

    async fn finish_stream_trim_markers(
        &mut self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> StorageResult<()> {
        self.model.finish_stream_trim_markers(finishes).await
    }
}

#[test]
fn given_same_scope_markers_when_constructing_wave_then_all_duplicate_versions_are_rejected() {
    use storage_types::{TableName, TimestampMillis};

    use crate::{StreamTrimDueMarker, StreamTrimPageLimit, maintenance::StreamRetentionWave};

    let now = TimestampMillis::from_timestamp(1);
    let scope = StreamTrimScope::table("table/one", TableName::new("one"));
    let marker = StreamTrimDueMarker::new(now, scope, 1);
    for version in [1, 2] {
        let mut duplicate = marker.clone();
        duplicate.policy_version = version;
        let wave = StreamRetentionWave::try_new(
            vec![marker.clone(), duplicate],
            now,
            now,
            StreamTrimPageLimit::try_from(1_usize).unwrap(),
        );
        assert!(
            wave.is_err(),
            "one scope cannot appear twice even with different versions"
        );
    }
}

#[test]
fn given_distinct_scope_markers_when_constructing_wave_then_order_and_markers_are_preserved() {
    use storage_types::{TableName, TimestampMillis};

    use crate::{StreamTrimDueMarker, StreamTrimPageLimit, maintenance::StreamRetentionWave};

    let now = TimestampMillis::from_timestamp(1);
    let markers = ["b", "a"]
        .map(|name| {
            StreamTrimDueMarker::new(now, StreamTrimScope::table(name, TableName::new(name)), 1)
        })
        .to_vec();
    let wave = StreamRetentionWave::try_new(
        markers.clone(),
        now,
        now,
        StreamTrimPageLimit::try_from(1_usize).unwrap(),
    )
    .expect("distinct scopes");
    assert!(wave.markers() == markers);
}

#[test]
fn given_max_policy_version_when_advancing_retention_policy_then_overflow_is_rejected() {
    use storage_types::{StreamRetentionDuration, TableName, TimestampMillis};

    use crate::StreamTrimState;

    let scope = StreamTrimScope::table("table/overflow", TableName::new("overflow"));
    let state = StreamTrimState {
        scope,
        policy_version: u64::MAX,
        retention: StreamRetentionDuration::FiniteHours(1),
        effective_retention: StreamRetentionDuration::FiniteHours(1),
        next_due_at: None,
        oldest_retained_version: None,
        oldest_retained_timestamp: None,
        latest_version: None,
        latest_timestamp: None,
        updated_at: TimestampMillis::from_timestamp(1),
    };

    assert!(matches!(StreamTrimState::next_policy_version(None), Ok(1)));
    assert!(StreamTrimState::next_policy_version(Some(&state)).is_err());
}

// These deterministic policy fixtures do not model native durability.
impl<T: Sync + ?Sized> infrastructure_provider::physical_attempt::PhysicalRecordAttempt
    for ModelTransaction<'_, T>
{
    fn read_protection(&self) -> storage_types::ReadProtection {
        storage_types::ReadProtection::Conflict
    }
    fn reject_settlement(&mut self) {
        self.rejected = true;
    }
    fn reject_settlement_error(&mut self, _: &storage_types::StorageError) {
        self.reject_settlement();
    }
    async fn has_records_in_namespace(
        &mut self,
        _: &str,
        _: storage_types::ReadProtection,
    ) -> StorageResult<bool> {
        panic!("policy model must not access protected physical records")
    }
    async fn load_records(
        &mut self,
        _: &[infrastructure_provider::record::DurableRecordKey],
        _: storage_types::ReadProtection,
        _: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<Vec<Option<infrastructure_provider::record::DurableRecord>>> {
        panic!("policy model must not access protected physical records")
    }
    async fn scan_records(
        &mut self,
        _: infrastructure_provider::page::RecordPageRequest,
        _: infrastructure_provider::limits::InfrastructureLimits,
    ) -> StorageResult<infrastructure_provider::page::RecordPage> {
        panic!("policy model must not access protected physical records")
    }
    async fn store_records(
        &mut self,
        _: &infrastructure_provider::batch::RecordBatch<'_>,
    ) -> StorageResult<infrastructure_provider::record::ConditionalWriteResult> {
        panic!("policy model must not access protected physical records")
    }
}
