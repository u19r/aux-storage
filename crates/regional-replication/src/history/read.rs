//! Bounded original-history reads over the same native manifests as delivery.

use std::sync::Arc;

use async_trait::async_trait;
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::interval::{ClosedSourceInterval, SourceIntervalManifest},
    identity::replica::{DatabaseGroupId, RegionId},
};
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::{journal::JournalStore, transport::TcpTransport};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum CanonicalHistoryCut {
    Source(super::source_cut::CanonicalSourceCut),
    Table(super::table_cut::CanonicalTableCut),
    Checkpoint(super::checkpoint::CanonicalCheckpointId),
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalHistoryRequest {
    pub selection: super::source_selection::CanonicalSourceSelection,
    pub range: ClosedSourceInterval,
    pub cut: Option<CanonicalHistoryCut>,
}

#[derive(Serialize)]
pub struct CanonicalHistoryPage {
    pub manifest: SourceIntervalManifest,
    pub originals: Vec<String>,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

#[derive(Serialize)]
pub(crate) struct CanonicalHistoryObservation {
    pub(crate) manifest: SourceIntervalManifest,
    pub(crate) originals: Vec<String>,
}

#[async_trait]
pub trait CanonicalHistoryOperations: Send + Sync {
    fn matches(&self, serving: (&RegionId, &DatabaseGroupId)) -> bool;
    async fn causal_stream(
        &self,
        request: super::causal_stream::CanonicalCausalStreamRequest,
    ) -> StorageResult<super::causal_stream::CanonicalCausalStreamPage>;
    async fn causal_read(
        &self,
        request: super::causal_read::CanonicalCausalReadRequest,
    ) -> StorageResult<super::causal_read::CanonicalCausalReadPage>;
    async fn reader_holds(
        &self,
        request: super::hold_discovery::ReaderHoldDiscoveryRequest,
    ) -> StorageResult<super::hold_discovery::ReaderHoldPage>;
    async fn discover(
        &self,
        request: crate::history::discovery::CanonicalHistoryDiscoveryRequest,
    ) -> StorageResult<crate::history::discovery::CanonicalHistorySources>;
    async fn read(&self, request: CanonicalHistoryRequest) -> StorageResult<CanonicalHistoryPage>;
    async fn view(
        &self,
        request: crate::history::view::CanonicalViewRequest,
    ) -> StorageResult<crate::history::view::CanonicalViewPage>;
    async fn checkpoint(
        &self,
        request: super::checkpoint::CanonicalCheckpointRequest,
    ) -> StorageResult<Option<super::checkpoint::CanonicalCheckpointStatus>>;
    async fn source_cut(
        &self,
        selection: super::source_selection::CanonicalSourceSelection,
    ) -> StorageResult<super::source_cut::CanonicalSourceCutObservation>;
    async fn table_cut(
        &self,
        request: super::table_cut::CanonicalTableCutRequest,
    ) -> StorageResult<super::table_cut::CanonicalTableCutObservation>;
    async fn portable_checkpoint(
        &self,
        request: super::portable_checkpoint::PortableCheckpointRequest,
    ) -> StorageResult<Option<super::portable_checkpoint::PortableCheckpointStatus>>;
}

pub(crate) struct NativeHistoryReader<P> {
    pub(crate) port: Arc<P>,
    pub(crate) journal: Arc<JournalStore>,
    pub(crate) codec: Arc<CanonicalJsonCodec>,
    pub(crate) transport: TcpTransport,
}

#[async_trait]
impl<P: DynamoPort + 'static> CanonicalHistoryOperations for NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    fn matches(&self, serving: (&RegionId, &DatabaseGroupId)) -> bool {
        let configured = self.transport.configuration().as_ref().as_ref();
        serving.0 == &configured.region && serving.1 == &configured.group
    }

    async fn causal_stream(
        &self,
        request: super::causal_stream::CanonicalCausalStreamRequest,
    ) -> StorageResult<super::causal_stream::CanonicalCausalStreamPage> {
        self.read_causal_stream(request).await
    }

    async fn causal_read(
        &self,
        request: super::causal_read::CanonicalCausalReadRequest,
    ) -> StorageResult<super::causal_read::CanonicalCausalReadPage> {
        self.read_causal_closure(request).await
    }

    async fn reader_holds(
        &self,
        request: super::hold_discovery::ReaderHoldDiscoveryRequest,
    ) -> StorageResult<super::hold_discovery::ReaderHoldPage> {
        self.discover_reader_holds(request).await
    }

    async fn discover(
        &self,
        request: crate::history::discovery::CanonicalHistoryDiscoveryRequest,
    ) -> StorageResult<crate::history::discovery::CanonicalHistorySources> {
        self.discover_sources(request).await
    }

    async fn view(
        &self,
        request: crate::history::view::CanonicalViewRequest,
    ) -> StorageResult<crate::history::view::CanonicalViewPage> {
        self.read_view(request).await
    }

    async fn checkpoint(
        &self,
        request: super::checkpoint::CanonicalCheckpointRequest,
    ) -> StorageResult<Option<super::checkpoint::CanonicalCheckpointStatus>> {
        self.change_checkpoint(request).await
    }

    async fn source_cut(
        &self,
        selection: super::source_selection::CanonicalSourceSelection,
    ) -> StorageResult<super::source_cut::CanonicalSourceCutObservation> {
        self.observe_source_cut(selection).await
    }

    async fn table_cut(
        &self,
        request: super::table_cut::CanonicalTableCutRequest,
    ) -> StorageResult<super::table_cut::CanonicalTableCutObservation> {
        self.observe_table_cut(request).await
    }

    async fn portable_checkpoint(
        &self,
        request: super::portable_checkpoint::PortableCheckpointRequest,
    ) -> StorageResult<Option<super::portable_checkpoint::PortableCheckpointStatus>> {
        self.change_portable_checkpoint(request).await
    }

    async fn read(&self, request: CanonicalHistoryRequest) -> StorageResult<CanonicalHistoryPage> {
        if !self.matches((
            &request.selection.serving_region,
            &request.selection.serving_group,
        )) {
            return Err(StorageError::validation(
                "History serving database does not match",
            ));
        }
        let source = self
            .transport
            .reserve_history_source()
            .await
            .map_err(|_| StorageError::from(storage_types::StorageEnum::HistoryUnavailable))?;
        let port = Arc::clone(&self.port);
        let journal = Arc::clone(&self.journal);
        let codec = Arc::clone(&self.codec);
        let limits = self.transport.source_limits();
        let request = Arc::new(request);
        crate::transport::settle_source_with(source, move |source| async move {
            let mode = if matches!(&request.cut, Some(CanonicalHistoryCut::Checkpoint(_))) {
                AttemptMode::ReadWrite
            } else {
                AttemptMode::ReadOnly
            };
            let observed = storage_common::retry::retry_native_conflicts(
                port.as_ref(),
                storage_provider::DYNAMO_WRITE_RETRY,
                || {
                    let port = Arc::clone(&port);
                    let journal = Arc::clone(&journal);
                    let codec = Arc::clone(&codec);
                    let request = Arc::clone(&request);
                    let limits = Arc::clone(&limits);
                    async move {
                        std::sync::Arc::clone(&journal)
                            .with_admitted_attempt(
                                (
                                    port.as_ref(),
                                    if matches!(mode, AttemptMode::ReadOnly) {
                                        storage_provider::native_admission::NativeWork::RangeRead
                                    } else {
                                        storage_provider::native_admission::NativeWork::Write
                                    },
                                ),
                                crate::journal::NativeAttempt::Items(mode),
                                move |attempt, observed_at| {
                                    Box::pin(async move {
                                        super::retention::settle_expiry(
                                            journal
                                                .load_bounded_canonical_history(
                                                    attempt,
                                                    &request,
                                                    (&limits, &codec),
                                                    observed_at,
                                                )
                                                .await,
                                        )
                                    })
                                },
                            )
                            .await
                    }
                },
            )
            .await??;
            Ok(CanonicalHistoryPage {
                manifest: observed.manifest,
                originals: observed.originals,
                _source: source,
            })
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native history reader failed"))
        })
    }
}
