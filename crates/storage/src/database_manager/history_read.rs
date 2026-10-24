//! Native history handles share the regional service lifetime and source
//! budget.

use regional_replication::history::{
    discovery::{CanonicalHistoryDiscoveryRequest, CanonicalHistorySources},
    encoded_response::HistoryResponsePayload,
    read::{CanonicalHistoryPage, CanonicalHistoryRequest},
};
use storage_types::{StorageError, StorageResult};

use super::{DatabaseManager, service_shutdown::BackgroundServiceState};

impl DatabaseManager {
    pub async fn read_canonical_causal_stream(
        &self,
        request: regional_replication::history::causal_stream::CanonicalCausalStreamRequest,
    ) -> StorageResult<regional_replication::history::causal_stream::CanonicalCausalStreamPage>
    {
        self.select_canonical_history((&request.view.serving_region, &request.view.serving_group))
            .await?
            .causal_stream(request)
            .await
    }

    pub async fn read_canonical_causal_history(
        &self,
        request: regional_replication::history::causal_read::CanonicalCausalReadRequest,
    ) -> StorageResult<regional_replication::history::causal_read::CanonicalCausalReadPage> {
        self.select_canonical_history((&request.serving_region, &request.serving_group))
            .await?
            .causal_read(request)
            .await
    }

    pub async fn discover_canonical_reader_holds(
        &self,
        request: regional_replication::history::hold_discovery::ReaderHoldDiscoveryRequest,
    ) -> StorageResult<regional_replication::history::hold_discovery::ReaderHoldPage> {
        self.select_canonical_history((&request.serving_region, &request.serving_group))
            .await?
            .reader_holds(request)
            .await
    }

    pub async fn read_canonical_history(
        &self,
        request: CanonicalHistoryRequest,
    ) -> StorageResult<CanonicalHistoryPage> {
        self.select_canonical_history((
            &request.selection.serving_region,
            &request.selection.serving_group,
        ))
        .await?
        .read(request)
        .await
    }

    pub async fn discover_canonical_history(
        &self,
        request: CanonicalHistoryDiscoveryRequest,
    ) -> StorageResult<CanonicalHistorySources> {
        self.select_canonical_history((&request.serving_region, &request.serving_group))
            .await?
            .discover(request)
            .await
    }

    pub async fn read_canonical_view(
        &self,
        request: regional_replication::history::view::CanonicalViewRequest,
    ) -> StorageResult<regional_replication::history::view::CanonicalViewPage> {
        self.select_canonical_history((&request.serving_region, &request.serving_group))
            .await?
            .view(request)
            .await
    }

    pub async fn change_canonical_checkpoint(
        &self,
        request: regional_replication::history::checkpoint::CanonicalCheckpointRequest,
    ) -> StorageResult<Option<regional_replication::history::checkpoint::CanonicalCheckpointStatus>>
    {
        self.select_canonical_history((&request.view.serving_region, &request.view.serving_group))
            .await?
            .checkpoint(request)
            .await
    }

    pub async fn observe_canonical_source_cut(
        &self,
        selection: regional_replication::history::source_selection::CanonicalSourceSelection,
    ) -> StorageResult<regional_replication::history::source_cut::CanonicalSourceCutObservation>
    {
        self.select_canonical_history((&selection.serving_region, &selection.serving_group))
            .await?
            .source_cut(selection)
            .await
    }

    pub async fn observe_canonical_table_cut(
        &self,
        request: regional_replication::history::table_cut::CanonicalTableCutRequest,
    ) -> StorageResult<regional_replication::history::table_cut::CanonicalTableCutObservation> {
        self.select_canonical_history((&request.serving_region, &request.serving_group))
            .await?
            .table_cut(request)
            .await
    }

    pub async fn change_portable_checkpoint(
        &self,
        request: regional_replication::history::portable_checkpoint::PortableCheckpointRequest,
    ) -> StorageResult<
        Option<regional_replication::history::portable_checkpoint::PortableCheckpointStatus>,
    > {
        self.select_canonical_history((&request.serving_region, &request.serving_group))
            .await?
            .portable_checkpoint(request)
            .await
    }

    async fn select_canonical_history(
        &self,
        serving: (
            &replication_types::identity::replica::RegionId,
            &replication_types::identity::replica::DatabaseGroupId,
        ),
    ) -> StorageResult<
        std::sync::Arc<dyn regional_replication::history::read::CanonicalHistoryOperations>,
    > {
        let state = self.background_services.lock().await;
        let BackgroundServiceState::Running(services) = &*state else {
            return Err(storage_types::StorageEnum::HistoryUnavailable.into());
        };
        services
            .regional
            .iter()
            .filter(|service| service.is_running())
            .map(|service| service.history())
            .find(|history| history.matches(serving))
            .ok_or_else(|| StorageError::validation("History serving database does not match"))
    }

    pub async fn read_canonical_history_response(
        &self,
        payload: serde_json::Value,
    ) -> StorageResult<super::CanonicalHistoryResponse> {
        let request = serde_json::from_value(payload)
            .map_err(|_| StorageError::validation("Invalid canonical history request"))?;
        let response = match request {
            HistoryRequest::CausalStream(request) => self
                .read_canonical_causal_stream(request)
                .await?
                .into_encoded()?,
            HistoryRequest::CausalRead(request) => self
                .read_canonical_causal_history(request)
                .await?
                .into_encoded()?,
            HistoryRequest::ReaderHolds(request) => self
                .discover_canonical_reader_holds(request)
                .await?
                .into_encoded()?,
            HistoryRequest::PortableCheckpoint(request) => self
                .change_portable_checkpoint(request)
                .await?
                .into_encoded()?,
            HistoryRequest::TableCut(request) => self
                .observe_canonical_table_cut(request)
                .await?
                .into_encoded()?,
            HistoryRequest::SourceCut(selection) => self
                .observe_canonical_source_cut(selection)
                .await?
                .into_encoded()?,
            HistoryRequest::Checkpoint(request) => self
                .change_canonical_checkpoint(request)
                .await?
                .into_encoded()?,
            HistoryRequest::View(request) => {
                self.read_canonical_view(request).await?.into_encoded()?
            }
            HistoryRequest::Read(request) => {
                self.read_canonical_history(request).await?.into_encoded()?
            }
            HistoryRequest::Discover(request) => self
                .discover_canonical_history(request)
                .await?
                .into_encoded()?,
        };
        Ok(response.into())
    }
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum HistoryRequest {
    CausalStream(regional_replication::history::causal_stream::CanonicalCausalStreamRequest),
    CausalRead(regional_replication::history::causal_read::CanonicalCausalReadRequest),
    ReaderHolds(regional_replication::history::hold_discovery::ReaderHoldDiscoveryRequest),
    PortableCheckpoint(
        regional_replication::history::portable_checkpoint::PortableCheckpointRequest,
    ),
    TableCut(regional_replication::history::table_cut::CanonicalTableCutRequest),
    SourceCut(regional_replication::history::source_selection::CanonicalSourceSelection),
    Checkpoint(regional_replication::history::checkpoint::CanonicalCheckpointRequest),
    View(regional_replication::history::view::CanonicalViewRequest),
    Read(CanonicalHistoryRequest),
    Discover(CanonicalHistoryDiscoveryRequest),
}
