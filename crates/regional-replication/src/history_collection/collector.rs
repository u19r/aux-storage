use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::proposal::ControlProposal;
use storage_provider::DynamoPort;

use super::executor::{accept_collection, begin_collection, observe_collection};
use crate::{
    history_capture::{
        self, HistoryCaptureDefinition, HistoryCaptureRequest, HistoryCaptureStatus,
        HistoryCaptureVectorPage,
    },
    journal::JournalStore,
    native_record::corrupt_record,
    transport::{TcpTransport, TransportError},
};

/// Each invocation performs bounded source work or commits one received page.
/// Native cursors survive controller restart and ambiguous replies.
pub(crate) struct HistoryCollector<P> {
    pub(super) port: Arc<P>,
    pub(super) journal: Arc<JournalStore>,
    pub(super) transport: TcpTransport,
}

impl<P: DynamoPort> HistoryCollector<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        journal.require_deployment(transport.configuration().as_ref().as_ref())?;
        Ok(Self {
            port,
            journal,
            transport,
        })
    }

    pub(crate) async fn collect_page(
        &self,
        proposal: Arc<ControlProposal>,
        source: &crate::transport::SourceReservation,
    ) -> Result<bool, TransportError> {
        source.require_live()?;
        if self.transport.configuration().as_ref().as_ref().region
            != proposal.operation().home_region
        {
            return Err(TransportError::Peer);
        }
        let Some(observation) = observe_collection(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            Arc::clone(&proposal),
        )
        .await?
        else {
            let request = Arc::new(HistoryCaptureRequest {
                proposal: proposal.as_ref().clone(),
                region: proposal.operation().home_region.clone(),
            });
            source.require_live()?;
            if self.advance_source(request).await? == HistoryCaptureStatus::Ready {
                source.require_live()?;
                begin_collection(self.port.as_ref(), Arc::clone(&self.journal), proposal).await?;
            }
            return Ok(false);
        };
        let Some(request) = observation.record.work_request()? else {
            return Ok(true);
        };
        let request = Arc::new(request);
        source.require_live()?;
        let Some(change) = self
            .fetch_collection_change(request, &observation.record, source)
            .await?
        else {
            return Ok(false);
        };
        source.require_live()?;
        accept_collection(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            proposal,
            (observation.revision, Arc::new(change)),
        )
        .await?;
        Ok(false)
    }

    pub(super) async fn advance_source(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> Result<HistoryCaptureStatus, TransportError> {
        let status = if self.is_local_source(&request) {
            history_capture::advance_history_capture(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await?
        } else {
            self.source_peer(&request)?
                .advance_history_capture(request)
                .await?
        };
        match status {
            HistoryCaptureStatus::Holding
            | HistoryCaptureStatus::CapturingHigh
            | HistoryCaptureStatus::Ready => Ok(status),
            _ => Err(corrupt_record().into()),
        }
    }

    pub(super) async fn fetch_source_definition(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> Result<HistoryCaptureDefinition, TransportError> {
        if self.is_local_source(&request) {
            Ok(history_capture::load_history_capture_definition(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await?)
        } else {
            self.source_peer(&request)?
                .fetch_history_capture_definition(request)
                .await
        }
    }

    pub(super) async fn fetch_source_page(
        &self,
        request: Arc<HistoryCaptureRequest>,
        after: u32,
    ) -> Result<HistoryCaptureVectorPage, TransportError> {
        if self.is_local_source(&request) {
            Ok(history_capture::load_history_capture_cuts(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
                after,
            )
            .await?)
        } else {
            self.source_peer(&request)?
                .fetch_history_capture_cuts(request, after)
                .await
        }
    }

    pub(super) async fn fetch_retained_baseline_definition(
        &self,
        request: Arc<HistoryCaptureRequest>,
    ) -> Result<crate::history_capture::RetainedBaselineDefinition, TransportError> {
        if self.is_local_source(&request) {
            Ok(history_capture::load_retained_baseline_definition(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
            )
            .await?)
        } else {
            self.source_peer(&request)?
                .fetch_retained_baseline_definition(request)
                .await
        }
    }

    pub async fn fetch_retired_source_catalog(
        &self,
        request: Arc<HistoryCaptureRequest>,
        scope: Arc<crate::history_capture::HistoryCatalogScope>,
    ) -> Result<crate::history_capture::RetiredHistoryCatalogPage, TransportError> {
        if self.is_local_source(&request) {
            Ok(history_capture::load_retired_source_catalog(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
                scope,
            )
            .await?)
        } else {
            self.source_peer(&request)?
                .fetch_retired_source_catalog(request, scope)
                .await
        }
    }

    pub async fn fetch_retired_source_page(
        &self,
        request: Arc<HistoryCaptureRequest>,
        scope: (
            replication_types::configuration::installed::RegionalSourceBinding,
            u32,
        ),
    ) -> Result<crate::history_capture::RetiredHistoryPage, TransportError> {
        if self.is_local_source(&request) {
            Ok(history_capture::load_retired_history_page(
                self.port.as_ref(),
                Arc::clone(&self.journal),
                request,
                scope,
            )
            .await?)
        } else {
            self.source_peer(&request)?
                .fetch_retired_history_page(request, scope)
                .await
        }
    }

    pub(super) fn is_local_source(&self, request: &HistoryCaptureRequest) -> bool {
        request.region == self.transport.configuration().as_ref().as_ref().region
    }

    pub(super) fn source_peer(
        &self,
        request: &HistoryCaptureRequest,
    ) -> Result<crate::transport::TcpPeerClient, TransportError> {
        let source = replication_provider::control::borrow_addition_source(
            &request.proposal,
            &request.region,
        )?;
        let binding = source
            .sources
            .iter()
            .find(|source| source.region == request.region)
            .ok_or(TransportError::Peer)?;
        self.transport.destination(binding)
    }
}

impl<P> Clone for HistoryCollector<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            journal: Arc::clone(&self.journal),
            transport: self.transport.clone(),
        }
    }
}
