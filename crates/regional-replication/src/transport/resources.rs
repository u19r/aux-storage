use std::{sync::Arc, time::Duration};

use replication_provider::runtime_configuration::deployment::QualifiedConfiguration;
use tokio::sync::Semaphore;
use tracing::{Instrument as _, instrument::WithSubscriber as _};

use super::{
    client::TcpPeerClient,
    error::{TransportError, TransportResult},
    message::WireIdentity,
    server::TcpImportServer,
};

#[derive(Clone)]
pub struct TcpTransport {
    pub(super) resources: Arc<TransportResources>,
}

/// Owned source buffers and, for public history, its bounded class reservation.
/// Neither reservation can be released separately while a response owns them.
pub struct SourceReservation {
    _source: tokio::sync::OwnedSemaphorePermit,
    _history: Option<tokio::sync::OwnedSemaphorePermit>,
    deadline: tokio::time::Instant,
}

impl SourceReservation {
    pub(crate) fn require_time_deadline(&self) -> TransportResult<tokio::time::Instant> {
        self.require_live()?;
        Ok(self.deadline)
    }

    pub(crate) fn require_live(&self) -> TransportResult<()> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err(TransportError::Timeout);
        }
        Ok(())
    }
}

pub(super) struct TransportResources {
    pub configuration: Arc<QualifiedConfiguration>,
    pub inbound: Arc<Semaphore>,
    pub outbound: Arc<Semaphore>,
    source: Arc<Semaphore>,
    history_source: Arc<Semaphore>,
    pub(super) source_capacity: u32,
    pub limit: usize,
    pub semantic_limit: usize,
    pub deadline: Duration,
    pub identity: WireIdentity,
}

impl TcpTransport {
    pub fn configuration(&self) -> &Arc<QualifiedConfiguration> {
        &self.resources.configuration
    }

    pub fn try_new(configuration: Arc<QualifiedConfiguration>) -> TransportResult<Self> {
        let configured = configuration.as_ref().as_ref();
        let limit = configured.limits.frame_bytes.get();
        let qualified = configuration.transport_capacity();
        let semantic_limit = qualified.semantic_bytes().get() as usize;
        let source_capacity = qualified.permits().get();
        let capacity = source_capacity as usize;
        if capacity > Semaphore::MAX_PERMITS {
            return Err(TransportError::Configuration);
        }
        let deadline = configured
            .timing
            .request_budget(1)
            .map_err(|_| TransportError::Configuration)?;
        let resources = TransportResources {
            identity: WireIdentity {
                deployment: configured.deployment,
                replica: configured.replica,
                region: configured.region.clone(),
                group: configured.group,
                generation: configured.journal_generation,
            },
            inbound: Arc::new(Semaphore::new(capacity)),
            outbound: Arc::new(Semaphore::new(capacity)),
            source: Arc::new(Semaphore::new(capacity)),
            history_source: Arc::new(Semaphore::new(capacity - 1)),
            source_capacity,
            limit,
            semantic_limit,
            deadline,
            configuration,
        };
        resources.require_chunk_capacity()?;
        Ok(Self {
            resources: Arc::new(resources),
        })
    }

    /// Source selection and receipt verification can each hold one complete
    /// immutable position. Wire fragments use the separate outbound permit.
    pub(crate) async fn reserve_source(&self) -> TransportResult<SourceReservation> {
        self.reserve_source_budget(self.resources.deadline).await
    }

    pub(crate) async fn reserve_source_budget(
        &self,
        budget: Duration,
    ) -> TransportResult<SourceReservation> {
        let deadline = tokio::time::Instant::now()
            .checked_add(budget)
            .ok_or(TransportError::Configuration)?;
        self.resources
            .await_capacity_admission(deadline, async {
                let source = self
                    .resources
                    .source
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| TransportError::Worker)?;
                Ok(SourceReservation {
                    _source: source,
                    _history: None,
                    deadline,
                })
            })
            .await
    }

    pub(crate) async fn reserve_history_source(&self) -> TransportResult<SourceReservation> {
        let deadline = self.resources.request_deadline()?;
        self.resources
            .await_capacity_admission(deadline, async {
                let history = self
                    .resources
                    .history_source
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| TransportError::Worker)?;
                let source = self
                    .resources
                    .source
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| TransportError::Worker)?;
                Ok(SourceReservation {
                    _source: source,
                    _history: Some(history),
                    deadline,
                })
            })
            .await
    }

    /// The host stops shared job dispatch before this barrier. Pending accepted
    /// transfers settle before receiver ownership and its capacity are
    /// released.
    pub(crate) async fn drain_sources(&self) -> TransportResult<()> {
        self.resources.history_source.close();
        let _all = self
            .resources
            .source
            .clone()
            .acquire_many_owned(self.resources.source_capacity)
            .await
            .map_err(|_| TransportError::Worker)?;
        self.resources.source.close();
        let _outbound = self
            .resources
            .outbound
            .clone()
            .acquire_many_owned(self.resources.source_capacity)
            .await
            .map_err(|_| TransportError::Worker)?;
        self.resources.outbound.close();
        Ok(())
    }

    pub(crate) fn source_limits(
        &self,
    ) -> Arc<replication_provider::runtime_configuration::limits::WorkLimits> {
        self.resources.source_limits()
    }

    pub fn peer(
        &self,
        replica: replication_types::identity::replica::ReplicaId,
    ) -> TransportResult<TcpPeerClient> {
        let peer = self
            .resources
            .configuration
            .as_ref()
            .as_ref()
            .peers
            .iter()
            .find(|peer| peer.replica == replica)
            .ok_or(TransportError::Peer)?
            .clone();
        Ok(TcpPeerClient {
            resources: Arc::clone(&self.resources),
            peer,
        })
    }

    pub fn destination(
        &self,
        destination: &replication_types::configuration::installed::RegionalSourceBinding,
    ) -> TransportResult<TcpPeerClient> {
        let peer = self
            .resources
            .configuration
            .as_ref()
            .as_ref()
            .peers
            .iter()
            .find(|peer| {
                peer.region == destination.region
                    && peer.group == destination.database_group
                    && peer.generation == destination.generation
            })
            .ok_or(TransportError::Peer)?;
        Ok(TcpPeerClient {
            resources: Arc::clone(&self.resources),
            peer: peer.clone(),
        })
    }

    pub fn receiver<P: storage_provider::DynamoPort>(
        &self,
        port: Arc<P>,
        journal: Arc<crate::journal::JournalStore>,
        codec: Arc<replication_provider::canonical_event_codec::CanonicalJsonCodec>,
    ) -> TransportResult<TcpImportServer<P>> {
        journal.require_deployment(self.resources.configuration.as_ref().as_ref())?;
        Ok(TcpImportServer {
            resources: Arc::clone(&self.resources),
            port,
            journal,
            codec,
        })
    }
}

impl TransportResources {
    pub(super) fn request_deadline(&self) -> TransportResult<tokio::time::Instant> {
        tokio::time::Instant::now()
            .checked_add(self.deadline)
            .ok_or(TransportError::Configuration)
    }

    async fn await_capacity_admission<T>(
        &self,
        deadline: tokio::time::Instant,
        admission: impl std::future::Future<Output = TransportResult<T>>,
    ) -> TransportResult<T> {
        tokio::time::timeout_at(deadline, admission)
            .await
            .map_err(|_| TransportError::Timeout)?
    }

    fn require_chunk_capacity(&self) -> TransportResult<()> {
        super::chunk_message::ChunkDeclaration::require_wire_capacity(
            (self.identity.clone(), self.identity.clone()),
            self.limit,
        )?;
        for peer in &self.configuration.as_ref().as_ref().peers {
            let destination = WireIdentity {
                deployment: self.identity.deployment,
                replica: peer.replica,
                region: peer.region.clone(),
                group: peer.group,
                generation: peer.generation,
            };
            super::chunk_message::ChunkDeclaration::require_wire_capacity(
                (self.identity.clone(), destination),
                self.limit,
            )?;
        }
        Ok(())
    }
}

/// A cancelled scheduler waiter cannot release source capacity while its
/// accepted native read, encoding or progress attempt still owns the buffers.
pub(crate) async fn settle_source<T: Send + 'static>(
    source: SourceReservation,
    transfer: impl std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>
    + Send
    + 'static,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>> {
    settle_source_with(source, move |source| async move {
        let _source = source;
        transfer.await
    })
    .await
}

/// The result may retain its reservation while exposing native source buffers.
pub(crate) async fn settle_source_with<T: Send + 'static, F, W>(
    source: SourceReservation,
    transfer: F,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    F: FnOnce(SourceReservation) -> W + Send + 'static,
    W: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>
        + Send
        + 'static,
{
    let deadline = source.deadline;
    // A timed-out waiter detaches accepted work; the task keeps its reservation
    // until native settlement or until its returned page is dropped.
    let settlement = metrics_facade::inherit_request_cost(transfer(source))
        .instrument(tracing::Span::current())
        .with_current_subscriber();
    tokio::time::timeout_at(deadline, tokio::spawn(settlement))
        .await
        .map_err(|_| {
            storage_types::StorageError::from(storage_types::StorageEnum::HistoryUnavailable)
        })?
        .map_err(|_| storage_types::StorageError::internal("source transfer task failed"))?
}

impl TransportResources {
    pub(super) fn source_limits(
        &self,
    ) -> Arc<replication_provider::runtime_configuration::limits::WorkLimits> {
        let mut limits = self.configuration.as_ref().as_ref().limits.clone();
        limits.frame_bytes = self
            .configuration
            .native_budgets()
            .maximum_original_position_bytes;
        Arc::new(limits)
    }
}
