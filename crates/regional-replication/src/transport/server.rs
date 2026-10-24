use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    regional_application::import_admission::BoundedImport,
    runtime_configuration::limits::WorkLimits,
};
use replication_types::regional_import::RegionalImport;
use storage_provider::DynamoPort;
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
    task::JoinSet,
};

use super::{
    connection::TransportConnection,
    error::{ImportNack, TransportError, TransportResult},
    framing,
    message::{ImportAcknowledgement, ImportReply, IncomingPage, WireIdentity},
    resources::TransportResources,
};
use crate::{
    constants::TRANSPORT_REPLY_KIND,
    import::{ImportExecution, execute_import},
    import_receipt::{ImportReceiptObservation, reconcile_import_pages},
    journal::JournalStore,
};

pub struct TcpImportServer<P> {
    pub(super) resources: Arc<TransportResources>,
    pub(super) port: Arc<P>,
    pub(super) journal: Arc<JournalStore>,
    pub(super) codec: Arc<CanonicalJsonCodec>,
}

impl<P: DynamoPort + 'static> TcpImportServer<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub async fn serve(
        self,
        listener: TcpListener,
        stop: watch::Receiver<bool>,
    ) -> TransportResult<()> {
        if listener.local_addr()? != self.resources.configuration.as_ref().as_ref().endpoint {
            return Err(TransportError::Configuration);
        }
        // Cancellation closes intake without aborting the JoinSet's accepted
        // work. The child retains every inbound reservation through request
        // settlement.
        let (_lifetime, cancelled) = oneshot::channel::<()>();
        let receiver = tokio::spawn(self.serve_until_stopped(listener, stop, cancelled));
        receiver.await.map_err(|_| TransportError::Worker)?
    }

    async fn serve_until_stopped(
        self,
        listener: TcpListener,
        mut stop: watch::Receiver<bool>,
        mut cancelled: oneshot::Receiver<()>,
    ) -> TransportResult<()> {
        let server = Arc::new(self);
        let mut requests = JoinSet::new();
        let mut failure = None;
        while !*stop.borrow() {
            tokio::select! {
                _ = &mut cancelled => { break; }
                changed = stop.changed() => { if changed.is_err() || *stop.borrow() { break; } }
                completed = requests.join_next(), if !requests.is_empty() => {
                    if completed.is_none_or(request_worker_failed) { failure = Some(TransportError::Worker); break; }
                }
                accepted = server.accept(&listener) => {
                    let (socket, reservation) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => { failure = Some(error); break; }
                    };
                    let server = Arc::clone(&server);
                    requests.spawn(async move { let _reservation = reservation; server.receive(socket).await });
                }
            }
        }
        drop(listener);
        while let Some(completed) = requests.join_next().await {
            if request_worker_failed(completed) {
                failure = Some(TransportError::Worker);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    async fn accept(
        &self,
        listener: &TcpListener,
    ) -> TransportResult<(TransportConnection, tokio::sync::OwnedSemaphorePermit)> {
        let reservation = self
            .resources
            .inbound
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransportError::Worker)?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let socket = TransportConnection::new(socket, self.resources.request_deadline()?);
        Ok((socket, reservation))
    }

    async fn receive(&self, mut socket: TransportConnection) -> TransportResult<()> {
        let request = framing::read_request(&mut socket, &self.resources, |from, to| {
            self.require_peer(from, to)
        })
        .await?;
        socket.require_live()?;
        match request {
            framing::IncomingRequest::Import(page) => self.receive_import(&mut socket, page).await,
            framing::IncomingRequest::Trim(message) => {
                self.receive_trim(&mut socket, message).await
            }
            framing::IncomingRequest::Control(message) => {
                self.receive_control(&mut socket, message).await
            }
            framing::IncomingRequest::Bootstrap(message) => {
                self.receive_bootstrap(&mut socket, *message).await
            }
            framing::IncomingRequest::HistoryCapture(message) => {
                self.receive_history_capture(&mut socket, *message).await
            }
            framing::IncomingRequest::Seal(message) => {
                self.receive_seal(&mut socket, *message).await
            }
        }
    }

    async fn receive_import(
        &self,
        socket: &mut TransportConnection,
        page: IncomingPage,
    ) -> TransportResult<()> {
        self.require_peer(&page.from, &page.to)?;
        let to = page.from.clone();
        let limits = Arc::new(
            self.resources
                .configuration
                .as_ref()
                .as_ref()
                .limits
                .clone(),
        );
        let result = match page.into_import(&self.codec, &limits) {
            Ok(import) => {
                socket.require_live()?;
                self.apply(Arc::new(import), limits).await
            }
            Err(_) => Err(ImportNack::InvalidPage),
        };
        super::chunk_exchange::write_serialized(
            socket,
            (
                TRANSPORT_REPLY_KIND,
                ImportReply {
                    from: self.resources.identity.clone(),
                    to: to.clone(),
                    result,
                },
            ),
            &self.resources,
            (self.resources.identity.clone(), to),
        )
        .await
    }

    pub(super) fn require_peer(
        &self,
        from: &WireIdentity,
        to: &WireIdentity,
    ) -> TransportResult<()> {
        let configured = self.resources.configuration.as_ref().as_ref();
        if to != &self.resources.identity
            || from.deployment != configured.deployment
            || !configured.peers.iter().any(|peer| {
                peer.replica == from.replica
                    && peer.region == from.region
                    && peer.group == from.group
                    && peer.generation == from.generation
            })
        {
            return Err(TransportError::Peer);
        }
        Ok(())
    }

    async fn apply(
        &self,
        import: Arc<RegionalImport>,
        limits: Arc<WorkLimits>,
    ) -> Result<ImportAcknowledgement, ImportNack> {
        let import =
            BoundedImport::try_new(import, &limits).map_err(|_| ImportNack::InvalidPage)?;
        let execution = ImportExecution {
            import: import.clone(),
            codec: Arc::clone(&self.codec),
        };
        match execute_import(self.port.as_ref(), Arc::clone(&self.journal), execution).await {
            Ok(committed) => Ok(ImportAcknowledgement {
                pages: committed.pages().to_vec(),
                completed: committed.completed().map(<[_]>::to_vec),
            }),
            Err(error) if error.is_commit_unknown() => self.reconcile(import).await,
            Err(error) => Err(classify_native_error(&error)),
        }
    }

    async fn reconcile(&self, import: BoundedImport) -> Result<ImportAcknowledgement, ImportNack> {
        match reconcile_import_pages(
            self.port.as_ref(),
            Arc::clone(&self.journal),
            (import, Arc::clone(&self.codec)),
        )
        .await
        {
            Ok(ImportReceiptObservation::Durable(committed)) => Ok(ImportAcknowledgement {
                pages: committed.pages().to_vec(),
                completed: committed.completed().map(<[_]>::to_vec),
            }),
            Ok(ImportReceiptObservation::Unresolved) => Err(ImportNack::UnresolvedCommit),
            Err(_) => Err(ImportNack::UnresolvedCommit),
        }
    }
}

fn request_worker_failed(completed: Result<TransportResult<()>, tokio::task::JoinError>) -> bool {
    matches!(completed, Err(_) | Ok(Err(TransportError::Worker)))
}

pub(super) fn classify_native_error(error: &storage_types::StorageError) -> ImportNack {
    use storage_types::{
        InfrastructureContext, InfrastructureResultKind, StorageEnum, context::WrappedError,
    };
    if error.is_commit_unknown() {
        return ImportNack::UnresolvedCommit;
    }
    if let Some(rejection) = error.native_attempt_rejection() {
        return match rejection {
            storage_types::NativeAttemptRejection::Conflict => ImportNack::Aborted,
            storage_types::NativeAttemptRejection::Capacity => ImportNack::CapacityRejected,
        };
    }
    match error.to_enum() {
        StorageEnum::InfrastructureResult(InfrastructureResultKind::Unavailable {
            context: InfrastructureContext::RegionalSchema,
        }) => ImportNack::SchemaUnavailable,
        StorageEnum::InfrastructureResult(InfrastructureResultKind::Encoding { .. })
        | StorageEnum::Unsupported { .. } => ImportNack::InvalidPage,
        _ => ImportNack::Unavailable,
    }
}
