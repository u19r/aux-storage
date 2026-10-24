//! Shared native and transport dependencies for bootstrap data transfer.
use std::sync::Arc;

use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use storage_provider::DynamoPort;

use crate::{
    journal::JournalStore,
    transport::{TcpTransport, TransportError},
};

pub(crate) struct BootstrapTransfer<P> {
    pub(super) port: Arc<P>,
    pub(super) journal: Arc<JournalStore>,
    pub(super) codec: Arc<CanonicalJsonCodec>,
    pub(super) transport: TcpTransport,
    pub(super) limits: Arc<WorkLimits>,
}

impl<P: DynamoPort + 'static> BootstrapTransfer<P> {
    pub(crate) fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        transport: TcpTransport,
    ) -> Result<Self, TransportError> {
        journal.require_deployment(transport.configuration().as_ref().as_ref())?;
        let limits = transport.source_limits();
        Ok(Self {
            port,
            journal,
            codec,
            transport,
            limits,
        })
    }
}

impl<P> Clone for BootstrapTransfer<P> {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
            transport: self.transport.clone(),
            limits: Arc::clone(&self.limits),
        }
    }
}
