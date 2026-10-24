use std::sync::Arc;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt, physical_store::PhysicalRecordStore,
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::protocol::PayloadBounds;
use storage_provider::{DynamoPort, native_admission::NativeAttemptSource};

use super::{initialization::initialize_journal, startup::RegionalStartup};
use crate::{
    journal::JournalStore,
    serving::RegionalDynamo,
    transport::{TcpImportServer, TcpTransport, TransportError},
};

/// One regional process shares transport reservations across its receiver,
/// delivery, and control handlers. It creates no private scheduler or timer.
pub struct RegionalRuntime<P> {
    pub(super) port: Arc<P>,
    pub(super) journal: Arc<JournalStore>,
    pub(super) codec: Arc<CanonicalJsonCodec>,
    pub(super) transport: TcpTransport,
    operations: Arc<RegionalDynamo<P>>,
}

impl<P: DynamoPort + PhysicalRecordStore + NativeAttemptSource> RegionalRuntime<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub async fn initialize(
        port: Arc<P>,
        startup: RegionalStartup,
    ) -> Result<Self, TransportError> {
        let configuration = Arc::new(startup.qualify(port.as_ref())?);
        let deployment = configuration.as_ref().as_ref();
        let native = configuration.native_budgets();

        let journal = Arc::new(JournalStore::try_new(
            (
                deployment.region.clone(),
                deployment.group,
                deployment.journal_generation,
                deployment.tenant,
            ),
            deployment.journal_partitions,
            startup.physical_limits,
            (deployment.backlog.clone(), deployment.timing.clock_alert_ms),
        )?);
        journal.bind_time_trust(&configuration)?;
        let codec = Arc::new(CanonicalJsonCodec::new(PayloadBounds {
            event_bytes: native.maximum_canonical_event_bytes,
            manifest_bytes: native.maximum_history_declaration_bytes,
        }));
        journal.require_original_capacity(native, deployment.limits.frame_events)?;

        let transport = TcpTransport::try_new(Arc::clone(&configuration))?;
        let physical_identity = port.initialize_records(journal.physical_limits()).await?;
        if let Some(root) = &deployment.archive_root {
            let archive = Arc::new(
                crate::archive::CanonicalArchive::try_new(
                    root,
                    physical_identity,
                    journal.physical_limits().max_record_bytes(),
                )
                .await?,
            );
            journal
                .archive
                .set(archive)
                .map_err(|_| TransportError::Configuration)?;
        }
        initialize_journal(port.as_ref(), Arc::clone(&journal), physical_identity).await?;
        let operations = Arc::new(RegionalDynamo::try_new(
            Arc::clone(&port),
            Arc::clone(&journal),
            Arc::clone(&codec),
            &configuration,
        )?);
        Ok(Self {
            port,
            journal,
            codec,
            transport,
            operations,
        })
    }
}

impl<P: DynamoPort> RegionalRuntime<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub fn uses_port(&self, port: &Arc<P>) -> bool {
        Arc::ptr_eq(&self.port, port)
    }

    pub fn operations(&self) -> Arc<RegionalDynamo<P>> {
        Arc::clone(&self.operations)
    }

    #[cfg(test)]
    pub(crate) fn fixture_journal(&self) -> Arc<JournalStore> {
        Arc::clone(&self.journal)
    }

    pub(super) fn receiver(&self) -> Result<TcpImportServer<P>, TransportError> {
        self.transport.receiver(
            Arc::clone(&self.port),
            Arc::clone(&self.journal),
            Arc::clone(&self.codec),
        )
    }
}

impl<P: stream_provider::StreamPort> RegionalRuntime<P> {
    pub fn streams(&self) -> Arc<crate::streams::RegionalStreams<P>> {
        Arc::new(crate::streams::RegionalStreams {
            port: Arc::clone(&self.port),
            capture: crate::streams::RegionalAppendCapture {
                journal: Arc::clone(&self.journal),
                codec: Arc::clone(&self.codec),
            },
        })
    }
}
