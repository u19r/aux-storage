use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use scheduled_bg_jobs::JobCatalog;
use storage_provider::DynamoPort;
use tokio::{
    net::TcpListener,
    sync::watch,
    task::{AbortHandle, JoinHandle},
};

use super::node::RegionalRuntime;
use crate::transport::{TcpImportServer, TcpTransport, TransportError};

/// Bound intake and registered shared jobs precede publication by the host.
pub struct BoundRegionalServices<P> {
    runtime: RegionalRuntime<P>,
    listener: TcpListener,
    receiver: TcpImportServer<P>,
    catalog: JobCatalog,
}

/// A bound receiver erased only after native provider qualification.
pub struct PreparedRegionalServices {
    journal: std::sync::Arc<crate::journal::JournalStore>,
    history: std::sync::Arc<dyn crate::history::read::CanonicalHistoryOperations>,
    transport: TcpTransport,
    stop: watch::Sender<bool>,
    receiver:
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), TransportError>> + Send>>,
}

/// Exact native binding has succeeded; intake is still unstarted.
pub struct AdmittedRegionalServices {
    prepared: PreparedRegionalServices,
    native_admission: std::sync::Arc<dyn storage_provider::native_admission::NativeAdmission>,
}

/// Receiver health and cancellation-independent settlement owned by the host.
pub struct RegionalServices {
    history: std::sync::Arc<dyn crate::history::read::CanonicalHistoryOperations>,
    stop: watch::Sender<bool>,
    receiver_status: AbortHandle,
    settlement: JoinHandle<Result<(), TransportError>>,
}

impl<P: DynamoPort + 'static> RegionalRuntime<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub async fn bind_services(self) -> Result<BoundRegionalServices<P>, TransportError> {
        let receiver = self.receiver()?;
        let endpoint = self.transport.configuration().as_ref().as_ref().endpoint;
        let listener = TcpListener::bind(endpoint).await?;
        let mut catalog = JobCatalog::new();
        self.register_jobs(&mut catalog)?;
        Ok(BoundRegionalServices {
            runtime: self,
            listener,
            receiver,
            catalog,
        })
    }
}

impl<P> BoundRegionalServices<P> {
    pub fn runtime(&self) -> &RegionalRuntime<P> {
        &self.runtime
    }

    /// Extend this exact registration with ordinary jobs in the host catalog.
    pub fn job_catalog(&self) -> JobCatalog {
        self.catalog.clone()
    }
}

impl<P: DynamoPort + 'static> BoundRegionalServices<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
        + storage_provider::PrimaryStateScanAttempt
        + storage_provider::ItemTombstoneScanAttempt
        + storage_provider::GsiProjectionScanAttempt
        + storage_provider::TtlProjectionScanAttempt
        + storage_provider::GsiProjectionMetadataAttempt
{
    pub fn prepare(self) -> PreparedRegionalServices {
        let (stop, stopped) = watch::channel(false);
        let receiver = Box::pin(self.receiver.serve(self.listener, stopped));
        let history = std::sync::Arc::new(crate::history::read::NativeHistoryReader {
            port: std::sync::Arc::clone(&self.runtime.port),
            journal: std::sync::Arc::clone(&self.runtime.journal),
            codec: std::sync::Arc::clone(&self.runtime.codec),
            transport: self.runtime.transport.clone(),
        });
        PreparedRegionalServices {
            journal: std::sync::Arc::clone(&self.runtime.journal),
            history,
            transport: self.runtime.transport.clone(),
            stop,
            receiver,
        }
    }
}

impl PreparedRegionalServices {
    /// Check host scheduling before its job-store bootstrap or worker
    /// publication.
    pub fn require_recovery_reserve(
        &self,
        configured: Option<std::num::NonZeroUsize>,
    ) -> storage_types::StorageResult<()> {
        let required = self
            .transport
            .configuration()
            .native_budgets()
            .reserved_attempts;
        if configured.is_none_or(|configured| configured < required) {
            return Err(storage_types::StorageError::validation(
                "regional recovery requires sufficient enabled host reserve",
            ));
        }
        Ok(())
    }

    pub fn bind_native_admission(
        self,
        connection_id: &str,
        admission: std::sync::Arc<dyn storage_provider::native_admission::NativeAdmission>,
    ) -> storage_types::StorageResult<AdmittedRegionalServices> {
        if connection_id != admission.connection_id() {
            return Err(storage_types::StorageError::validation(
                "regional native admission belongs to a different host connection",
            ));
        }
        admission.require_reserved_attempts(
            self.transport
                .configuration()
                .native_budgets()
                .reserved_attempts,
        )?;
        self.journal
            .native_admission
            .bind(std::sync::Arc::clone(&admission))?;
        Ok(AdmittedRegionalServices {
            prepared: self,
            native_admission: admission,
        })
    }
}

impl AdmittedRegionalServices {
    pub fn start(self) -> RegionalServices {
        let prepared = self.prepared;
        let receiver = tokio::spawn(prepared.receiver);
        let receiver_status = receiver.abort_handle();
        let settlement = tokio::spawn(async move {
            let received = receiver.await.map_err(|_| TransportError::Worker);
            let sources = prepared.transport.drain_sources().await;
            let native = self
                .native_admission
                .drain_native()
                .await
                .map_err(TransportError::Native);
            received??;
            sources?;
            native
        });
        RegionalServices {
            history: prepared.history,
            stop: prepared.stop,
            receiver_status,
            settlement,
        }
    }
}

impl RegionalServices {
    pub fn history(&self) -> std::sync::Arc<dyn crate::history::read::CanonicalHistoryOperations> {
        std::sync::Arc::clone(&self.history)
    }

    pub fn is_running(&self) -> bool {
        !self.receiver_status.is_finished()
    }

    pub fn request_shutdown(&self) {
        self.stop.send_replace(true);
    }

    /// Stop socket intake, preserving accepted native attempts and their
    /// replies. The host first stops public mutation intake and its shared
    /// job driver.
    pub async fn shutdown(self) -> Result<(), TransportError> {
        self.request_shutdown();
        self.wait().await
    }

    pub async fn wait(mut self) -> Result<(), TransportError> {
        (&mut self.settlement)
            .await
            .map_err(|_| TransportError::Worker)?
    }
}

impl Drop for RegionalServices {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}
