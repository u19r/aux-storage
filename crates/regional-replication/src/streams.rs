use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use futures::future::BoxFuture;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::CatalogAttempt;
use storage_types::{AttemptMode, Clock, StorageResult, TimestampMillis};
use stream_provider::{
    StreamAttempt, StreamItemIdAllocation, StreamPartitioningSupport, StreamPort,
    TableAppendCapture, UserStreamAppend,
};

use crate::journal::JournalStore;

/// Shared ordinary stream execution with native table-history capture.
pub struct RegionalStreams<P> {
    pub(crate) port: Arc<P>,
    pub(crate) capture: RegionalAppendCapture,
}

#[derive(Clone)]
pub struct RegionalAppendCapture {
    pub(crate) journal: Arc<JournalStore>,
    pub(crate) codec: Arc<CanonicalJsonCodec>,
}

impl<A: StreamAttempt + stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt>
    TableAppendCapture<A> for RegionalAppendCapture
{
    type Captured = crate::journal::CapturedTableAppend;

    async fn capture(
        &self,
        attempt: &mut A,
        request: &stream_provider::TableAppendRequest,
        append: &UserStreamAppend,
    ) -> StorageResult<Option<Self::Captured>> {
        self.journal
            .capture_table_append(attempt, request, (append, &self.codec))
            .await
    }

    async fn store_response(
        &self,
        attempt: &mut A,
        captured: Self::Captured,
        response: stream_provider::StreamAppendAllocation,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            captured.store_response(attempt, response).await?;
            self.journal.require_acceptance_capacity(attempt).await
        }
        .await;
        settlement.complete(result)
    }

    async fn reconcile(
        &self,
        attempt: &mut A,
        request: &stream_provider::TableAppendRequest,
    ) -> StorageResult<stream_provider::TableAppendReconciliation> {
        self.journal
            .reconcile_table_append(attempt, request, &self.codec)
            .await
    }
}

impl<P: Clock> Clock for RegionalStreams<P> {
    fn now(&self) -> TimestampMillis {
        self.port.now()
    }

    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        self.port.sleep(duration)
    }
}

impl<P: StreamPort + 'static> StreamPort for RegionalStreams<P>
where for<'native> P::Attempt<'native>:
        stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt
{
    type Attempt<'native> = P::Attempt<'native>;
    type AppendCapture = RegionalAppendCapture;

    fn table_append_capture(&self) -> Self::AppendCapture {
        self.capture.clone()
    }

    fn partitioning_support(&self) -> StreamPartitioningSupport {
        self.port.partitioning_support()
    }

    fn item_id_allocation(&self) -> StreamItemIdAllocation {
        self.port.item_id_allocation()
    }

    async fn with_stream_attempt<R, F>(&self, mode: AttemptMode, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.capture.journal.native_admission.require()?;
        self.port.with_stream_attempt(mode, operation).await
    }

    async fn with_topology_attempt<R, F>(&self, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.capture.journal.native_admission.require()?;
        self.port.with_topology_attempt(operation).await
    }

    async fn with_committed_stream_attempt<R, F>(
        &self,
        operation: F,
    ) -> StorageResult<(R, Option<[u8; 10]>)>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.capture.journal.native_admission.require()?;
        self.port.with_committed_stream_attempt(operation).await
    }
}
