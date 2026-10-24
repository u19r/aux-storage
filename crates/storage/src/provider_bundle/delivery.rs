//! Scheduled delivery uses free foreground capacity without queueing ahead of
//! users.
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use futures::future::BoxFuture;
use pubsub_provider::PubsubPort;
use queue_provider::QueuePort;
use storage_provider::native_admission::NativeWork;
use storage_types::{AttemptMode, Clock, StorageResult, TimestampMillis};

use crate::admission::AdmissionController;

pub(crate) struct DeliveryProviders {
    pub(crate) pubsub: Arc<dyn pubsub_provider::PubsubProvider>,
    pub(crate) queue: Arc<dyn queue_provider::QueueOperations>,
}

pub(crate) trait DeliveryProviderSource: Send + Sync {
    fn bind(&self, admission: AdmissionController) -> DeliveryProviders;
}

pub(super) struct NativeDeliverySource<P> {
    port: Arc<P>,
}

struct NativeDeliveryPort<P> {
    port: Arc<P>,
    admission: AdmissionController,
}

impl<P> NativeDeliverySource<P> {
    pub(super) fn new(port: Arc<P>) -> Self {
        Self { port }
    }
}

impl<P: PubsubPort + QueuePort + 'static> DeliveryProviderSource for NativeDeliverySource<P> {
    fn bind(&self, admission: AdmissionController) -> DeliveryProviders {
        let port = Arc::new(NativeDeliveryPort {
            port: Arc::clone(&self.port),
            admission,
        });
        DeliveryProviders {
            pubsub: port.clone(),
            queue: port,
        }
    }
}

impl<P: Clock> Clock for NativeDeliveryPort<P> {
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

impl<P: PubsubPort + 'static> PubsubPort for NativeDeliveryPort<P> {
    type Attempt<'native>
        = P::Attempt<'native>
    where Self: 'native;

    fn with_pubsub_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let port = Arc::clone(&self.port);
        self.admission
            .settle_native(NativeWork::Background, move || async move {
                port.with_pubsub_attempt(mode, operation).await
            })
    }
}

impl<P: QueuePort + 'static> QueuePort for NativeDeliveryPort<P> {
    type Attempt<'native>
        = P::Attempt<'native>
    where Self: 'native;

    fn with_queue_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let port = Arc::clone(&self.port);
        self.admission
            .settle_native(NativeWork::Background, move || async move {
                port.with_queue_attempt(mode, operation).await
            })
    }
}
