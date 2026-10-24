use std::sync::Arc;

use futures::future::BoxFuture;
use queue_provider::{QueueMessageCleanupRepository, QueuePort};
use storage_types::{AttemptMode, Clock, StorageResult, TimestampMillis};

use crate::native_admission::{NativeAdmissionBinding, NativeWork};

struct AdmittedQueuePort<P> {
    port: Arc<P>,
    binding: Arc<NativeAdmissionBinding>,
}

pub fn admitted_queue_cleanup<P: QueuePort + 'static>(
    port: Arc<P>,
    binding: Arc<NativeAdmissionBinding>,
) -> Arc<dyn QueueMessageCleanupRepository> {
    Arc::new(AdmittedQueuePort { port, binding })
}

impl<P: QueuePort> Clock for AdmittedQueuePort<P> {
    fn now(&self) -> TimestampMillis {
        self.port.now()
    }

    fn sleep(
        &self,
        duration: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        self.port.sleep(duration)
    }
}

impl<P: QueuePort + 'static> QueuePort for AdmittedQueuePort<P> {
    type Attempt<'native>
        = P::Attempt<'native>
    where Self: 'native;

    async fn with_queue_attempt<R, F>(&self, mode: AttemptMode, operation: F) -> StorageResult<R>
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        let port = Arc::clone(&self.port);
        self.binding
            .settle(NativeWork::Background, move || async move {
                port.with_queue_attempt(mode, operation).await
            })
            .await
    }
}
