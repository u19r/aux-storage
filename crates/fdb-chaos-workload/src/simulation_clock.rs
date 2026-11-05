//! Retry clocks hand delays to the simulator-owned workload future. Native
//! operations keep their normal retry policy without a Tokio reactor.

use std::{future::Future, pin::Pin, rc::Rc, sync::Arc, task::Poll, time::Duration};

use foundationdb_simulation::WorkloadContext;
use futures::{
    channel::{mpsc, oneshot},
    stream::{FuturesUnordered, StreamExt},
};
use storage_types::{Clock, StorageError, StorageResult, TimestampMillis};

pub(crate) enum SimulationPhase {
    Setup,
    Start,
    Check,
}

pub(crate) struct SimulationClock {
    requests: mpsc::UnboundedSender<DelayRequest>,
}

pub(crate) struct DelayRequest {
    duration: Duration,
    completion: oneshot::Sender<StorageResult<()>>,
}

impl DelayRequest {
    fn schedule(self, context: &WorkloadContext) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let delay = context.delay(self.duration);
        Box::pin(async move {
            let result = delay
                .await
                .map_err(|_| StorageError::internal("simulation delay failed"));
            let _ = self.completion.send(result);
        })
    }
}

impl SimulationClock {
    pub(crate) fn new() -> (Arc<Self>, mpsc::UnboundedReceiver<DelayRequest>) {
        let (requests, receiver) = mpsc::unbounded();
        (Arc::new(Self { requests }), receiver)
    }
}

impl Clock for SimulationClock {
    fn now(&self) -> TimestampMillis {
        // Persisted event timestamps retain the workload's existing wall-time
        // contract. Only asynchronous retry waits use simulated time.
        TimestampMillis::now()
    }

    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async move {
            let (completion, result) = oneshot::channel();
            self.requests
                .unbounded_send(DelayRequest {
                    duration,
                    completion,
                })
                .map_err(|_| StorageError::internal("simulation clock owner stopped"))?;
            result
                .await
                .map_err(|_| StorageError::internal("simulation delay stopped"))?
        })
    }
}

pub(crate) async fn drive_operation(
    context: Rc<WorkloadContext>,
    mut requests: mpsc::UnboundedReceiver<DelayRequest>,
    operation: impl Future<Output = ()>,
) {
    let mut operation = std::pin::pin!(operation);
    let mut pending: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> =
        FuturesUnordered::new();
    futures::future::poll_fn(|waker| {
        loop {
            if operation.as_mut().poll(waker).is_ready() {
                return Poll::Ready(());
            }
            let mut advanced = false;
            while let Poll::Ready(Some(request)) = requests.poll_next_unpin(waker) {
                pending.push(request.schedule(&context));
                advanced = true;
            }
            while let Poll::Ready(Some(())) = pending.poll_next_unpin(waker) {
                advanced = true;
            }
            // Deliver requests and completions produced during this poll before
            // yielding. The FDB callback owner must have a native delay armed;
            // a channel wake alone cannot supply that simulator event.
            if !advanced {
                return Poll::Pending;
            }
        }
    })
    .await;
}
