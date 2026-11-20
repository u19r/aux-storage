//! Retry clocks hand delays to the simulator-owned workload future. Native
//! operations keep their normal retry policy without a Tokio reactor.

use std::{
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

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
        // The simulation ambient source makes this simulated time, the same
        // time that storage code without an injected clock reads.
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

/// The current phase clock as the attempt clock of a simulation backend.
pub(crate) fn phase_clock(clock: Option<&Arc<SimulationClock>>) -> Result<Arc<dyn Clock>, String> {
    clock
        .map(|clock| Arc::clone(clock) as Arc<dyn Clock>)
        .ok_or_else(|| "simulation phase clock is absent".to_owned())
}

pub(crate) async fn drive_operation(
    context: Rc<WorkloadContext>,
    mut requests: mpsc::UnboundedReceiver<DelayRequest>,
    operation: impl Future<Output = ()>,
) {
    let mut operation = std::pin::pin!(operation);
    let mut pending: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>> =
        FuturesUnordered::new();
    futures::future::poll_fn(|outer| {
        loop {
            // `foundationdb-simulation` polls woken tasks from inside each
            // native future callback. A callback that fires while this task
            // is already being polled finds the task slot empty and drops the
            // wake. Record every wake of this pass and poll again instead.
            let flag = Arc::new(WakeFlag {
                woken: AtomicBool::new(false),
                outer: outer.waker().clone(),
            });
            let waker = Waker::from(Arc::clone(&flag));
            let waker = &mut Context::from_waker(&waker);
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
            if !advanced && !flag.woken.load(Ordering::Acquire) {
                return Poll::Pending;
            }
        }
    })
    .await;
}

/// Wakes the simulator task and records that a wake happened.
struct WakeFlag {
    woken: AtomicBool,
    outer: Waker,
}

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::Release);
        self.outer.wake_by_ref();
    }
}
