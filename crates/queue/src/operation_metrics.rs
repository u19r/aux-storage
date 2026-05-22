use std::{future::Future, time::Instant};

use queue_provider::QueueResult;

pub(crate) async fn record_queue_operation<T, Fut>(
    operation: &'static str,
    future: Fut,
) -> QueueResult<T>
where
    Fut: Future<Output = QueueResult<T>>,
{
    let started = Instant::now();

    #[cfg(feature = "opt-loop-profiling")]
    let result = opt_loop_probe::measure_future(future).await;

    #[cfg(not(feature = "opt-loop-profiling"))]
    let result = future.await;

    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    tracing::debug!(operation, elapsed_ms, "queue operation completed");

    #[cfg(feature = "opt-loop-profiling")]
    opt_loop_probe::record_queue_call(operation, 0);

    result
}
