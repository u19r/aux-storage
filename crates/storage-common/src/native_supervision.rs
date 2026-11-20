//! Detached ownership of native execution, including panic propagation.

use std::{future::Future, sync::Arc};

use futures::future::{Either, select};
use storage_types::{AttemptPhase, DeadlineClass, StorageResult};
#[cfg(feature = "fault-injection")]
use tracing::{Instrument as _, instrument::WithSubscriber as _};

use crate::{native_attempt_lifecycle::AttemptLifecycle, native_observer::NativeObserver};

/// Labels of `storage.native.attempt.deadline.total`: the provider and the
/// deadline class of one native attempt. Never a table, key or payload.
#[derive(Clone, Copy, Debug)]
pub struct AttemptLabel {
    pub provider: &'static str,
    pub class: DeadlineClass,
}

const fn class_label(class: DeadlineClass) -> &'static str {
    match class {
        DeadlineClass::PointRead => "point_read",
        DeadlineClass::RangeRead => "range_read",
        DeadlineClass::Write => "write",
        DeadlineClass::Transaction => "transaction",
        DeadlineClass::SchemaChange => "schema_change",
        DeadlineClass::BackgroundPage => "background_page",
        DeadlineClass::LongPollProbe => "long_poll_probe",
    }
}

const fn phase_label(phase: AttemptPhase) -> &'static str {
    match phase {
        AttemptPhase::Acquire => "acquire",
        AttemptPhase::Queued => "queued",
        AttemptPhase::Begin => "begin",
        AttemptPhase::Statement => "statement",
        AttemptPhase::Rollback => "rollback",
        AttemptPhase::Settle => "settle",
    }
}

/// Counts an attempt that settled with a deadline error. This is the one place
/// that does, because every provider settles through `supervise_attempt`.
pub(crate) fn record_attempt_deadline<R>(label: AttemptLabel, result: &StorageResult<R>) {
    if let Err(error) = result
        && let Some(phase) = error.deadline_phase()
    {
        metrics::counter!(
            "storage.native.attempt.deadline.total",
            "provider" => label.provider,
            "class" => class_label(label.class),
            "phase" => phase_label(phase),
        )
        .increment(1);
    }
}

/// Dropping the response waiter does not stop the task settling native work.
/// The supervisor creates the attempt's lifecycle and lends it to `attempt`.
/// `observer` is `None` in production; a backend's fault-injection installer
/// supplies it.
pub async fn supervise_attempt<R, A>(
    label: AttemptLabel,
    observer: Option<Arc<dyn NativeObserver>>,
    attempt: impl FnOnce(Arc<AttemptLifecycle>) -> A,
    stopped_message: &'static str,
) -> StorageResult<R>
where
    R: Send + 'static,
    A: Future<Output = StorageResult<R>> + Send + 'static,
{
    let lifecycle = Arc::new(AttemptLifecycle::observed(observer));
    let attempt = attempt(Arc::clone(&lifecycle));
    // Counting inside the detached task keeps the count when the waiter is
    // dropped.
    let attempt = metrics_facade::inherit_request_cost(async move {
        let result = attempt.await;
        record_attempt_deadline(label, &result);
        result
    });
    #[cfg(feature = "fault-injection")]
    let attempt = attempt
        .instrument(tracing::Span::current())
        .with_subscriber(tracing::dispatcher::get_default(Clone::clone));
    match tokio::spawn(attempt).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // A stopped task: known until the commit was submitted, unknown after.
        // The lifecycle is abandoned first, so a worker cannot commit later.
        Err(_) => Err(lifecycle.stopped_error(stopped_message)),
    }
}

/// Waits for `work` until `expired` completes. Past it, `work` runs on
/// detached so that it keeps what it owns until it ends, and the caller gets
/// `None`.
pub async fn finish_or_detach<R: Send + 'static>(
    expired: impl Future<Output = ()>,
    work: impl Future<Output = R> + Send + 'static,
) -> Option<R> {
    match select(Box::pin(work), std::pin::pin!(expired)).await {
        Either::Left((value, _)) => Some(value),
        Either::Right(((), work)) => {
            tokio::spawn(work);
            None
        }
    }
}
