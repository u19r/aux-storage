use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use storage_types::{
    AttemptDeadlines, AttemptPhase, DeadlineClass, NativeUnavailable, StorageError, StorageResult,
};

use crate::{
    native_attempt_lifecycle::{AttemptLifecycle, LifecycleState},
    native_observer::NativeObserver,
    native_observer_tests::RecordingObserver,
    native_supervision::{AttemptLabel, record_attempt_deadline, supervise_attempt},
};

#[derive(Default)]
struct Counts(Mutex<BTreeMap<String, u64>>);

struct Cell(Arc<Counts>, String);

impl CounterFn for Cell {
    fn increment(&self, value: u64) {
        *self.0.0.lock().unwrap().entry(self.1.clone()).or_default() += value;
    }
    fn absolute(&self, _: u64) {}
}

struct CountingRecorder(Arc<Counts>);

impl Recorder for CountingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let labels: Vec<String> = key
            .labels()
            .map(|l| format!("{}={}", l.key(), l.value()))
            .collect();
        Counter::from_arc(Arc::new(Cell(
            Arc::clone(&self.0),
            format!("{}[{}]", key.name(), labels.join(",")),
        )))
    }
    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn deadline(phase: AttemptPhase) -> StorageResult<()> {
    Err(StorageError::from(
        storage_types::AttemptFailure::Unavailable(NativeUnavailable::Deadline(phase)),
    ))
}

#[test]
fn given_attempt_results_when_recorded_then_only_deadline_errors_are_counted_by_provider_class_and_phase()
 {
    let counts = Arc::new(Counts::default());
    let label = AttemptLabel {
        provider: "sqlite",
        class: DeadlineClass::Write,
    };
    metrics::with_local_recorder(&CountingRecorder(Arc::clone(&counts)), || {
        record_attempt_deadline(label, &deadline(AttemptPhase::Statement));
        record_attempt_deadline(label, &deadline(AttemptPhase::Statement));
        record_attempt_deadline(label, &deadline(AttemptPhase::Settle));
        record_attempt_deadline(label, &Ok(()));
        record_attempt_deadline(
            label,
            &Err::<(), _>(StorageError::from(
                storage_types::AttemptFailure::Unavailable(NativeUnavailable::WriterQueueFull),
            )),
        );
    });
    let counts = counts.0.lock().unwrap().clone();
    let name = "storage.native.attempt.deadline.total";
    assert_eq!(
        counts,
        BTreeMap::from([
            (
                format!("{name}[provider=sqlite,class=write,phase=statement]"),
                2
            ),
            (
                format!("{name}[provider=sqlite,class=write,phase=settle]"),
                1
            ),
        ])
    );
}

/// Drives a lifecycle to `state` as a driver would.
pub(crate) fn drive_to(lifecycle: &AttemptLifecycle, state: LifecycleState) {
    if state == LifecycleState::Queued {
        return;
    }
    assert!(lifecycle.open());
    if state == LifecycleState::Open {
        return;
    }
    let deadline = AttemptDeadlines::default().start(DeadlineClass::Write);
    let ticket = lifecycle.submit_commit(&deadline).unwrap();
    match state {
        LifecycleState::CommitSubmitted => drop(ticket),
        LifecycleState::Committed => ticket.settle(Ok(())).unwrap(),
        LifecycleState::Aborted => {
            let _ = ticket.settle::<()>(Err(StorageError::internal("aborted").into()));
        }
        other => unreachable!("{other:?}"),
    }
}

#[test]
fn given_attempt_task_cancelled_when_supervised_then_lifecycle_decides_known_or_unknown() {
    let label = AttemptLabel {
        provider: "sqlite",
        class: DeadlineClass::Write,
    };
    // (state at the stop, unknown outcome, state after the stop)
    for (state, unknown, after) in [
        (LifecycleState::Queued, false, LifecycleState::Abandoned),
        (LifecycleState::Open, false, LifecycleState::Abandoned),
        (
            LifecycleState::CommitSubmitted,
            true,
            LifecycleState::CommitSubmitted,
        ),
        (LifecycleState::Committed, true, LifecycleState::Committed),
        (LifecycleState::Aborted, false, LifecycleState::Aborted),
    ] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        let (reached, wait) = std::sync::mpsc::channel();
        let (lent, lifecycle) = std::sync::mpsc::channel();
        // Shutting the runtime down cancels the detached attempt task while
        // this waiter runs outside the runtime.
        let stopper = std::thread::spawn(move || {
            wait.recv().unwrap();
            runtime.shutdown_background();
        });
        let result = {
            let _context = handle.enter();
            futures::executor::block_on(supervise_attempt(
                label,
                None,
                move |lifecycle: Arc<AttemptLifecycle>| async move {
                    drive_to(&lifecycle, state);
                    lent.send(lifecycle).unwrap();
                    reached.send(()).unwrap();
                    std::future::pending::<StorageResult<()>>().await
                },
                "attempt task stopped",
            ))
        };
        stopper.join().unwrap();
        let error = result.unwrap_err();
        assert_eq!(error.is_commit_unknown(), unknown, "{state:?}");
        assert_eq!(lifecycle.recv().unwrap().state(), after, "{state:?}");
    }
}

#[tokio::test]
async fn supervisor_lends_the_observer_to_the_attempt() {
    let label = AttemptLabel {
        provider: "sqlite",
        class: DeadlineClass::Write,
    };
    let observer: Arc<dyn NativeObserver> = Arc::new(RecordingObserver::new(false));
    let observed = supervise_attempt(
        label,
        Some(observer),
        |lifecycle| async move { Ok(lifecycle.observer().is_some()) },
        "stopped",
    )
    .await;
    assert!(observed.expect("attempt ends"));
    let unobserved = supervise_attempt(
        label,
        None,
        |lifecycle| async move { Ok(lifecycle.observer().is_some()) },
        "stopped",
    )
    .await;
    assert!(!unobserved.expect("attempt ends"));
}
