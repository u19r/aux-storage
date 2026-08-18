use std::sync::Arc;

use tokio::time::{Duration, timeout};

use super::{
    BackgroundJobName, DatabaseJobKind,
    job_lock::{
        GatedJobLockStore, InMemoryJobLockStore, JobLockAttempt, JobLockStore, JobStartGate,
    },
};

#[tokio::test]
async fn gated_store_waits_for_startup_completion() {
    let gate = JobStartGate::new();
    let store = Arc::new(GatedJobLockStore::new(
        Arc::new(InMemoryJobLockStore::new("test-worker")),
        gate.clone(),
    ));
    let job_id = BackgroundJobName::Database {
        kind: DatabaseJobKind::TtlSweep,
    };
    let mut attempt = tokio::spawn({
        let store = Arc::clone(&store);
        async move { store.try_acquire(job_id, 10, 0).await }
    });

    assert!(
        timeout(Duration::from_millis(20), &mut attempt)
            .await
            .is_err()
    );
    assert!(!gate.is_open());

    gate.open();
    let result = timeout(Duration::from_secs(1), &mut attempt)
        .await
        .expect("gated lock attempt did not resume")
        .expect("gated lock task panicked")
        .expect("in-memory lock failed");
    assert_eq!(result, JobLockAttempt::Acquired { lease_until_ms: 10 });
}
