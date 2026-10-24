use std::{panic::AssertUnwindSafe, sync::Arc};

use futures::FutureExt;
use lru_ttl_cache::RefreshQueue;
use tokio::{sync::Semaphore, task::JoinHandle};

use super::{
    PointReadClient, PointReadReservation,
    constants::{POINT_READ_BACKGROUND_COLLECTION, POINT_READ_WORKERS},
    executor::BatchExecutor,
    source::SourceKey,
};

pub(super) struct SourceWorkers {
    tasks: Vec<JoinHandle<()>>,
}

impl SourceWorkers {
    pub(super) fn start(
        db: Arc<dyn PointReadClient>,
        queue: RefreshQueue<SourceKey>,
        budget: Arc<Semaphore>,
    ) -> Self {
        let tasks = (0..POINT_READ_WORKERS)
            .map(|_| {
                let db = Arc::clone(&db);
                let queue = queue.clone();
                let budget = Arc::clone(&budget);
                tokio::spawn(Self::drain_queue(db, queue, budget))
            })
            .collect();
        Self { tasks }
    }

    async fn drain_queue(
        db: Arc<dyn PointReadClient>,
        queue: RefreshQueue<SourceKey>,
        budget: Arc<Semaphore>,
    ) {
        while let Some(batch) = queue.next_batch(POINT_READ_BACKGROUND_COLLECTION).await {
            let batch = Arc::new(batch);
            let sources: Vec<_> = batch
                .jobs()
                .iter()
                .filter(|job| job.is_current())
                .map(|job| job.ticket().key().clone())
                .collect();
            let executor = BatchExecutor::new(
                db.as_ref(),
                &budget,
                PointReadReservation::new(Arc::clone(&batch)),
            );
            // Detached native work retains its own reservation after this
            // wrapper is cancelled. Recover panics without losing a worker.
            let _ = AssertUnwindSafe(async {
                tokio::select! {
                    _ = batch.cancelled() => {},
                    _ = executor.execute(&sources) => {},
                }
            })
            .catch_unwind()
            .await;
            for source in &sources {
                source.publish_result(|| Err(crate::types::StorageError::service_unavailable(1)));
            }
            drop(sources);
            drop(batch);
        }
    }
}

impl Drop for SourceWorkers {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}
