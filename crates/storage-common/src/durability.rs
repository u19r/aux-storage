//! Group durability acknowledgements without sharing customer transactions.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use storage_types::{StorageError, StorageResult};
use tokio::sync::{OnceCell, mpsc, oneshot};

use crate::constants::{DURABILITY_BATCH_CAPACITY, DURABILITY_BATCH_WINDOW_MS};

type Barrier =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send>> + Send + Sync>;
type Acknowledgement = oneshot::Sender<StorageResult<()>>;

/// Register only after native commit, and never while holding the writer slot.
/// The barrier must synchronize every commit preceding its invocation.
pub struct DurabilityBatch {
    pending: OnceCell<mpsc::Sender<Acknowledgement>>,
    barrier: Barrier,
}

impl DurabilityBatch {
    pub fn new<F, Fut>(barrier: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = StorageResult<()>> + Send + 'static,
    {
        Self {
            pending: OnceCell::new(),
            barrier: Arc::new(move || Box::pin(barrier())),
        }
    }

    pub async fn wait(&self) -> StorageResult<()> {
        let pending = self
            .pending
            .get_or_init(|| async {
                let (sender, receiver) = mpsc::channel(DURABILITY_BATCH_CAPACITY);
                tokio::spawn(Self::synchronize_groups(
                    Arc::clone(&self.barrier),
                    receiver,
                ));
                sender
            })
            .await;
        let (answer, result) = oneshot::channel();
        pending
            .send(answer)
            .await
            .map_err(|_| unknown_durability())?;
        result.await.map_err(|_| unknown_durability())?
    }

    async fn synchronize_groups(barrier: Barrier, mut pending: mpsc::Receiver<Acknowledgement>) {
        while let Some(first) = pending.recv().await {
            tokio::time::sleep(Duration::from_millis(DURABILITY_BATCH_WINDOW_MS)).await;
            let mut group = vec![first];
            while group.len() < DURABILITY_BATCH_CAPACITY {
                let Ok(answer) = pending.try_recv() else {
                    break;
                };
                group.push(answer);
            }
            let result = barrier().await;

            for answer in group {
                let _ = answer.send(
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|_| unknown_durability()),
                );
            }
        }
    }
}

fn unknown_durability() -> StorageError {
    StorageError::sync_submission_ambiguous(None, "Native durability acknowledgement is unknown")
}
