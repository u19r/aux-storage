//! Serialized native attempts without a per-attempt blocking task or mutex.

use std::sync::Arc;

use rocksdb::OptimisticTransactionDB;
use storage_types::{StorageError, StorageResult};
use tokio::sync::{mpsc, oneshot};

use super::{attempt::Command, transaction::drive_attempt};
use crate::AttemptMode;

pub(super) struct WriteAttempt {
    pub database: Arc<OptimisticTransactionDB>,
    pub mode: AttemptMode,
    pub settlement: Arc<storage_types::NativeAttemptSettlement>,
    pub commands: mpsc::UnboundedReceiver<Command>,
    pub settled: oneshot::Sender<StorageResult<()>>,
}

pub(super) struct RocksDbWriter {
    sender: Option<mpsc::UnboundedSender<WriteAttempt>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl RocksDbWriter {
    pub(super) fn try_new() -> StorageResult<Arc<Self>> {
        let (sender, mut attempts) = mpsc::unbounded_channel::<WriteAttempt>();
        let worker = std::thread::Builder::new()
            .name("rocksdb-writer".into())
            .spawn(move || {
                while let Some(attempt) = attempts.blocking_recv() {
                    let result = drive_attempt(
                        attempt.database,
                        attempt.mode,
                        attempt.commands,
                        attempt.settlement,
                    );
                    let _ = attempt.settled.send(result);
                }
            })
            .map_err(|_| StorageError::internal("start RocksDB writer failed"))?;
        Ok(Arc::new(Self {
            sender: Some(sender),
            worker: Some(worker),
        }))
    }

    pub(super) fn submit_attempt(&self, attempt: WriteAttempt) -> StorageResult<()> {
        self.sender
            .as_ref()
            .ok_or_else(|| StorageError::internal("RocksDB writer stopped"))?
            .send(attempt)
            .map_err(|_| StorageError::internal("RocksDB writer stopped"))
    }
}

impl Drop for RocksDbWriter {
    fn drop(&mut self) {
        // Attempt supervisors retain this owner until their command channel
        // closes. Join native cleanup before process exit can destroy RocksDB's
        // global scheduler.
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
