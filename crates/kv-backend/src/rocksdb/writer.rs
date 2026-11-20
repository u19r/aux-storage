//! Serialized native attempts without a per-attempt blocking task or mutex.

use std::sync::Arc;

use rocksdb::OptimisticTransactionDB;
use storage_common::writer_occupancy::{
    ForeignAwaitPolicy, QueuedWriter, SerializedWriter, WriterOccupancy,
};
use storage_types::{AttemptPhase, StorageError, StorageResult, expired_in};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::{
    attempt::Command,
    transaction::{NativeStart, drive_attempt},
};
use crate::{AttemptMode, native_deadline::NativeDeadline};

pub(super) struct WriteAttempt {
    pub database: Arc<OptimisticTransactionDB>,
    pub mode: AttemptMode,
    pub settlement: Arc<storage_types::NativeAttemptSettlement>,
    pub commands: mpsc::UnboundedReceiver<Command>,
    pub start: NativeStart,
    pub settled: oneshot::Sender<StorageResult<()>>,
    /// Released after the writer ends this attempt, or drops it unbegun.
    pub slot: OwnedSemaphorePermit,
    /// Leaves the writer queue when the writer starts this attempt.
    pub queued: QueuedWriter,
}

pub(super) struct RocksDbWriter {
    /// Bounds queued and running write attempts together.
    slots: Arc<Semaphore>,
    pub(super) capacity: usize,
    pub(super) occupancy: Arc<WriterOccupancy>,
    sender: Option<mpsc::UnboundedSender<WriteAttempt>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl RocksDbWriter {
    pub(super) fn try_new(capacity: usize, policy: ForeignAwaitPolicy) -> StorageResult<Arc<Self>> {
        let (sender, mut attempts) = mpsc::unbounded_channel::<WriteAttempt>();
        let worker = std::thread::Builder::new()
            .name("rocksdb-writer".into())
            .spawn(move || {
                while let Some(attempt) = attempts.blocking_recv() {
                    let WriteAttempt {
                        database,
                        mode,
                        settlement,
                        commands,
                        start,
                        settled,
                        slot,
                        queued,
                    } = attempt;
                    let hold = queued.start();
                    let result =
                        drive_attempt(database, mode, commands, settlement, start, Some(&hold));
                    // The hold ends before the reply: the next attempt's
                    // hold cannot overlap this one in the counters.
                    drop(hold);
                    let _ = settled.send(result);
                    drop(slot);
                }
            })
            .map_err(|_| StorageError::internal("start RocksDB writer failed"))?;
        Ok(Arc::new(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            occupancy: Arc::new(WriterOccupancy::new(SerializedWriter::RocksDb, policy)),
            sender: Some(sender),
            worker: Some(worker),
        }))
    }

    /// Waits for a writer slot until the attempt deadline. Expiry is a known
    /// rejection before any native work. The attempt is in the writer queue
    /// from here until the writer starts it.
    pub(super) async fn reserve(
        &self,
        deadline: &NativeDeadline,
    ) -> StorageResult<(OwnedSemaphorePermit, QueuedWriter)> {
        let queued = self.occupancy.enqueue(Arc::clone(&deadline.clock));
        let acquire = Arc::clone(&self.slots).acquire_owned();
        match futures::future::select(
            Box::pin(acquire),
            std::pin::pin!(deadline.deadline.expired(deadline.clock.as_ref())),
        )
        .await
        {
            futures::future::Either::Left((slot, _)) => slot
                .map(|slot| (slot, queued))
                .map_err(|_| StorageError::internal("RocksDB writer stopped")),
            futures::future::Either::Right(_) => Err(expired_in(AttemptPhase::Acquire)),
        }
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
