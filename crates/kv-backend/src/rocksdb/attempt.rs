use std::sync::{Arc, Mutex};

use storage_types::{StorageError, StorageResult};
use tokio::sync::{mpsc, oneshot};

use crate::{
    AttemptMode, KvAttempt, KvMutation, KvRange, KvRangePage, KvVersionstampedMutation,
    ReadProtection,
};

// RocksDB's synchronous transaction borrows its DB on a blocking worker, so
// this channel is its async bridge. FoundationDB owns an async transaction and
// implements the same KvAttempt trait without a command channel.
pub(super) enum Command {
    HasKeyInRange {
        begin: Vec<u8>,
        end: Vec<u8>,
        protection: ReadProtection,
        answer: oneshot::Sender<StorageResult<bool>>,
    },
    GetBatch {
        keys: Vec<Vec<u8>>,
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
        answer: oneshot::Sender<StorageResult<Vec<Option<Vec<u8>>>>>,
    },
    ScanBatch {
        ranges: Vec<KvRange>,
        protection: ReadProtection,
        answer: oneshot::Sender<StorageResult<Vec<KvRangePage>>>,
    },
    ApplyBatch {
        changes: Vec<KvMutation>,
        answer: oneshot::Sender<StorageResult<()>>,
    },
    Finish {
        commit: bool,
        changes: Vec<KvMutation>,
        answer: oneshot::Sender<StorageResult<()>>,
        #[cfg(feature = "fault-injection")]
        probe: Option<(tokio::runtime::Handle, Arc<dyn crate::KvFaultProbe>)>,
    },
}

/// Async handle to a native transaction held only by the blocking worker.
pub struct RocksDbAttempt {
    settlement: Arc<storage_types::NativeAttemptSettlement>,
    ownership: stream_provider::retained_image::RetainedOwnershipChanges,
    sender: mpsc::UnboundedSender<Command>,
    mode: AttemptMode,
    // Reads flush this buffer first so they observe preceding writes.
    pending: Mutex<Vec<KvMutation>>,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
}

impl RocksDbAttempt {
    pub(super) fn new(
        sender: mpsc::UnboundedSender<Command>,
        mode: AttemptMode,
        settlement: Arc<storage_types::NativeAttemptSettlement>,
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::KvFaultProbe>>,
    ) -> Self {
        Self {
            ownership: Default::default(),
            settlement,
            sender,
            mode,
            pending: Mutex::new(Vec::new()),
            #[cfg(feature = "fault-injection")]
            fault_probe,
        }
    }

    pub(super) async fn finish(&mut self, commit: bool) -> StorageResult<()> {
        let (changes, flush) = if commit {
            match self
                .settlement
                .require_open()
                .and_then(|()| self.take_pending())
            {
                Ok(changes) => (changes, Ok(())),
                Err(error) => (Vec::new(), Err(error)),
            }
        } else {
            (Vec::new(), Ok(()))
        };
        let (answer, reply) = oneshot::channel();
        self.sender
            .send(Command::Finish {
                commit: commit && flush.is_ok(),
                changes,
                answer,
                #[cfg(feature = "fault-injection")]
                probe: self
                    .fault_probe
                    .as_ref()
                    .map(|probe| (tokio::runtime::Handle::current(), Arc::clone(probe))),
            })
            .map_err(|_| StorageError::internal("RocksDB attempt ended before settlement"))?;
        let settled = reply.await.map_err(|_| {
            StorageError::sync_submission_ambiguous(None, "RocksDB settlement was not acknowledged")
        })?;
        settled?;
        flush
    }

    fn take_pending(&self) -> StorageResult<Vec<KvMutation>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StorageError::internal("RocksDB pending writes were poisoned"))?;
        Ok(std::mem::take(&mut *pending))
    }

    fn dispatch_pending(&self) -> StorageResult<Option<oneshot::Receiver<StorageResult<()>>>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StorageError::internal("RocksDB pending writes were poisoned"))?;
        if pending.is_empty() {
            return Ok(None);
        }
        self.settlement.require_open()?;
        let changes = std::mem::take(&mut *pending);
        let (answer, reply) = oneshot::channel();
        self.sender
            .send(Command::ApplyBatch { changes, answer })
            .map_err(|_| StorageError::internal("RocksDB write worker stopped"))?;
        Ok(Some(reply))
    }

    async fn flush_pending(&self) -> StorageResult<()> {
        let Some(reply) = self.dispatch_pending()? else {
            return Ok(());
        };
        reply
            .await
            .map_err(|_| StorageError::internal("RocksDB write was not answered"))?
    }
}

impl KvAttempt for RocksDbAttempt {
    fn settlement(&self) -> &storage_types::NativeAttemptSettlement {
        &self.settlement
    }

    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges {
        &mut self.ownership
    }

    async fn native_has_key_in_range(
        &self,
        begin: &[u8],
        end: &[u8],
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_data(
                begin
                    .len()
                    .checked_add(end.len())
                    .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?,
            )?;
            let begin = crate::backend::copy_native_key(begin)?;
            let end = crate::backend::copy_native_key(end)?;
            self.flush_pending().await?;
            let (answer, reply) = oneshot::channel();
            self.sender
                .send(Command::HasKeyInRange {
                    begin,
                    end,
                    protection,
                    answer,
                })
                .map_err(|_| StorageError::internal("RocksDB range worker stopped"))?;
            reply
                .await
                .map_err(|_| StorageError::internal("RocksDB range was not answered"))?
        }
        .await;
        call.complete(result)
    }

    fn backend_engine(&self) -> storage_common::diagnostics::NativeEngine {
        storage_common::diagnostics::NativeEngine::RocksDb
    }

    #[cfg(feature = "fault-injection")]
    fn fault_probe(&self) -> Option<&dyn crate::KvFaultProbe> {
        self.fault_probe.as_deref()
    }

    fn native_value_limit(&self) -> usize {
        usize::MAX
    }

    async fn native_get_batch(
        &self,
        keys: &[Vec<u8>],
        protection: ReadProtection,
        allowance: crate::KvValueAllowance,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            crate::work_admission::NativeReadWork::from(&*self.settlement).charge_keys(keys)?;
            let keys = crate::backend::copy_native_keys(keys)?;
            self.flush_pending().await?;
            let (answer, reply) = oneshot::channel();
            self.sender
                .send(Command::GetBatch {
                    keys,
                    protection,
                    allowance,
                    answer,
                })
                .map_err(|_| StorageError::internal("RocksDB read worker stopped"))?;
            reply
                .await
                .map_err(|_| StorageError::internal("RocksDB read was not answered"))?
        }
        .await;
        let result = result.and_then(|values| {
            crate::work_admission::NativeReadWork::from(&*self.settlement)
                .charge_values(&values)?;
            Ok(values)
        });
        call.complete(result)
    }

    async fn native_scan(
        &self,
        range: KvRange,
        protection: ReadProtection,
    ) -> StorageResult<KvRangePage> {
        let call = self.settlement.begin_call()?;
        let result = async {
            let mut ranges = Vec::new();
            ranges
                .try_reserve_exact(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            ranges.push(range);
            let mut pages = self.native_scan_batch(ranges, protection).await?;
            pages
                .pop()
                .ok_or_else(|| StorageError::internal("RocksDB range page is missing"))
        }
        .await;
        call.complete(result)
    }

    async fn native_scan_batch(
        &self,
        ranges: Vec<KvRange>,
        protection: ReadProtection,
    ) -> StorageResult<Vec<KvRangePage>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            for range in &ranges {
                range.charge_work(&self.settlement)?;
            }
            self.flush_pending().await?;
            let (answer, reply) = oneshot::channel();
            self.sender
                .send(Command::ScanBatch {
                    ranges,
                    protection,
                    answer,
                })
                .map_err(|_| StorageError::internal("RocksDB range worker stopped"))?;
            reply
                .await
                .map_err(|_| StorageError::internal("RocksDB range was not answered"))?
        }
        .await;
        let result = result.and_then(|pages| {
            crate::work_admission::NativeReadWork::from(&*self.settlement).charge_pages(&pages)?;
            Ok(pages)
        });
        call.complete(result)
    }

    fn native_stage(&mut self, changes: Vec<KvMutation>) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = (|| {
            self.settlement.charge_calls(changes.len())?;
            for change in &changes {
                change.charge_work(&self.settlement)?;
            }
            self.mode.require_write()?;
            let pending = self
                .pending
                .get_mut()
                .map_err(|_| StorageError::internal("RocksDB pending writes were poisoned"))?;
            pending
                .try_reserve(changes.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            pending.extend(changes);
            Ok(())
        })();
        call.complete(result)
    }

    fn native_stage_versionstamped(
        &mut self,
        _changes: Vec<KvVersionstampedMutation>,
    ) -> StorageResult<()> {
        let call = self.settlement.begin_call()?;
        let result = Err(StorageError::unsupported(
            "RocksDB has no native versionstamped keys",
        ));
        call.complete(result)
    }
}
