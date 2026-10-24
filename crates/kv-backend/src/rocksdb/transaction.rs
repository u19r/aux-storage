use std::sync::Arc;

use rocksdb::{OptimisticTransactionDB, OptimisticTransactionOptions, ReadOptions, WriteOptions};
use storage_types::{NativeAttemptSettlement, StorageResult};
use tokio::sync::{mpsc, oneshot};

use super::{
    attempt::Command,
    error::{map_commit_error, map_operation_error},
};
use crate::{AttemptMode, KvMutation, KvRange, KvRangePage, ReadProtection};

pub(super) fn drive_attempt(
    database: Arc<OptimisticTransactionDB>,
    mode: AttemptMode,
    mut receiver: mpsc::UnboundedReceiver<Command>,
    settlement: Arc<NativeAttemptSettlement>,
) -> StorageResult<()> {
    let mut writes = WriteOptions::default();
    writes.set_sync(false);
    let mut options = OptimisticTransactionOptions::default();
    options.set_snapshot(true);
    let native = database.transaction_opt(&writes, &options);
    let snapshot = native.snapshot();
    let mut reads = ReadOptions::default();
    reads.set_snapshot(&snapshot);
    // The serialized write queue protects point and range observations.
    // Native read tracking would duplicate that protection.
    let mut worker = NativeCommandExecutor {
        native: &native,
        reads: &reads,
        mode,
        settlement,
    };
    while let Some(command) = receiver.blocking_recv() {
        let Some(finish) = worker.dispatch(command) else {
            continue;
        };
        let permission = worker.settlement.require_open();
        drop(worker);
        drop(reads);
        drop(snapshot);
        finish.settle(native, mode, permission);
        return Ok(());
    }
    // Native drop rolls back before the writer takes the next attempt.
    Ok(())
}

/// The worker's native transaction cannot commit failed or abandoned commands.
struct NativeCommandExecutor<'worker, 'database> {
    native: &'worker rocksdb::Transaction<'database, OptimisticTransactionDB>,
    reads: &'worker ReadOptions,
    mode: AttemptMode,
    settlement: Arc<NativeAttemptSettlement>,
}

impl NativeCommandExecutor<'_, '_> {
    fn dispatch(&mut self, command: Command) -> Option<NativeSettlementCommand> {
        match command {
            Command::HasKeyInRange {
                begin,
                end,
                protection,
                answer,
            } => {
                let result = self.execute_command(&answer, || {
                    protection
                        .validate(self.mode)
                        .and_then(|()| has_native_key_in_range(self.native, &begin, &end))
                });
                self.reply(answer, result);
            }
            Command::GetBatch {
                keys,
                protection,
                allowance,
                answer,
            } => {
                let result = self.execute_command(&answer, || {
                    protection
                        .validate(self.mode)
                        .and_then(|()| load_native_values(self.native, self.reads, keys, allowance))
                });
                self.reply(answer, result);
            }
            Command::ScanBatch {
                ranges,
                protection,
                answer,
            } => {
                let result = self.execute_command(&answer, || {
                    let mut pages = Vec::new();
                    pages.try_reserve_exact(ranges.len()).map_err(|_| {
                        storage_types::InfrastructureResultKind::NativeCapacityRejected
                    })?;
                    for range in ranges {
                        pages.push(scan_native(self.native, range, protection, self.mode)?);
                    }
                    Ok(pages)
                });
                self.reply(answer, result);
            }
            Command::ApplyBatch { changes, answer } => {
                // These puts update only the private write batch; Finish owns
                // commit.
                let result = self
                    .execute_command(&answer, || apply_changes(self.native, self.mode, changes));
                self.reply(answer, result);
            }
            Command::Finish {
                commit,
                changes,
                answer,
                #[cfg(feature = "fault-injection")]
                probe,
            } => {
                return Some(NativeSettlementCommand {
                    commit,
                    changes,
                    answer,
                    #[cfg(feature = "fault-injection")]
                    probe,
                });
            }
        }
        None
    }

    fn execute_command<T>(
        &self,
        answer: &oneshot::Sender<StorageResult<T>>,
        operation: impl FnOnce() -> StorageResult<T>,
    ) -> StorageResult<T> {
        let call = self.settlement.begin_call()?;
        let result = if answer.is_closed() {
            Err(storage_types::StorageError::internal(
                "RocksDB command waiter was cancelled before native dispatch",
            ))
        } else {
            operation()
        };
        call.complete(result)
    }

    fn reply<T>(&mut self, answer: oneshot::Sender<StorageResult<T>>, result: StorageResult<T>) {
        if answer.send(result).is_err() {
            self.settlement.reject();
        }
    }
}

/// Native settlement takes ownership only after command dispatch has stopped.
struct NativeSettlementCommand {
    commit: bool,
    changes: Vec<KvMutation>,
    answer: oneshot::Sender<StorageResult<()>>,
    #[cfg(feature = "fault-injection")]
    probe: Option<(tokio::runtime::Handle, Arc<dyn crate::KvFaultProbe>)>,
}

impl NativeSettlementCommand {
    fn settle(
        self,
        native: rocksdb::Transaction<'_, OptimisticTransactionDB>,
        mode: AttemptMode,
        permission: StorageResult<()>,
    ) {
        let result = if self.commit && mode != AttemptMode::ReadOnly {
            let applied = permission.and_then(|()| apply_changes(&native, mode, self.changes));
            #[cfg(feature = "fault-injection")]
            let applied = applied.and_then(|()| observe_before_commit(self.probe));
            applied.and_then(|()| native.commit().map_err(map_commit_error))
        } else {
            // Rollback/read completion releases the native owner before its
            // reply.
            drop(native);
            if self.commit { permission } else { Ok(()) }
        };
        let _ = self.answer.send(result);
    }
}

#[cfg(feature = "fault-injection")]
fn observe_before_commit(
    probe: Option<(tokio::runtime::Handle, Arc<dyn crate::KvFaultProbe>)>,
) -> StorageResult<()> {
    let Some((runtime, probe)) = probe else {
        return Ok(());
    };
    let _runtime = runtime.enter();
    futures::executor::block_on(
        crate::fault_injection::KvCommitBoundary::BeforeCommit.observe(Some(probe.as_ref())),
    )
}

fn apply_changes(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    mode: AttemptMode,
    changes: Vec<KvMutation>,
) -> StorageResult<()> {
    mode.require_write()?;
    changes.into_iter().try_for_each(|change| match change {
        KvMutation::Put { key, value } | KvMutation::PutUnchunked { key, value } => {
            native.put(key, value).map_err(map_operation_error)
        }
        KvMutation::Delete { key } | KvMutation::DeleteUnchunked { key } => {
            native.delete(key).map_err(map_operation_error)
        }
        KvMutation::DeleteRange { .. } => Err(storage_types::StorageError::internal(
            "RocksDB does not chunk logical values",
        )),
    })
}

fn has_native_key_in_range(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    begin: &[u8],
    end: &[u8],
) -> StorageResult<bool> {
    if begin >= end {
        return Ok(false);
    }
    let snapshot = native.snapshot();
    let mut reads = ReadOptions::default();
    reads.set_snapshot(&snapshot);
    let mut iterator = native.raw_iterator_opt(reads);
    iterator.seek(begin);
    iterator.status().map_err(map_operation_error)?;
    Ok(iterator.key().is_some_and(|key| key < end))
}

fn scan_native(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    range: KvRange,
    protection: ReadProtection,
    mode: AttemptMode,
) -> StorageResult<KvRangePage> {
    protection.validate(mode)?;
    if range.begin >= range.end {
        return Ok(KvRangePage {
            entries: Vec::new(),
            more: false,
        });
    }
    let snapshot = native.snapshot();
    let mut reads = ReadOptions::default();
    reads.set_snapshot(&snapshot);
    let mut iterator = native.raw_iterator_opt(reads);
    if range.reverse {
        iterator.seek_for_prev(&range.end);
        if iterator.key() == Some(range.end.as_slice()) {
            iterator.prev();
        }
    } else {
        iterator.seek(&range.begin);
    }
    load_native_page(&mut iterator, &range)
}

fn load_native_values(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    reads: &ReadOptions,
    keys: Vec<Vec<u8>>,
    mut allowance: crate::KvValueAllowance,
) -> StorageResult<Vec<Option<Vec<u8>>>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for key in keys {
        let value = native
            .get_pinned_opt(key, reads)
            .map_err(map_operation_error)?;
        values.push(allowance.copy_value(value.as_ref().map(|bytes| bytes.as_ref()))?);
    }
    Ok(values)
}

fn load_native_page(
    iterator: &mut rocksdb::DBRawIteratorWithThreadMode<
        '_,
        rocksdb::Transaction<'_, OptimisticTransactionDB>,
    >,
    range: &KvRange,
) -> StorageResult<KvRangePage> {
    // The row limit can be very large even when the byte budget is small.
    let mut buffer = crate::range::KvRangeBuffer::default();
    let mut more = false;
    while let Some((key, value)) = iterator.item() {
        if key < range.begin.as_slice() || key >= range.end.as_slice() {
            break;
        }
        if !buffer.append(range, key, value)? {
            more = true;
            break;
        }
        if range.reverse {
            iterator.prev();
        } else {
            iterator.next();
        }
    }
    iterator.status().map_err(map_operation_error)?;
    Ok(buffer.into_page(more))
}
