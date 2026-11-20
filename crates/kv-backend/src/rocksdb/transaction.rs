use std::sync::Arc;

use rocksdb::{OptimisticTransactionDB, OptimisticTransactionOptions, ReadOptions, WriteOptions};
use storage_common::{
    native_attempt_lifecycle::AttemptLifecycle,
    native_observer::{CommitBoundary, NativeObserver, observe_commit_boundary},
    writer_occupancy::{WriterHold, time_native_call},
};
use storage_types::{
    AttemptDeadline, AttemptPhase, NativeAttemptSettlement, StorageResult, expired_in,
};
use tokio::sync::{mpsc, oneshot};

use super::{
    attempt::Command,
    error::{commit_error, map_operation_error},
};
use crate::{AttemptMode, KvMutation, KvRange, KvRangePage, ReadProtection};

/// What the worker checks before it opens the native transaction and before
/// it commits it.
pub(super) struct NativeStart {
    pub lifecycle: Arc<AttemptLifecycle>,
    pub deadline: AttemptDeadline,
}

pub(super) fn drive_attempt(
    database: Arc<OptimisticTransactionDB>,
    mode: AttemptMode,
    mut receiver: mpsc::UnboundedReceiver<Command>,
    settlement: Arc<NativeAttemptSettlement>,
    start: NativeStart,
    writer: Option<&WriterHold>,
) -> StorageResult<()> {
    // An abandoned attempt already answered; it opens no transaction.
    if !start.lifecycle.open() {
        return Err(expired_in(AttemptPhase::Queued));
    }
    let mut writes = WriteOptions::default();
    writes.set_sync(false);
    let mut options = OptimisticTransactionOptions::default();
    options.set_snapshot(true);
    let native = time_native_call(writer, || database.transaction_opt(&writes, &options));
    let snapshot = native.snapshot();
    let mut reads = ReadOptions::default();
    reads.set_snapshot(&snapshot);
    // The serialized write queue protects point and range observations.
    // Native read tracking would duplicate that protection.
    let mut worker = NativeCommandExecutor {
        snapshot_sequence: snapshot.sequence_number(),
        native: &native,
        reads: &reads,
        mode,
        settlement,
    };
    while let Some(command) = receiver.blocking_recv() {
        let Some(finish) = time_native_call(writer, || worker.dispatch(command)) else {
            continue;
        };
        let permission = worker.settlement.require_open();
        drop(worker);
        drop(reads);
        drop(snapshot);
        time_native_call(writer, || finish.settle(native, mode, permission, &start));
        return Ok(());
    }
    // Native drop rolls back before the writer takes the next attempt.
    Ok(())
}

/// The worker's native transaction cannot commit failed or abandoned commands.
struct NativeCommandExecutor<'worker, 'database> {
    snapshot_sequence: u64,
    native: &'worker rocksdb::Transaction<'database, OptimisticTransactionDB>,
    reads: &'worker ReadOptions,
    mode: AttemptMode,
    settlement: Arc<NativeAttemptSettlement>,
}

impl NativeCommandExecutor<'_, '_> {
    fn dispatch(&mut self, command: Command) -> Option<NativeSettlementCommand> {
        match command {
            Command::SnapshotSequence { answer } => {
                let result = self.execute_command(&answer, || Ok(self.snapshot_sequence));
                self.reply(answer, result);
            }
            Command::FirstKeyInRange {
                begin,
                end,
                protection,
                answer,
            } => {
                let result = self.execute_command(&answer, || {
                    protection.validate(self.mode).and_then(|()| {
                        load_first_native_key(self.native, &begin, &end, &self.settlement)
                    })
                });
                self.reply(answer, result);
            }
            Command::GetBatch {
                keys,
                protection,
                allowance,
                probe,
                answer,
            } => {
                let result = self.execute_command(&answer, || {
                    protection.validate(self.mode).and_then(|()| {
                        load_native_values(
                            self.native,
                            self.reads,
                            keys,
                            allowance,
                            probe.as_deref(),
                        )
                    })
                });
                self.reply(answer, result);
            }
            Command::ScanBatch {
                ranges,
                protection,
                probe,
                answer,
            } => {
                let result = self.execute_command(&answer, || {
                    let mut pages = Vec::new();
                    pages
                        .try_reserve_exact(ranges.len())
                        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                    for range in ranges {
                        pages.push(scan_native(
                            self.native,
                            range,
                            protection,
                            self.mode,
                            probe.as_deref(),
                        )?);
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
                recorder,
                answer,
                probe,
            } => {
                return Some(NativeSettlementCommand {
                    commit,
                    changes,
                    recorder,
                    answer,
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
    recorder: Option<storage_common::durability::CommitRecorder>,
    answer: oneshot::Sender<StorageResult<()>>,
    probe: Option<(tokio::runtime::Handle, Arc<dyn crate::KvFaultProbe>)>,
}

impl NativeSettlementCommand {
    // Fault builds watch the answer while a commit boundary is held.
    fn settle(
        mut self,
        native: rocksdb::Transaction<'_, OptimisticTransactionDB>,
        mode: AttemptMode,
        permission: StorageResult<()>,
        start: &NativeStart,
    ) {
        let result = if self.commit && mode != AttemptMode::ReadOnly {
            let applied = permission.and_then(|()| apply_changes(&native, mode, self.changes));
            let applied = applied.and_then(|()| {
                observe_before_commit(
                    &native,
                    mode,
                    self.probe,
                    start.lifecycle.observer(),
                    &mut self.answer,
                )
            });
            // The one commit-submission point: before it an expiry or a stopped
            // task drops the unwritten batch, a known rollback; after it the
            // batch write may apply.
            let committed = applied
                .and_then(|()| start.lifecycle.submit_commit(&start.deadline))
                .and_then(|ticket| ticket.settle(native.commit().map_err(commit_error)));
            if committed.is_ok()
                && let Some(recorder) = &self.recorder
            {
                recorder.record();
            }
            committed
        } else {
            // Rollback/read completion releases the native owner before its
            // reply.
            drop(native);
            if self.commit { permission } else { Ok(()) }
        };
        let _ = self.answer.send(result);
    }
}

/// Reports the write batch size and holds the commit at its observer
/// boundary. Without a probe it returns at once.
fn observe_before_commit(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    mode: AttemptMode,
    probe: Option<(tokio::runtime::Handle, Arc<dyn crate::KvFaultProbe>)>,
    observer: Option<&dyn NativeObserver>,
    answer: &mut oneshot::Sender<StorageResult<()>>,
) -> StorageResult<()> {
    let Some((runtime, probe)) = probe else {
        return Ok(());
    };
    probe.observe(crate::KvNativeEvent::WriteBatchBytes {
        bytes: native.get_writebatch().size_in_bytes(),
    });
    let Some(observer) = observer else {
        return Ok(());
    };
    let _runtime = runtime.enter();
    // A stopped attempt task can no longer release the boundary; the writer
    // must not wait for it.
    let boundary = observe_commit_boundary(observer, CommitBoundary::BeforeCommit, mode);
    futures::executor::block_on(async {
        match futures::future::select(std::pin::pin!(boundary), std::pin::pin!(answer.closed()))
            .await
        {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(((), _)) => {
                // Before submission: no ticket, nothing written (an abandoned
                // call).
                Err(storage_types::StorageError::attempt_abandoned())
            }
        }
    })
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
        KvMutation::ClearUnchunkedRange { begin, end } => {
            clear_unchunked_range(native, &begin, &end)
        }
        KvMutation::DeleteRange { .. } => Err(storage_types::StorageError::internal(
            "RocksDB does not chunk logical values",
        )),
    })
}

/// Deletes each key in `[begin, end)` that this transaction sees, including
/// its own earlier writes. RocksDB applies writers one at a time, so no
/// concurrent writer can add a key to the range between this scan and the
/// commit. Each delete is validated by the optimistic commit.
fn clear_unchunked_range(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    begin: &[u8],
    end: &[u8],
) -> StorageResult<()> {
    if begin >= end {
        return Ok(());
    }
    let mut keys = Vec::new();
    {
        let mut iterator = native.raw_iterator();
        iterator.seek(begin);
        while let Some(key) = iterator.key().filter(|key| *key < end) {
            keys.push(key.to_vec());
            iterator.next();
        }
        iterator.status().map_err(map_operation_error)?;
    }
    keys.into_iter()
        .try_for_each(|key| native.delete(key).map_err(map_operation_error))
}

fn load_first_native_key(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    begin: &[u8],
    end: &[u8],
    settlement: &NativeAttemptSettlement,
) -> StorageResult<Option<Vec<u8>>> {
    if begin >= end {
        return Ok(None);
    }
    let snapshot = native.snapshot();
    let mut reads = ReadOptions::default();
    reads.set_snapshot(&snapshot);
    let mut iterator = native.raw_iterator_opt(reads);
    iterator.seek(begin);
    iterator.status().map_err(map_operation_error)?;
    let Some(key) = iterator.key().filter(|key| *key < end) else {
        return Ok(None);
    };
    settlement.charge_data(key.len())?;
    crate::backend::copy_native_key(key).map(Some)
}

fn scan_native(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    range: KvRange,
    protection: ReadProtection,
    mode: AttemptMode,
    probe: Option<&dyn crate::KvFaultProbe>,
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
    load_native_page(&mut iterator, &range, probe)
}

fn load_native_values(
    native: &rocksdb::Transaction<'_, OptimisticTransactionDB>,
    reads: &ReadOptions,
    keys: Vec<Vec<u8>>,
    mut allowance: crate::KvValueAllowance,
    probe: Option<&dyn crate::KvFaultProbe>,
) -> StorageResult<Vec<Option<Vec<u8>>>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(keys.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for key in keys {
        let value = native
            .get_pinned_opt(key, reads)
            .map_err(map_operation_error)?;
        if let Some(probe) = probe {
            probe.observe(crate::KvNativeEvent::ReturnedData {
                rows: usize::from(value.is_some()),
                bytes: value.as_ref().map_or(0, |bytes| bytes.len()),
            });
        }
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
    probe: Option<&dyn crate::KvFaultProbe>,
) -> StorageResult<KvRangePage> {
    // The row limit can be very large even when the byte budget is small.
    let mut buffer = crate::range::KvRangeBuffer::default();
    let mut more = false;
    while let Some((key, value)) = iterator.item() {
        if let Some(probe) = probe {
            probe.observe(crate::KvNativeEvent::ReturnedData {
                rows: 1,
                bytes: key.len() + value.len(),
            });
        }
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
