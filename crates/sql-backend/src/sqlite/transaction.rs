use std::{future::Future, panic::AssertUnwindSafe, task::Poll};

use futures::future::BoxFuture;
use rusqlite::{Connection, TransactionBehavior};
use storage_common::{
    native_attempt_lifecycle::AttemptLifecycle,
    native_outcome::{NativeCommitError, classify_commit},
    writer_occupancy::{WriterHold, time_native_call},
};
use storage_types::{AttemptDeadline, AttemptPhase, NativeCallPhase, StorageError, StorageResult};
use tokio::sync::{mpsc, oneshot};

use super::{
    attempt::DurableWriteSlot,
    error::{commit_error, map_native_error},
};
use crate::AttemptMode;

type Statement = Box<dyn FnOnce(&Connection) + Send>;
pub(super) type Commands = mpsc::UnboundedReceiver<Statement>;

/// Statements are drained on the same worker that polls the domain operation.
pub(super) struct Session {
    commands: mpsc::UnboundedSender<Statement>,
}

impl Session {
    pub(super) fn open() -> (Self, Commands) {
        let (commands, receiver) = mpsc::unbounded_channel();
        (Self { commands }, receiver)
    }

    pub(super) async fn call<F, R>(&self, function: F) -> StorageResult<R>
    where
        F: FnOnce(&Connection) -> StorageResult<R> + Send + 'static,
        R: Send + 'static,
    {
        let (reply, receiver) = oneshot::channel();
        self.commands
            .send(Box::new(move |connection| {
                let _ = reply.send(function(connection));
            }))
            .map_err(|_| StorageError::internal("SQLite attempt is closed"))?;
        receiver
            .await
            .map_err(|_| StorageError::internal("SQLite attempt worker stopped"))?
    }
}

/// Runs one attempt on its connection. Returns the native commit version at
/// its end when it commits nothing (read inside its transaction, after its
/// last read), and records the commit ticket of a registered writer right
/// after the native commit returns.
pub(super) fn execute_attempt<R>(
    connection: &mut Connection,
    (mode, deadline): (AttemptMode, AttemptDeadline),
    lifecycle: &AttemptLifecycle,
    operation: BoxFuture<'_, StorageResult<R>>,
    commands: Commands,
    durable_write: &DurableWriteSlot,
    writer: Option<&WriterHold>,
) -> (StorageResult<R>, Option<u64>) {
    let behavior = match mode {
        AttemptMode::ReadOnly => TransactionBehavior::Deferred,
        AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => TransactionBehavior::Immediate,
    };
    // FIN-b: a BEGIN that waits on another process's lock ends by the deadline.
    let busy = match crate::backend::local_busy_wait(&deadline) {
        Ok(busy) => busy,
        Err(error) => return (Err(error), None),
    };
    if let Err(error) = connection.busy_timeout(busy) {
        return (Err(map_native_error(error, NativeCallPhase::Begin)), None);
    }
    let transaction =
        match time_native_call(writer, || connection.transaction_with_behavior(behavior)) {
            Ok(transaction) => transaction,
            Err(error) => {
                let error = map_native_error(error, NativeCallPhase::Begin);
                return (
                    Err(crate::backend::local_begin_failure(error, &deadline)),
                    None,
                );
            }
        };
    let outcome = poll_operation(operation, &transaction, commands, writer);
    // An attempt that commits nothing answers from what it read, also with a
    // data-dependent error after which a writer rolls back.
    let observed = match &outcome {
        Ok(Ok(_)) if durable_write.is_registered() => None,
        Ok(_) => super::durability::read_data_version(&transaction).ok(),
        Err(_) => None,
    };
    (
        time_native_call(writer, || {
            settle_transaction(
                transaction,
                outcome,
                (mode, deadline),
                lifecycle,
                durable_write,
            )
        }),
        observed,
    )
}

/// On the writer, each pending poll must leave a statement queued: a
/// `Session::call` sends during the poll and this loop runs it in place, so
/// an empty queue means the operation waits for something else.
fn poll_operation<R>(
    operation: BoxFuture<'_, StorageResult<R>>,
    connection: &Connection,
    mut commands: Commands,
    writer: Option<&WriterHold>,
) -> Result<StorageResult<R>, Box<dyn std::any::Any + Send>> {
    let mut operation = operation;
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        futures::executor::block_on(futures::future::poll_fn(|context| {
            loop {
                let operation = std::pin::Pin::new(&mut operation);
                let result = match writer {
                    Some(writer) => {
                        writer
                            .occupancy()
                            .poll_write_operation(operation, context, || !commands.is_empty())
                    }
                    None => operation.poll(context),
                };
                if result.is_ready() {
                    return result;
                }
                let Ok(statement) = commands.try_recv() else {
                    return Poll::Pending;
                };
                time_native_call(writer, || statement(connection));
            }
        }))
    }))
}

fn settle_transaction<R>(
    transaction: rusqlite::Transaction<'_>,
    outcome: Result<StorageResult<R>, Box<dyn std::any::Any + Send>>,
    (mode, deadline): (AttemptMode, AttemptDeadline),
    lifecycle: &AttemptLifecycle,
    durable_write: &DurableWriteSlot,
) -> StorageResult<R> {
    match outcome {
        Ok(Ok(value)) => {
            // The one commit-submission point. Nothing separates it from COMMIT
            // on this worker. A refusal (deadline passed, or the task stopped
            // and abandoned the attempt) is a known abort. A read commits
            // nothing and keeps only the Settle deadline check.
            let ticket = if mode == AttemptMode::ReadOnly {
                deadline
                    .require_remaining(AttemptPhase::Settle)
                    .map(|()| None)
            } else {
                lifecycle.submit_commit(&deadline).map(Some)
            };
            let ticket = match ticket {
                Ok(ticket) => ticket,
                Err(error) => {
                    transaction.rollback().map_err(|_| {
                        StorageError::commit_unknown("SQLite rollback outcome is unknown")
                    })?;
                    return Err(error);
                }
            };
            // COMMIT through the open transaction, so that a busy answer can
            // be rolled back here: SQLite leaves the transaction open on
            // SQLITE_BUSY and wrote nothing.
            let committed = match transaction.execute_batch("COMMIT") {
                Ok(()) => Ok(()),
                Err(native) => match commit_error(&native) {
                    NativeCommitError::Conflict(cause) => match transaction.rollback() {
                        Ok(()) => Err(NativeCommitError::Conflict(cause)),
                        Err(_) => Err(NativeCommitError::Unproven {
                            reason: "SQLite busy commit was not rolled back",
                            cause: Some(cause),
                        }),
                    },
                    other => Err(other),
                },
            };
            match ticket {
                Some(ticket) => ticket.settle(committed),
                // A ReadOnly attempt cannot write, so its COMMIT failure is
                // known.
                None => committed.map_err(|error| classify_commit(false, error)),
            }?;
            durable_write.record_commit();
            Ok(value)
        }
        Ok(Err(error)) => {
            // Only the outer owner can commit. Failed-call admission is closed;
            // autocommit here proves native automatic rollback of this attempt.
            if matches!(
                error.attempt_kind(),
                Some(
                    storage_types::AttemptKind::Capacity
                        | storage_types::AttemptKind::TransactionBytes
                )
            ) && transaction.is_autocommit()
            {
                return Err(error);
            }
            transaction
                .rollback()
                .map_err(|_| StorageError::commit_unknown("SQLite rollback outcome is unknown"))?;
            Err(error)
        }
        Err(_) => {
            let _ = transaction.rollback();
            Err(StorageError::internal("SQLite operation panicked"))
        }
    }
}
