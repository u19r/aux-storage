use std::{future::Future, panic::AssertUnwindSafe, task::Poll};

use futures::future::BoxFuture;
use rusqlite::{Connection, TransactionBehavior};
use storage_types::{StorageError, StorageResult};
use tokio::sync::{mpsc, oneshot};

use super::error::map_driver_error;
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

pub(super) fn execute_attempt<R>(
    connection: &mut Connection,
    mode: AttemptMode,
    operation: BoxFuture<'_, StorageResult<R>>,
    commands: Commands,
) -> StorageResult<R> {
    let behavior = match mode {
        AttemptMode::ReadOnly => TransactionBehavior::Deferred,
        AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => TransactionBehavior::Immediate,
    };
    let transaction = connection
        .transaction_with_behavior(behavior)
        .map_err(map_driver_error)?;
    let outcome = poll_operation(operation, &transaction, commands);
    settle_transaction(transaction, outcome)
}

fn poll_operation<R>(
    operation: BoxFuture<'_, StorageResult<R>>,
    connection: &Connection,
    mut commands: Commands,
) -> Result<StorageResult<R>, Box<dyn std::any::Any + Send>> {
    let mut operation = operation;
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        futures::executor::block_on(futures::future::poll_fn(|context| {
            loop {
                let result = std::pin::Pin::new(&mut operation).poll(context);
                if result.is_ready() {
                    return result;
                }
                let Ok(statement) = commands.try_recv() else {
                    return Poll::Pending;
                };
                statement(connection);
            }
        }))
    }))
}

fn settle_transaction<R>(
    transaction: rusqlite::Transaction<'_>,
    outcome: Result<StorageResult<R>, Box<dyn std::any::Any + Send>>,
) -> StorageResult<R> {
    match outcome {
        Ok(Ok(value)) => {
            transaction
                .commit()
                .map_err(|_| ambiguous("SQLite commit outcome is unknown"))?;
            Ok(value)
        }
        Ok(Err(error)) => {
            // Only the outer owner can commit. Failed-call admission is closed;
            // autocommit here proves native automatic rollback of this attempt.
            if error.native_attempt_rejection()
                == Some(storage_types::NativeAttemptRejection::Capacity)
                && transaction.is_autocommit()
            {
                return Err(error);
            }
            transaction
                .rollback()
                .map_err(|_| ambiguous("SQLite rollback outcome is unknown"))?;
            Err(error)
        }
        Err(_) => {
            let _ = transaction.rollback();
            Err(StorageError::internal("SQLite operation panicked"))
        }
    }
}

fn ambiguous(message: &'static str) -> StorageError {
    StorageError::sync_submission_ambiguous(None, message)
}
