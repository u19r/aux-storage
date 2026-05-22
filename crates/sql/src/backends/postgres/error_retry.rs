use queue_provider::{QueueError, QueueResult};
use storage_types::{StorageEnum, StorageError, StorageResult};
use stream_provider::{StreamEnum, StreamError, StreamResult};
use tokio_postgres::error::SqlState;

use crate::backends::postgres::{
    POSTGRES_BASE_BACKOFF_MS, POSTGRES_MAX_CONFLICT_RETRIES, PostgresStorageProvider,
};

impl PostgresStorageProvider {
    pub(super) fn map_postgres_client_acquire_error(err: impl std::fmt::Display) -> StorageError {
        StorageError::internal(&format!("postgres client acquire failed: {err}"))
    }

    pub(super) fn map_postgres_error(context: &str, err: impl std::fmt::Display) -> StorageError {
        let message = err.to_string();
        if Self::is_postgres_conflict_message(&message) {
            return StorageEnum::TransactionConflict {
                message: format!("postgres {context} conflict: {message}"),
            }
            .into();
        }
        StorageError::internal(&format!("postgres {context} failed: {message}"))
    }

    pub(super) fn map_postgres_write_error(
        context: &str,
        err: tokio_postgres::Error,
    ) -> StorageError {
        if Self::is_postgres_retryable_conflict(&err) {
            return StorageEnum::TransactionConflict {
                message: format!("postgres {context} conflict: {err}"),
            }
            .into();
        }
        Self::map_postgres_error(context, err)
    }

    pub(super) fn is_postgres_constraint_error(error: &tokio_postgres::Error) -> bool {
        let Some(db_error) = error.as_db_error() else {
            return false;
        };
        matches!(
            db_error.code(),
            &SqlState::UNIQUE_VIOLATION
                | &SqlState::NOT_NULL_VIOLATION
                | &SqlState::CHECK_VIOLATION
                | &SqlState::EXCLUSION_VIOLATION
        )
    }

    pub(super) fn postgres_conflict_backoff(attempt: u32) -> std::time::Duration {
        let exp = POSTGRES_BASE_BACKOFF_MS.saturating_mul(1u64 << attempt.min(8));
        let jitter = rand::random::<u64>() % (exp + 1);
        std::time::Duration::from_millis(exp + (jitter / 2))
    }

    pub(super) fn is_postgres_retryable_conflict(error: &tokio_postgres::Error) -> bool {
        let Some(db_error) = error.as_db_error() else {
            return false;
        };
        matches!(
            db_error.code(),
            &SqlState::T_R_SERIALIZATION_FAILURE | &SqlState::T_R_DEADLOCK_DETECTED
        )
    }

    pub(super) fn is_postgres_conflict_message(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("sqlstate 40001")
            || lower.contains("sqlstate 40p01")
            || lower.contains("could not serialize access")
            || lower.contains("serialization failure")
            || lower.contains("deadlock detected")
    }

    pub(super) fn is_storage_conflict(error: &StorageError) -> bool {
        matches!(
            error.as_ref(),
            StorageEnum::TransactionConflict { .. } | StorageEnum::TransactionInProgress { .. }
        )
    }

    pub(super) async fn retry_postgres_conflicts<T, F, Fut>(
        &self,
        operation_name: &str,
        mut operation: F,
    ) -> StorageResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = StorageResult<T>>,
    {
        for attempt in 0..POSTGRES_MAX_CONFLICT_RETRIES {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error)
                    if Self::is_storage_conflict(&error)
                        && attempt + 1 < POSTGRES_MAX_CONFLICT_RETRIES =>
                {
                    let sleep = Self::postgres_conflict_backoff(attempt);
                    tracing::debug!(
                        attempt,
                        sleep_ms = sleep.as_millis(),
                        error = %error,
                        operation = operation_name,
                        "retrying postgres conflict"
                    );
                    tokio::time::sleep(sleep).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: format!(
                "postgres {operation_name} exhausted conflict retry budget \
                 ({POSTGRES_MAX_CONFLICT_RETRIES} attempts)"
            ),
        }
        .into())
    }

    pub(super) fn map_stream_error(context: &str, err: impl std::fmt::Display) -> StreamError {
        let message = err.to_string();
        if Self::is_postgres_conflict_message(&message) {
            return StreamError::from(StorageError::Base(StorageEnum::TransactionConflict {
                message: format!("postgres {context} conflict: {message}"),
            }));
        }
        StreamError::internal(format!("postgres {context} failed: {message}"))
    }

    pub(super) fn map_queue_error(context: &str, err: impl std::fmt::Display) -> QueueError {
        let message = err.to_string();
        if Self::is_postgres_conflict_message(&message) {
            return QueueError::from(StorageError::Base(StorageEnum::TransactionConflict {
                message: format!("postgres {context} conflict: {message}"),
            }));
        }
        QueueError::from(StorageError::internal(&format!(
            "postgres {context} failed: {message}"
        )))
    }

    pub(super) fn is_queue_conflict(error: &QueueError) -> bool {
        match error {
            QueueError::StorageError(storage) | QueueError::TransactWrite(storage) => {
                Self::is_storage_conflict(storage)
            }
            _ => Self::is_postgres_conflict_message(&error.to_string()),
        }
    }

    pub(super) fn is_stream_conflict(error: &StreamError) -> bool {
        match error.as_ref() {
            StreamEnum::StorageError(storage) => Self::is_storage_conflict(storage),
            _ => Self::is_postgres_conflict_message(&error.to_string()),
        }
    }

    pub(super) async fn retry_postgres_queue_conflicts<T, F, Fut>(
        &self,
        operation_name: &str,
        mut operation: F,
    ) -> QueueResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = QueueResult<T>>,
    {
        for attempt in 0..POSTGRES_MAX_CONFLICT_RETRIES {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error)
                    if Self::is_queue_conflict(&error)
                        && attempt + 1 < POSTGRES_MAX_CONFLICT_RETRIES =>
                {
                    let sleep = Self::postgres_conflict_backoff(attempt);
                    tracing::debug!(
                        attempt,
                        sleep_ms = sleep.as_millis(),
                        error = %error,
                        operation = operation_name,
                        "retrying postgres queue conflict"
                    );
                    tokio::time::sleep(sleep).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(QueueError::from(StorageError::Base(
            StorageEnum::TransactionConflict {
                message: format!(
                    "postgres {operation_name} exhausted conflict retry budget \
                     ({POSTGRES_MAX_CONFLICT_RETRIES} attempts)"
                ),
            },
        )))
    }

    pub(super) async fn retry_postgres_stream_conflicts<T, F, Fut>(
        &self,
        operation_name: &str,
        mut operation: F,
    ) -> StreamResult<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = StreamResult<T>>,
    {
        for attempt in 0..POSTGRES_MAX_CONFLICT_RETRIES {
            match operation().await {
                Ok(value) => return Ok(value),
                Err(error)
                    if Self::is_stream_conflict(&error)
                        && attempt + 1 < POSTGRES_MAX_CONFLICT_RETRIES =>
                {
                    let sleep = Self::postgres_conflict_backoff(attempt);
                    tracing::debug!(
                        attempt,
                        sleep_ms = sleep.as_millis(),
                        error = %error,
                        operation = operation_name,
                        "retrying postgres stream conflict"
                    );
                    tokio::time::sleep(sleep).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StreamError::from(StorageError::Base(
            StorageEnum::TransactionConflict {
                message: format!(
                    "postgres {operation_name} exhausted conflict retry budget \
                     ({POSTGRES_MAX_CONFLICT_RETRIES} attempts)"
                ),
            },
        )))
    }
}
