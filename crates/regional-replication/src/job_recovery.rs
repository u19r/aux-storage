//! Scheduler continuation preserves native pending work across recoverable
//! outages.
use std::{error::Error, future::Future, io::ErrorKind, time::Duration};

use scheduled_bg_jobs::{ExecutionContext, HandlerFuture, JobOutcome, Timestamp};
use storage_types::{InfrastructureResultKind, StorageEnum, StorageError};

use crate::transport::{ImportNack, TransportError};

/// Stable seeds distinguish each controller and native journal incarnation.
pub(crate) fn derive_controller_seed(
    kind: &str,
    configured: &replication_provider::runtime_configuration::deployment::DeploymentConfiguration,
) -> Result<scheduled_bg_jobs::JobId, TransportError> {
    let digest = replication_types::protocol::CanonicalDigest::of_json(&(
        kind,
        &configured.region,
        configured.group,
        configured.journal_generation,
    ))
    .map_err(|_| TransportError::Configuration)?;
    Ok(scheduled_bg_jobs::JobId::from(
        <[u8; 16]>::try_from(&digest.as_ref()[..16]).map_err(|_| TransportError::Configuration)?,
    ))
}

pub(crate) fn require_unit_payload(
    payload: &[u8],
) -> Result<(), scheduled_bg_jobs::ValidationError> {
    if !payload.is_empty() {
        return Err(scheduled_bg_jobs::ValidationError::InvalidValue {
            field: "regional unit job payload",
        });
    }
    Ok(())
}

/// Unit controllers validate their payload and skip work after cancellation.
/// Active native progress runs again immediately; idle or recoverable failure
/// waits for the configured interval without abandoning durable pending work.
pub(crate) fn advance_pending_controller<'a>(
    context: ExecutionContext<'a>,
    payload: &'a [u8],
    idle_interval: Duration,
    advance: impl Future<Output = Result<bool, Box<dyn Error + Send + Sync>>> + Send + 'a,
) -> HandlerFuture<'a> {
    let now = context.wall_now();
    recover_controller_turn(
        async move {
            require_unit_payload(payload)?;
            let active = !context.is_cancelled() && advance.await?;
            let not_before = if active {
                context.wall_now()
            } else {
                context.wall_now().checked_add(idle_interval)?
            };
            Ok(JobOutcome::Continue {
                not_before,
                checkpoint: Default::default(),
            })
        },
        now,
        idle_interval,
    )
}

pub(crate) fn recover_controller_turn<'a>(
    turn: impl Future<Output = Result<JobOutcome, Box<dyn Error + Send + Sync>>> + Send + 'a,
    now: Timestamp,
    delay: Duration,
) -> HandlerFuture<'a> {
    Box::pin(async move {
        match turn.await {
            Err(error) if is_recoverable_failure(error.as_ref()) => Ok(JobOutcome::Continue {
                not_before: now.checked_add(delay)?,
                checkpoint: Default::default(),
            }),
            result => result,
        }
    })
}

fn is_recoverable_failure(error: &(dyn Error + 'static)) -> bool {
    if let Some(error) = error.downcast_ref::<TransportError>() {
        return match error {
            TransportError::Timeout => true,
            TransportError::Nack(nack) => matches!(
                nack,
                ImportNack::Unavailable
                    | ImportNack::SchemaUnavailable
                    | ImportNack::UnresolvedCommit
                    | ImportNack::Aborted
            ),
            TransportError::Native(error) => is_recoverable_native_failure(error),
            TransportError::Io(error) => is_recoverable_connection_failure(error.kind()),
            _ => false,
        };
    }
    if let Some(error) = error.downcast_ref::<StorageError>() {
        return is_recoverable_native_failure(error);
    }
    false
}

fn is_recoverable_native_failure(error: &StorageError) -> bool {
    error.is_commit_unknown()
        || error.is_retryable_write()
        || matches!(
            error.as_ref(),
            StorageEnum::ServiceUnavailable { .. }
                | StorageEnum::HistoryUnavailable
                | StorageEnum::InfrastructureResult(
                    InfrastructureResultKind::Unavailable { .. }
                        | InfrastructureResultKind::ProtectedConflict { .. }
                )
        )
}

fn is_recoverable_connection_failure(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::NotConnected
            | ErrorKind::TimedOut
            | ErrorKind::WouldBlock
            | ErrorKind::Interrupted
            | ErrorKind::AddrNotAvailable
            | ErrorKind::NetworkUnreachable
            | ErrorKind::HostUnreachable
            | ErrorKind::BrokenPipe
            | ErrorKind::UnexpectedEof
    )
}
