//! One observation and transition decision for scheduled and direct GSI work.
use storage_backfill::{
    GsiBuild, GsiChange, GsiObservation, GsiPageLimit, GsiReadRequest, GsiScanPageEnd,
};
use storage_types::{StorageError, StorageResult};

use super::GsiBuildTransaction;

pub(super) async fn evaluate<Transaction: GsiBuildTransaction>(
    transaction: &mut Transaction,
    build: &GsiBuild<Transaction::Cursor>,
    limit: GsiPageLimit,
) -> StorageResult<GsiChange<Transaction::Cursor>> {
    let read = build
        .read_request(limit)
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    match Box::pin(read_observation(transaction, build, read)).await {
        Ok(observation) => build
            .decide_next_change(observation)
            .map_err(|error| StorageError::internal(&error.to_string())),
        Err(error)
            if matches!(
                error.as_ref(),
                storage_types::StorageEnum::HistoryUnavailable
            ) =>
        {
            Ok(build.block_unavailable_history())
        }
        Err(error) => Err(error),
    }
}

async fn read_observation<Transaction: GsiBuildTransaction>(
    transaction: &mut Transaction,
    build: &GsiBuild<Transaction::Cursor>,
    read: GsiReadRequest<&Transaction::Cursor>,
) -> StorageResult<GsiObservation<Transaction::Cursor>> {
    Ok(match read {
        GsiReadRequest::Scan { after, limit } => {
            read_scan(transaction, build, after, limit).await?
        }
        GsiReadRequest::CatchUp {
            after,
            through,
            limit,
        } => GsiObservation::Stream(
            transaction
                .read_gsi_catch_up(&build.target, after, through, limit)
                .await?,
        ),
        GsiReadRequest::Continue {
            after,
            pending_high,
            limit,
        } => GsiObservation::Stream(
            transaction
                .read_gsi_continuation(&build.target, after, pending_high, limit)
                .await?,
        ),
        GsiReadRequest::InspectActivation {
            required_from,
            through,
        } => GsiObservation::Activation(
            transaction
                .read_gsi_activation(&build.target, required_from, through)
                .await?,
        ),
    })
}

async fn read_scan<Transaction: GsiBuildTransaction>(
    transaction: &mut Transaction,
    build: &GsiBuild<Transaction::Cursor>,
    after: Option<&Transaction::Cursor>,
    limit: GsiPageLimit,
) -> StorageResult<GsiObservation<Transaction::Cursor>> {
    let observation = transaction
        .read_gsi_scan(&build.target, after, limit)
        .await?;
    if let GsiScanPageEnd::Complete(high) = &observation.end {
        transaction
            .validate_gsi_frontier(build.state.required_stream_history(), high)
            .await?;
    }
    Ok(GsiObservation::Scan(observation))
}
