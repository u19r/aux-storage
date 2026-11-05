//! One logical SystemStream page and checkpoint decision on a native attempt.

use storage_common::retry::retry_native_conflicts;
use storage_types::{
    AttemptMode, StorageEnum, StorageError, StorageResult, StreamRecord, SystemStreamCheckpoint,
    SystemStreamCheckpointToken, SystemStreamContractError,
};

use super::{
    SystemStreamCheckpointIssue, SystemStreamCheckpointIssueInput, SystemStreamCheckpointLookup,
    SystemStreamHistoryPage, SystemStreamIssuedCheckpoint, SystemStreamPage,
    SystemStreamPageRequest, page_preparation::PreparedSystemStreamPage,
};
use crate::{
    StreamAttempt, StreamError, StreamPort, StreamReadKind, StreamResult, SystemStreamReadKind,
    constants::{STREAM_WRITE_RETRY, SYSTEM_STREAM_HISTORY_READ_BATCH},
};

#[tracing::instrument(
    name = "system_stream_operation",
    skip_all,
    fields(operation = "GetSystemStreamPage", stream_name = "system")
)]
pub(crate) async fn get_system_stream_page<P: StreamPort>(
    port: &P,
    request: SystemStreamPageRequest,
) -> StreamResult<SystemStreamPage> {
    let prepared = PreparedSystemStreamPage::prepare(request, port.now())
        .map_err(StreamError::from_storage_error)?;
    retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let prepared = prepared.clone();
        port.with_stream_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let mut settlement =
                    infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(
                        attempt,
                    );
                let result = read_page_in_attempt(settlement.as_mut(), prepared).await;
                settlement.complete(result)
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)
}

async fn read_page_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    prepared: PreparedSystemStreamPage,
) -> StorageResult<SystemStreamPage> {
    let request = &prepared.request;
    let (history, receipt) = attempt
        .read_system_page_start_snapshot(request.checkpoint())
        .await?;
    history
        .validate_scope(request.source_scope())
        .map_err(contract_error)?;
    let (input, initial_visibility_cutoff) = match request.checkpoint() {
        Some(token) => {
            let receipt = receipt
                .ok_or(SystemStreamContractError::UnknownCheckpoint)
                .map_err(contract_error)?;
            if receipt.token != *token {
                return Err(history_unavailable(
                    "issued checkpoint token differs from its durable receipt",
                    "system_stream.read.checkpoint_receipt",
                ));
            }
            let checkpoint = SystemStreamCheckpoint::decode_state(&receipt.result_state)
                .map_err(contract_error)?;
            checkpoint
                .validate_scope(&receipt.scope)
                .map_err(contract_error)?;
            (checkpoint, receipt.initial_visibility_cutoff)
        }
        None => {
            let origins = history.retirement_origins().map_err(contract_error)?;
            let retired = attempt.load_system_retired_frontiers(&origins).await?;
            if retired.len() != origins.len() {
                return Err(StorageError::internal("Source retirement count differs"));
            }
            let checkpoint = history
                .retained_checkpoint(&retired.into_iter().flatten().collect::<Vec<_>>())
                .map_err(contract_error)?;
            (
                checkpoint,
                prepared.issued_at - crate::constants::SYSTEM_HISTORY_WINDOW_MILLIS,
            )
        }
    };
    let high = history.capture_high().map_err(contract_error)?;
    let plan = history
        .plan_page(&input, &high, request.limit())
        .map_err(contract_error)?;
    let (records, next) =
        read_verified_records(attempt, &plan, &prepared, initial_visibility_cutoff)
            .await
            .inspect_err(|error| {
                if matches!(error.as_ref(), StorageEnum::HistoryUnavailable) {
                    tracing::error!(
                        error_code = "HistoryUnavailable",
                        diagnostic =
                            "required history references or source progress are unavailable",
                        failure_site = "system_stream.read.references",
                        message = "required logical history could not be verified"
                    );
                }
            })?;
    let has_more = next != high;
    let token = issue_checkpoint(
        attempt,
        &prepared,
        (&input, initial_visibility_cutoff),
        &next,
    )
    .await?;
    let issued = SystemStreamIssuedCheckpoint::new(token, next).map_err(contract_error)?;
    SystemStreamPage::new(request.source_scope().clone(), records, issued, !has_more)
        .map_err(contract_error)
}

async fn read_verified_records<A: StreamAttempt>(
    attempt: &mut A,
    plan: &SystemStreamHistoryPage,
    prepared: &PreparedSystemStreamPage,
    initial_visibility_cutoff: storage_types::TimestampMillis,
) -> StorageResult<(Vec<StreamRecord>, SystemStreamCheckpoint)> {
    let mut assembly =
        super::snapshot_page::SystemSnapshotPage::new(plan, prepared, initial_visibility_cutoff);
    for reads in plan.as_ref().chunks(SYSTEM_STREAM_HISTORY_READ_BATCH) {
        let pages = attempt.read_system_history_pages(reads).await?;
        if assembly.include_pages(attempt, reads, pages).await? {
            break;
        }
    }
    assembly.finish()
}

async fn issue_checkpoint<A: StreamAttempt>(
    attempt: &mut A,
    prepared: &PreparedSystemStreamPage,
    input: (&SystemStreamCheckpoint, storage_types::TimestampMillis),
    next: &SystemStreamCheckpoint,
) -> StorageResult<SystemStreamCheckpointToken> {
    let (input, initial_visibility_cutoff) = input;
    let token = prepared.request.checkpoint();
    if let Some(token) = token.filter(|_| input == next) {
        return Ok(token.clone());
    }
    let issue = SystemStreamCheckpointIssue::try_from(SystemStreamCheckpointIssueInput {
        input_token: token,
        input_checkpoint: input,
        result_checkpoint: next,
        limit: prepared.request.limit(),
        issued_token: prepared.issued_token.clone(),
        issued_at: prepared.issued_at,
        initial_visibility_cutoff,
    })?;
    if let Some(receipt) = attempt
        .read_stream(StreamReadKind::System(SystemStreamReadKind::Checkpoint(
            SystemStreamCheckpointLookup::Operation(&issue.operation_key),
        )))
        .await?
        .into_system_checkpoint()?
    {
        if receipt.scope != *next.scope() || !issue.matches_receipt(&receipt) {
            return Err(StorageError::internal(
                "SystemStream checkpoint receipt differs",
            ));
        }
        return Ok(receipt.token);
    }
    attempt.write_system_stream_checkpoint(&issue, next).await?;
    Ok(issue.issued_token)
}

fn contract_error(error: SystemStreamContractError) -> StorageError {
    tracing::warn!(
        error_code = "ValidationException",
        diagnostic = %error,
        failure_site = "system_stream.read.contract",
        message = "system stream request rejected"
    );
    StorageError::validation(error.to_string())
}

fn history_unavailable(diagnostic: &'static str, failure_site: &'static str) -> StorageError {
    tracing::error!(
        error_code = "HistoryUnavailable",
        diagnostic,
        failure_site,
        message = "required logical history could not be verified"
    );
    StorageEnum::HistoryUnavailable.into()
}
