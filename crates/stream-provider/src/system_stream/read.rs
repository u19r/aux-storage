//! One logical SystemStream page and checkpoint decision on a native attempt.

use storage_common::retry::retry_native_conflicts;
use storage_types::{
    AttemptMode, StorageEnum, StorageError, StorageResult, StreamRecord, SystemStreamCheckpoint,
    SystemStreamCheckpointToken, SystemStreamContractError,
};

use super::{
    SystemStreamCheckpointIssue, SystemStreamCheckpointLookup, SystemStreamHistoryObservation,
    SystemStreamHistoryPage, SystemStreamIssuedCheckpoint, SystemStreamPage, SystemStreamPagePlan,
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
    let prepared =
        PreparedSystemStreamPage::prepare(request).map_err(StreamError::from_storage_error)?;
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
    let (history, receipt) = attempt.read_system_page_start(request.checkpoint()).await?;
    history
        .validate_scope(request.source_scope())
        .map_err(contract_error)?;
    let input = match request.checkpoint() {
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
            checkpoint
        }
        None => history.initial_checkpoint().map_err(contract_error)?,
    };
    let high = history.capture_high().map_err(contract_error)?;
    let plan = history
        .plan_page(&input, &high, request.limit())
        .map_err(contract_error)?;
    let (candidates, verified) = read_verified_records(attempt, &plan).await?;
    let page = SystemStreamPagePlan::try_new(
        plan.expanded_checkpoint(),
        candidates,
        verified != high,
        request.limit(),
    )?;
    let next = if page.has_more {
        SystemStreamCheckpoint::try_new(request.source_scope().clone(), page.next_frontier)
            .map_err(contract_error)?
    } else {
        verified
    };
    let token = issue_checkpoint(attempt, &prepared, &input, &next).await?;
    let issued = SystemStreamIssuedCheckpoint::new(token, next).map_err(contract_error)?;
    SystemStreamPage::new(
        request.source_scope().clone(),
        page.records,
        issued,
        !page.has_more,
    )
    .map_err(contract_error)
}

async fn read_verified_records<A: StreamAttempt>(
    attempt: &mut A,
    plan: &SystemStreamHistoryPage,
) -> StorageResult<(Vec<StreamRecord>, SystemStreamCheckpoint)> {
    let mut records = Vec::new();
    let mut observations = Vec::new();
    let mut more = Vec::new();
    let mut encoded_bytes = 0usize;
    'pages: for reads in plan.as_ref().chunks(SYSTEM_STREAM_HISTORY_READ_BATCH) {
        let pages = attempt.read_system_history_pages(reads).await?;
        if pages.len() != reads.len() {
            return Err(history_unavailable(
                "backend returned a different number of history pages than requested",
                "system_stream.read.page_count",
            ));
        }
        for (read, result) in reads.iter().zip(pages) {
            let crate::SystemStreamHistoryReadPage {
                retired_through,
                records: page_records,
                observations: page_observations,
                pointers: _,
                more: page_more,
            } = result;
            if page_records.len() != page_observations.len() {
                return Err(history_unavailable(
                    "history records and pointer observations have different lengths",
                    "system_stream.read.observation_count",
                ));
            }
            let short = page_observations.len() < read.limit as usize;
            for (record, observation) in page_records.into_iter().zip(&page_observations) {
                if record.position != observation.position {
                    return Err(history_unavailable(
                        "history record position differs from its pointer observation",
                        "system_stream.read.observation_position",
                    ));
                }
                let record = record.into_stream_record()?;
                encoded_bytes = encoded_bytes.saturating_add(serde_json::to_vec(&record)?.len());
                records.push(record);
            }
            more.push(page_more);
            observations.push(SystemStreamHistoryObservation {
                retired_through,
                read,
                records: page_observations,
            });
            if short
                && (read.ordering == super::SystemStreamHistoryOrdering::Contiguous || page_more)
                || encoded_bytes >= storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize
            {
                break 'pages;
            }
        }
    }
    let checkpoint = plan.finish_bounded(&observations, &more).map_err(|_| {
        history_unavailable(
            "history page is not contiguous or its pointer evidence differs",
            "system_stream.read.finish_bounded",
        )
    })?;
    Ok((records, checkpoint))
}

async fn issue_checkpoint<A: StreamAttempt>(
    attempt: &mut A,
    prepared: &PreparedSystemStreamPage,
    input: &SystemStreamCheckpoint,
    next: &SystemStreamCheckpoint,
) -> StorageResult<SystemStreamCheckpointToken> {
    let token = prepared.request.checkpoint();
    if let Some(token) = token.filter(|_| input == next) {
        return Ok(token.clone());
    }
    let issue = SystemStreamCheckpointIssue::try_new(
        token,
        input,
        prepared.request.limit(),
        next,
        prepared.issued_token.clone(),
        prepared.issued_at,
    )?;
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
