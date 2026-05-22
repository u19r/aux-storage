use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};

use http_error::HttpApiError;
use serde::de::DeserializeOwned;
use storage_sync::{
    SyncMutationResponse, SyncProposalId, SyncProposalResponse, SyncWriteProposalRequest,
    SyncWriteRequest,
};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_api_manager::{record_sync_proposal_wait_time, record_sync_write_reject},
    },
    sync_response_correlation::{
        SyncResponseCorrelationDecision, SyncResponseCorrelationGate,
        plan_sync_response_correlation,
    },
};

impl StorageApiManagerImpl {
    pub(super) async fn propose_sync_write_if_configured(
        &self,
        request: SyncWriteRequest,
    ) -> Result<Option<SyncProposalResponse>, HttpApiError> {
        let Some(proposer) = self.sync_write_proposer.as_ref() else {
            return Ok(None);
        };
        let _admission = self.sync_proposal_pipeline.admit(&request)?;
        let proposal_id = sync_proposal_id_for_request(&request)?;
        let started = std::time::Instant::now();
        let result = proposer
            .propose_sync_write(SyncWriteProposalRequest::new(proposal_id, request))
            .await
            .map(Some);
        record_sync_proposal_wait_time(started.elapsed());
        if result.is_err() {
            record_sync_write_reject("raft_proposal");
        }
        result
    }
}

fn sync_proposal_id_for_request(
    request: &SyncWriteRequest,
) -> Result<SyncProposalId, HttpApiError> {
    let proposal_id = match request {
        SyncWriteRequest::TransactWriteItems(request) => request
            .client_request_token
            .as_deref()
            .map(str::trim)
            .filter(|token| !token.is_empty())
            .map(|token| format!("TransactWriteItems#client_request_token#{token}")),
        _ => None,
    }
    .unwrap_or_else(|| next_generated_sync_proposal_id(request.operation_name()));
    SyncProposalId::new(proposal_id)
        .map_err(|error| HttpApiError::validation_error(error.to_string()))
}

fn next_generated_sync_proposal_id(operation_name: &str) -> String {
    static PROCESS_PREFIX: OnceLock<String> = OnceLock::new();
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);

    let prefix = PROCESS_PREFIX.get_or_init(|| uuid::Uuid::new_v4().simple().to_string());
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{operation_name}#process#{prefix}#{sequence}")
}

pub(super) fn sync_response_at<T>(
    response: &SyncProposalResponse,
    index: usize,
    default: T,
) -> Result<T, HttpApiError>
where
    T: DeserializeOwned,
{
    match plan_response_correlation(response, index, false) {
        SyncResponseCorrelationDecision::UseDefault => Ok(default),
        SyncResponseCorrelationDecision::DecodePayload => {
            sync_response(response.responses.get(index), default)
        }
        SyncResponseCorrelationDecision::MissingEntry
        | SyncResponseCorrelationDecision::MissingPayload => {
            Err(HttpApiError::internal_server_error(
                "optional sync response correlation produced required-only decision",
            ))
        }
    }
}

pub(super) fn required_sync_response_at<T>(
    response: &SyncProposalResponse,
    index: usize,
    operation_name: &str,
) -> Result<T, HttpApiError>
where
    T: DeserializeOwned,
{
    match plan_response_correlation(response, index, true) {
        SyncResponseCorrelationDecision::DecodePayload => response
            .responses
            .get(index)
            .and_then(|response| response.response_json.as_ref())
            .ok_or_else(|| {
                HttpApiError::internal_server_error(
                    "required sync response correlation lost payload",
                )
            })
            .and_then(|response_json| {
                serde_json::from_str(response_json)
                    .map_err(|error| HttpApiError::internal_server_error(error.to_string()))
            }),
        SyncResponseCorrelationDecision::MissingEntry => Err(HttpApiError::internal_server_error(
            format!("{operation_name} sync proposal response missing response entry {index}"),
        )),
        SyncResponseCorrelationDecision::MissingPayload => {
            Err(HttpApiError::internal_server_error(format!(
                "{operation_name} sync proposal response missing response payload"
            )))
        }
        SyncResponseCorrelationDecision::UseDefault => Err(HttpApiError::internal_server_error(
            "required sync response correlation produced optional-only decision",
        )),
    }
}

fn sync_response<T>(
    response: Option<&SyncMutationResponse>,
    default: T,
) -> Result<T, HttpApiError>
where
    T: DeserializeOwned,
{
    let Some(response) = response else {
        return Ok(default);
    };
    let Some(response_json) = response.response_json.as_ref() else {
        return Ok(default);
    };
    serde_json::from_str(response_json)
        .map_err(|error| HttpApiError::internal_server_error(error.to_string()))
}

fn plan_response_correlation(
    response: &SyncProposalResponse,
    index: usize,
    required: bool,
) -> SyncResponseCorrelationDecision {
    plan_sync_response_correlation(SyncResponseCorrelationGate {
        response_count: response.responses.len(),
        index,
        payload_present: response
            .responses
            .get(index)
            .and_then(|response| response.response_json.as_ref())
            .is_some(),
        required,
    })
}
