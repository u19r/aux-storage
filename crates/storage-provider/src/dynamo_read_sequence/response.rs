//! Wire response assembly shared by SQL and KV ReadSequence attempts.

use storage_types::{
    READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES, ReadSequencePlan, ReadSequenceRequest,
    ReadSequenceResponse, ReturnConsumedCapacity, StorageError, StorageResult,
};

use super::node::NodeOutcome;

pub(super) fn response(
    request: &ReadSequenceRequest,
    plan: &ReadSequencePlan,
    outcomes: &[Option<NodeOutcome>],
    prior_completed: &[usize],
    total_read_items: u32,
    next_sequence_token: Option<String>,
) -> ReadSequenceResponse {
    let nodes = plan
        .graph
        .outputs
        .iter()
        .filter_map(|node_id| {
            let index = node_id.index();
            (!prior_completed.contains(&index))
                .then(|| {
                    outcomes[index]
                        .as_ref()
                        .map(|outcome| outcome.result.clone())
                })
                .flatten()
        })
        .collect();
    ReadSequenceResponse {
        nodes,
        partial: next_sequence_token.is_some(),
        next_sequence_token,
        consumed_capacity: consumed_capacity(request.return_consumed_capacity, total_read_items),
        read_consistency: request.read_consistency,
    }
}

pub(super) fn enforce_response_limit(
    request: &ReadSequenceRequest,
    response: &ReadSequenceResponse,
) -> StorageResult<()> {
    let limit = request
        .max_response_bytes
        .unwrap_or(READ_SEQUENCE_DEFAULT_MAX_RESPONSE_BYTES);
    let bytes = serde_json::to_vec(response).map_err(|error| {
        StorageError::internal(&format!("serialize ReadSequence response: {error}"))
    })?;
    if bytes.len() > limit as usize {
        return Err(StorageError::validation(
            "ReadSequence response exceeds MaxResponseBytes",
        ));
    }
    Ok(())
}

fn consumed_capacity(
    capacity: Option<ReturnConsumedCapacity>,
    read_count: u32,
) -> Option<serde_json::Value> {
    match capacity? {
        ReturnConsumedCapacity::Total | ReturnConsumedCapacity::Indexes => {
            let units = f64::from(read_count).max(0.5);
            Some(serde_json::json!({
                "TableName": "ReadSequence",
                "CapacityUnits": units,
                "ReadCapacityUnits": units,
            }))
        }
        ReturnConsumedCapacity::None => None,
    }
}
