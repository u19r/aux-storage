use serde_json::json;

use crate::{SyncProposalPipelineLimits, SyncWriteRequest};

#[test]
fn proposal_limits_count_batch_write_operations() {
    let request = SyncWriteRequest::BatchWriteItem(
        json!({
            "RequestItems": {
                "Orders": [
                    {"PutRequest": {"Item": {"pk": {"S": "1"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "2"}}}}
                ]
            }
        })
        .try_into()
        .expect("batch write request"),
    );
    let limits = SyncProposalPipelineLimits {
        max_batch_operations: 1,
        ..SyncProposalPipelineLimits::default()
    };

    let error = limits
        .validate_request(&request)
        .expect_err("batch operation limit should reject");

    assert!(
        error
            .to_string()
            .contains("operation count 2 exceeds limit 1")
    );
}

#[test]
fn proposal_limits_reject_oversized_serialized_request() {
    let request = SyncWriteRequest::PutItem(
        json!({
            "TableName": "Orders",
            "Item": {
                "pk": {"S": "1"},
                "payload": {"S": "larger-than-limit"}
            }
        })
        .try_into()
        .expect("put request"),
    );
    let limits = SyncProposalPipelineLimits {
        max_batch_bytes: 8,
        ..SyncProposalPipelineLimits::default()
    };

    let error = limits
        .validate_request(&request)
        .expect_err("byte limit should reject");

    assert!(error.to_string().contains("byte count"));
}
