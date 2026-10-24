//! ReadSequence consistency validation at the request boundary.

use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::ReadSequenceRequest;

/// Validation rule from the canary's strong GSI scenario: reject the shape
/// before any provider read, without logging request values.
#[tokio::test]
async fn given_gsi_query_when_strong_read_sequence_is_parsed_then_validation_rejects_without_io() {
    let private_key = "private_partition_marker_7b1c";
    let parsed = observe(async {
        ReadSequenceRequest::try_from(json!({
            "ReadConsistency": "STRONG",
            "Nodes": [{"Name": "gsi", "Operation": {"Query": {
                "TableName": "Customers",
                "IndexName": "ByGroup",
                "KeyConditionExpression": "gsi_pk = :group",
                "ExpressionAttributeValues": {":group": {"S": private_key}}
            }}}],
            "Outputs": ["gsi"]
        }))
    })
    .await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    assert!(parsed.events.is_empty());
    assert!(parsed.spans.is_empty());
    assert_eq!(
        parsed.result.unwrap_err(),
        "ReadSequence STRONG consistency cannot read GSIs"
    );
}
