use storage_types::TimestampMillis;

use crate::{PublishChunkClaimLimit, PublishChunkClaimRequest};

#[test]
fn publish_chunk_claim_limit_rejects_invalid_bounds() {
    assert!(PublishChunkClaimLimit::try_from(0usize).is_err());
    assert!(PublishChunkClaimLimit::try_from(32usize).is_ok());
    assert!(PublishChunkClaimLimit::try_from(33usize).is_err());
}

#[test]
fn publish_chunk_claim_limit_preserves_numeric_wire_shape() {
    let request = PublishChunkClaimRequest {
        limit: PublishChunkClaimLimit::try_from(25usize).expect("valid claim limit"),
        now: TimestampMillis::from_timestamp(1_700_000_000_000),
    };
    let encoded = serde_json::to_value(&request).expect("serialize claim request");

    assert_eq!(encoded["limit"], 25);
    assert!(
        serde_json::from_value::<PublishChunkClaimRequest>(serde_json::json!({
            "limit": 0,
            "now": 1_700_000_000_000_i64,
        }))
        .is_err()
    );
}
