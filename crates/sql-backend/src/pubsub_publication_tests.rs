use storage_types::{AttemptFailure, ConflictCause, MissExpectation, PhysicalWriteMiss};

use super::pubsub_publication::*;

#[test]
fn given_existing_identity_when_expect_insert_then_absent_miss() {
    let error = expect_insert(0).unwrap_err();
    assert_eq!(
        error.attempt_failure(),
        Some(&AttemptFailure::Conflict(ConflictCause::WriteMiss(
            PhysicalWriteMiss {
                record: "pubsub_publish_identity",
                expected: MissExpectation::Absent,
            }
        )))
    );
}
