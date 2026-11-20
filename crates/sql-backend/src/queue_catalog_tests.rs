use storage_types::{AttemptFailure, ConflictCause, MissExpectation, PhysicalWriteMiss};

use super::queue_catalog::*;

#[test]
fn given_zero_rows_when_queue_catalog_expect_one_then_write_miss_rows() {
    let error = expect_one(0, "SQL queue catalog changed during write").unwrap_err();
    assert_eq!(
        error.attempt_failure(),
        Some(&AttemptFailure::Conflict(ConflictCause::WriteMiss(
            PhysicalWriteMiss {
                record: "SQL queue catalog changed during write",
                expected: MissExpectation::Rows {
                    expected: 1,
                    found: 0,
                },
            }
        )))
    );
}
