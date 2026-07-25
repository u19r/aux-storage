use queue_provider::{QueueError, QueueInternalKind, QueueValidationKind};

use crate::manager::batch_error_entry;

#[test]
fn given_request_failure_when_building_batch_error_then_sender_fault_is_true() {
    let entry = batch_error_entry(
        "request".to_string(),
        &QueueError::validation(QueueValidationKind::InvalidParameterValue),
    );

    assert!(entry.sender_fault);
}

#[test]
fn given_internal_failure_when_building_batch_error_then_sender_fault_is_false() {
    let entry = batch_error_entry(
        "storage".to_string(),
        &QueueError::internal(QueueInternalKind::MissingQueuePartitionState),
    );

    assert!(!entry.sender_fault);
}
