use std::time::Duration;

use scheduled_bg_jobs::QueueName;

use super::{DatabaseJobIntervals, JobIntervalMillis, JobRowState, StorageJob, job_row_state};

#[test]
fn given_queue_message_cleanup_when_scheduled_then_its_own_cadence_is_used() {
    let intervals = DatabaseJobIntervals {
        queue_message_cleanup_interval_ms: JobIntervalMillis(7_000),
        ..DatabaseJobIntervals::default()
    };

    assert_eq!(
        StorageJob::QueueMessageCleanup.interval(intervals),
        Duration::from_secs(7)
    );
}

#[test]
fn given_a_foreign_scheduler_queue_when_binding_a_job_then_it_is_rejected() {
    let expiration_queue = StorageJob::TtlSweep.queue();
    let gsi_queue = StorageJob::GsiBackfill.queue();

    assert!(StorageJob::TtlSweep.owns_queue(&expiration_queue));
    assert!(!StorageJob::TtlSweep.owns_queue(&gsi_queue));
    assert!(!StorageJob::GsiBackfill.owns_queue(&expiration_queue));
    assert!(StorageJob::GsiBackfill.owns_queue(&gsi_queue));
    assert!(
        !StorageJob::GsiBackfill
            .owns_queue(&QueueName::try_new("maintenance").expect("test queue is valid"))
    );
}

#[test]
fn given_job_bytes_when_stated_then_single_machine_decides() {
    assert_eq!(job_row_state(None, None, 8, 8), JobRowState::Missing);
    assert_eq!(
        job_row_state(Some(&[1; 9]), None, 8, 8),
        JobRowState::KeyTooLarge
    );
    assert_eq!(
        job_row_state(Some(&[1; 4]), None, 8, 8),
        JobRowState::Missing
    );
    assert_eq!(
        job_row_state(Some(&[1; 4]), Some(&[2; 9]), 8, 8),
        JobRowState::ValueTooLarge
    );
    assert_eq!(
        job_row_state(Some(&[1; 4]), Some(&[2; 4]), 8, 8),
        JobRowState::Present
    );
}
