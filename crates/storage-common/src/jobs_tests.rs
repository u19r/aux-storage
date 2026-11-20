use std::time::Duration;

use super::{DatabaseJobIntervals, JobIntervalMillis, StorageJob};

#[test]
fn given_default_intervals_when_stale_subscription_sweep_is_read_then_hourly_and_named() {
    let job = StorageJob::PubsubStaleSubscriptionSweep;

    assert_eq!(
        job.interval(DatabaseJobIntervals::default()),
        Duration::from_secs(3_600)
    );
    assert_eq!(
        "pubsub-stale-subscription-sweep"
            .parse::<StorageJob>()
            .unwrap(),
        job
    );
    for other in StorageJob::ALL.into_iter().filter(|other| *other != job) {
        assert!(
            other.job_id() != job.job_id(),
            "{other} shares the sweep job id"
        );
    }
}

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
