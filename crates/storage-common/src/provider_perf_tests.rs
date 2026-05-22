use std::time::Duration;

use crate::provider_perf::{record, record_amount, reset_provider, snapshot_provider};

#[test]
fn provider_perf_snapshot_is_scoped_to_provider_and_sorted_by_total_duration() {
    reset_provider("sqlite-test");
    reset_provider("rocks-test");

    record("sqlite-test", "fast", Duration::from_millis(5));
    record("sqlite-test", "slow", Duration::from_millis(10));
    record("sqlite-test", "slow", Duration::from_millis(20));
    record("rocks-test", "slow", Duration::from_secs(60));

    let snapshot = snapshot_provider("sqlite-test");

    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot[0].name, "slow");
    assert_eq!(snapshot[0].calls, 2);
    assert_eq!(snapshot[0].total, Duration::from_millis(30));
    assert_eq!(snapshot[0].max, Duration::from_millis(20));
    assert_eq!(snapshot[1].name, "fast");
    assert_eq!(snapshot[1].calls, 1);
}

#[test]
fn provider_perf_amount_counters_saturate_and_track_max_amount() {
    reset_provider("amount-test");

    record_amount("amount-test", "bytes", u64::MAX);
    record_amount("amount-test", "bytes", 10);

    let snapshot = snapshot_provider("amount-test");

    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].calls, 2);
    assert_eq!(snapshot[0].total_amount, u64::MAX);
    assert_eq!(snapshot[0].max_amount, u64::MAX);
}

#[test]
fn provider_perf_reset_removes_only_one_provider() {
    reset_provider("reset-a");
    reset_provider("reset-b");
    record("reset-a", "op", Duration::from_millis(1));
    record("reset-b", "op", Duration::from_millis(1));

    reset_provider("reset-a");

    assert!(snapshot_provider("reset-a").is_empty());
    assert_eq!(snapshot_provider("reset-b").len(), 1);
}
