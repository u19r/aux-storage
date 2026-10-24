use super::pubsub_delivery_audit::recovery_timestamp;
use crate::imports::*;

#[test]
fn recovery_clock_includes_slow_first_pass_and_lease_delay() {
    let started = TimestampMillis::from_timestamp(1_000);
    assert_eq!(
        recovery_timestamp(started, 10.0, 55.001)
            .unwrap()
            .timestamp_millis(),
        46_001
    );
}

#[test]
fn recovery_clock_rejects_invalid_simulator_time() {
    for end in [f64::NAN, f64::INFINITY, 9.0, i64::MAX as f64] {
        assert!(recovery_timestamp(TimestampMillis::from_timestamp(1_000), 10.0, end).is_err());
    }
}

#[test]
fn recovery_clock_stays_after_far_future_request_lease() {
    let request_now = TimestampMillis::from_timestamp(9_000_000);
    let recovery = recovery_timestamp(request_now, 20.0, 20.032).unwrap();
    assert!(recovery > request_now + 30);
}
