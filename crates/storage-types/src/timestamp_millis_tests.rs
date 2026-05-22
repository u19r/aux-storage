use chrono::Duration as ChronoDuration;

use crate::{DurationSeconds, TimestampMillis, timestamp_bounds};

#[test]
fn timestamp_millis_reports_seconds_by_flooring_toward_negative_infinity() {
    assert_eq!(TimestampMillis::from_timestamp(1_999).timestamp(), 1);
    assert_eq!(TimestampMillis::from_timestamp(-1).timestamp(), -1);
}

#[test]
fn timestamp_millis_adds_domain_durations_in_milliseconds() {
    let timestamp = TimestampMillis::from_timestamp(1_700_000_000_000);
    let duration = DurationSeconds::from(45);

    assert_eq!((timestamp + duration).timestamp_millis(), 1_700_000_045_000);
    assert_eq!(
        timestamp.add_duration_seconds(&duration).timestamp_millis(),
        1_700_000_045_000
    );
}

#[test]
fn timestamp_millis_checked_add_signed_returns_none_on_overflow() {
    let timestamp = TimestampMillis::from_timestamp(i64::MAX);

    assert_eq!(
        timestamp.checked_add_signed(ChronoDuration::milliseconds(1)),
        None
    );
}

#[test]
fn timestamp_bounds_defaults_to_full_scan_range_when_bounds_are_absent() {
    let (start, end) = timestamp_bounds::<TimestampMillis, TimestampMillis>(None, None);

    assert_eq!(start.timestamp_millis(), i64::MIN);
    assert_eq!(end.timestamp_millis(), i64::MAX);
}

#[test]
fn timestamp_bounds_preserves_explicit_bounds() {
    let since = TimestampMillis::from_timestamp(100);
    let until = TimestampMillis::from_timestamp(200);

    let (start, end) = timestamp_bounds(Some(since), Some(until));

    assert_eq!(start, since);
    assert_eq!(end, until);
}
