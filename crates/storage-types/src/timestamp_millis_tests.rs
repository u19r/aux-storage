use std::fmt::Write as _;

use alloc_counter::AllocationGuard;
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

fn write_legacy_display(value: TimestampMillis, output: &mut String) {
    let datetime = chrono::DateTime::from_timestamp_millis(value.timestamp_millis())
        .unwrap_or_else(chrono::Utc::now);
    let rfc3339 = datetime.to_rfc3339();
    write!(output, "{rfc3339}").expect("String accepts all writes");
}

fn write_candidate_display(value: TimestampMillis, output: &mut String) {
    write!(output, "{value}").expect("String accepts all writes");
}

fn measure_display_allocations(
    label: &'static str,
    write_display: fn(TimestampMillis, &mut String),
) -> alloc_counter::AllocationReport<'static> {
    let value = TimestampMillis::from_timestamp(1_700_000_000_123);
    let mut output = String::with_capacity(32);
    let guard = AllocationGuard::start(
        module_path!(),
        "timestamp_millis_display_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        output.clear();
        write_display(value, &mut output);
        std::hint::black_box(&output);
    }
    guard.finish()
}

#[test]
fn given_millisecond_timestamp_when_displayed_then_rfc3339_bytes_are_preserved() {
    assert_eq!(
        TimestampMillis::from_timestamp(1_700_000_000_123).to_string(),
        "2023-11-14T22:13:20.123+00:00"
    );
}

#[test]
fn given_repeated_timestamp_displays_when_profiled_then_direct_formatting_avoids_allocations() {
    let legacy = measure_display_allocations("legacy_to_rfc3339", write_legacy_display);
    let candidate = measure_display_allocations("direct_formatter", write_candidate_display);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < legacy.allocation_count);
    assert!(candidate.allocated_bytes < legacy.allocated_bytes);
}
