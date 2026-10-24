use std::{collections::HashMap, fmt::Write as _};

use alloc_counter::AllocationGuard;
use chrono::Duration as ChronoDuration;
use serde::Deserialize;

use crate::{AttributeValue, TimestampMillis, TimestampSeconds, from_hashmap};

#[test]
fn timestamp_seconds_deserializes_integer_json_number() {
    let parsed: TimestampSeconds =
        serde_json::from_str("1700000000").expect("integer timestamp should deserialize");
    assert_eq!(parsed.as_seconds(), 1_700_000_000);
}

#[test]
fn timestamp_seconds_rejects_fractional_json_number() {
    let err = serde_json::from_str::<TimestampSeconds>("1700000000.5")
        .expect_err("fractional timestamp must fail");
    assert!(
        err.to_string().contains("unix timestamp in whole seconds"),
        "unexpected error: {err}"
    );
}

#[derive(Debug, Deserialize)]
struct TimestampSecondsInput {
    ttl: TimestampSeconds,
}

#[test]
fn from_hashmap_accepts_integer_ttl() {
    let mut map = HashMap::new();
    map.insert(
        "ttl".to_string(),
        AttributeValue::N("1700000000".to_string()),
    );

    let parsed = from_hashmap::<TimestampSecondsInput>(map).expect("integer ttl must deserialize");
    assert_eq!(parsed.ttl.as_seconds(), 1_700_000_000);
}

#[test]
fn from_hashmap_rejects_fractional_ttl() {
    let mut map = HashMap::new();
    map.insert(
        "ttl".to_string(),
        AttributeValue::N("1700000000.5".to_string()),
    );

    let err = from_hashmap::<TimestampSecondsInput>(map)
        .expect_err("fractional ttl must fail deserialization");
    assert!(
        err.to_string().contains("unix timestamp in whole seconds"),
        "unexpected error: {err}"
    );
}

#[test]
fn timestamp_seconds_converts_millis_by_flooring_toward_negative_infinity() {
    let positive = TimestampSeconds::from(TimestampMillis::from_timestamp(1_999));
    let negative = TimestampSeconds::from(TimestampMillis::from_timestamp(-1));

    assert_eq!(positive.as_seconds(), 1);
    assert_eq!(negative.as_seconds(), -1);
}

#[test]
fn timestamp_seconds_addition_saturates_at_i64_bounds() {
    let max = TimestampSeconds::from_timestamp(i64::MAX);
    let min = TimestampSeconds::from_timestamp(i64::MIN);

    assert_eq!((max + 1).as_seconds(), i64::MAX);
    assert_eq!((min + ChronoDuration::seconds(-1)).as_seconds(), i64::MIN);
}

#[test]
fn timestamp_seconds_converts_to_millis_with_saturation() {
    let too_large = TimestampSeconds::from_timestamp(i64::MAX);
    let millis = TimestampMillis::from(too_large);

    assert_eq!(millis.timestamp_millis(), i64::MAX);
}

fn write_legacy_display(value: TimestampSeconds, output: &mut String) {
    let datetime = chrono::DateTime::from_timestamp(value.as_seconds(), 0);
    let rfc3339 = datetime.unwrap_or_else(chrono::Utc::now).to_rfc3339();
    write!(output, "{rfc3339}").expect("String accepts all writes");
}

fn write_candidate_display(value: TimestampSeconds, output: &mut String) {
    write!(output, "{value}").expect("String accepts all writes");
}

fn measure_display_allocations(
    label: &'static str,
    write_display: fn(TimestampSeconds, &mut String),
) -> alloc_counter::AllocationReport<'static> {
    let value = TimestampSeconds::from_timestamp(1_700_000_000);
    let mut output = String::with_capacity(32);
    let guard = AllocationGuard::start(
        module_path!(),
        "timestamp_seconds_display_allocation_profile",
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
fn timestamp_seconds_display_preserves_rfc3339_bytes() {
    let value = TimestampSeconds::from_timestamp(1_700_000_000);
    let mut legacy = String::new();
    write_legacy_display(value, &mut legacy);
    assert_eq!(value.to_string(), legacy);
    assert_eq!(legacy, "2023-11-14T22:13:20+00:00");

    for value in [
        TimestampSeconds::from_timestamp(-1),
        TimestampSeconds::from_timestamp(0),
        TimestampSeconds::from_timestamp(253_402_300_800),
    ] {
        let expected = chrono::DateTime::from_timestamp(value.as_seconds(), 0)
            .expect("timestamp in chrono range")
            .to_rfc3339();
        assert_eq!(value.to_string(), expected);
    }
}

#[test]
fn timestamp_seconds_display_avoids_valid_timestamp_allocations() {
    let legacy = measure_display_allocations("legacy_to_rfc3339", write_legacy_display);
    let candidate = measure_display_allocations("direct_formatter", write_candidate_display);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < legacy.allocation_count);
    assert!(candidate.allocated_bytes < legacy.allocated_bytes);
}
