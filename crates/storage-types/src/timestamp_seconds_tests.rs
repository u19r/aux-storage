use std::collections::HashMap;

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
