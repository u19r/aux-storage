use crate::{TimestampMillis, TimestampSecondsFractional};

#[test]
fn timestamp_seconds_fractional_deserializes_fractional_json_number_tests() {
    let parsed: TimestampSecondsFractional =
        serde_json::from_str("1700000000.554").expect("fractional timestamp should deserialize");
    assert!((parsed.as_seconds() - 1_700_000_000.554).abs() < f64::EPSILON);
}

#[test]
fn timestamp_seconds_fractional_deserializes_integer_json_number_tests() {
    let parsed: TimestampSecondsFractional =
        serde_json::from_str("1700000000").expect("integer timestamp should deserialize");
    assert!((parsed.as_seconds() - 1_700_000_000.0).abs() < f64::EPSILON);
}

#[test]
fn timestamp_seconds_fractional_from_millis_preserves_subsecond_precision_tests() {
    let millis = TimestampMillis::from_timestamp(1_700_000_000_554);
    let seconds = TimestampSecondsFractional::from(millis);
    assert!((seconds.as_seconds() - 1_700_000_000.554).abs() < f64::EPSILON);
}

#[test]
fn timestamp_seconds_fractional_to_millis_floors_fractional_part_tests() {
    let seconds = TimestampSecondsFractional::from_seconds(1_700_000_000.554_9);
    let millis = TimestampMillis::from(seconds);
    assert_eq!(millis.timestamp_millis(), 1_700_000_000_554);
}

#[test]
fn timestamp_seconds_fractional_to_millis_handles_non_finite_values() {
    let millis = TimestampMillis::from(TimestampSecondsFractional::from_seconds(f64::INFINITY));

    assert_eq!(millis.timestamp_millis(), 0);
}

#[test]
fn timestamp_seconds_fractional_to_millis_saturates_at_i64_bounds() {
    let too_large = TimestampMillis::from(TimestampSecondsFractional::from_seconds(
        (i64::MAX as f64 / 1000.0) * 2.0,
    ));
    let too_small = TimestampMillis::from(TimestampSecondsFractional::from_seconds(
        (i64::MIN as f64 / 1000.0) * 2.0,
    ));

    assert_eq!(too_large.timestamp_millis(), i64::MAX);
    assert_eq!(too_small.timestamp_millis(), i64::MIN);
}
