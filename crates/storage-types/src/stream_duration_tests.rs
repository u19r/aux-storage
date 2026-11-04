use serde_json::json;

use crate::{StreamRetentionDuration, TimestampMillis, constants::MAX_STREAM_RETENTION_HOURS};

#[test]
fn given_table_retention_when_parsed_then_forever_and_at_least_twenty_four_hours_are_accepted() {
    assert_eq!(
        StreamRetentionDuration::try_from(-1),
        Ok(StreamRetentionDuration::Forever)
    );
    assert_eq!(
        StreamRetentionDuration::try_from(24),
        Ok(StreamRetentionDuration::from_hours(24).unwrap())
    );
    assert_eq!(
        StreamRetentionDuration::try_from(i64::from(MAX_STREAM_RETENTION_HOURS)),
        Ok(StreamRetentionDuration::from_hours(i64::from(MAX_STREAM_RETENTION_HOURS)).unwrap())
    );
}

#[test]
fn stream_retention_duration_rejects_zero_negative_and_too_large_hours() {
    assert!(StreamRetentionDuration::try_from(1).is_err());
    assert!(StreamRetentionDuration::try_from(23).is_err());
    assert!(StreamRetentionDuration::try_from(0).is_err());
    assert!(StreamRetentionDuration::try_from(-2).is_err());
    assert!(StreamRetentionDuration::try_from(i64::from(MAX_STREAM_RETENTION_HOURS) + 1).is_err());
}

#[test]
fn stream_retention_duration_rejects_fractional_and_string_values() {
    assert!(serde_json::from_value::<StreamRetentionDuration>(json!(1.5)).is_err());
    assert!(serde_json::from_value::<StreamRetentionDuration>(json!("72")).is_err());
}

#[test]
fn finite_retention_computes_its_cutoff_and_forever_has_none() {
    let reference = TimestampMillis::from_timestamp(86_400_001);
    assert_eq!(
        StreamRetentionDuration::from_hours(24)
            .unwrap()
            .cutoff_at(reference),
        Some(TimestampMillis::from_timestamp(1))
    );
    assert_eq!(StreamRetentionDuration::Forever.cutoff_at(reference), None);
}
