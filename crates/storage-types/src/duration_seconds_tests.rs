use crate::{DurationSeconds, TimestampMillis};

#[test]
fn time_from_now_adds_seconds_as_milliseconds() {
    let before = TimestampMillis::now();
    let visible_at = DurationSeconds::from(30).time_from_now();
    let after = TimestampMillis::now();

    assert!(
        *visible_at >= *before + 30_000,
        "visibility timestamp should be at least 30 seconds in the future"
    );
    assert!(
        *visible_at <= *after + 30_100,
        "visibility timestamp should not add more than a small scheduling margin"
    );
}

#[test]
fn duration_seconds_adds_seconds_and_displays_raw_duration() {
    let duration = DurationSeconds::from(30) + 15;

    assert_eq!(*duration, 45);
    assert_eq!(duration.to_string(), "45");
}
