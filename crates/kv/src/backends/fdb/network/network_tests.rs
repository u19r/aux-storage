use super::format_grv_cache_lag_seconds;

#[test]
fn grv_cache_lag_knob_uses_seconds_without_losing_millisecond_precision() {
    assert_eq!(format_grv_cache_lag_seconds(5), "0.005");
    assert_eq!(format_grv_cache_lag_seconds(50), "0.05");
    assert_eq!(format_grv_cache_lag_seconds(1_000), "1");
    assert_eq!(format_grv_cache_lag_seconds(65_535), "65.535");
}
