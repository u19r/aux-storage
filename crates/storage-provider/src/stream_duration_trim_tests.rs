use crate::StreamTrimPageLimit;

#[test]
fn stream_trim_page_limit_rejects_invalid_bounds() {
    assert!(StreamTrimPageLimit::try_from(0usize).is_err());
    assert!(StreamTrimPageLimit::try_from(10_000usize).is_ok());
    assert!(StreamTrimPageLimit::try_from(10_001usize).is_err());
}
