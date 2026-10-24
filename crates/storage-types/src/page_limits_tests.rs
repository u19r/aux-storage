use super::PageLimits;

#[test]
fn given_zero_or_wide_limits_when_converted_then_reject_invalid_page_bounds() {
    assert!(PageLimits::try_from((0, 1)).is_err());
    assert!(PageLimits::try_from((1, 0)).is_err());
    if let Ok(wide) = usize::try_from(u64::from(u32::MAX) + 1) {
        assert!(PageLimits::try_from((wide, 1)).is_err());
        assert!(PageLimits::try_from((1, wide)).is_err());
    }
}

#[test]
fn given_non_zero_u32_limits_when_converted_then_preserve_bounded_values() {
    let limits = PageLimits::try_from((500, 4 * 1024 * 1024)).expect("page limits");

    assert_eq!(limits.max_items.get(), 500);
    assert_eq!(limits.max_bytes.get(), 4 * 1024 * 1024);
}
