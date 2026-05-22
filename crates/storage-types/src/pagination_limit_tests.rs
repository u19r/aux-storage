use crate::PaginationLimit;

#[test]
fn pagination_limit_clamps_absent_and_out_of_range_requests() {
    let limit = PaginationLimit::with_min(5, 25, 100);

    assert_eq!(limit.min_limit(), 5);
    assert_eq!(limit.default_limit(), 25);
    assert_eq!(limit.max_limit(), 100);
    assert_eq!(limit.clamp(None), 25);
    assert_eq!(limit.clamp(Some(1)), 5);
    assert_eq!(limit.clamp(Some(250)), 100);
}

#[test]
fn pagination_limit_clamp_usize_treats_too_large_usize_as_maximum() {
    let limit = PaginationLimit::new(10, 50);

    assert_eq!(limit.clamp_usize(Some(usize::MAX)), 50);
    assert_eq!(limit.clamp_usize(None), 10);
}

#[test]
fn pagination_limit_validation_reports_allowed_range() {
    let limit = PaginationLimit::with_min(5, 25, 100);

    assert_eq!(limit.validate(50).expect("valid limit should pass"), 50);

    let error = limit
        .validate(4)
        .expect_err("below-minimum limit should fail");
    assert_eq!(error.provided(), 4);
    assert_eq!(error.min_limit(), 5);
    assert_eq!(error.max_limit(), 100);
    assert_eq!(error.to_string(), "limit must be between 5 and 100 (got 4)");
}
