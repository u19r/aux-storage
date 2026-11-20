use crate::pagination::normalize_limit;

#[test]
fn given_no_limit_when_normalized_then_max_is_used() {
    assert_eq!(normalize_limit(None, 100).unwrap(), 100);
}

#[test]
fn zero_invalid() {
    assert!(normalize_limit(Some(0), 100).is_err());
}

#[test]
fn clamp_applied() {
    assert_eq!(normalize_limit(Some(500), 100).unwrap(), 100);
}

#[test]
fn within_bounds() {
    assert_eq!(normalize_limit(Some(10), 100).unwrap(), 10);
}
