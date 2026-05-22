use crate::pagination::normalize_limit;

#[test]
fn default_used() {
    assert_eq!(normalize_limit(None, 25, 100).unwrap(), 25);
}

#[test]
fn zero_invalid() {
    assert!(normalize_limit(Some(0), 25, 100).is_err());
}

#[test]
fn clamp_applied() {
    assert_eq!(normalize_limit(Some(500), 25, 100).unwrap(), 100);
}

#[test]
fn within_bounds() {
    assert_eq!(normalize_limit(Some(10), 25, 100).unwrap(), 10);
}
