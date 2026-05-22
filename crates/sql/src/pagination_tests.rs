//! Tests for pagination & limit semantics (improvement #5).
//!
//! Covers:
//! - Default limit when none provided
//! - Capping at max
//! - Rejecting zero limit
use storage_common::normalize_limit;

use crate::helpers::{DEFAULT_QUERY_LIMIT, DEFAULT_SCAN_LIMIT, MAX_QUERY_LIMIT, MAX_SCAN_LIMIT};

#[test]
fn scan_limit_defaults() {
    let eff = normalize_limit(None, DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT).unwrap();
    assert_eq!(eff, DEFAULT_SCAN_LIMIT);
}

#[test]
fn scan_limit_caps() {
    let eff = normalize_limit(
        Some(MAX_SCAN_LIMIT + 50),
        DEFAULT_SCAN_LIMIT,
        MAX_SCAN_LIMIT,
    )
    .unwrap();
    assert_eq!(eff, MAX_SCAN_LIMIT);
}

#[test]
fn query_limit_defaults() {
    let eff = normalize_limit(None, DEFAULT_QUERY_LIMIT, MAX_QUERY_LIMIT).unwrap();
    assert_eq!(eff, DEFAULT_QUERY_LIMIT);
}

#[test]
fn query_limit_caps() {
    let eff = normalize_limit(
        Some(MAX_QUERY_LIMIT + 5),
        DEFAULT_QUERY_LIMIT,
        MAX_QUERY_LIMIT,
    )
    .unwrap();
    assert_eq!(eff, MAX_QUERY_LIMIT);
}

#[test]
fn limit_zero_rejected() {
    assert!(normalize_limit(Some(0), DEFAULT_SCAN_LIMIT, MAX_SCAN_LIMIT).is_err());
}
