use std::collections::BTreeSet;

use crate::table_recreate_audit::{PutOutcome, PutSeen, audit_recreate, is_vacuous};

const LAST_CREATE: (i64, i64) = (1_000, 1_100);

fn put(key: &str, called: i64, returned: i64, outcome: PutOutcome) -> PutSeen {
    PutSeen {
        key: key.to_owned(),
        called_at: called,
        returned_at: returned,
        outcome,
    }
}

fn keys(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|key| (*key).to_owned()).collect()
}

fn old_and_new() -> Vec<PutSeen> {
    vec![
        put("o", 0, 50, PutOutcome::Done),
        put("n", 1_200, 1_250, PutOutcome::Done),
    ]
}

#[test]
fn given_old_put_absent_and_new_put_present_when_audited_then_ok() {
    let puts = old_and_new();
    assert!(audit_recreate(&puts, LAST_CREATE, &keys(&["n"]), &keys(&["n"])).is_ok());
    assert!(!is_vacuous(&puts, LAST_CREATE));
}

#[test]
fn given_old_put_present_when_audited_then_r1() {
    let present = keys(&["o", "n"]);
    let error = audit_recreate(&old_and_new(), LAST_CREATE, &present, &present).unwrap_err();
    assert!(error.starts_with("R1"));
}

#[test]
fn given_stray_key_present_when_audited_then_r1() {
    let present = keys(&["n", "zz"]);
    let error = audit_recreate(&old_and_new(), LAST_CREATE, &present, &present).unwrap_err();
    assert!(error.starts_with("R1"));
}

#[test]
fn given_new_acked_put_absent_when_audited_then_r2() {
    let error = audit_recreate(&old_and_new(), LAST_CREATE, &keys(&[]), &keys(&[])).unwrap_err();
    assert!(error.starts_with("R2"));
}

#[test]
fn given_overlapping_put_absent_when_audited_then_ok() {
    let puts = vec![put("p", 900, 1_050, PutOutcome::Done)];
    assert!(audit_recreate(&puts, LAST_CREATE, &keys(&[]), &keys(&[])).is_ok());
}

#[test]
fn given_missing_put_present_when_audited_then_r3() {
    let puts = vec![put("m", 1_200, 1_250, PutOutcome::Missing)];
    let present = keys(&["m"]);
    let error = audit_recreate(&puts, LAST_CREATE, &present, &present).unwrap_err();
    assert!(error.starts_with("R3"));
}

#[test]
fn given_cached_and_fresh_views_differ_when_audited_then_r4() {
    let error = audit_recreate(&old_and_new(), LAST_CREATE, &keys(&["n"]), &keys(&[])).unwrap_err();
    assert!(error.starts_with("R4"));
}

#[test]
fn given_no_put_after_last_create_when_checked_then_vacuous() {
    let puts = vec![put("o", 0, 50, PutOutcome::Done)];
    assert!(is_vacuous(&puts, LAST_CREATE));
}

#[test]
fn given_put_after_last_create_missing_table_when_audited_then_r2() {
    let puts = vec![put("m", 1_200, 1_250, PutOutcome::Missing)];
    let error = audit_recreate(&puts, LAST_CREATE, &keys(&[]), &keys(&[])).unwrap_err();
    assert!(error.starts_with("R2"));
}
