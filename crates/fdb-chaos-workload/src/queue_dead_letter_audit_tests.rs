use std::collections::BTreeMap;

use crate::{
    queue_claim_audit::ClaimSeen,
    queue_dead_letter_audit::{TargetSeen, audit_dead_letter},
};

const MAX_RECEIVE_COUNT: u64 = 2;

fn claim(body: &str, count: u64) -> ClaimSeen {
    ClaimSeen {
        body: body.to_owned(),
        receive_count: count,
        node: 0,
        called_at: 0,
        returned_at: 10,
    }
}

fn target(body: &str, id: &str, delete_acked: bool) -> TargetSeen {
    TargetSeen {
        body: body.to_owned(),
        message_id: id.to_owned(),
        delete_acked,
    }
}

fn ids(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(body, id)| ((*body).to_owned(), (*id).to_owned()))
        .collect()
}

fn audit(
    claims: &[ClaimSeen],
    ids: &BTreeMap<String, String>,
    target: &[TargetSeen],
) -> Result<u64, String> {
    audit_dead_letter(claims, ids, target, MAX_RECEIVE_COUNT)
}

#[test]
fn given_one_copy_with_same_id_when_audited_then_moved_count_is_one() {
    let result = audit(
        &[claim("a", 1), claim("a", 2)],
        &ids(&[("a", "m1")]),
        &[target("a", "m1", true)],
    );
    assert_eq!(result, Ok(1));
}

#[test]
fn given_source_claim_past_limit_when_audited_then_d1() {
    let error = audit(
        &[claim("a", 1), claim("a", 2), claim("a", 3)],
        &ids(&[("a", "m1")]),
        &[],
    )
    .unwrap_err();
    assert!(error.starts_with("D1"));
}

#[test]
fn given_two_target_ids_for_one_body_when_audited_then_d2() {
    let error = audit(
        &[],
        &ids(&[]),
        &[target("a", "m1", true), target("a", "m2", true)],
    )
    .unwrap_err();
    assert!(error.starts_with("D2"));
}

#[test]
fn given_redelivery_after_acked_delete_when_audited_then_d2() {
    let error = audit(
        &[],
        &ids(&[]),
        &[target("a", "m1", true), target("a", "m1", true)],
    )
    .unwrap_err();
    assert!(error.starts_with("D2"));
}

#[test]
fn given_redelivery_after_unknown_delete_when_audited_then_ok() {
    let result = audit(
        &[],
        &ids(&[]),
        &[target("a", "m1", false), target("a", "m1", true)],
    );
    assert_eq!(result, Ok(1));
}

#[test]
fn given_changed_identity_when_audited_then_d3() {
    let error = audit(&[], &ids(&[("a", "m1")]), &[target("a", "m9", true)]).unwrap_err();
    assert!(error.starts_with("D3"));
}
