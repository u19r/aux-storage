use crate::{
    common::QueueCall,
    imports::*,
    queue_claim_audit::{
        ClaimEvidence, ClaimSeen, ReleaseKind, ReleaseOutcome, ReleaseSeen, audit_claims,
    },
};

const V: i64 = 3_000;
const SKEW: i64 = 500;

fn claim(body: &str, count: u64, node: u16, called: i64, returned: i64) -> ClaimSeen {
    ClaimSeen {
        body: body.to_owned(),
        receive_count: count,
        node,
        called_at: called,
        returned_at: returned,
    }
}

fn release(
    body: &str,
    count: u64,
    kind: ReleaseKind,
    outcome: ReleaseOutcome,
    called: i64,
    returned: i64,
) -> ReleaseSeen {
    ReleaseSeen {
        body: body.to_owned(),
        receive_count: count,
        kind,
        outcome,
        called_at: called,
        returned_at: returned,
    }
}

fn evidence(
    acked: &[&str],
    claims: Vec<ClaimSeen>,
    releases: Vec<ReleaseSeen>,
    drained: &[&str],
) -> ClaimEvidence {
    ClaimEvidence {
        acked_bodies: acked.iter().map(|body| (*body).to_owned()).collect(),
        claims,
        releases,
        drained: drained.iter().map(|body| (*body).to_owned()).collect(),
        ..ClaimEvidence::default()
    }
}

fn error_of(evidence: &ClaimEvidence) -> String {
    audit_claims(evidence, V, SKEW).expect_err("audit must fail")
}

#[test]
fn given_unknown_storage_error_when_classified_then_queue_call_is_unknown() {
    let error = QueueError::StorageError(StorageError::commit_unknown("x"));
    assert!(matches!(
        QueueCall::<u32>::from(Err(error)),
        QueueCall::Unknown
    ));
}

#[test]
fn given_conflict_when_classified_then_queue_call_is_aborted() {
    let error = QueueError::StorageError(StorageError::from(StorageEnum::Attempt(
        storage_types::AttemptFailure::Conflict(storage_types::ConflictCause::Logical("c")),
    )));
    assert!(matches!(
        QueueCall::<u32>::from(Err(error)),
        QueueCall::Aborted
    ));
}

#[test]
fn given_validation_error_when_classified_then_queue_call_failed() {
    let error = QueueError::validation(queue::QueueValidationKind::InvalidParameterValue);
    assert!(matches!(
        QueueCall::<u32>::from(Err(error)),
        QueueCall::Failed(_)
    ));
    assert!(matches!(QueueCall::<u32>::from(Ok(5)), QueueCall::Done(5)));
}

#[test]
fn given_sequential_claims_after_lease_when_audited_then_ok() {
    let evidence = evidence(
        &["a"],
        vec![claim("a", 1, 0, 0, 10), claim("a", 2, 0, 3_600, 3_610)],
        vec![release(
            "a",
            2,
            ReleaseKind::Delete,
            ReleaseOutcome::Acked,
            3_620,
            3_630,
        )],
        &[],
    );
    let counts = audit_claims(&evidence, V, SKEW).unwrap();
    assert_eq!(counts.effective_deletes, 1);
    assert_eq!(counts.undetermined_deletes, 0);
}

#[test]
fn given_same_count_on_two_nodes_when_audited_then_c1() {
    let evidence = evidence(
        &["a"],
        vec![claim("a", 1, 0, 0, 10), claim("a", 1, 1, 5, 15)],
        vec![],
        &["a"],
    );
    assert!(error_of(&evidence).starts_with("C1"));
}

#[test]
fn given_reclaim_inside_lease_when_audited_then_c2() {
    let evidence = evidence(
        &["a"],
        vec![claim("a", 1, 0, 0, 10), claim("a", 2, 1, 2_990, 3_000)],
        vec![],
        &["a"],
    );
    assert!(error_of(&evidence).starts_with("C2"));
}

#[test]
fn given_release_before_reclaim_when_audited_then_ok() {
    let evidence = evidence(
        &["a"],
        vec![claim("a", 1, 0, 0, 10), claim("a", 2, 1, 190, 200)],
        vec![release(
            "a",
            1,
            ReleaseKind::Visibility,
            ReleaseOutcome::Acked,
            100,
            110,
        )],
        &["a"],
    );
    assert!(audit_claims(&evidence, V, SKEW).is_ok());
}

#[test]
fn given_reclaim_after_effective_delete_when_audited_then_c3() {
    let evidence = evidence(
        &["a"],
        vec![claim("a", 1, 0, 0, 10), claim("a", 2, 1, 4_000, 4_010)],
        vec![release(
            "a",
            1,
            ReleaseKind::Delete,
            ReleaseOutcome::Acked,
            15,
            20,
        )],
        &[],
    );
    assert!(error_of(&evidence).starts_with("C3"));
}

#[test]
fn given_late_acked_delete_when_audited_then_undetermined() {
    let evidence = evidence(
        &["a"],
        vec![claim("a", 1, 0, 0, 10)],
        vec![release(
            "a",
            1,
            ReleaseKind::Delete,
            ReleaseOutcome::Acked,
            3_590,
            3_600,
        )],
        &["a"],
    );
    let counts = audit_claims(&evidence, V, SKEW).unwrap();
    assert_eq!(counts.undetermined_deletes, 1);
    assert_eq!(counts.effective_deletes, 0);
}

#[test]
fn given_acked_body_never_seen_when_audited_then_c4() {
    let evidence = evidence(
        &["a", "b"],
        vec![claim("a", 1, 0, 0, 10)],
        vec![release(
            "a",
            1,
            ReleaseKind::Delete,
            ReleaseOutcome::Acked,
            15,
            20,
        )],
        &[],
    );
    assert!(error_of(&evidence).starts_with("C4"));
}

#[test]
fn given_unknown_body_absent_when_audited_then_ok() {
    let mut evidence = evidence(&[], vec![], vec![], &[]);
    evidence.unknown_bodies.insert("u".to_owned());
    assert!(audit_claims(&evidence, V, SKEW).is_ok());
}

#[test]
fn given_unknown_send_that_did_not_commit_when_audited_then_ok_but_claims_fall_short_of_message_count()
 {
    // Decision needed: the workload's vacuity guard (`claims.len() <
    // message_count`) is not part of the audit. Two messages were sent, `u` had
    // an unknown outcome and never committed, so only `a` can be claimed. The
    // audit passes; the guard would then fire.
    let message_count = 2_usize;
    let mut evidence = evidence(&["a"], vec![claim("a", 1, 0, 0, 10)], vec![], &["a"]);
    evidence.unknown_bodies.insert("u".to_owned());
    assert!(audit_claims(&evidence, V, SKEW).is_ok());
    assert!(evidence.claims.len() < message_count);
}

#[test]
fn given_foreign_body_when_audited_then_c5() {
    let evidence = evidence(&["a"], vec![claim("x", 1, 0, 0, 10)], vec![], &["a"]);
    assert!(error_of(&evidence).starts_with("C5"));
}

#[test]
fn given_zero_receive_count_when_audited_then_c6() {
    let evidence = evidence(&["a"], vec![claim("a", 0, 0, 0, 10)], vec![], &["a"]);
    assert!(error_of(&evidence).starts_with("C6"));
}
