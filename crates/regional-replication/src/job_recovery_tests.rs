use super::job_recovery::configured_controller_manifest;

#[test]
fn given_journal_generations_when_controller_membership_changes_then_revisions_advance_without_losing_supported_disabled_ids()
 {
    let qualified = crate::transport::test_support::configuration(
        1,
        "127.0.0.1:41001".parse().unwrap(),
        "127.0.0.1:41002".parse().unwrap(),
    );
    let mut configured = qualified.as_ref().as_ref().clone();
    configured.journal_generation = 0.into();
    let first = configured_controller_manifest(&configured).unwrap();
    assert_eq!(first.revision().get(), 2);
    assert_eq!(first.seed_ids().len(), 6);
    let archive = super::job_recovery::derive_controller_seed(
        crate::constants::ARCHIVE_JOB_KIND,
        &configured,
    )
    .unwrap();
    assert!(first.seed_ids().contains(&archive));
    configured.journal_generation = 1.into();
    let next = configured_controller_manifest(&configured).unwrap();
    assert!(next.revision() > first.revision());
    assert_ne!(next.seed_ids(), first.seed_ids());
    configured.journal_generation = u64::MAX.into();
    assert!(configured_controller_manifest(&configured).is_err());
}
