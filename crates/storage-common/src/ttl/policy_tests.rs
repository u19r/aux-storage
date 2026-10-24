use storage_types::{TableName, TimeToLiveSpecification, TimeToLiveStatus, TimestampMillis};

use super::{TtlConfigRecord, TtlPolicyChange, TtlPolicyGeneration, ttl_tracking_enabled};

fn change(enabled: bool, existing: Option<&TtlConfigRecord>) -> TtlPolicyChange {
    TtlPolicyChange::try_new(
        &TableName::new("policy"),
        &TimeToLiveSpecification {
            attribute_name: "ttl".into(),
            enabled,
        },
        existing,
        TimestampMillis::from_timestamp(1000),
    )
    .unwrap()
    .into_result()
    .unwrap()
}

#[test]
fn given_policy_epoch_when_disabled_and_reenabled_then_replacement_has_new_identity() {
    let TtlPolicyChange::Enable(mut current) = change(true, None) else {
        panic!("enable");
    };
    for epoch in 1..=3 {
        assert_eq!(u64::from(current.policy_generation), epoch);
        current.status = TimeToLiveStatus::Enabled;
        assert!(matches!(
            change(true, Some(&current)),
            TtlPolicyChange::Unchanged
        ));
        let TtlPolicyChange::Disable(disabled) = change(false, Some(&current)) else {
            panic!("disable");
        };
        assert!(!ttl_tracking_enabled(Some(&disabled)));
        assert_eq!(u64::from(disabled.policy_generation), epoch);
        assert!(
            TtlConfigRecord::describe(Some(disabled.clone()))
                .attribute_name
                .is_none()
        );
        assert!(matches!(
            change(false, Some(&disabled)),
            TtlPolicyChange::Unchanged
        ));
        let bytes = storage_types::storage_serde::to_bytes(&disabled).unwrap();
        let reopened = storage_types::storage_serde::from_bytes(&bytes).unwrap();
        let TtlPolicyChange::Enable(replacement) = change(true, Some(&reopened)) else {
            panic!("replacement");
        };
        assert!(replacement.policy_generation != current.policy_generation);
        current = replacement;
    }
}

#[test]
fn given_exhausted_policy_epoch_when_reenabled_then_transition_fails_closed() {
    let TtlPolicyChange::Enable(config) = change(true, None) else {
        panic!("enable");
    };
    let TtlPolicyChange::Disable(mut disabled) = change(false, Some(&config)) else {
        panic!("disable");
    };
    disabled.policy_generation = TtlPolicyGeneration::try_from(u64::MAX).unwrap();
    assert!(
        TtlPolicyChange::try_new(
            &TableName::new("policy"),
            &TimeToLiveSpecification {
                attribute_name: "ttl".into(),
                enabled: true
            },
            Some(&disabled),
            TimestampMillis::from_timestamp(1000)
        )
        .is_err()
    );
}
