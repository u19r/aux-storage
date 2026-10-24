use std::collections::HashMap;

use storage_types::{AttributeValue, KeyAttributes, TableName, WireItem};

use super::observation::{
    WriteObservation, WriteObservationIdentity, WriteObservationRequirement,
    WriteObservationRequirements, WriteObservationRole, WriteObservationState,
    validate_observation_coverage,
};

fn identity(ordinal: usize, role: WriteObservationRole) -> WriteObservationIdentity {
    WriteObservationIdentity::new(
        TableName::new("items"),
        KeyAttributes::from(HashMap::from([(
            "pk".to_string(),
            AttributeValue::S(format!("key-{ordinal}")),
        )])),
        ordinal,
        role,
    )
}

#[test]
fn absent_is_real_evidence() {
    let requirement = WriteObservationRequirement::new(
        identity(0, WriteObservationRole::Write),
        WriteObservationRequirements::OldItem,
    );
    let absent = WriteObservation::new(requirement.identity.clone(), WriteObservationState::Absent);
    validate_observation_coverage(std::slice::from_ref(&requirement), vec![absent]).unwrap();
}

#[test]
fn live_legacy_observation_is_completed_with_its_bound_key() {
    let requirement = WriteObservationRequirement::new(
        identity(0, WriteObservationRole::Write),
        WriteObservationRequirements::DurableOldItem,
    );
    let observation = WriteObservation::new(
        requirement.identity.clone(),
        WriteObservationState::Live {
            item: Box::new(
                WireItem::try_from(&HashMap::from([(
                    "status".to_owned(),
                    AttributeValue::S("open".to_owned()),
                )]))
                .unwrap(),
            ),
            revision: storage_types::ItemRevision::initial(),
            provenance: vec![2].into(),
        },
    );

    let accepted = validate_observation_coverage(&[requirement], [observation]).unwrap();
    let observations = accepted.into_observations();
    let [
        WriteObservation {
            state: WriteObservationState::Live { item, .. },
            ..
        },
    ] = observations.as_slice()
    else {
        panic!("expected one live observation");
    };

    assert_eq!(
        item.attribute_value("pk").unwrap(),
        Some(AttributeValue::S("key-0".to_owned()))
    );
}

#[test]
fn coverage_accepts_reordered_identity_bound_evidence_and_rejects_swapped_payloads() {
    let first = WriteObservationRequirement::new(
        identity(0, WriteObservationRole::Write),
        WriteObservationRequirements::DurableOldItem,
    );
    let second = WriteObservationRequirement::new(
        identity(1, WriteObservationRole::Check),
        WriteObservationRequirements::OldItem,
    );
    let live = WriteObservationState::Live {
        item: Box::new(WireItem::dynamo_json(br#"{"pk":{"S":"key-0"}}"#.to_vec())),
        revision: storage_types::ItemRevision::initial(),
        provenance: vec![2].into(),
    };
    let live_second = WriteObservationState::Live {
        item: Box::new(WireItem::dynamo_json(br#"{"pk":{"S":"key-1"}}"#.to_vec())),
        revision: storage_types::ItemRevision::initial(),
        provenance: vec![2].into(),
    };
    let reordered = [
        WriteObservation::new(second.identity.clone(), live_second),
        WriteObservation::new(first.identity.clone(), WriteObservationState::Absent),
    ];
    validate_observation_coverage(&[first.clone(), second.clone()], reordered).unwrap();

    let wrong_payload = [
        WriteObservation::new(second.identity.clone(), live.clone()),
        WriteObservation::new(first.identity.clone(), live.clone()),
    ];
    assert!(
        validate_observation_coverage(&[first.clone(), second.clone()], wrong_payload).is_err(),
        "the evidence identity must match its original member"
    );

    let duplicate = [
        WriteObservation::new(first.identity.clone(), live),
        WriteObservation::new(first.identity.clone(), WriteObservationState::Absent),
    ];
    assert!(validate_observation_coverage(&[first, second], duplicate).is_err());
}

#[test]
fn coverage_rejects_duplicate_requirement_identity() {
    let first = WriteObservationRequirement::new(
        identity(0, WriteObservationRole::Write),
        WriteObservationRequirements::OldItem,
    );
    let duplicate = first.clone();
    let observations = [
        WriteObservation::new(first.identity.clone(), WriteObservationState::Absent),
        WriteObservation::new(duplicate.identity.clone(), WriteObservationState::Absent),
    ];
    assert!(validate_observation_coverage(&[first, duplicate], observations).is_err());
}

#[test]
fn direct_member_can_return_metadata_without_publishing_an_old_image() {
    let identity = identity(0, WriteObservationRole::Write);
    let metadata = WriteObservationState::LiveMetadata {
        revision: storage_types::ItemRevision::try_from(4_u64).unwrap(),
        provenance: vec![5].into(),
    };
    let no_image = WriteObservationRequirement::new(
        identity.clone(),
        WriteObservationRequirements::DurableMetadata,
    );
    validate_observation_coverage(
        std::slice::from_ref(&no_image),
        vec![WriteObservation::new(identity.clone(), metadata.clone())],
    )
    .unwrap();

    let needs_image = WriteObservationRequirement::new(
        identity.clone(),
        WriteObservationRequirements::DurableOldItem,
    );
    assert!(
        validate_observation_coverage(
            &[needs_image],
            vec![WriteObservation::new(identity, metadata)]
        )
        .is_err()
    );
}

#[test]
fn given_live_write_without_history_when_validated_then_rejected() {
    let identity = identity(0, WriteObservationRole::Write);
    let requirement =
        WriteObservationRequirement::new(identity.clone(), WriteObservationRequirements::OldItem);
    let observation = WriteObservation::new(
        identity,
        WriteObservationState::LiveImage {
            item: Box::new(WireItem::dynamo_json(br#"{"pk":{"S":"key-0"}}"#.to_vec())),
        },
    );
    assert!(validate_observation_coverage(&[requirement], [observation]).is_err());
}

#[test]
fn coverage_accepts_sparse_source_ordinals() {
    let requirement = WriteObservationRequirement::new(
        identity(7, WriteObservationRole::Write),
        WriteObservationRequirements::OldItem,
    );
    let observation =
        WriteObservation::new(requirement.identity.clone(), WriteObservationState::Absent);

    validate_observation_coverage(std::slice::from_ref(&requirement), [observation]).unwrap();
}
