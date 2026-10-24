use std::collections::HashMap;

use storage_types::{AttributeValue, KeyAttributes, WireItem};

use crate::maintenance::{
    ExpirationCandidateBatch, ExpirationCandidateObservation, ExpirationIndexLocator,
    ExpirationObservedItem, ExpirationScannedCandidate,
};

fn key(value: &str) -> KeyAttributes {
    KeyAttributes::from(HashMap::from([(
        "pk".to_owned(),
        AttributeValue::S(value.to_owned()),
    )]))
}

fn candidate(value: &str, expiration: i64) -> ExpirationScannedCandidate {
    ExpirationScannedCandidate {
        index_locator: ExpirationIndexLocator {
            indexed_expiration_seconds: expiration,
            ttl_item_key: value.to_owned(),
        },
        key: key(value),
    }
}

fn item(value: &str, ttl: AttributeValue) -> WireItem {
    WireItem::try_from(&HashMap::from([
        ("pk".to_owned(), AttributeValue::S(value.to_owned())),
        ("ttl".to_owned(), ttl),
    ]))
    .unwrap()
}

#[test]
fn given_matching_current_ttl_when_candidate_is_planned_then_item_and_index_are_deleted() {
    let expected_key = key("A");
    let batch = ExpirationCandidateBatch {
        ttl_attribute: "ttl".to_owned(),
        cutoff_seconds: 10,
        candidates: vec![candidate("A", 1)],
    };

    let deletions = batch
        .plan_deletions(ExpirationCandidateObservation {
            candidate_count: 1,
            present: vec![ExpirationObservedItem {
                candidate_index: 0,
                item: item("A", AttributeValue::N("1".to_owned())),
            }],
        })
        .unwrap();

    assert_eq!(deletions.index_entries.len(), 1);
    assert_eq!(deletions.items.len(), 1);
    assert_eq!(deletions.items[0].key, expected_key);
}

#[test]
fn given_changed_or_absent_item_when_candidates_are_planned_then_only_stale_indexes_are_deleted() {
    let batch = ExpirationCandidateBatch {
        ttl_attribute: "ttl".to_owned(),
        cutoff_seconds: 10,
        candidates: vec![candidate("A", 1), candidate("B", 2)],
    };

    let deletions = batch
        .plan_deletions(ExpirationCandidateObservation {
            candidate_count: 2,
            present: vec![ExpirationObservedItem {
                candidate_index: 0,
                item: item("A", AttributeValue::N("4000000000".to_owned())),
            }],
        })
        .unwrap();

    assert_eq!(deletions.index_entries.len(), 2);
    assert!(deletions.items.is_empty());
}

#[test]
fn given_malformed_authoritative_ttl_when_candidate_is_planned_then_evaluation_fails_closed() {
    let batch = ExpirationCandidateBatch {
        ttl_attribute: "ttl".to_owned(),
        cutoff_seconds: 10,
        candidates: vec![candidate("A", 1)],
    };

    let result = batch.plan_deletions(ExpirationCandidateObservation {
        candidate_count: 1,
        present: vec![ExpirationObservedItem {
            candidate_index: 0,
            item: item("A", AttributeValue::N("not-a-number".to_owned())),
        }],
    });

    assert!(result.is_err());
}

#[test]
fn given_unordered_current_observations_when_candidate_is_planned_then_evaluation_rejects_them() {
    let batch = ExpirationCandidateBatch {
        ttl_attribute: "ttl".to_owned(),
        cutoff_seconds: 10,
        candidates: vec![candidate("A", 1), candidate("B", 2)],
    };

    let result = batch.plan_deletions(ExpirationCandidateObservation {
        candidate_count: 2,
        present: vec![
            ExpirationObservedItem {
                candidate_index: 1,
                item: item("B", AttributeValue::N("2".to_owned())),
            },
            ExpirationObservedItem {
                candidate_index: 0,
                item: item("A", AttributeValue::N("1".to_owned())),
            },
        ],
    });

    assert!(result.is_err());
}
