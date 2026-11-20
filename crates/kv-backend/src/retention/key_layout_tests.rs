use std::num::NonZeroU64;

use storage_provider::{
    StreamTrimDueMarker, StreamTrimScope, StreamTrimState, TableHistoryTrimFrontier,
    table_retention_scope_id,
};
use storage_types::{
    StorageEnum, StorageError, StreamRetentionDuration, SystemStreamScope, TableHistoryId,
    TableName, TimestampMillis,
};

use super::key_layout::*;

fn marker(due_at: i64, id: u32, name: &str, policy_version: u64) -> StreamTrimDueMarker {
    StreamTrimDueMarker::new(
        TimestampMillis::from_timestamp(due_at),
        StreamTrimScope {
            scope_id: table_retention_scope_id(TableHistoryId::from(id)),
            table_name: TableName::try_from(name).unwrap(),
        },
        policy_version,
    )
}

fn assert_internal(error: &StorageError, expected: &str) {
    assert!(
        matches!(error.as_ref(), StorageEnum::InternalServerError { message } if message == expected),
        "unexpected error: {error}"
    );
}

fn state(scope: StreamTrimScope, next_due_at: Option<i64>) -> StreamTrimState {
    StreamTrimState {
        scope,
        policy_version: 3,
        frontier: TableHistoryTrimFrontier {
            table_history_id: TableHistoryId::from(7_u32),
            table_policy_version: NonZeroU64::MIN,
            source_scope: SystemStreamScope::try_new("retention", 1).unwrap(),
            lineages: Vec::new(),
        },
        retention: StreamRetentionDuration::from_hours(48).unwrap(),
        next_due_at: next_due_at.map(TimestampMillis::from_timestamp),
        updated_at: TimestampMillis::from_timestamp(0),
    }
}

#[test]
fn given_marker_when_encoded_then_key_and_value_decode_to_it() {
    for (due_at, id, name, policy) in [
        (-5, 7, "t1", 3),
        (1_700_000_000_000, u32::MAX - 1, "a-b.c_9", u64::MAX),
    ] {
        let expected = marker(due_at, id, name, policy);
        let key = due_key(&expected).unwrap();
        assert_eq!(key.len(), 21);
        let decoded = decode_due_marker(&key, &due_value(&expected)).unwrap();
        assert_eq!(decoded.due_at, expected.due_at);
        assert_eq!(decoded.scope.scope_id, expected.scope.scope_id);
        assert_eq!(decoded.scope.table_name, expected.scope.table_name);
        assert_eq!(decoded.policy_version, expected.policy_version);
    }
}

#[test]
fn given_markers_of_two_tables_then_keys_order_by_due_time_first() {
    let key = |due_at, id| due_key(&marker(due_at, id, "t", 1)).unwrap();
    assert!(key(1, 9) < key(2, 1));
    assert!(key(-1, 1) < key(1, 1));
}

#[test]
fn given_scope_without_table_then_due_key_is_internal() {
    let scope = StreamTrimScope {
        scope_id: "system".into(),
        table_name: TableName::try_from("t").unwrap(),
    };
    let Err(error) = due_key(&StreamTrimDueMarker::new(
        TimestampMillis::from_timestamp(1),
        scope,
        1,
    )) else {
        panic!("a scope without a table has no due key");
    };
    assert_internal(&error, "KV retention scope has no table");
}

#[test]
fn given_bad_key_or_name_then_decode_is_invalid() {
    let good = marker(1, 7, "t1", 1);
    let key = due_key(&good).unwrap();
    let value = due_value(&good);
    let mut wrong_family = key.clone();
    wrong_family[0] = b'x';
    for (key, value) in [
        (key[..20].to_vec(), value.clone()),
        (wrong_family, value),
        (key, vec![0xff]),
    ] {
        let Err(error) = decode_due_marker(&key, &value) else {
            panic!("a malformed due entry must not decode");
        };
        assert_internal(&error, "KV stream trim due key is invalid");
    }
}

#[test]
fn given_state_without_schedule_then_no_entry() {
    let scope = marker(1, 7, "t1", 3).scope;
    assert!(
        derived_due_entry(&state(scope.clone(), None))
            .unwrap()
            .is_none()
    );
    let scheduled = state(scope, Some(5_000));
    let marker = scheduled.scheduled_marker().unwrap();
    let Some((key, value)) = derived_due_entry(&scheduled).unwrap() else {
        panic!("a scheduled state derives a due entry");
    };
    assert_eq!(key, due_key(&marker).unwrap());
    assert_eq!(value, due_value(&marker));
}
