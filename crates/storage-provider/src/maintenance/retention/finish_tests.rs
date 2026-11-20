use storage_types::{AttemptKind, StorageEnum, TableName, TimestampMillis};

use super::{
    StreamRetentionFinishRecord, StreamTrimDueMarker, StreamTrimScope, StreamTrimStateRecord,
    finish::*,
};

fn marker() -> StreamTrimDueMarker {
    StreamTrimDueMarker {
        due_at: TimestampMillis::from_timestamp(10),
        scope: StreamTrimScope {
            scope_id: "scope".into(),
            table_name: TableName::try_from("t").unwrap(),
        },
        policy_version: 1,
    }
}

fn state(write: bool) -> StreamRetentionFinishRecord {
    StreamRetentionFinishRecord {
        due_at: 10,
        scope_id: "scope".into(),
        policy_version: 1,
        expected_state_blob: write.then(|| vec![1]),
        valid_update: true,
        state_write: write.then(|| StreamTrimStateRecord {
            scope_id: "scope".into(),
            state_blob: vec![2],
            updated_at: 0,
            next_marker: None,
            replaced_marker: None,
        }),
    }
}

fn prepared(write: bool) -> PreparedRetentionFinish {
    PreparedRetentionFinish {
        marker: marker(),
        marker_blob: vec![],
        record: state(write),
    }
}

#[test]
fn given_required_state_write_when_marker_not_completed_then_write_miss() {
    let Err(error) = prepared(true).settle(false) else {
        panic!("a required state write must miss when the marker was not deleted");
    };
    assert_eq!(error.attempt_kind(), Some(AttemptKind::Conflict));
    assert!(!matches!(
        error.as_ref(),
        StorageEnum::InternalServerError { .. }
    ));
}

#[test]
fn given_no_state_write_when_marker_not_completed_then_nothing() {
    assert!(matches!(prepared(false).settle(false), Ok(None)));
}

#[test]
fn given_no_state_write_when_marker_completed_then_nothing_follows() {
    assert!(matches!(prepared(false).settle(true), Ok(None)));
}

#[test]
fn given_state_write_when_marker_completed_then_state_follows_with_marker_scope() {
    let Ok(Some(write)) = prepared(true).settle(true) else {
        panic!("a completed marker with a state record must yield the state write");
    };
    assert!(write.scope == marker().scope);
    assert_eq!(write.record.state_blob, vec![2]);
}
