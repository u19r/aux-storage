use storage_common::{TtlConfigRecord, TtlMaintenanceCursor};
use storage_types::{Revision, TableName, TimeToLiveStatus};

use super::dynamo_update_ttl::TtlCheckpointChange;

#[test]
fn given_checkpoint_cursor_cycles_when_pages_finish_then_previous_bytes_never_repeat() {
    let mut config = TtlConfigRecord::try_new(
        "ttl".to_owned(),
        &TableName::new("checkpoint"),
        TimeToLiveStatus::Enabling,
    )
    .unwrap();
    let mut observations = Vec::new();
    for cursor in [
        None,
        Some(TtlMaintenanceCursor::try_from("cursor").unwrap()),
        None,
        None,
    ] {
        let change = TtlCheckpointChange::try_new(config, cursor).unwrap();
        let previous = storage_types::storage_serde::to_bytes(&change.previous).unwrap();
        assert!(!observations.contains(&previous));
        observations.push(previous);
        assert_eq!(
            change.next.checkpoint.revision,
            change.previous.revision.checked_next().unwrap()
        );
        config = change.next;
    }
    assert_eq!(config.checkpoint.revision, Revision::from(4));
}

#[test]
fn given_exhausted_checkpoint_when_page_advances_then_no_next_checkpoint_is_accepted() {
    let mut config = TtlConfigRecord::try_new(
        "ttl".to_owned(),
        &TableName::new("checkpoint"),
        TimeToLiveStatus::Enabling,
    )
    .unwrap();
    config.checkpoint.revision = Revision::from(u64::MAX);
    assert!(TtlCheckpointChange::try_new(config, None).is_err());
}
