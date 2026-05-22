use storage_types::{ItemStreamVersion, StreamItemId};

use crate::{
    is_active_backfill_session_key, merge_protected_backfill_cursor, parse_active_backfill_session,
};

#[test]
fn active_backfill_session_keys_cover_sync_and_bootstrap_shapes() {
    assert!(is_active_backfill_session_key("peer#region-b"));
    assert!(is_active_backfill_session_key("bootstrap#region-c"));
    assert!(is_active_backfill_session_key("catchup#learner-2"));
    assert!(!is_active_backfill_session_key("config#replication"));
}

#[test]
fn active_backfill_session_parses_legacy_replication_cursor_field() {
    let cursor = StreamItemId::from(ItemStreamVersion::new(7));
    let payload = serde_json::json!({
        "last_system_stream_cursor": cursor,
    });

    let session = parse_active_backfill_session("peer#region-b", &payload.to_string())
        .unwrap()
        .expect("session");

    assert_eq!(session.protected_system_stream_cursor, cursor);
}

#[test]
fn active_backfill_session_parses_shared_protected_cursor_field() {
    let cursor = StreamItemId::from(ItemStreamVersion::new(9));
    let payload = serde_json::json!({
        "protected_stream_cursor": cursor,
    });

    let session = parse_active_backfill_session("catchup#learner-1", &payload.to_string())
        .unwrap()
        .expect("session");

    assert_eq!(session.protected_system_stream_cursor, cursor);
}

#[test]
fn inactive_control_plane_rows_are_ignored() {
    let session =
        parse_active_backfill_session("config#replication", r#"{"last_system_stream_cursor": 1}"#)
            .unwrap();

    assert!(session.is_none());
}

#[test]
fn active_sessions_without_cursor_fail_closed() {
    let error = parse_active_backfill_session("bootstrap#region-c", "{}").unwrap_err();

    assert!(
        error
            .to_string()
            .contains("missing a protected stream cursor")
    );
}

#[test]
fn protected_cursor_merge_uses_oldest_active_session_cursor() {
    let later = StreamItemId::from(ItemStreamVersion::new(9));
    let earlier = StreamItemId::from(ItemStreamVersion::new(7));

    let floor = merge_protected_backfill_cursor(
        None,
        "peer#region-b",
        &serde_json::json!({ "last_system_stream_cursor": later }).to_string(),
    )
    .unwrap();
    let floor = merge_protected_backfill_cursor(
        floor,
        "catchup#learner-1",
        &serde_json::json!({ "protected_stream_cursor": earlier }).to_string(),
    )
    .unwrap();
    let floor = merge_protected_backfill_cursor(
        floor,
        "config#ignored",
        &serde_json::json!({ "protected_stream_cursor": StreamItemId::from(ItemStreamVersion::new(1)) }).to_string(),
    )
    .unwrap();

    assert_eq!(floor, Some(earlier));
}
