use std::{hint::black_box, time::Instant};

use storage_types::{ItemStreamVersion, StreamItemId};

use crate::{
    errors::ActiveBackfillSessionError, session_protection::merge_protected_backfill_cursor,
};

#[test]
fn active_backfill_session_keys_cover_sync_and_bootstrap_shapes() {
    let cursor = StreamItemId::from(ItemStreamVersion::new(7));
    let payload = serde_json::json!({ "protected_stream_cursor": cursor }).to_string();
    assert_eq!(
        merge_protected_backfill_cursor(None, "peer#region-b", &payload).unwrap(),
        Some(cursor)
    );
    assert_eq!(
        merge_protected_backfill_cursor(None, "bootstrap#region-c", &payload).unwrap(),
        Some(cursor)
    );
    assert_eq!(
        merge_protected_backfill_cursor(None, "catchup#learner-2", &payload).unwrap(),
        Some(cursor)
    );
    assert_eq!(
        merge_protected_backfill_cursor(None, "config#replication", &payload).unwrap(),
        None
    );
}

#[test]
fn active_backfill_session_parses_legacy_replication_cursor_field() {
    let cursor = StreamItemId::from(ItemStreamVersion::new(7));
    let payload = serde_json::json!({
        "last_system_stream_cursor": cursor,
    });

    let floor =
        merge_protected_backfill_cursor(None, "peer#region-b", &payload.to_string()).unwrap();

    assert_eq!(floor, Some(cursor));
}

#[test]
fn active_backfill_session_parses_shared_protected_cursor_field() {
    let cursor = StreamItemId::from(ItemStreamVersion::new(9));
    let payload = serde_json::json!({
        "protected_stream_cursor": cursor,
    });

    let floor =
        merge_protected_backfill_cursor(None, "catchup#learner-1", &payload.to_string()).unwrap();

    assert_eq!(floor, Some(cursor));
}

#[test]
fn inactive_control_plane_rows_are_ignored() {
    let current = StreamItemId::from(ItemStreamVersion::new(4));
    let floor = merge_protected_backfill_cursor(
        Some(current),
        "config#replication",
        r#"{"last_system_stream_cursor": 1}"#,
    )
    .unwrap();

    assert_eq!(floor, Some(current));
}

#[test]
fn active_sessions_without_cursor_fail_closed() {
    let error = merge_protected_backfill_cursor(None, "bootstrap#region-c", "{}").unwrap_err();

    assert!(
        error
            .to_string()
            .contains("missing a protected stream cursor")
    );
}

#[test]
fn active_sessions_reject_malformed_payloads_at_the_owner() {
    let error = merge_protected_backfill_cursor(None, "peer#region-b", "not-json").unwrap_err();

    assert!(matches!(
        error,
        ActiveBackfillSessionError::InvalidPayloadJson { .. }
    ));
}

#[test]
fn protected_cursor_merge_preserves_zero_and_max_ordering() {
    let zero = StreamItemId::from(ItemStreamVersion::new(0));
    let max = StreamItemId::from(ItemStreamVersion::new(u64::MAX));
    let payload = |cursor| serde_json::json!({ "protected_stream_cursor": cursor }).to_string();

    let floor = merge_protected_backfill_cursor(None, "catchup#zero", &payload(zero)).unwrap();
    assert_eq!(floor, Some(zero));
    let floor = merge_protected_backfill_cursor(floor, "catchup#max", &payload(max)).unwrap();
    assert_eq!(floor, Some(zero));
    let floor = merge_protected_backfill_cursor(floor, "catchup#zero", &payload(zero)).unwrap();
    assert_eq!(floor, Some(zero));
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

fn baseline_merge(
    current_floor: Option<StreamItemId>,
    session_key: &str,
    payload_json: &str,
) -> Option<StreamItemId> {
    if !(session_key.starts_with("peer#")
        || session_key.starts_with("bootstrap#")
        || session_key.starts_with("catchup#"))
    {
        return current_floor;
    }
    let payload: serde_json::Value = serde_json::from_str(payload_json).expect("payload");
    let cursor = payload
        .get("last_system_stream_cursor")
        .or_else(|| payload.get("protected_stream_cursor"))
        .expect("cursor");
    let cursor: StreamItemId = serde_json::from_value(cursor.clone()).expect("cursor");
    Some(current_floor.map_or(cursor, |floor| floor.min(cursor)))
}

#[test]
#[ignore = "parent-coordinated paired allocation measurement"]
fn given_session_payloads_when_cursor_merged_then_report_baseline_candidate_work() {
    let session_key = format!("peer#{}", "r".repeat(95));
    let payload = serde_json::json!({
        "protected_stream_cursor": StreamItemId::from(ItemStreamVersion::new(7)),
        "attribute-00": "x".repeat(100),
        "attribute-01": "x".repeat(100),
        "attribute-02": "x".repeat(100),
        "attribute-03": "x".repeat(100),
        "attribute-04": "x".repeat(100),
        "attribute-05": "x".repeat(100),
        "attribute-06": "x".repeat(100),
        "attribute-07": "x".repeat(100),
        "attribute-08": "x".repeat(100),
        "attribute-09": "x".repeat(100),
    })
    .to_string();
    for repetition in 0..5 {
        for (name, merge) in [
            (
                "baseline",
                baseline_merge as fn(Option<StreamItemId>, &str, &str) -> Option<StreamItemId>,
            ),
            ("candidate", |floor, key, payload| {
                merge_protected_backfill_cursor(floor, key, payload).expect("merge")
            }),
        ] {
            let guard = alloc_counter::AllocationGuard::start(
                module_path!(),
                "session_merge",
                file!(),
                line!(),
                None,
            );
            let started = Instant::now();
            for _ in 0..4096 {
                black_box(merge(
                    Some(StreamItemId::from(ItemStreamVersion::new(99))),
                    &session_key,
                    &payload,
                ));
            }
            let elapsed = started.elapsed();
            let allocation = guard.finish();
            println!(
                "session_merge,version={name},rep={repetition},ops=4096,ns_per_op={},\
                 allocations_per_op={},bytes_per_op={}",
                elapsed.as_nanos() / 4096,
                allocation.allocation_count / 4096,
                allocation.allocated_bytes / 4096,
            );
        }
    }
}
