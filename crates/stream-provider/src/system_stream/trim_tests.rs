use storage_types::{SystemStreamPosition, SystemStreamScope, TimestampMillis};

use super::{SystemStreamTrimPageRequest, SystemStreamTrimRecord};

fn record(lane: &str, offset: u64, time: i64) -> SystemStreamTrimRecord {
    SystemStreamTrimRecord {
        position: SystemStreamPosition::try_new(lane, lane, offset.to_string()).unwrap(),
        created_at: TimestampMillis::from_timestamp(time),
        table_history_id: Some(1_u32.into()),
    }
}

fn request() -> SystemStreamTrimPageRequest {
    SystemStreamTrimPageRequest::try_new(
        SystemStreamScope::try_new("source", 1).unwrap(),
        TimestampMillis::from_timestamp(3000),
        10,
        None,
    )
    .unwrap()
}

#[test]
fn given_an_unexpired_head_when_older_suffix_crosses_pages_then_the_suffix_stays_protected() {
    let request = request();
    let (selected, receipt) = request
        .select_records(vec![record("a", 1, 5000)], true, &boundaries())
        .unwrap();
    assert!(selected.is_empty());
    let page = receipt.replay(&request).unwrap();
    let next = SystemStreamTrimPageRequest::try_new(
        request.scope().clone(),
        request.cutoff_timestamp(),
        10,
        page.next_continuation().cloned(),
    )
    .unwrap();
    next.validate_predecessor(Some(&receipt)).unwrap();
    let (selected, receipt) = next
        .select_records(vec![record("a", 2, 1000)], false, &boundaries())
        .unwrap();
    assert!(selected.is_empty());
    assert!(receipt.replay(&next).unwrap().next_continuation().is_none());
}

#[test]
fn given_a_blocked_lineage_when_another_lineage_arrives_then_its_expired_prefix_can_retire() {
    let request = request();
    let (selected, _) = request
        .select_records(
            vec![
                record("a", 1, 5000),
                record("a", 2, 1000),
                record("b", 1, 1000),
            ],
            false,
            &boundaries(),
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].position, record("b", 1, 1000).position);
}

#[test]
fn given_an_expired_prefix_when_timestamps_reverse_then_only_the_prefix_retires() {
    let (selected, _) = request()
        .select_records(
            vec![
                record("a", 1, 1000),
                record("a", 2, 5000),
                record("a", 3, 1000),
            ],
            false,
            &boundaries(),
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].position, record("a", 1, 1000).position);
}

fn boundaries() -> Vec<SystemStreamPosition> {
    ["a", "b"]
        .map(|lane| SystemStreamPosition::try_new(lane, lane, "100").unwrap())
        .to_vec()
}

#[test]
fn given_unsettled_native_prefix_when_later_rows_exist_then_trim_preserves_them() {
    let boundary = [SystemStreamPosition::try_new("a", "a", "1").unwrap()];
    let (selected, _) = request()
        .select_records(
            vec![record("a", 1, 1000), record("a", 3, 1000)],
            false,
            &boundary,
        )
        .unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].position, record("a", 1, 1000).position);
}
