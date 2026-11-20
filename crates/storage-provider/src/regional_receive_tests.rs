use regional_replication::message::{LineageRef, ReplicationPage};
use storage_types::{
    PairId, RegionId, RegionalMark, RegionalMarker, RegionalOrder, RegionalRole, StoredWireItem,
    SystemStreamOffset, SystemStreamPosition, TableName, WireItem,
};

use crate::{
    LineageCursor,
    regional_receive::{
        Coverage, Incoming, ItemChange, KeyOutcome, KeyState, MarkerChange, coverage, resolve_key,
    },
};

fn region(n: u16) -> RegionId {
    RegionId(std::num::NonZeroU16::new(n).unwrap())
}

fn o(stamp: u64, r: u16) -> RegionalOrder {
    RegionalOrder::from_parts(stamp, region(r), false)
}

fn item() -> StoredWireItem {
    StoredWireItem {
        item: WireItem::dynamo_json(br#"{"pk":{"S":"k"}}"#.to_vec()),
        indexers: Vec::new(),
    }
}

fn off(n: u64) -> SystemStreamOffset {
    SystemStreamOffset::try_new(format!("{n:020}-vs00000000")).unwrap()
}

fn marker(order: RegionalOrder, received: bool) -> RegionalMarker {
    RegionalMarker { order, received }
}

fn mark(order: RegionalOrder, role: RegionalRole) -> RegionalMark {
    RegionalMark {
        order,
        role,
        hidden: false,
    }
}

#[test]
fn given_absent_when_put_then_put_and_keep() {
    let item = item();
    let outcome = resolve_key(KeyState::Absent, false, [Incoming::Put(o(5, 2), &item)]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Put {
                order: o(5, 2),
                item: &item
            },
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_live_higher_when_put_lower_then_none() {
    let item = item();
    let outcome = resolve_key(
        KeyState::Live(o(9, 1)),
        false,
        [Incoming::Put(o(5, 2), &item)],
    );
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_live_equal_when_put_then_none() {
    let item = item();
    let outcome = resolve_key(
        KeyState::Live(o(5, 2)),
        false,
        [Incoming::Put(o(5, 2), &item)],
    );
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_marker_when_put_higher_then_put_and_delete_marker() {
    let item = item();
    let initial = KeyState::Marker(marker(o(5, 1), false));
    let outcome = resolve_key(initial, false, [Incoming::Put(o(6, 2), &item)]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Put {
                order: o(6, 2),
                item: &item
            },
            marker: MarkerChange::Delete
        }
    );
}

#[test]
fn given_marker_higher_when_put_then_none() {
    let item = item();
    let initial = KeyState::Marker(marker(o(9, 1), false));
    let outcome = resolve_key(initial, false, [Incoming::Put(o(5, 2), &item)]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_live_when_delete_higher_active_then_remove_echo_and_keep() {
    let outcome = resolve_key(KeyState::Live(o(5, 1)), false, [Incoming::Delete(o(6, 2))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Remove {
                mark: mark(o(6, 2), RegionalRole::Echo)
            },
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_absent_when_delete_active_then_hidden_echo() {
    let outcome = resolve_key(KeyState::Absent, false, [Incoming::Delete(o(6, 2))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Hidden {
                mark: mark(o(6, 2), RegionalRole::Echo)
            },
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_origin_marker_equal_when_delete_then_hidden_echo_and_delete_marker() {
    let initial = KeyState::Marker(marker(o(5, 1), false));
    let outcome = resolve_key(initial, false, [Incoming::Delete(o(5, 1))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Hidden {
                mark: mark(o(5, 1), RegionalRole::Echo)
            },
            marker: MarkerChange::Delete
        }
    );
}

#[test]
fn given_origin_marker_higher_when_delete_lower_then_none() {
    let initial = KeyState::Marker(marker(o(9, 1), false));
    let outcome = resolve_key(initial, false, [Incoming::Delete(o(5, 2))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_joining_absent_when_delete_then_store_received_marker_and_no_entry() {
    let outcome = resolve_key(KeyState::Absent, true, [Incoming::Delete(o(5, 2))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Store(marker(o(5, 2), true))
        }
    );
}

#[test]
fn given_joining_live_when_delete_then_remove_applied_and_store_received() {
    let outcome = resolve_key(KeyState::Live(o(3, 1)), true, [Incoming::Delete(o(5, 2))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Remove {
                mark: mark(o(5, 2), RegionalRole::Applied)
            },
            marker: MarkerChange::Store(marker(o(5, 2), true))
        }
    );
}

#[test]
fn given_origin_marker_when_echo_same_order_then_delete_marker_no_entry() {
    let initial = KeyState::Marker(marker(o(5, 1), false));
    let outcome = resolve_key(initial, false, [Incoming::Echo(o(5, 1))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Delete
        }
    );
}

#[test]
fn given_origin_marker_when_echo_other_order_then_keep() {
    let initial = KeyState::Marker(marker(o(5, 1), false));
    let outcome = resolve_key(initial, false, [Incoming::Echo(o(4, 1))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_received_marker_when_echo_then_keep() {
    let initial = KeyState::Marker(marker(o(5, 1), true));
    let outcome = resolve_key(initial, false, [Incoming::Echo(o(5, 1))]);
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::None,
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_put_then_delete_in_one_page_when_absent_then_hidden_echo_of_delete() {
    let item = item();
    let outcome = resolve_key(
        KeyState::Absent,
        false,
        [Incoming::Put(o(5, 2), &item), Incoming::Delete(o(6, 2))],
    );
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Hidden {
                mark: mark(o(6, 2), RegionalRole::Echo)
            },
            marker: MarkerChange::Keep
        }
    );
}

#[test]
fn given_live_when_delete_then_put_in_one_page_then_put_modify() {
    let item = item();
    let outcome = resolve_key(
        KeyState::Live(o(1, 1)),
        false,
        [Incoming::Delete(o(2, 2)), Incoming::Put(o(3, 2), &item)],
    );
    assert_eq!(
        outcome,
        KeyOutcome {
            item: ItemChange::Put {
                order: o(3, 2),
                item: &item
            },
            marker: MarkerChange::Keep
        }
    );
}

fn page(after: Option<u64>, through: Option<u64>, sealed: bool) -> ReplicationPage {
    let position = SystemStreamPosition::try_new("s1", "l1", "0").unwrap();
    ReplicationPage {
        pair: PairId::from_bytes([1; 16]),
        table: TableName::new("t"),
        lineage: LineageRef {
            shard: position.shard_id().clone(),
            lineage: position.lineage_id().clone(),
        },
        parents: Vec::new(),
        after: after.map(off),
        through: through.map(off),
        sealed,
        entries: Vec::new(),
    }
}

fn cursor(through: Option<u64>, sealed: bool) -> LineageCursor {
    LineageCursor {
        through: through.map(off),
        sealed,
    }
}

#[test]
fn given_cursor_and_page_when_coverage_then_gap_covered_or_apply() {
    let cases = [
        (
            cursor(None, false),
            page(None, Some(2), false),
            Coverage::Apply { skip_through: None },
        ),
        (
            cursor(Some(2), false),
            page(None, Some(2), false),
            Coverage::Covered,
        ),
        (
            cursor(Some(2), false),
            page(Some(1), Some(3), false),
            Coverage::Apply {
                skip_through: Some(off(2)),
            },
        ),
        (
            cursor(Some(1), false),
            page(Some(2), Some(3), false),
            Coverage::Gap,
        ),
        (
            cursor(Some(2), false),
            page(Some(2), Some(2), true),
            Coverage::Apply { skip_through: None },
        ),
        (
            cursor(Some(2), true),
            page(Some(2), Some(2), true),
            Coverage::Covered,
        ),
    ];
    for (index, (cursor, page, expected)) in cases.into_iter().enumerate() {
        assert_eq!(coverage(&cursor, &page).unwrap(), expected, "case {index}");
    }
}
