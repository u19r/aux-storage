//! Receive attempt through real provider operations (REG-6): contiguity,
//! per-key LWW, echo, markers.

use std::sync::Arc;

use regional_replication::message::{
    LineageRef, PageAck, PageChange, PageEntry, RegionalRejection, RegionalResponse,
    ReplicationPage,
};
use serde_json::json;
use storage_types::{
    AttributeValue, GetItemRequest, GetRecordsRequest, KeyAttributes, PairId, PairState,
    RegionalBinding, RegionalOrder, RegionalRole, StoredWireItem, SystemStreamOffset,
    SystemStreamPosition, TableName, WireItem,
};
use stream_provider::{PositionedStreamEntry, StreamChange};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, find_populated_shard, iterator},
    regional_write_tests::{delete, pair, put, region},
    test_support::TestStore,
};

pub(super) fn off(n: u64) -> SystemStreamOffset {
    SystemStreamOffset::try_new(format!("{n:020}-vs00000000")).unwrap()
}

pub(super) fn o(stamp: u64) -> RegionalOrder {
    RegionalOrder::from_parts(stamp, region(2), false)
}

pub(super) fn lineage(shard: &str, lineage: &str) -> LineageRef {
    let position = SystemStreamPosition::try_new(shard, lineage, "0").unwrap();
    LineageRef {
        shard: position.shard_id().clone(),
        lineage: position.lineage_id().clone(),
    }
}

pub(super) fn keys(key: &str) -> KeyAttributes {
    KeyAttributes::from_iter([("pk".into(), AttributeValue::S(key.into()))])
}

fn stored(key: &str) -> StoredWireItem {
    StoredWireItem {
        item: WireItem::dynamo_json(
            serde_json::to_vec(&json!({"pk": {"S": key}, "value": {"S": "received"}})).unwrap(),
        ),
        indexers: Vec::new(),
    }
}

pub(super) fn put_entry(n: u64, key: &str, stamp: u64) -> PageEntry {
    PageEntry {
        position: off(n),
        keys: keys(key),
        order: o(stamp),
        change: PageChange::Put {
            item: Box::new(stored(key)),
        },
    }
}

pub(super) fn delete_entry(n: u64, key: &str, stamp: u64) -> PageEntry {
    PageEntry {
        position: off(n),
        keys: keys(key),
        order: o(stamp),
        change: PageChange::Delete,
    }
}

pub(super) fn echo_entry(n: u64, key: &str, order: RegionalOrder) -> PageEntry {
    PageEntry {
        position: off(n),
        keys: keys(key),
        order,
        change: PageChange::Echo,
    }
}

pub(super) struct Lane<'a> {
    pub(super) pair: PairId,
    pub(super) table: &'a TableName,
    pub(super) lineage: LineageRef,
    pub(super) parents: Vec<LineageRef>,
}

impl Lane<'_> {
    pub(super) fn page(
        &self,
        after: Option<u64>,
        through: u64,
        sealed: bool,
        entries: Vec<PageEntry>,
    ) -> ReplicationPage {
        ReplicationPage {
            pair: self.pair,
            table: self.table.clone(),
            lineage: self.lineage.clone(),
            parents: if after.is_none() {
                self.parents.clone()
            } else {
                Vec::new()
            },
            after: after.map(off),
            through: Some(off(through)),
            sealed,
            entries,
        }
    }
}

pub(super) fn lane<'a>(pair: &RegionalBinding, table: &'a TableName, name: &str) -> Lane<'a> {
    Lane {
        pair: pair.pair,
        table,
        lineage: lineage("s1", name),
        parents: Vec::new(),
    }
}

pub(super) async fn receive(store: &TestStore, page: ReplicationPage) -> RegionalResponse {
    store.regional_receive.receive_page(page).await.unwrap()
}

pub(super) fn ack(response: RegionalResponse) -> PageAck {
    match response {
        RegionalResponse::Ack(ack) => ack,
        other => panic!("ack expected, got {other:?}"),
    }
}

pub(super) async fn get(
    store: &TestStore,
    table: &TableName,
    key: &str,
) -> Option<serde_json::Value> {
    let read = store
        .operations
        .get_item(
            GetItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": key}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    serde_json::to_value(read).unwrap().get("Item").cloned()
}

/// Canonical history entries of the table, ordered by their regional order.
pub(super) async fn history(store: &TestStore, table: &TableName) -> Vec<PositionedStreamEntry> {
    store.wait_until_changes_are_readable().await;
    let reads = super::image_retention_tests::canonical_history_reads(store, table)
        .await
        .unwrap();
    if reads.is_empty() {
        return Vec::new();
    }
    let mut entries = (store.canonical_history_read)(reads)
        .await
        .unwrap()
        .into_iter()
        .flat_map(|page| page.entries)
        .collect::<Vec<_>>();
    entries.sort_by_key(|row| row.entry.regional().map(|mark| mark.order));
    entries
}

pub(super) fn for_key<'a>(
    entries: &'a [PositionedStreamEntry],
    key: &str,
) -> Vec<&'a PositionedStreamEntry> {
    entries
        .iter()
        .filter(|row| row.entry.keys() == &keys(key))
        .collect()
}

async fn public_count(store: &TestStore, table: &TableName, arn: &str) -> usize {
    store.wait_until_changes_are_readable().await;
    let shard = find_populated_shard(store, arn).await;
    let start = iterator(store, arn, &shard, "TRIM_HORIZON", None).await;
    store
        .operations
        .get_records(GetRecordsRequest {
            shard_iterator: start,
            limit: Some(100),
        })
        .await
        .unwrap_or_else(|error| panic!("get_records on {table:?} failed: {error:?}"))
        .records
        .len()
}

async fn active_table(store: &TestStore) -> (TableName, String, RegionalBinding) {
    let (table, arn) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(store, &table, PairState::Active).await;
    (table, arn, binding)
}

#[tokio::test]
async fn given_active_pair_when_page_of_puts_then_items_with_order_and_applied_entries() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    let lane = lane(&binding, &table, "l1");
    let response = receive(
        &store,
        lane.page(
            None,
            2,
            false,
            vec![put_entry(1, "k1", 1), put_entry(2, "k2", 2)],
        ),
    )
    .await;
    assert_eq!(
        ack(response),
        PageAck {
            through: Some(off(2)),
            sealed: false
        }
    );
    assert!(get(&store, &table, "k1").await.is_some());
    assert!(get(&store, &table, "k2").await.is_some());
    let entries = history(&store, &table).await;
    assert_eq!(entries.len(), 2);
    for (entry, stamp) in entries.iter().zip([1, 2]) {
        let mark = entry.entry.regional().unwrap();
        assert_eq!(mark.role, RegionalRole::Applied);
        assert_eq!(mark.order, o(stamp));
        assert!(!mark.hidden);
    }
    store.finish().await;
}

#[tokio::test]
async fn given_applied_page_when_repeated_then_covered_and_no_new_entry() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    let lane = lane(&binding, &table, "l1");
    let entries = || vec![put_entry(1, "k1", 1), put_entry(2, "k2", 2)];
    receive(&store, lane.page(None, 2, false, entries())).await;
    let response = receive(&store, lane.page(None, 2, false, entries())).await;
    assert_eq!(
        ack(response),
        PageAck {
            through: Some(off(2)),
            sealed: false
        }
    );
    assert_eq!(history(&store, &table).await.len(), 2);
    store.finish().await;
}

#[tokio::test]
async fn given_cursor_when_page_after_ahead_then_gap_and_no_change() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    let lane = lane(&binding, &table, "l1");
    receive(
        &store,
        lane.page(
            None,
            2,
            false,
            vec![put_entry(1, "k1", 1), put_entry(2, "k2", 2)],
        ),
    )
    .await;
    let response = receive(
        &store,
        lane.page(Some(5), 6, false, vec![put_entry(6, "k3", 3)]),
    )
    .await;
    match response {
        RegionalResponse::Gap { expected } => assert_eq!(expected, Some(off(2))),
        other => panic!("gap expected, got {other:?}"),
    }
    assert!(get(&store, &table, "k3").await.is_none());
    assert_eq!(history(&store, &table).await.len(), 2);
    store.finish().await;
}

#[tokio::test]
async fn given_lost_ack_when_larger_page_overlaps_then_only_new_entries_apply() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    let lane = lane(&binding, &table, "l1");
    receive(
        &store,
        lane.page(
            None,
            2,
            false,
            vec![put_entry(1, "k1", 1), put_entry(2, "k2", 2)],
        ),
    )
    .await;
    // The overlap repeats position 2 as a delete above k2; it is below the
    // cursor and must be filtered.
    let page = lane.page(
        None,
        3,
        false,
        vec![
            put_entry(1, "k1", 1),
            delete_entry(2, "k2", 50),
            put_entry(3, "k3", 3),
        ],
    );
    assert_eq!(
        ack(receive(&store, page).await),
        PageAck {
            through: Some(off(3)),
            sealed: false
        }
    );
    assert!(get(&store, &table, "k2").await.is_some());
    assert!(get(&store, &table, "k3").await.is_some());
    assert_eq!(history(&store, &table).await.len(), 3);
    store.finish().await;
}

#[tokio::test]
async fn given_local_item_when_lower_order_put_received_then_beaten_and_cursor_advances() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    put(&store, &table, "k").await.unwrap();
    let before = get(&store, &table, "k").await.unwrap();
    let lane = lane(&binding, &table, "l1");
    let response = receive(
        &store,
        lane.page(None, 1, false, vec![put_entry(1, "k", 1)]),
    )
    .await;
    assert_eq!(
        ack(response),
        PageAck {
            through: Some(off(1)),
            sealed: false
        }
    );
    assert_eq!(get(&store, &table, "k").await.unwrap(), before);
    assert_eq!(history(&store, &table).await.len(), 1);
    store.finish().await;
}

#[tokio::test]
async fn given_absent_key_when_delete_received_then_hidden_echo_entry() {
    let store = TestStore::new().await;
    let (table, arn, binding) = active_table(&store).await;
    put(&store, &table, "p").await.unwrap();
    let public_before = public_count(&store, &table, &arn).await;
    let lane = lane(&binding, &table, "l1");
    receive(
        &store,
        lane.page(None, 1, false, vec![delete_entry(1, "z", 100)]),
    )
    .await;
    let entries = history(&store, &table).await;
    let for_z = for_key(&entries, "z");
    assert_eq!(for_z.len(), 1);
    let mark = for_z[0].entry.regional().unwrap();
    assert_eq!(
        (mark.order, mark.role, mark.hidden),
        (o(100), RegionalRole::Echo, true)
    );
    assert_eq!(public_count(&store, &table, &arn).await, public_before);
    store.finish().await;
}

#[tokio::test]
async fn given_live_key_when_delete_received_then_public_remove_with_echo_role() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    put(&store, &table, "k").await.unwrap();
    let high = (u64::try_from(store.now().timestamp_millis()).unwrap() << 10) + 1_000;
    let lane = lane(&binding, &table, "l1");
    receive(
        &store,
        lane.page(None, 1, false, vec![delete_entry(1, "k", high)]),
    )
    .await;
    assert!(get(&store, &table, "k").await.is_none());
    let entries = history(&store, &table).await;
    let for_k = for_key(&entries, "k");
    let removed = for_k.last().unwrap();
    assert_eq!(removed.entry.change(), StreamChange::Remove);
    assert!(removed.entry.is_public());
    let mark = removed.entry.regional().unwrap();
    assert_eq!((mark.order, mark.role), (o(high), RegionalRole::Echo));
    store.finish().await;
}

#[tokio::test]
async fn given_origin_marker_when_echo_of_its_order_then_lower_put_applies() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    delete(&store, &table, "m").await.unwrap();
    delete(&store, &table, "c").await.unwrap();
    let entries = history(&store, &table).await;
    let marker_order = |key: &str| entries_order(&entries, key);
    let (m_order, c_order) = (marker_order("m"), marker_order("c"));
    let lane = lane(&binding, &table, "l1");
    // Echo of m's delete removes its marker; c keeps its marker (no echo).
    ack(receive(
        &store,
        lane.page(None, 1, false, vec![echo_entry(1, "m", m_order)]),
    )
    .await);
    // The echo removed m's observed marker (SQL: exactly one row, D1); c's
    // stays.
    let left =
        (store.regional_markers)(table.clone(), None, std::num::NonZeroU32::new(10).unwrap())
            .await
            .unwrap()
            .0
            .markers;
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].1.order, c_order);
    let page = lane.page(
        Some(1),
        3,
        false,
        vec![
            put_entry(2, "m", m_order.stamp() - 1),
            put_entry(3, "c", c_order.stamp() - 1),
        ],
    );
    ack(receive(&store, page).await);
    assert!(get(&store, &table, "m").await.is_some());
    assert!(get(&store, &table, "c").await.is_none());
    store.finish().await;
}

fn entries_order(entries: &[PositionedStreamEntry], key: &str) -> RegionalOrder {
    for_key(entries, key)[0].entry.regional().unwrap().order
}

#[tokio::test]
async fn given_joining_when_delete_received_then_no_echo_and_marker_blocks_older_put() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let joining = pair(&store, &table, PairState::Joining).await;
    let lane = lane(&joining, &table, "l1");
    ack(receive(
        &store,
        lane.page(None, 1, false, vec![delete_entry(1, "z", 300)]),
    )
    .await);
    ack(receive(
        &store,
        lane.page(Some(1), 2, false, vec![put_entry(2, "z", 200)]),
    )
    .await);
    let active = RegionalBinding {
        state: PairState::Active,
        ..joining.clone()
    };
    assert!(
        store
            .regional_bindings
            .replace_regional_binding(&table, Some(joining.pair), Some(active))
            .await
            .unwrap()
    );
    assert!(get(&store, &table, "z").await.is_none());
    assert!(history(&store, &table).await.is_empty());
    store.finish().await;
}

#[tokio::test]
async fn given_child_lineage_when_parent_open_then_rejected_until_parent_sealed() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    let parent = lane(&binding, &table, "l1");
    let child = Lane {
        pair: binding.pair,
        table: &table,
        lineage: lineage("s2", "l2"),
        parents: vec![parent.lineage.clone()],
    };
    ack(receive(
        &store,
        parent.page(None, 1, false, vec![put_entry(1, "k", 1)]),
    )
    .await);
    let rejected = receive(
        &store,
        child.page(None, 1, false, vec![put_entry(1, "j", 2)]),
    )
    .await;
    assert!(
        matches!(
            rejected,
            RegionalResponse::Rejected(RegionalRejection::ParentNotSealed)
        ),
        "{rejected:?}"
    );
    let sealed = ack(receive(&store, parent.page(Some(1), 1, true, Vec::new())).await);
    assert!(sealed.sealed);
    ack(receive(
        &store,
        child.page(None, 1, false, vec![put_entry(1, "j", 2)]),
    )
    .await);
    assert!(get(&store, &table, "j").await.is_some());
    store.finish().await;
}

#[tokio::test]
async fn given_other_pair_or_unpaired_table_when_page_then_unknown_pair() {
    let store = TestStore::new().await;
    let (table, _, binding) = active_table(&store).await;
    let other = Lane {
        pair: PairId::generate(),
        table: &table,
        lineage: lineage("s1", "l1"),
        parents: Vec::new(),
    };
    let response = receive(
        &store,
        other.page(None, 1, false, vec![put_entry(1, "k", 1)]),
    )
    .await;
    assert!(
        matches!(
            response,
            RegionalResponse::Rejected(RegionalRejection::UnknownPair)
        ),
        "{response:?}"
    );
    let (unpaired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let lane = lane(&binding, &unpaired, "l1");
    let response = receive(
        &store,
        lane.page(None, 1, false, vec![put_entry(1, "k", 1)]),
    )
    .await;
    assert!(
        matches!(
            response,
            RegionalResponse::Rejected(RegionalRejection::UnknownPair)
        ),
        "{response:?}"
    );
    assert!(get(&store, &table, "k").await.is_none());
    store.finish().await;
}

#[cfg(any(
    feature = "sqlite",
    feature = "turso",
    feature = "postgres",
    feature = "rocksdb",
    feature = "foundationdb"
))]
async fn counts_of_one_and_twenty_puts() -> (
    super::native_census_test_support::CensusCounts,
    super::native_census_test_support::CensusCounts,
    bool,
) {
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _, binding) = active_table(&store).await;
    // Warm statement and binding caches on a lineage that the measured ones do
    // not use.
    let warm = lane(&binding, &table, "warm");
    ack(receive(
        &store,
        warm.page(None, 1, false, vec![put_entry(1, "w", 1)]),
    )
    .await);
    let mut counted = Vec::new();
    for (name, count) in [("one", 1_u64), ("twenty", 20)] {
        let lane = lane(&binding, &table, name);
        let entries = (1..=count)
            .map(|n| put_entry(n, &format!("{name}-{n:02}"), n))
            .collect();
        census.reset();
        ack(receive(&store, lane.page(None, count, false, entries)).await);
        let counts = census.snapshot();
        println!("REG_RECEIVE_COST puts={count}: {counts}");
        counted.push(counts);
    }
    #[cfg(feature = "turso")]
    let turso = store.turso_path().is_some();
    #[cfg(not(feature = "turso"))]
    let turso = false;
    store.finish().await;
    let twenty = counted.pop().unwrap();
    let one = counted.pop().unwrap();
    (one, twenty, turso)
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
#[tokio::test]
async fn given_pages_of_1_and_20_puts_when_measured_then_equal_waves_kv() {
    let (one, twenty, _) = counts_of_one_and_twenty_puts().await;
    assert_eq!(twenty.waves, one.waves);
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
#[tokio::test]
async fn given_pages_of_1_and_20_puts_when_measured_then_equal_waves_sql() {
    let (one, twenty, turso) = counts_of_one_and_twenty_puts().await;
    if turso {
        // Turso runs a batch statement by statement, so each extra put is its
        // own wave.
        assert_eq!(twenty.rows_read, one.rows_read);
    } else {
        assert_eq!(twenty.waves, one.waves);
    }
}

/// REG-14 B1. From code, confirm on the first run: the routes wave (scope get
/// plus lineage scan, families `~s/s` and `~s/l`) is read once for the first
/// page that writes entries (cold cache); the second such page installs the
/// cached routes, so it costs two waves less (FDB after REG-COST-1, warm table
/// entry: metadata version, routes (2), observe, progress, commit and the read
/// version = 7; warm routes = 5). A page without
/// entries stages only the progress record and never reads the routes.
#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
#[tokio::test]
async fn given_kv_receive_pages_when_measured_then_routes_read_once_and_never_for_empty_page() {
    const ROUTE_FAMILIES: [&str; 2] = ["~s/s", "~s/l"];
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _, binding) = active_table(&store).await;
    let touched_routes = |counts: &super::native_census_test_support::CensusCounts| {
        ROUTE_FAMILIES
            .iter()
            .filter(|family| counts.touched_read.contains(**family))
            .count()
    };
    // An empty page on a cold cache: no route key is touched.
    let empty = lane(&binding, &table, "empty");
    census.reset();
    ack(receive(&store, empty.page(None, 1, false, Vec::new())).await);
    let none = census.snapshot();
    println!("REG14_RECEIVE_EMPTY_PAGE: {none}");
    assert_eq!(touched_routes(&none), 0, "{none}");
    // First page that writes: cold route cache.
    let first_lane = lane(&binding, &table, "first");
    census.reset();
    ack(receive(
        &store,
        first_lane.page(None, 1, false, vec![put_entry(1, "f", 1)]),
    )
    .await);
    let first = census.snapshot();
    println!("REG14_RECEIVE_FIRST_WRITING_PAGE: {first}");
    assert_eq!(touched_routes(&first), ROUTE_FAMILIES.len(), "{first}");
    // Second page that writes: warm route cache, two waves less.
    let second_lane = lane(&binding, &table, "second");
    census.reset();
    ack(receive(
        &store,
        second_lane.page(None, 1, false, vec![put_entry(1, "s", 1)]),
    )
    .await);
    let second = census.snapshot();
    println!("REG14_RECEIVE_SECOND_WRITING_PAGE: {second}");
    assert_eq!(touched_routes(&second), 0, "{second}");
    // The route point read and the dependent lineage range read are two waves
    // (REG-COST-1: FoundationDB 7 -> 5, RocksDB 6 -> 4).
    assert_eq!(
        second.waves + 2,
        first.waves,
        "first {first}; second {second}"
    );
    // An empty page on a warm cache still touches no route key.
    census.reset();
    ack(receive(&store, empty.page(Some(1), 2, false, Vec::new())).await);
    let warm_none = census.snapshot();
    println!("REG14_RECEIVE_EMPTY_PAGE_WARM: {warm_none}");
    assert_eq!(touched_routes(&warm_none), 0, "{warm_none}");
    store.finish().await;
}

/// REG-COST-1: a warm table entry replaces the catalog lookup and metadata
/// reads of a received page (FoundationDB and RocksDB: by the catalog version
/// read; SQL: the stamp read stays, the table read goes).
#[tokio::test]
async fn given_warm_table_cache_when_pages_received_then_census_has_no_catalog_table_read() {
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _, binding) = active_table(&store).await;
    // Warm the table entry, the writer routes and the statement caches on
    // lineages the measured pages do not use.
    let warm = lane(&binding, &table, "warm");
    ack(receive(
        &store,
        warm.page(None, 1, false, vec![put_entry(1, "w", 1)]),
    )
    .await);
    let warm_empty = lane(&binding, &table, "warm-empty");
    ack(receive(&store, warm_empty.page(None, 1, false, Vec::new())).await);
    let one = lane(&binding, &table, "one");
    census.reset();
    ack(receive(
        &store,
        one.page(None, 1, false, vec![put_entry(1, "one", 1)]),
    )
    .await);
    let write = census.snapshot();
    println!("REG_COST_RECEIVE_WRITE: {write}");
    let empty = lane(&binding, &table, "empty");
    census.reset();
    ack(receive(&store, empty.page(None, 1, false, Vec::new())).await);
    let none = census.snapshot();
    println!("REG_COST_RECEIVE_EMPTY: {none}");
    match TestStore::selected_backend().as_str() {
        "foundationdb" => {
            for counts in [&write, &none] {
                assert!(
                    !counts.touched_read.contains("lookup")
                        && !counts.touched_read.contains("meta")
                        && counts.touched_read.contains("xff"),
                    "{counts}"
                );
            }
            // Read version, metadata version, observe, progress, commit. The
            // metadata version is a snapshot read; the table lookup and route
            // scope key are protected without a read.
            assert_eq!(
                (write.waves, write.keys, write.conflicts.read_keys),
                (5, 4, 5),
                "{write}"
            );
            // Read version, metadata version, progress, commit.
            assert_eq!((none.waves, none.keys), (4, 2), "{none}");
        }
        // Today 7: the table statement is gone.
        "postgres" => assert_eq!(write.waves, 6, "{write}"),
        _ => {}
    }
    store.finish().await;
}
