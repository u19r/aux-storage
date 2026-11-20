//! Sender halves through real provider operations (REG-9): page selection, ACK
//! prefix, retention hold. Needs `TestStore.regional_send: Arc<dyn
//! storage_provider::RegionalSendOperations>` (see hand-back).

use std::{cmp::Ordering, num::NonZeroU32, sync::Arc, time::Duration};

use futures::FutureExt as _;
use regional_replication::message::{
    PageChange, PageEntry, PairTable, RegionalRejection, RegionalResponse, ReplicationPage,
};
use storage_provider::{LineageCursor, SendLimits, SendTurn};
use storage_types::{
    AttributeValue, DeleteTableRequest, KeyAttributes, PairId, PairState, RegionalBinding,
    RegionalOrder, StoredWireItem, SystemStreamOffset, SystemStreamPosition, TableName, WireItem,
};
use stream_provider::{
    DynamoMutationRoute, SystemStreamLineage, SystemStreamLineageCatalog, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

use super::{
    dynamodb_stream_history_tests::create_stream_table,
    regional_receive_tests::{get, lane, put_entry, receive},
    regional_write_tests::{delete, pair, put, region},
    test_support::{TestStore, hash_table_request},
};

fn limits(entries: u32) -> SendLimits {
    SendLimits {
        entries: NonZeroU32::new(entries).unwrap(),
        bytes: 1 << 20,
    }
}

fn keys(key: &str) -> KeyAttributes {
    KeyAttributes::from_iter([("pk".into(), AttributeValue::S(key.into()))])
}

fn is_key(entry: &PageEntry, key: &str) -> bool {
    entry.keys == keys(key)
}

async fn state_of(store: &TestStore, table: &TableName, pair: PairId) -> Option<PairState> {
    store
        .regional_pairing
        .regional_binding(table, pair)
        .await
        .unwrap()
        .map(|binding| binding.state)
}

/// Send every page and store each as acknowledged, until the sender is idle.
async fn drain(
    store: &TestStore,
    table: &TableName,
    pair: PairId,
    entries_per_page: u32,
) -> Vec<ReplicationPage> {
    let mut pages = Vec::new();
    let mut next_lane = 0;
    for _ in 0..100 {
        match store
            .regional_send
            .next_send_page(table, pair, next_lane, limits(entries_per_page))
            .await
            .unwrap()
        {
            SendTurn::Idle => return pages,
            SendTurn::PairGone => panic!("pair gone"),
            SendTurn::Page {
                page,
                next_lane: lane,
                ..
            } => {
                let cursor = LineageCursor {
                    through: page.through.clone(),
                    sealed: page.sealed,
                };
                store
                    .regional_send
                    .store_send_progress(table, pair, page.lineage.clone(), cursor, false)
                    .await
                    .unwrap();
                next_lane = lane;
                pages.push(*page);
            }
        }
    }
    panic!("the sender never became idle");
}

async fn next_page(
    store: &TestStore,
    table: &TableName,
    pair: PairId,
    entries: u32,
) -> ReplicationPage {
    match store
        .regional_send
        .next_send_page(table, pair, 0, limits(entries))
        .await
        .unwrap()
    {
        SendTurn::Page { page, .. } => *page,
        other => panic!("page expected, got {other:?}"),
    }
}

#[tokio::test]
async fn given_paired_writes_when_send_pages_built_then_origin_entries_in_order_and_through_head() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    put(&store, &table, "a").await.unwrap();
    delete(&store, &table, "a").await.unwrap();
    delete(&store, &table, "b").await.unwrap();
    store.wait_until_changes_are_readable().await;
    let pages = drain(&store, &table, binding.pair, 100).await;
    let entries = pages
        .iter()
        .flat_map(|page| page.entries.iter())
        .collect::<Vec<_>>();
    let for_a = entries
        .iter()
        .filter(|entry| is_key(entry, "a"))
        .collect::<Vec<_>>();
    let for_b = entries
        .iter()
        .filter(|entry| is_key(entry, "b"))
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 3);
    assert_eq!(for_a.len(), 2);
    assert!(matches!(for_a[0].change, PageChange::Put { .. }));
    assert!(matches!(for_a[1].change, PageChange::Delete));
    assert!(for_a[0].order < for_a[1].order);
    assert_eq!(for_b.len(), 1);
    assert!(matches!(for_b[0].change, PageChange::Delete));
    // Every page ends at its lineage head: nothing remains once the sender is
    // idle.
    assert!(
        pages
            .iter()
            .all(|page| page.through.is_some() && page.after != page.through)
    );
    assert!(matches!(
        store
            .regional_send
            .next_send_page(&table, binding.pair, 0, limits(100))
            .await
            .unwrap(),
        SendTurn::Idle
    ));
    store.finish().await;
}

#[tokio::test]
async fn given_received_page_when_send_then_applied_not_sent_and_echo_sent() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    let lineage = {
        let position = SystemStreamPosition::try_new("peer-shard", "peer-lineage", "0").unwrap();
        regional_replication::message::LineageRef {
            shard: position.shard_id().clone(),
            lineage: position.lineage_id().clone(),
        }
    };
    let off = |n: u64| SystemStreamOffset::try_new(format!("{n:020}-vs00000000")).unwrap();
    let order = |stamp: u64| RegionalOrder::from_parts(stamp, region(2), false);
    let item = StoredWireItem {
        item: WireItem::dynamo_json(br#"{"pk":{"S":"x"}}"#.to_vec()),
        indexers: Vec::new(),
    };
    let page = ReplicationPage {
        pair: binding.pair,
        table: table.clone(),
        lineage,
        parents: Vec::new(),
        after: None,
        through: Some(off(2)),
        sealed: false,
        entries: vec![
            PageEntry {
                position: off(1),
                keys: keys("x"),
                order: order(5),
                change: PageChange::Put {
                    item: Box::new(item),
                },
            },
            PageEntry {
                position: off(2),
                keys: keys("y"),
                order: order(6),
                change: PageChange::Delete,
            },
        ],
    };
    let response = store.regional_receive.receive_page(page).await.unwrap();
    assert!(matches!(response, RegionalResponse::Ack(_)), "{response:?}");
    store.wait_until_changes_are_readable().await;
    let pages = drain(&store, &table, binding.pair, 100).await;
    let entries = pages
        .iter()
        .flat_map(|page| page.entries.iter())
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1);
    assert!(is_key(entries[0], "y"));
    assert!(matches!(entries[0].change, PageChange::Echo));
    assert_eq!(entries[0].order, order(6));
    store.finish().await;
}

#[tokio::test]
async fn given_ack_then_older_ack_when_stored_then_progress_never_moves_back() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    for _ in 0..3 {
        put(&store, &table, "a").await.unwrap();
    }
    store.wait_until_changes_are_readable().await;
    let ack = |page: &ReplicationPage| LineageCursor {
        through: page.through.clone(),
        sealed: page.sealed,
    };

    let first = next_page(&store, &table, binding.pair, 1).await;
    let p1 = first.through.clone().unwrap();
    store
        .regional_send
        .store_send_progress(
            &table,
            binding.pair,
            first.lineage.clone(),
            ack(&first),
            false,
        )
        .await
        .unwrap();
    let second = next_page(&store, &table, binding.pair, 1).await;
    assert_eq!(second.after.as_ref(), Some(&p1));
    let p2 = second.through.clone().unwrap();
    store
        .regional_send
        .store_send_progress(
            &table,
            binding.pair,
            second.lineage.clone(),
            ack(&second),
            false,
        )
        .await
        .unwrap();

    // An older acknowledgment does not move the record back.
    store
        .regional_send
        .store_send_progress(
            &table,
            binding.pair,
            first.lineage.clone(),
            ack(&first),
            false,
        )
        .await
        .unwrap();
    let third = next_page(&store, &table, binding.pair, 1).await;
    assert_eq!(third.after.as_ref(), Some(&p2));

    // A gap answer rewinds to the peer's prefix.
    store
        .regional_send
        .store_send_progress(
            &table,
            binding.pair,
            first.lineage.clone(),
            ack(&first),
            true,
        )
        .await
        .unwrap();
    let resent = next_page(&store, &table, binding.pair, 1).await;
    assert_eq!(resent.after.as_ref(), Some(&p1));
    store.finish().await;
}

#[tokio::test]
async fn given_unpaired_or_other_pair_when_next_send_page_then_pair_gone() {
    let store = TestStore::new().await;
    let (unpaired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    pair(&store, &paired, PairState::Active).await;
    for (table, pair_id) in [
        (&unpaired, PairId::generate()),
        (&paired, PairId::generate()),
    ] {
        let turn = store
            .regional_send
            .next_send_page(table, pair_id, 0, limits(10))
            .await
            .unwrap();
        assert!(matches!(turn, SendTurn::PairGone), "{turn:?}");
    }
    store.finish().await;
}

#[tokio::test]
async fn given_paired_table_with_short_retention_when_trim_runs_then_unacked_entries_kept() {
    let store = TestStore::new().await;
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (control, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &paired, PairState::Active).await;
    for _ in 0..3 {
        put(&store, &paired, "a").await.unwrap();
        put(&store, &control, "a").await.unwrap();
    }
    store.wait_until_changes_are_readable().await;
    let paired_reads = super::image_retention_tests::canonical_history_reads(&store, &paired)
        .await
        .unwrap();
    let control_reads = super::image_retention_tests::canonical_history_reads(&store, &control)
        .await
        .unwrap();
    assert_eq!(
        super::table_history_retention_tests::history_count(&store, &paired_reads)
            .await
            .unwrap(),
        3
    );

    // The peer acknowledged only the first entry.
    let first = next_page(&store, &paired, binding.pair, 1).await;
    store
        .regional_send
        .store_send_progress(
            &paired,
            binding.pair,
            first.lineage.clone(),
            LineageCursor {
                through: first.through.clone(),
                sealed: first.sealed,
            },
            false,
        )
        .await
        .unwrap();

    store.advance_time(Duration::from_secs(49 * 3600));
    for _ in 0..4 {
        super::table_history_retention_tests::cleanup_page(&store, 10)
            .await
            .unwrap();
        store.advance_time(Duration::from_secs(60));
    }
    let kept = super::table_history_retention_tests::history_count(&store, &paired_reads)
        .await
        .unwrap();
    assert!(
        kept >= 2,
        "unacknowledged entries were trimmed: {kept} remain"
    );
    let control_left = super::table_history_retention_tests::history_count(&store, &control_reads)
        .await
        .unwrap();
    assert_eq!(control_left, 0);
    store.finish().await;
}

#[tokio::test]
async fn given_idle_paired_table_when_trim_runs_past_its_head_then_sender_still_sends() {
    let store = TestStore::new().await;
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (other, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &paired, PairState::Active).await;
    // One paired write, so the send records exist; the sender is then idle at
    // the heads.
    put(&store, &paired, "a").await.unwrap();
    store.wait_until_changes_are_readable().await;
    drain(&store, &paired, binding.pair, 100).await;

    // Only another table writes: the shared head moves while the paired lane
    // stays empty.
    for _ in 0..3 {
        put(&store, &other, "x").await.unwrap();
    }
    store.wait_until_changes_are_readable().await;
    store.advance_time(Duration::from_secs(49 * 3600));
    for _ in 0..2 {
        super::table_history_retention_tests::cleanup_page(&store, 10)
            .await
            .unwrap();
        store.advance_time(Duration::from_secs(60));
    }

    let turn = store
        .regional_send
        .next_send_page(&paired, binding.pair, 0, limits(100))
        .await;
    assert!(
        turn.is_ok(),
        "the sender must not refuse history retired past its record: {turn:?}"
    );
    let pages = drain(&store, &paired, binding.pair, 100).await;
    assert!(
        pages.iter().all(|page| page.entries.is_empty()),
        "nothing new was written to the paired table"
    );
    assert!(matches!(
        store
            .regional_send
            .next_send_page(&paired, binding.pair, 0, limits(100))
            .await
            .unwrap(),
        SendTurn::Idle
    ));
    store.finish().await;
}

#[tokio::test]
async fn given_page_without_sendable_entries_when_it_reaches_the_head_then_caught_up_and_cut_page_is_not()
 {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    // An applied entry (received from the peer) is never sent back: the page is
    // empty.
    let lineage = {
        let position = SystemStreamPosition::try_new("peer-shard", "peer-lineage", "0").unwrap();
        regional_replication::message::LineageRef {
            shard: position.shard_id().clone(),
            lineage: position.lineage_id().clone(),
        }
    };
    let off = |n: u64| SystemStreamOffset::try_new(format!("{n:020}-vs00000000")).unwrap();
    let item = StoredWireItem {
        item: WireItem::dynamo_json(br#"{"pk":{"S":"x"}}"#.to_vec()),
        indexers: Vec::new(),
    };
    let received = ReplicationPage {
        pair: binding.pair,
        table: table.clone(),
        lineage,
        parents: Vec::new(),
        after: None,
        through: Some(off(1)),
        sealed: false,
        entries: vec![PageEntry {
            position: off(1),
            keys: keys("x"),
            order: RegionalOrder::from_parts(5, region(2), false),
            change: PageChange::Put {
                item: Box::new(item),
            },
        }],
    };
    let response = store.regional_receive.receive_page(received).await.unwrap();
    assert!(matches!(response, RegionalResponse::Ack(_)), "{response:?}");
    store.wait_until_changes_are_readable().await;
    match store
        .regional_send
        .next_send_page(&table, binding.pair, 0, limits(10))
        .await
        .unwrap()
    {
        SendTurn::Page {
            page, caught_up, ..
        } => {
            assert!(page.entries.is_empty());
            assert!(
                caught_up,
                "an empty page that reached the head is caught up"
            );
        }
        other => panic!("page expected, got {other:?}"),
    }
    // A page cut by the entry limit has not reached the head.
    for _ in 0..3 {
        put(&store, &table, "a").await.unwrap();
    }
    store.wait_until_changes_are_readable().await;
    let mut cut = 0;
    let mut next_lane = 0;
    for _ in 0..20 {
        match store
            .regional_send
            .next_send_page(&table, binding.pair, next_lane, limits(1))
            .await
            .unwrap()
        {
            SendTurn::Page {
                page,
                next_lane: lane,
                caught_up,
                ..
            } => {
                if !caught_up {
                    assert!(page.through.is_some());
                    cut += 1;
                }
                let cursor = LineageCursor {
                    through: page.through.clone(),
                    sealed: page.sealed,
                };
                store
                    .regional_send
                    .store_send_progress(&table, binding.pair, page.lineage.clone(), cursor, false)
                    .await
                    .unwrap();
                next_lane = lane;
            }
            _ => break,
        }
    }
    assert!(cut >= 1, "a one-entry limit over three entries cuts a page");
    store.finish().await;
}

#[tokio::test]
async fn given_idle_paired_table_when_head_moves_then_census_shows_idle_turn_writes_nothing() {
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (other, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &paired, PairState::Active).await;
    put(&store, &paired, "a").await.unwrap();
    store.wait_until_changes_are_readable().await;
    assert_eq!(drain(&store, &paired, binding.pair, 100).await.len(), 1);
    // Another table writes: the shared publication bound moves on every
    // provider.
    put(&store, &other, "x").await.unwrap();
    store.wait_until_changes_are_readable().await;
    // Warm statement caches with the measured shape.
    let warm = store
        .regional_send
        .next_send_page(&paired, binding.pair, 0, limits(100))
        .await
        .unwrap();
    assert!(matches!(warm, SendTurn::Idle), "{warm:?}");
    census.reset();
    let turn = store
        .regional_send
        .next_send_page(&paired, binding.pair, 0, limits(100))
        .await
        .unwrap();
    let counts = census.snapshot();
    println!(
        "REG14_IDLE_TURN waves {} statements {} keys {} ranges {} rows_read {} rows_written {} \
         begins {} commits {}",
        counts.waves,
        counts.statements,
        counts.keys,
        counts.ranges,
        counts.rows_read,
        counts.rows_written,
        counts.begins,
        counts.commits
    );
    assert!(matches!(turn, SendTurn::Idle), "{turn:?}");
    assert_eq!(counts.rows_written, 0, "an idle turn writes nothing");
    // REG-COST-1: the warm table entry replaces the lookup and metadata reads.
    match TestStore::selected_backend().as_str() {
        "foundationdb" => {
            assert_eq!(
                counts.waves, 7,
                "read version, metadata version, history (2), retention, progress, lanes"
            );
            assert!(
                !counts.touched_read.contains("lookup") && !counts.touched_read.contains("meta"),
                "{:?}",
                counts.touched_read
            );
        }
        "postgres" => assert_eq!(
            counts.waves, 6,
            "the cached table skips its statement (today 7)"
        ),
        _ => {}
    }
    // FoundationDB's bound moves with time alone: a later turn is still idle.
    assert!(matches!(
        store
            .regional_send
            .next_send_page(&paired, binding.pair, 0, limits(100))
            .await
            .unwrap(),
        SendTurn::Idle
    ));
    store.finish().await;
}

#[tokio::test]
async fn given_leaving_table_when_head_moves_then_empty_page_reaches_frozen_heads() {
    let store = TestStore::new().await;
    let (paired, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let (other, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &paired, PairState::Active).await;
    put(&store, &paired, "a").await.unwrap();
    store.wait_until_changes_are_readable().await;
    drain(&store, &paired, binding.pair, 100).await;
    let target = PairTable {
        pair: binding.pair,
        table: paired.clone(),
    };
    assert_eq!(
        store.regional_unpair.begin_leave(&target).await.unwrap(),
        Some(PairState::Leaving)
    );
    let heads = store
        .regional_unpair
        .history_heads(&paired, binding.pair)
        .await
        .unwrap()
        .unwrap();
    put(&store, &other, "x").await.unwrap();
    store.wait_until_changes_are_readable().await;
    // No `drain` here: while Leaving, an empty page is sent every turn by
    // design.
    let page = match store
        .regional_send
        .next_send_page(&paired, binding.pair, 0, limits(100))
        .await
        .unwrap()
    {
        SendTurn::Page {
            page, caught_up, ..
        } => {
            assert!(caught_up);
            page
        }
        other => panic!("a Leaving table sends an empty page through the head, got {other:?}"),
    };
    assert!(page.entries.is_empty());
    let head = heads
        .iter()
        .find(|head| head.lineage == page.lineage)
        .unwrap();
    let order = page
        .through
        .as_ref()
        .unwrap()
        .compare_order(head.through.as_ref().unwrap())
        .unwrap();
    assert_ne!(
        order,
        Ordering::Less,
        "the page reaches the frozen Finish head"
    );
    store.finish().await;
}

/// Split the table's writer lineage into two children and drain the sealed
/// parent. Returns the child lineage ids and, per child, a key routed to it.
async fn split_into_two_lanes(
    store: &TestStore,
    table: &TableName,
    pair: PairId,
) -> [(&'static str, Vec<String>); 2] {
    const CHILDREN: [&str; 2] = ["send-left", "send-right"];
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let catalog = |lineages| SystemStreamLineageCatalog::try_new(scope.clone(), lineages).unwrap();
    let request = hash_table_request(table.clone());
    let route =
        |key: &str| DynamoMutationRoute::try_new(table, &request.key_schema, &keys(key)).unwrap();
    let before = catalog(
        store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap(),
    );
    let parent = before
        .writer_lineage(route("seed").routing_key())
        .unwrap()
        .clone();
    let children = CHILDREN.map(|id| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                id,
                id,
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    store
        .stream_operations
        .split_system_stream(
            SystemStreamSplitRequest::try_new(
                scope.clone(),
                parent,
                children,
                SystemStreamSplitOperationId::try_new("regional-send-lanes").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    store.wait_until_changes_are_readable().await;
    // The sealed parent is sent and acknowledged; both children then wait.
    drain(store, table, pair, 100).await;
    let after = catalog(
        store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap(),
    );
    let mut routed = CHILDREN.map(|id| (id, Vec::new()));
    for n in 0..400 {
        let key = format!("k{n:03}");
        let owner = after
            .writer_lineage(route(&key).routing_key())
            .unwrap()
            .lineage_id()
            .as_str()
            .to_owned();
        for (id, found) in &mut routed {
            if *id == owner {
                found.push(key.clone());
            }
        }
    }
    routed
}

/// Lane 0 keeps a backlog of more than one page per turn for the whole test;
/// lane 1 has one entry. Either lane may be first in the sender's rotation, so
/// both assignments run.
async fn lane_with_one_entry_is_sent_within_two_turns(busy: usize) {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    let routed = split_into_two_lanes(&store, &table, binding.pair).await;
    let (busy_id, busy_keys) = &routed[busy];
    let (light_id, light_keys) = &routed[1 - busy];
    // Idle lanes sort before the busy lane and their heads move (Postgres xmin,
    // FoundationDB read version): the busy lane is probed and then read again
    // with the full limit (REG-COST-2); the six-entry backlog outlasts the two
    // measured turns.
    for key in busy_keys.iter().take(6) {
        put(&store, &table, key).await.unwrap();
    }
    put(&store, &table, &light_keys[0]).await.unwrap();
    store.wait_until_changes_are_readable().await;
    let mut next_lane = 0;
    let mut light_turn = None;
    for turn in 1..=2 {
        let SendTurn::Page {
            page,
            next_lane: lane,
            ..
        } = store
            .regional_send
            .next_send_page(&table, binding.pair, next_lane, limits(2))
            .await
            .unwrap()
        else {
            panic!("a page was expected on turn {turn}");
        };
        println!(
            "REG14_LANE_TURN busy={busy_id} turn={turn} sent_lineage={} entries={}",
            page.lineage.lineage.as_str(),
            page.entries.len()
        );
        if page.lineage.lineage.as_str() == *busy_id {
            assert_eq!(
                page.entries.len(),
                2,
                "a busy lane always gets a full page (turn {turn})"
            );
        }
        if page.lineage.lineage.as_str() == *light_id {
            assert!(
                page.entries
                    .iter()
                    .any(|entry| is_key(entry, &light_keys[0]))
            );
            light_turn = Some(turn);
        }
        let cursor = LineageCursor {
            through: page.through.clone(),
            sealed: page.sealed,
        };
        store
            .regional_send
            .store_send_progress(&table, binding.pair, page.lineage.clone(), cursor, false)
            .await
            .unwrap();
        next_lane = lane;
    }
    assert!(
        light_turn.is_some(),
        "the lane with one entry was not sent within 2 turns while lane {busy_id} had a backlog"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_busy_lane_first_when_other_lane_has_one_entry_then_it_is_sent_within_two_turns() {
    lane_with_one_entry_is_sent_within_two_turns(0).await;
}

#[tokio::test]
async fn given_busy_lane_second_when_other_lane_has_one_entry_then_it_is_sent_within_two_turns() {
    lane_with_one_entry_is_sent_within_two_turns(1).await;
}

/// REG-COST-2: when other lanes precede the busy lane in rotation, the busy
/// lane is only probed (limit 1) and then read again with the full limit in the
/// same attempt instead of sending a one-entry probe page.
#[tokio::test]
async fn given_idle_lane_before_busy_lane_when_measured_then_busy_lane_gets_full_page() {
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    let routed = split_into_two_lanes(&store, &table, binding.pair).await;
    let (busy_id, busy_keys) = &routed[0];
    for key in busy_keys.iter().take(6) {
        put(&store, &table, key).await.unwrap();
    }
    store.wait_until_changes_are_readable().await;
    let turn = |next_lane: u32| {
        let store = &store;
        let table = &table;
        async move {
            match store
                .regional_send
                .next_send_page(table, binding.pair, next_lane, limits(2))
                .await
                .unwrap()
            {
                SendTurn::Page {
                    page, next_lane, ..
                } => (page, next_lane),
                other => panic!("a page was expected, got {other:?}"),
            }
        }
    };
    // The busy lane is the only non-empty lane, so every turn sends it and
    // moves the rotation one lane past it. Lane order differs by provider
    // (FoundationDB: the busy lane is the first candidate from lane 0), so the
    // rotated turn starts right after the busy lane: every other lane comes
    // first and the busy lane is a probed candidate whenever another lane is
    // behind its head (FoundationDB read version, Postgres xmin).
    let (_, after) = turn(0).await;
    assert!(
        after > 0,
        "the busy lane is not the last lane: next_lane {after}"
    );
    let busy_lane = after - 1;
    // Warms the table entry and the statement shapes of both measured turns.
    let _ = turn(after).await;
    let _ = turn(busy_lane).await;
    census.reset();
    let (rotated_page, rotated_next) = turn(after).await;
    let rotated = census.snapshot();
    println!("REG_COST_PROBE_TURN: start={after} next={rotated_next} {rotated}");
    assert_eq!(rotated_page.lineage.lineage.as_str(), *busy_id);
    assert_eq!(
        rotated_page.entries.len(),
        2,
        "the busy lane gets a full page"
    );
    assert_eq!(
        rotated_next, after,
        "a re-read page moves the rotation past the busy lane"
    );
    census.reset();
    let (direct_page, direct_next) = turn(busy_lane).await;
    let direct = census.snapshot();
    println!("REG_COST_DIRECT_TURN: start={busy_lane} next={direct_next} {direct}");
    assert_eq!(direct_page.lineage.lineage.as_str(), *busy_id);
    assert_eq!(direct_page.entries.len(), 2);
    assert_eq!(direct_next, after);
    // Both turns read the same candidate lanes; only the re-read differs.
    match TestStore::selected_backend().as_str() {
        // Direct 7: read version; metadata version (xff); history checkpoint
        // and scope gets (~s/c, ~s/s); lineage scan (~s/l); retention get
        // (trim_state); progress scan; the five lane scans in one wave.
        // Rotated 9: the re-read range is two native calls. Its first call
        // returns only the two rows the probe already read in this transaction
        // (FoundationDB read-your-writes cache, `more` set); the second
        // fetches the rest of the page.
        "foundationdb" => assert_eq!(
            (rotated.waves, rotated.ranges, direct.waves, direct.ranges),
            (9, 8, 7, 7),
            "rotated {rotated}; direct {direct}"
        ),
        // The re-read is one more statement in its own wave.
        "postgres" => assert_eq!(
            rotated.waves,
            direct.waves + 1,
            "the probed lane costs one re-read wave; rotated {rotated}; direct {direct}"
        ),
        _ => {}
    }
    store.finish().await;
}

#[tokio::test]
async fn given_warm_table_cache_when_ack_and_binding_read_then_census_has_no_catalog_table_read() {
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let binding = pair(&store, &table, PairState::Active).await;
    put(&store, &table, "a").await.unwrap();
    store.wait_until_changes_are_readable().await;
    // Warms the table entry and the statement caches.
    assert_eq!(
        state_of(&store, &table, binding.pair).await,
        Some(PairState::Active)
    );
    let SendTurn::Page { page, .. } = store
        .regional_send
        .next_send_page(&table, binding.pair, 0, limits(100))
        .await
        .unwrap()
    else {
        panic!("a page was expected");
    };
    let cursor = LineageCursor {
        through: page.through.clone(),
        sealed: page.sealed,
    };
    census.reset();
    store
        .regional_send
        .store_send_progress(&table, binding.pair, page.lineage.clone(), cursor, false)
        .await
        .unwrap();
    let acked = census.snapshot();
    println!("REG_COST_ACK: {acked}");
    census.reset();
    let state = state_of(&store, &table, binding.pair).await;
    let bound = census.snapshot();
    println!("REG_COST_BINDING: {bound}");
    assert_eq!(state, Some(PairState::Active));
    assert!(acked.rows_written > 0, "the ack moved the cursor: {acked}");
    if TestStore::selected_backend() == "foundationdb" {
        for counts in [&acked, &bound] {
            assert!(
                !counts.touched_read.contains("lookup") && !counts.touched_read.contains("meta"),
                "{counts}"
            );
        }
        // Read version, metadata version, progress get, commit.
        assert_eq!((acked.waves, acked.keys), (4, 2), "{acked}");
        // Read version, metadata version.
        assert_eq!((bound.waves, bound.keys), (2, 1), "{bound}");
    }
    store.finish().await;
}

/// REG-COST-1: a warm table entry is only a hint. Another node's binding change
/// (Leaving), unpair and table delete are seen by the next regional attempt of
/// this node: the stamp or catalog version differs from the cached entry.
#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires native fixture"
)]
async fn given_other_store_changes_pair_when_warm_regional_operations_run_then_they_see_it() {
    if !matches!(
        TestStore::selected_backend().as_str(),
        "foundationdb" | "postgres"
    ) {
        return;
    }
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let other = store.open_same_database().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        let (moving, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        let binding = pair(&store, &table, PairState::Active).await;
        put(&store, &table, "a").await.unwrap();
        store.wait_until_changes_are_readable().await;
        let pages = drain(&store, &table, binding.pair, 100).await;
        let sent = pages.last().expect("one page was sent").lineage.clone();
        // Equal: the warm entry answers.
        assert_eq!(
            state_of(&store, &table, binding.pair).await,
            Some(PairState::Active)
        );
        // Changed: another node moves the pair to Leaving.
        let leaving = RegionalBinding {
            state: PairState::Leaving,
            ..binding.clone()
        };
        assert!(
            other
                .regional_bindings
                .replace_regional_binding(&table, Some(binding.pair), Some(leaving))
                .await
                .unwrap()
        );
        assert_eq!(
            state_of(&store, &table, binding.pair).await,
            Some(PairState::Leaving)
        );
        // Moves the Postgres head; FoundationDB's moves with time.
        put(&store, &moving, "x").await.unwrap();
        store.wait_until_changes_are_readable().await;
        match store
            .regional_send
            .next_send_page(&table, binding.pair, 0, limits(100))
            .await
            .unwrap()
        {
            SendTurn::Page {
                page, caught_up, ..
            } => {
                assert!(caught_up && page.entries.is_empty());
            }
            other => panic!("a Leaving table sends an empty page through its head, got {other:?}"),
        }
        // Pair absent: another node removes the binding.
        assert!(
            other
                .regional_bindings
                .replace_regional_binding(&table, Some(binding.pair), None)
                .await
                .unwrap()
        );
        assert_eq!(state_of(&store, &table, binding.pair).await, None);
        assert!(matches!(
            store
                .regional_send
                .next_send_page(&table, binding.pair, 0, limits(100))
                .await
                .unwrap(),
            SendTurn::PairGone
        ));
        census.reset();
        store
            .regional_send
            .store_send_progress(
                &table,
                binding.pair,
                sent.clone(),
                LineageCursor::default(),
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            census.snapshot().rows_written,
            0,
            "an ack of a removed pair writes nothing"
        );
        let absent = lane(&binding, &table, "absent");
        assert!(matches!(
            receive(
                &store,
                absent.page(None, 1, false, vec![put_entry(1, "r", 1)])
            )
            .await,
            RegionalResponse::Rejected(RegionalRejection::UnknownPair)
        ));
        assert!(get(&store, &table, "r").await.is_none());
        // Table absent: another node deletes the table.
        other
            .operations
            .delete_table(DeleteTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert_eq!(state_of(&store, &table, binding.pair).await, None);
        assert!(matches!(
            store
                .regional_send
                .next_send_page(&table, binding.pair, 0, limits(100))
                .await
                .unwrap(),
            SendTurn::PairGone
        ));
        census.reset();
        store
            .regional_send
            .store_send_progress(
                &table,
                binding.pair,
                sent.clone(),
                LineageCursor::default(),
                true,
            )
            .await
            .unwrap();
        assert_eq!(
            census.snapshot().rows_written,
            0,
            "an ack of a deleted table writes nothing"
        );
        let gone = lane(&binding, &table, "gone");
        assert!(matches!(
            receive(
                &store,
                gone.page(None, 1, false, vec![put_entry(1, "g", 1)])
            )
            .await,
            RegionalResponse::Rejected(RegionalRejection::UnknownPair)
        ));
    })
    .catch_unwind()
    .await;
    drop(other);
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
