//! Enable, prepare, scan, activate, sweep through real provider operations
//! (REG-10). One store plays both regions: source table `src` (region 1) and
//! target table `tgt` (region 2). Needs `TestStore.regional_pairing: Arc<dyn
//! storage_provider::RegionalPairingOperations>`.

use std::num::NonZeroU32;

use regional_replication::message::{
    PairTable, PrepareTarget, RegionalRejection, RegionalResponse, ScanPage,
};
use serde_json::json;
use storage_provider::{ScanTurn, SweepTurn};
use storage_types::{
    BatchGetItemRequest, GetItemRequest, PairId, PairState, QueryRequest, RegionalOrder,
    RegionalRole, ScanRequest, StorageEnum, TableName, TransactGetItemsRequest,
};

use super::{
    dynamodb_stream_history_tests::create_stream_table,
    regional_receive_tests::{Lane, delete_entry, for_key, get, history, lineage, o, receive},
    regional_write_tests::{delete, put, region},
    test_support::{TestStore, hash_table_request, unique_table_name},
};

pub(super) fn limit(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

/// Source table with two items `a`, `b` (b written after pairing), paired
/// Seeding, plus the prepare request renamed for the target table.
pub(super) async fn seeded(store: &TestStore) -> (TableName, PairId, TableName, PrepareTarget) {
    let (src, _) = create_stream_table(store, "NEW_AND_OLD_IMAGES").await;
    put(store, &src, "a").await.unwrap();
    let pair = store
        .regional_pairing
        .begin_pairing(&src, region(1), region(2), "peer")
        .await
        .unwrap();
    put(store, &src, "b").await.unwrap();
    store.wait_until_changes_are_readable().await;
    let mut request = store
        .regional_pairing
        .prepare_request(&src, pair)
        .await
        .unwrap()
        .expect("seeding");
    let tgt = unique_table_name();
    request.table.table_name = tgt.clone();
    (src, pair, tgt, request)
}

pub(super) async fn prepared(store: &TestStore) -> (TableName, PairId, TableName) {
    let (src, pair, tgt, request) = seeded(store).await;
    let response = store
        .regional_pairing
        .prepare_target(request, region(2), region(1), "peer")
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::Prepared),
        "{response:?}"
    );
    (src, pair, tgt)
}

async fn state(store: &TestStore, table: &TableName, pair: PairId) -> Option<PairState> {
    store
        .regional_pairing
        .regional_binding(table, pair)
        .await
        .unwrap()
        .map(|binding| binding.state)
}

/// Scan the whole source into the target, one item per page; returns the
/// applied pages.
pub(super) async fn scan_all(
    store: &TestStore,
    src: &TableName,
    pair: PairId,
    tgt: &TableName,
) -> Vec<ScanPage> {
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let ScanTurn::Page { items, next } = store
            .regional_pairing
            .next_scan_page(src, pair, after, limit(1))
            .await
            .unwrap()
        else {
            panic!("scan stopped");
        };
        if !items.is_empty() {
            let page = ScanPage {
                pair,
                table: tgt.clone(),
                items,
            };
            let response = store
                .regional_pairing
                .apply_scan_page(page.clone())
                .await
                .unwrap();
            assert!(
                matches!(response, RegionalResponse::ScanApplied),
                "{response:?}"
            );
            pages.push(page);
        }
        match next {
            Some(next) => after = Some(next),
            None => return pages,
        }
    }
}

pub(super) async fn activate(store: &TestStore, tgt: &TableName, pair: PairId) -> RegionalResponse {
    store
        .regional_pairing
        .activate_target(PairTable {
            pair,
            table: tgt.clone(),
        })
        .await
        .unwrap()
}

fn is_not_found(error: &storage_types::StorageError) -> bool {
    matches!(
        error.base_error(),
        Some(StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. })
    )
}

#[tokio::test]
async fn given_table_with_stream_when_begin_pairing_then_seeding_and_send_holds_at_heads() {
    let store = TestStore::new().await;
    let (src, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put(&store, &src, "a").await.unwrap();
    put(&store, &src, "b").await.unwrap();
    store.wait_until_changes_are_readable().await;
    let pair = store
        .regional_pairing
        .begin_pairing(&src, region(1), region(2), "peer")
        .await
        .unwrap();
    assert_eq!(state(&store, &src, pair).await, Some(PairState::Seeding));
    let request = store
        .regional_pairing
        .prepare_request(&src, pair)
        .await
        .unwrap()
        .unwrap();
    let frontier_lanes = store
        .operations
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            src.clone(),
            None,
            Some(1),
        ))
        .await
        .unwrap()
        .table_frontier()
        .unwrap()
        .lanes
        .len();
    assert_eq!(request.heads.len(), frontier_lanes);
    assert!(request.heads.iter().any(|head| head.through.is_some()));
    // The same peer resumes the same pair; another peer is refused.
    assert_eq!(
        store
            .regional_pairing
            .begin_pairing(&src, region(1), region(2), "peer")
            .await
            .unwrap(),
        pair
    );
    assert!(
        store
            .regional_pairing
            .begin_pairing(&src, region(1), region(3), "peer")
            .await
            .is_err()
    );
    store.finish().await;
}

#[tokio::test]
async fn given_table_without_stream_when_begin_pairing_then_validation_and_unpaired() {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .operations
        .create_table(hash_table_request(table.clone()))
        .await
        .unwrap();
    assert!(
        store
            .regional_pairing
            .begin_pairing(&table, region(1), region(2), "peer")
            .await
            .is_err()
    );
    assert!(
        store
            .regional_pairing
            .regional_binding(&table, PairId::generate())
            .await
            .unwrap()
            .is_none()
    );
    put(&store, &table, "k").await.unwrap();
    store.finish().await;
}

#[tokio::test]
async fn given_prepare_when_target_created_then_joining_with_receive_cursors_and_idempotent() {
    let store = TestStore::new().await;
    let (_, pair, tgt, request) = seeded(&store).await;
    let heads = request.heads.clone();
    for _ in 0..2 {
        let response = store
            .regional_pairing
            .prepare_target(request.clone(), region(2), region(1), "peer")
            .await
            .unwrap();
        assert!(
            matches!(response, RegionalResponse::Prepared),
            "{response:?}"
        );
    }
    store
        .operations
        .describe_table(storage_types::DescribeTableRequest {
            table_name: tgt.clone(),
        })
        .await
        .unwrap();
    assert_eq!(state(&store, &tgt, pair).await, Some(PairState::Joining));
    // The receive cursors equal the heads: a page that repeats a head is
    // covered, not a gap.
    for head in heads.iter().filter(|head| head.through.is_some()) {
        let lane = Lane {
            pair,
            table: &tgt,
            lineage: head.lineage.clone(),
            parents: Vec::new(),
        };
        let mut page = lane.page(None, 0, head.sealed, Vec::new());
        page.after = head.through.clone();
        page.through = head.through.clone();
        match receive(&store, page).await {
            RegionalResponse::Ack(ack) => assert_eq!(ack.through, head.through),
            other => panic!("covered page expected, got {other:?}"),
        }
    }
    let other = PrepareTarget {
        pair: PairId::generate(),
        ..request
    };
    let response = store
        .regional_pairing
        .prepare_target(other, region(2), region(1), "peer")
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            RegionalResponse::Rejected(RegionalRejection::TableExists)
        ),
        "{response:?}"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_joining_target_when_read_then_resource_not_found_and_describe_succeeds() {
    let store = TestStore::new().await;
    let (_, _, tgt) = prepared(&store).await;
    let name = tgt.as_ref();
    for _ in 0..2 {
        let error = store
            .operations
            .get_item(
                GetItemRequest::try_from(json!({"TableName": name, "Key": {"pk": {"S": "a"}}}))
                    .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(is_not_found(&error), "get_item {error:?}");
        let error = store
            .operations
            .query(
                QueryRequest::try_from(json!({
                    "TableName": name, "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": {":p": {"S": "a"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(is_not_found(&error), "query {error:?}");
        let error = store
            .operations
            .scan(ScanRequest::try_from(json!({"TableName": name})).unwrap())
            .await
            .unwrap_err();
        assert!(is_not_found(&error), "scan {error:?}");
        let error = store
            .operations
            .batch_get_item(
                BatchGetItemRequest::try_from(json!({
                    "RequestItems": {name: {"Keys": [{"pk": {"S": "a"}}]}}
                }))
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(is_not_found(&error), "batch_get_item {error:?}");
        let error = store
            .operations
            .transact_get_items(
                TransactGetItemsRequest::try_from(json!({
                    "TransactItems": [{"Get": {"TableName": name, "Key": {"pk": {"S": "a"}}}}]
                }))
                .unwrap(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                error.base_error(),
                Some(StorageEnum::ResourceNotFound {
                    resource_type: "table",
                    ..
                })
            ),
            "transact_get_items {error:?}"
        );
        let error = put(&store, &tgt, "a").await.unwrap_err();
        assert!(is_not_found(&error), "put_item {error:?}");
    }
    store
        .operations
        .describe_table(storage_types::DescribeTableRequest {
            table_name: tgt.clone(),
        })
        .await
        .unwrap();
    store.finish().await;
}

#[tokio::test]
async fn given_source_items_when_scanned_and_applied_then_target_has_items_with_orders() {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    let pages = scan_all(&store, &src, pair, &tgt).await;
    assert_eq!(pages.len(), 2);
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    for key in ["a", "b"] {
        let at_source = get(&store, &src, key).await.expect("source item");
        assert_eq!(
            get(&store, &tgt, key).await.expect("target item"),
            at_source,
            "{key}"
        );
    }
    let entries = history(&store, &tgt).await;
    assert_eq!(entries.len(), 2);
    let baseline = for_key(&entries, "a")[0].entry.regional().unwrap();
    assert_eq!(
        (baseline.order, baseline.role),
        (RegionalOrder::baseline(region(1)), RegionalRole::Applied)
    );
    let local = for_key(&entries, "b")[0].entry.regional().unwrap();
    assert!(local.order > RegionalOrder::baseline(region(1)));
    assert_eq!(history(&store, &tgt).await.len(), 2);
    store.finish().await;
}

#[tokio::test]
async fn given_first_scan_page_when_applied_again_then_scan_applied_and_entry_count_unchanged() {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    let pages = scan_all(&store, &src, pair, &tgt).await;
    // A head's `through` is a publication bound, not the last entry: on
    // Postgres it follows the database snapshot xmin and moves with any commit.
    // Lineages, sealing and the entry count are what a repeated page must keep.
    let lanes = |heads: Vec<regional_replication::message::LineageHead>| {
        heads
            .into_iter()
            .map(|head| (head.lineage, head.sealed))
            .collect::<Vec<_>>()
    };
    let before = lanes(
        store
            .regional_unpair
            .history_heads(&tgt, pair)
            .await
            .unwrap()
            .unwrap(),
    );
    let response = store
        .regional_pairing
        .apply_scan_page(pages[0].clone())
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::ScanApplied),
        "{response:?}"
    );
    assert_eq!(
        lanes(
            store
                .regional_unpair
                .history_heads(&tgt, pair)
                .await
                .unwrap()
                .unwrap()
        ),
        before
    );
    // The history is public once Active: one entry per scanned item, as
    // without the repeated page.
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    assert_eq!(history(&store, &tgt).await.len(), pages.len());
    store.finish().await;
}

#[tokio::test]
async fn given_page_delete_before_scan_copy_when_applied_then_item_stays_absent_with_received_marker()
 {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    let lane = Lane {
        pair,
        table: &tgt,
        lineage: lineage("s1", "l1"),
        parents: Vec::new(),
    };
    receive(
        &store,
        lane.page(None, 1, false, vec![delete_entry(1, "a", 5)]),
    )
    .await;
    scan_all(&store, &src, pair, &tgt).await;
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    assert!(get(&store, &tgt, "a").await.is_none());
    assert!(get(&store, &tgt, "b").await.is_some());
    // The received marker of `a` is kept until the sweep (observed in the sweep
    // test).
    store.finish().await;
}

#[tokio::test]
async fn given_activate_then_active_idempotent_and_scan_rejected() {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    let pages = scan_all(&store, &src, pair, &tgt).await;
    for _ in 0..2 {
        assert!(matches!(
            activate(&store, &tgt, pair).await,
            RegionalResponse::Activated
        ));
    }
    assert_eq!(state(&store, &tgt, pair).await, Some(PairState::Active));
    let unknown = activate(&store, &tgt, PairId::generate()).await;
    assert!(
        matches!(
            unknown,
            RegionalResponse::Rejected(RegionalRejection::UnknownPair)
        ),
        "{unknown:?}"
    );
    let late = store
        .regional_pairing
        .apply_scan_page(pages[0].clone())
        .await
        .unwrap();
    assert!(
        matches!(
            late,
            RegionalResponse::Rejected(RegionalRejection::NotJoining)
        ),
        "{late:?}"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_received_and_origin_markers_when_swept_then_received_become_hidden_echoes() {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    let lane = Lane {
        pair,
        table: &tgt,
        lineage: lineage("s1", "l1"),
        parents: Vec::new(),
    };
    receive(
        &store,
        lane.page(None, 1, false, vec![delete_entry(1, "a", 5)]),
    )
    .await;
    scan_all(&store, &src, pair, &tgt).await;
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    delete(&store, &tgt, "c").await.unwrap();
    let before = history(&store, &tgt).await.len();

    let sweep = |after: Option<Vec<u8>>| {
        let (store, tgt) = (&store, &tgt);
        async move {
            store
                .regional_pairing
                .sweep_received_markers(tgt, pair, after, limit(1))
                .await
                .unwrap()
        }
    };
    let mut pages = 0;
    let mut after = None;
    loop {
        let SweepTurn::Page { next } = sweep(after).await else {
            panic!("sweep stopped")
        };
        pages += 1;
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert!(pages >= 2, "two markers with limit 1 need several pages");
    let entries = history(&store, &tgt).await;
    assert_eq!(entries.len(), before + 1);
    let echoes = for_key(&entries, "a")
        .into_iter()
        .filter_map(|row| row.entry.regional())
        .filter(|mark| mark.role == RegionalRole::Echo)
        .collect::<Vec<_>>();
    assert_eq!(echoes.len(), 1);
    assert!(echoes[0].hidden);
    assert_eq!(echoes[0].order, o(5));

    // A second sweep changes nothing: the origin marker of `c` stays, `a` has
    // none.
    let mut after = None;
    loop {
        let SweepTurn::Page { next } = sweep(after).await else {
            panic!("sweep stopped")
        };
        match next {
            Some(next) => after = Some(next),
            None => break,
        }
    }
    assert_eq!(history(&store, &tgt).await.len(), before + 1);
    store.finish().await;
}

#[tokio::test]
async fn given_seeding_with_markers_when_abandoned_then_unpaired_and_cleared() {
    let store = TestStore::new().await;
    let (src, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let pair = store
        .regional_pairing
        .begin_pairing(&src, region(1), region(2), "peer")
        .await
        .unwrap();
    delete(&store, &src, "x").await.unwrap();
    assert!(
        store
            .regional_pairing
            .abandon_pairing(&src, pair)
            .await
            .unwrap()
    );
    assert!(
        store
            .regional_pairing
            .regional_binding(&src, pair)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !store
            .regional_pairing
            .abandon_pairing(&src, pair)
            .await
            .unwrap()
    );
    // Send progress is gone with the binding.
    let limits = storage_provider::SendLimits {
        entries: limit(10),
        bytes: 1 << 20,
    };
    let turn = store
        .regional_send
        .next_send_page(&src, pair, 0, limits)
        .await
        .unwrap();
    assert!(
        matches!(turn, storage_provider::SendTurn::PairGone),
        "{turn:?}"
    );
    // A fresh pairing starts with a new pair id and no old marker or progress.
    let fresh = store
        .regional_pairing
        .begin_pairing(&src, region(1), region(2), "peer")
        .await
        .unwrap();
    assert_ne!(fresh, pair);
    store.finish().await;
}

#[tokio::test]
async fn given_joining_table_with_ttl_when_expiration_runs_then_skipped_until_active() {
    use std::num::{NonZeroU64, NonZeroUsize};

    use scheduled_bg_jobs::{JobId, Namespace};
    use storage_provider::maintenance::ExpirationRequest;

    let store = TestStore::new().await;
    let (src, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    store
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": src.as_ref(), "Item": {"pk": {"S": "t"}, "ttl": {"N": "1"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let pair = store
        .regional_pairing
        .begin_pairing(&src, region(1), region(2), "peer")
        .await
        .unwrap();
    store.wait_until_changes_are_readable().await;
    let mut request = store
        .regional_pairing
        .prepare_request(&src, pair)
        .await
        .unwrap()
        .unwrap();
    let tgt = unique_table_name();
    request.table.table_name = tgt.clone();
    store
        .regional_pairing
        .prepare_target(request, region(2), region(1), "peer")
        .await
        .unwrap();
    store
        .operations
        .update_time_to_live(storage_types::UpdateTimeToLiveRequest {
            table_name: tgt.clone(),
            time_to_live_specification: storage_types::TimeToLiveSpecification {
                attribute_name: "ttl".into(),
                enabled: true,
            },
        })
        .await
        .unwrap();
    scan_all(&store, &src, pair, &tgt).await;

    let pass = |fence: u64| ExpirationRequest {
        destination: Namespace::try_new("conformance:regional-pairing").expect("namespace"),
        job: JobId::from([9; 16]),
        fence: NonZeroU64::new(fence).expect("fence"),
        requested_cutoff: storage_types::TimestampMillis::from_timestamp(2_000_000_000_000),
        candidate_limit: NonZeroUsize::new(100).unwrap(),
    };
    store.maintenance.advance_expiration(pass(1)).await.unwrap();
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    // The Joining table was skipped: the due item is still there.
    assert!(get(&store, &tgt, "t").await.is_some());
    store.maintenance.advance_expiration(pass(2)).await.unwrap();
    assert!(get(&store, &tgt, "t").await.is_none());
    store.finish().await;
}

#[tokio::test]
async fn given_markers_over_two_pages_when_listed_then_keys_decode_in_order() {
    let store = TestStore::new().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    store
        .regional_pairing
        .begin_pairing(&table, region(1), region(2), "peer")
        .await
        .unwrap();
    // A paired absent delete leaves an origin marker; written out of key order.
    for key in ["m3", "m1", "m2"] {
        delete(&store, &table, key).await.unwrap();
    }
    let (first, first_keys) = (store.regional_markers)(table.clone(), None, limit(2))
        .await
        .unwrap();
    assert_eq!(first.markers.len(), 2);
    assert!(first.more);
    assert_eq!(
        first_keys,
        [for_key_attributes("m1"), for_key_attributes("m2")]
    );
    let after = first.markers.last().map(|(key, _)| key.clone());
    let (rest, rest_keys) = (store.regional_markers)(table.clone(), after, limit(2))
        .await
        .unwrap();
    assert_eq!(rest.markers.len(), 1);
    assert!(!rest.more);
    assert_eq!(rest_keys, [for_key_attributes("m3")]);
    store.finish().await;
}

fn for_key_attributes(key: &str) -> storage_types::KeyAttributes {
    super::regional_receive_tests::keys(key)
}
