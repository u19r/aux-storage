//! Leave, finish, force and pair end through real provider operations (REG-11).
//! Needs `TestStore.regional_unpair: Arc<dyn
//! storage_provider::RegionalUnpairOperations>`.

use regional_replication::message::{
    FinishPair, LineageHead, PairTable, RegionalRejection, RegionalResponse,
};
use storage_types::{PairId, PairState, StorageEnum, SystemStreamOffset};

use super::{
    regional_pairing_tests::{activate, prepared, scan_all, seeded},
    regional_receive_tests::{Lane, delete_entry, for_key, history, receive},
    regional_write_tests::{delete, delete_table, put, region},
    test_support::TestStore,
};

/// The offset one order step past `offset`, in the same width and with the same
/// suffix.
fn past(offset: &SystemStreamOffset) -> SystemStreamOffset {
    let text = offset.as_str();
    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let next = offset.order().unwrap() + 1;
    SystemStreamOffset::try_new(format!(
        "{next:0width$}{}",
        text.get(split..).unwrap(),
        width = split
    ))
    .unwrap()
}

fn target(pair: PairId, table: &storage_types::TableName) -> PairTable {
    PairTable {
        pair,
        table: table.clone(),
    }
}

async fn binding_state(store: &TestStore, table: &storage_types::TableName) -> Option<PairState> {
    store
        .regional_unpair
        .current_binding(table)
        .await
        .unwrap()
        .map(|binding| binding.state)
}

#[tokio::test]
async fn given_active_pair_when_begin_leave_then_leaving_writes_rejected_and_idempotent() {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    scan_all(&store, &src, pair, &tgt).await;
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    for _ in 0..2 {
        assert_eq!(
            store
                .regional_unpair
                .begin_leave(&target(pair, &tgt))
                .await
                .unwrap(),
            Some(PairState::Leaving)
        );
    }
    let error = put(&store, &tgt, "w").await.unwrap_err();
    assert!(
        matches!(error.base_error(), Some(StorageEnum::RawValidation { .. })),
        "{error:?}"
    );
    // The source is still Seeding; an unknown pair is not found.
    assert_eq!(
        store
            .regional_unpair
            .begin_leave(&target(pair, &src))
            .await
            .unwrap(),
        Some(PairState::Seeding)
    );
    assert_eq!(
        store
            .regional_unpair
            .begin_leave(&target(PairId::generate(), &tgt))
            .await
            .unwrap(),
        None
    );
    store.finish().await;
}

#[tokio::test]
async fn given_uncovered_last_when_finish_then_pending_and_pair_kept() {
    let store = TestStore::new().await;
    let (src, pair, tgt, request) = seeded(&store).await;
    let response = store
        .regional_pairing
        .prepare_target(request.clone(), region(2), region(1), "peer")
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::Prepared),
        "{response:?}"
    );
    scan_all(&store, &src, pair, &tgt).await;
    activate(&store, &tgt, pair).await;
    let last = request
        .heads
        .iter()
        .cloned()
        .map(|head| LineageHead {
            through: head.through.as_ref().map(past),
            sealed: false,
            ..head
        })
        .collect::<Vec<_>>();
    assert!(!last.is_empty());
    let response = store
        .regional_unpair
        .finish_pair(FinishPair {
            pair,
            table: tgt.clone(),
            last,
        })
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::Pending),
        "{response:?}"
    );
    assert_eq!(binding_state(&store, &tgt).await, Some(PairState::Active));
    store.finish().await;
}

#[tokio::test]
async fn given_covered_last_when_finish_then_finished_and_cleanup_committed() {
    let store = TestStore::new().await;
    let (src, pair, tgt, request) = seeded(&store).await;
    let response = store
        .regional_pairing
        .prepare_target(request.clone(), region(2), region(1), "peer")
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::Prepared),
        "{response:?}"
    );
    // A received marker (delete while Joining) and an origin marker (local
    // delete once Active).
    let lane = Lane {
        pair,
        table: &tgt,
        lineage: super::regional_receive_tests::lineage("s1", "l1"),
        parents: Vec::new(),
    };
    receive(
        &store,
        lane.page(None, 1, false, vec![delete_entry(1, "a", 5)]),
    )
    .await;
    scan_all(&store, &src, pair, &tgt).await;
    activate(&store, &tgt, pair).await;
    delete(&store, &tgt, "c").await.unwrap();
    // The receive cursors equal the heads of the prepare request.
    let request = FinishPair {
        pair,
        table: tgt.clone(),
        last: request.heads.clone(),
    };
    let response = store
        .regional_unpair
        .finish_pair(request.clone())
        .await
        .unwrap();
    assert!(
        matches!(response, RegionalResponse::Finished),
        "{response:?}"
    );
    assert_eq!(binding_state(&store, &tgt).await, None);
    let again = store.regional_unpair.finish_pair(request).await.unwrap();
    assert!(
        matches!(
            again,
            RegionalResponse::Rejected(RegionalRejection::UnknownPair)
        ),
        "{again:?}"
    );
    // The cleanup committed with the binding: the table can be deleted again.
    delete_table(&store, &tgt).await.unwrap();
    store.finish().await;
}

#[tokio::test]
async fn given_force_when_any_state_then_unpaired_and_delete_table_leaves_no_rows() {
    let store = TestStore::new().await;
    // Seeding.
    let (seeding, _, joining) = prepared(&store).await;
    // Leaving: a second pair whose target is activated and then left.
    let (src2, pair2, leaving) = prepared(&store).await;
    scan_all(&store, &src2, pair2, &leaving).await;
    activate(&store, &leaving, pair2).await;
    store
        .regional_unpair
        .begin_leave(&target(pair2, &leaving))
        .await
        .unwrap();
    for (table, expected) in [
        (&seeding, PairState::Seeding),
        (&joining, PairState::Joining),
        (&leaving, PairState::Leaving),
    ] {
        assert_eq!(binding_state(&store, table).await, Some(expected));
        assert!(
            store
                .regional_unpair
                .force_unpair(table, None)
                .await
                .unwrap()
        );
        assert_eq!(binding_state(&store, table).await, None);
        delete_table(&store, table).await.unwrap();
    }
    store.finish().await;
}

#[tokio::test]
async fn given_unpaired_after_end_when_delete_item_then_no_marker_and_public_entry_only() {
    let store = TestStore::new().await;
    let (src, pair, _) = prepared(&store).await;
    assert!(
        store
            .regional_unpair
            .force_unpair(&src, Some(pair))
            .await
            .unwrap()
    );
    put(&store, &src, "k").await.unwrap();
    delete(&store, &src, "k").await.unwrap();
    delete(&store, &src, "q").await.unwrap();
    let entries = history(&store, &src).await;
    let for_k = for_key(&entries, "k");
    let last = for_k.last().expect("remove entry");
    assert!(last.entry.regional().is_none());
    assert!(last.entry.is_public());
    assert!(
        for_key(&entries, "q").is_empty(),
        "an unpaired absent delete writes nothing"
    );
    store.finish().await;
}

#[tokio::test]
async fn given_force_with_other_pair_then_false_and_unchanged() {
    let store = TestStore::new().await;
    let (src, pair, _) = prepared(&store).await;
    assert!(
        !store
            .regional_unpair
            .force_unpair(&src, Some(PairId::generate()))
            .await
            .unwrap()
    );
    let binding = store
        .regional_unpair
        .current_binding(&src)
        .await
        .unwrap()
        .expect("still paired");
    assert_eq!(binding.pair, pair);
    store.finish().await;
}

#[tokio::test]
async fn given_leaving_target_when_activate_repeated_then_activated() {
    let store = TestStore::new().await;
    let (src, pair, tgt) = prepared(&store).await;
    scan_all(&store, &src, pair, &tgt).await;
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    assert_eq!(
        store
            .regional_unpair
            .begin_leave(&target(pair, &tgt))
            .await
            .unwrap(),
        Some(PairState::Leaving)
    );
    // A lost `Activated` reply followed by a removal: the repeated Activate is
    // still answered.
    assert!(matches!(
        activate(&store, &tgt, pair).await,
        RegionalResponse::Activated
    ));
    assert_eq!(binding_state(&store, &tgt).await, Some(PairState::Leaving));
    store.finish().await;
}
