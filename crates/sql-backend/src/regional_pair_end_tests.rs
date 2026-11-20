//! Pair end (finish, force-unpair, abandon) against a paired local write that
//! stages a regional marker under the shared catalog lock and commits while
//! the pair end waits for the exclusive catalog lock. The marker must not
//! survive the pair end.

use std::{num::NonZeroU16, time::Duration};

use regional_replication::message::{FinishPair, RegionalResponse};
use storage_provider::{
    CatalogAttempt, DynamoOperations, DynamoPort, DynamoStreamWriteAttempt, RegionalMarkerAttempt,
    RegionalPairingOperations, RegionalUnpairOperations,
};
use storage_types::{
    AttemptDeadlineSource, AttemptMode, AttemptPlan, AttributeValue, CreateTableRequest,
    DeadlineClass, KeyAttributes, PairState, RegionId, RegionalMarker, RegionalOrder,
    StorageResult, TableName,
};
use tokio::sync::oneshot;

use crate::{SqlStore, postgres::PostgresBackend};

/// A wait for a database state gives up after this long.
const STATE_WAIT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum PairEnd {
    Finish,
    ForceUnpair,
    Abandon,
}

fn region(id: u16) -> RegionId {
    RegionId(NonZeroU16::new(id).unwrap())
}

async fn wait_for_blocked_exclusive_lock(admin: &tokio_postgres::Client) {
    let deadline = tokio::time::Instant::now() + STATE_WAIT;
    loop {
        let waiting: i64 = admin
            .query_one(
                "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE \
                 l.locktype = 'advisory' AND NOT l.granted AND a.datname = current_database()",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if waiting > 0 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "waited {STATE_WAIT:?} for a backend blocked on an ungranted advisory (catalog) lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn check(end: PairEnd) {
    let dsn = std::env::var("TEST_POSTGRES_DSN").expect("TEST_POSTGRES_DSN");
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let schema = format!("pair_end_{}", uuid::Uuid::new_v4().simple());
    admin
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    let separator = if dsn.contains('?') { '&' } else { '?' };
    let scoped = format!("{dsn}{separator}options=-csearch_path%3D{schema}");
    let backend = PostgresBackend::initialize(
        &scoped,
        4,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store =
        SqlStore::initialize_schema(backend, storage_types::InitialStreamLineages::ONE, true)
            .await
            .unwrap();

    let name = TableName::new("pair_end_orders");
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "pair_end_orders",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }))
    .unwrap();
    DynamoOperations::create_table(&store, request)
        .await
        .unwrap();
    let pair =
        RegionalPairingOperations::begin_pairing(&store, &name, region(1), region(2), "peer")
            .await
            .unwrap();
    if matches!(end, PairEnd::Finish) {
        let state = RegionalPairingOperations::advance_pair_state(
            &store,
            &name,
            pair,
            PairState::Seeding,
            PairState::Active,
        )
        .await
        .unwrap();
        assert_eq!(state, Some(PairState::Active));
    }

    // The paired local write: shared catalog lock, marker staged and flushed,
    // attempt held open until released.
    let (staged_tx, staged_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let writer = {
        let store = store.clone();
        let name = name.clone();
        tokio::spawn(async move {
            let request = AttemptPlan::new(AttemptMode::ReadWriteItems, DeadlineClass::Write)
                .scoped(storage_types::AttemptScope::Data)
                .start(store.attempt_deadlines());
            DynamoPort::with_attempt(&store, request, move |attempt| {
                Box::pin(async move {
                    let stamp = attempt
                        .read_catalog_stamp(&name)
                        .await?
                        .expect("the paired table exists");
                    let table = attempt.read_table_at_stamp(&name, stamp).await?;
                    let mut key = KeyAttributes::new();
                    key.insert("pk", AttributeValue::S("marked".into()));
                    let marker = RegionalMarker {
                        order: RegionalOrder::from_parts(1_000, region(1), false),
                        received: false,
                    };
                    attempt.stage_regional_marker(&table, &key, Some(marker))?;
                    attempt.finish_dynamo_write().await?;
                    let _ = staged_tx.send(());
                    let _ = release_rx.await;
                    StorageResult::Ok(())
                })
            })
            .await
        })
    };
    tokio::time::timeout(STATE_WAIT, staged_rx)
        .await
        .expect("the writer stages its marker")
        .expect("the writer stays alive until released");

    // The pair end starts after the writer holds the shared lock and blocks on
    // the exclusive lock.
    let unpair = {
        let store = store.clone();
        let name = name.clone();
        tokio::spawn(async move {
            match end {
                PairEnd::Finish => {
                    let response = RegionalUnpairOperations::finish_pair(
                        &store,
                        FinishPair {
                            pair,
                            table: name,
                            last: vec![],
                        },
                    )
                    .await
                    .unwrap();
                    assert!(
                        matches!(response, RegionalResponse::Finished),
                        "{response:?}"
                    );
                }
                PairEnd::ForceUnpair => assert!(
                    RegionalUnpairOperations::force_unpair(&store, &name, Some(pair))
                        .await
                        .unwrap()
                ),
                PairEnd::Abandon => assert!(
                    RegionalPairingOperations::abandon_pairing(&store, &name, pair)
                        .await
                        .unwrap()
                ),
            }
        })
    };
    wait_for_blocked_exclusive_lock(&admin).await;
    assert!(
        !unpair.is_finished(),
        "{end:?} must wait for the shared lock holder"
    );

    release_tx.send(()).unwrap();
    writer
        .await
        .unwrap()
        .unwrap_or_else(|error| panic!("{end:?}: the paired write commits: {:?}", error.as_ref()));
    unpair
        .await
        .unwrap_or_else(|_| panic!("{end:?}: pair end task"));

    let markers: i64 = admin
        .query_one(
            &format!("SELECT count(*) FROM {schema}.regional_markers"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(markers, 0, "{end:?}: no marker survives the pair end");
    assert!(
        RegionalUnpairOperations::current_binding(&store, &name)
            .await
            .unwrap()
            .is_none(),
        "{end:?}: the pair is ended"
    );
    drop(store);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_paired_write_committing_during_exclusive_wait_when_force_unpair_then_no_marker_survives()
 {
    check(PairEnd::ForceUnpair).await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_paired_write_committing_during_exclusive_wait_when_finish_pair_then_no_marker_survives()
 {
    check(PairEnd::Finish).await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_paired_write_committing_during_exclusive_wait_when_abandon_pairing_then_no_marker_survives()
 {
    check(PairEnd::Abandon).await;
}
