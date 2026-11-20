use storage_types::context::WrappedError as _;

use super::stateful_test_support::StatefulTestBackend;

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_paginated_build_tracks_items_that_move_and_delete() {
    let fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    assert_moving_projection(fixture).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn turso_paginated_build_tracks_items_that_move_and_delete() {
    let fixture = StatefulTestBackend::turso()
        .await
        .expect("construct Turso stateful fixture");
    assert_moving_projection(fixture).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_paginated_build_tracks_items_that_move_and_delete() {
    let fixture = StatefulTestBackend::rocksdb()
        .await
        .expect("construct RocksDB stateful fixture");
    assert_moving_projection(fixture).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_paginated_build_tracks_items_that_move_and_delete() {
    let Some(fixture) = StatefulTestBackend::postgres()
        .await
        .expect("construct Postgres stateful fixture")
    else {
        return;
    };
    assert_moving_projection(fixture).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn foundationdb_paginated_build_tracks_items_that_move_and_delete() {
    let Some(fixture) = StatefulTestBackend::foundationdb()
        .await
        .expect("construct FoundationDB stateful fixture")
    else {
        return;
    };
    assert_moving_projection(fixture).await;
}

async fn assert_moving_projection(fixture: StatefulTestBackend) {
    fixture
        .create_table("gsi_moving_projection")
        .await
        .expect("create source table");
    fixture
        .put_initial_gsi_items()
        .await
        .expect("put A and B before the GSI snapshot begins");
    fixture
        .create_index()
        .await
        .expect("P02 must connect index creation with a two-page build");
    fixture
        .move_gsi_item()
        .await
        .unwrap_or_else(|error| panic!("move A to group y: {:?}", error.to_enum()));
    fixture
        .delete_gsi_item()
        .await
        .unwrap_or_else(|error| panic!("delete B: {error:?}"));
    for attempt in 0..64 {
        fixture
            .drive_gsi_until_idle()
            .await
            .unwrap_or_else(|error| panic!("drive scan and catch-up: {error:?}"));
        if fixture
            .observe()
            .await
            .expect("observe build readiness")
            .active
        {
            break;
        }
        assert!(
            attempt < 63,
            "GSI did not activate within bounded scheduler cycles"
        );
    }

    let rows = fixture.query_index().await.expect("query the public index");
    assert_eq!(rows, ["A:y"]);
}
