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

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_required_history_gap_blocks_index_activation() {
    let fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    assert_history_gap_blocks_activation(fixture).await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn turso_required_history_gap_blocks_index_activation() {
    let fixture = StatefulTestBackend::turso()
        .await
        .expect("construct Turso stateful fixture");
    assert_history_gap_blocks_activation(fixture).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_required_history_gap_blocks_index_activation() {
    let fixture = StatefulTestBackend::rocksdb()
        .await
        .expect("construct RocksDB stateful fixture");
    assert_history_gap_blocks_activation(fixture).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_required_history_gap_blocks_index_activation() {
    let Some(fixture) = StatefulTestBackend::postgres()
        .await
        .expect("construct Postgres stateful fixture")
    else {
        return;
    };
    assert_history_gap_blocks_activation(fixture).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
async fn foundationdb_required_history_gap_blocks_index_activation() {
    let Some(fixture) = StatefulTestBackend::foundationdb()
        .await
        .expect("construct FoundationDB stateful fixture")
    else {
        return;
    };
    assert_history_gap_blocks_activation(fixture).await;
}

async fn assert_history_gap_blocks_activation(mut fixture: StatefulTestBackend) {
    fixture
        .create_table("gsi_history_gap")
        .await
        .expect("create source table");
    fixture
        .put_initial_gsi_items()
        .await
        .expect("put source rows before capturing the retained boundary");
    fixture
        .establish_non_origin_gsi_history()
        .await
        .expect("establish a physical SystemStream boundary before the target build");
    fixture
        .create_index()
        .await
        .expect("P02 must connect retained-history build setup");
    fixture
        .discard_required_gsi_history()
        .await
        .unwrap_or_else(|error| {
            panic!(
                "remove history required by the build: {:?}",
                error.recursive_context(Vec::new())
            )
        });
    fixture
        .drive_gsi_until_idle()
        .await
        .expect("drive build after removing required history");

    let observed = fixture
        .observe()
        .await
        .unwrap_or_else(|error| panic!("observe build status: {error:?}"));
    assert!(!observed.active);
}
