#![cfg(feature = "sqlite")]

use super::stateful_test_support::{FaultPoint, StatefulTestBackend};

#[tokio::test]
async fn given_expired_candidate_when_ttl_changes_then_newer_item_survives() {
    let fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    fixture
        .create_expiration_target("reclamation_ttl_race")
        .await
        .expect("create TTL table");
    let paused = fixture
        .pause_before_commit(FaultPoint::MaintenanceCommit)
        .await
        .expect("P03 must connect TTL candidate commit barrier");
    fixture
        .put()
        .await
        .expect("move candidate TTL into the future");
    paused
        .release()
        .await
        .expect("paused TTL drive must settle");

    let observed = fixture.observe().await.expect("observe TTL outcome");
    assert_eq!(observed.visible_items, ["A"]);
    assert_eq!(observed.effect_count, 0);
}

#[tokio::test]
async fn given_live_hold_when_trim_races_split_then_history_survives_until_release() {
    assert_stream_reclamation_hold_race(
        StatefulTestBackend::sqlite()
            .await
            .expect("construct SQLite stateful fixture"),
    )
    .await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_live_hold_on_turso_when_trim_races_split_then_history_survives_until_release() {
    assert_stream_reclamation_hold_race(
        StatefulTestBackend::turso()
            .await
            .expect("construct Turso stateful fixture"),
    )
    .await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_live_hold_on_rocksdb_when_trim_races_split_then_history_survives_until_release() {
    assert_stream_reclamation_hold_race(
        StatefulTestBackend::rocksdb()
            .await
            .expect("construct RocksDB stateful fixture"),
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn given_live_hold_on_postgres_when_trim_races_split_then_history_survives_until_release() {
    let fixture = StatefulTestBackend::postgres()
        .await
        .expect("construct Postgres stateful fixture")
        .expect("Postgres stateful fixture requires a configured test DSN");
    assert_stream_reclamation_hold_race(fixture).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn given_live_hold_on_foundationdb_when_trim_races_split_then_history_survives_until_release()
{
    let fixture = StatefulTestBackend::foundationdb()
        .await
        .expect("construct FoundationDB stateful fixture")
        .expect("FoundationDB stateful fixture requires a configured cluster file");
    assert_stream_reclamation_hold_race(fixture).await;
}

async fn assert_stream_reclamation_hold_race(fixture: StatefulTestBackend) {
    fixture
        .establish_stream_reclamation_hold("reclamation_hold_race")
        .await
        .expect("establish protected SystemStream history");

    let before = fixture
        .read_system_history_ids()
        .await
        .expect("physical history before trim");
    let protected_record = *before.first().expect("eligible protected record");
    let reclaimed_while_held = fixture
        .race_stream_trim_and_split()
        .await
        .expect("split and trim race must serialize safely");
    assert_eq!(reclaimed_while_held, 0);
    assert!(
        fixture
            .read_held_system_stream()
            .await
            .expect("retained history must remain readable")
            > 0
    );

    assert_eq!(
        fixture
            .read_system_history_ids()
            .await
            .expect("held physical history"),
        before,
        "the same physical records must survive the hold and split"
    );
    fixture
        .release_stream_reclamation_hold()
        .await
        .expect("release protected SystemStream history");
    let reclaimed_after_release = fixture
        .trim_released_system_stream()
        .await
        .expect("released history must become reclaimable");
    assert!(reclaimed_after_release > 0);
    assert!(
        !fixture
            .read_system_history_ids()
            .await
            .expect("released physical history")
            .contains(&protected_record),
        "the exact record protected before release must now be reclaimed"
    );
}
