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
async fn given_checkpoint_when_trim_races_split_then_checkpoint_does_not_delay_retirement() {
    assert_stream_reclamation_checkpoint_race(
        StatefulTestBackend::sqlite()
            .await
            .expect("construct SQLite stateful fixture"),
    )
    .await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn given_checkpoint_on_turso_when_trim_races_split_then_checkpoint_does_not_delay_retirement()
{
    assert_stream_reclamation_checkpoint_race(
        StatefulTestBackend::turso()
            .await
            .expect("construct Turso stateful fixture"),
    )
    .await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_checkpoint_on_rocksdb_when_trim_races_split_then_checkpoint_does_not_delay_retirement()
 {
    assert_stream_reclamation_checkpoint_race(
        StatefulTestBackend::rocksdb()
            .await
            .expect("construct RocksDB stateful fixture"),
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn given_checkpoint_on_postgres_when_trim_races_split_then_checkpoint_does_not_delay_retirement()
 {
    let fixture = StatefulTestBackend::postgres()
        .await
        .expect("construct Postgres stateful fixture")
        .expect("Postgres stateful fixture requires a configured test DSN");
    assert_stream_reclamation_checkpoint_race(fixture).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires a configured external database service"]
async fn given_checkpoint_on_foundationdb_when_trim_races_split_then_checkpoint_does_not_delay_retirement()
 {
    let fixture = StatefulTestBackend::foundationdb()
        .await
        .expect("construct FoundationDB stateful fixture")
        .expect("FoundationDB stateful fixture requires a configured cluster file");
    assert_stream_reclamation_checkpoint_race(fixture).await;
}

async fn assert_stream_reclamation_checkpoint_race(fixture: StatefulTestBackend) {
    fixture
        .establish_stream_reclamation_checkpoint("reclamation_checkpoint_race")
        .await
        .expect("establish qualified SystemStream checkpoint");
    let before = fixture
        .read_system_history_positions()
        .await
        .expect("source history before trim");
    assert_eq!(before.len(), 4);
    let reclaimed = fixture
        .race_stream_trim_and_split()
        .await
        .expect("split and trim must serialize safely");
    assert_eq!(
        reclaimed, 4,
        "reader checkpoints cannot delay source retirement"
    );
    assert!(
        fixture
            .read_system_history_positions()
            .await
            .expect("fresh source history")
            .is_empty()
    );
    assert!(
        fixture.read_checkpoint_system_stream().await.is_err(),
        "retired checkpoint must report a gap"
    );
    assert_eq!(
        fixture
            .read_reclamation_table_records()
            .await
            .expect("independent table history"),
        4,
        "system retirement must preserve longer table history and its item bodies"
    );
}
