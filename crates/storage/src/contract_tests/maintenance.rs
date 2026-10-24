#![cfg(feature = "sqlite")]

#[cfg(feature = "remote")]
use storage_provider::{
    RemoteCredentialStrategy, RemoteStorageSettings, StorageBackend, StorageConfig,
};
#[cfg(feature = "remote")]
use storage_types::StorageEnum;

use super::stateful_test_support::{FaultPoint, StatefulTestBackend};
#[cfg(feature = "remote")]
use crate::{DatabaseManager, DatabaseManagerRuntimeOptions};

#[tokio::test]
async fn given_old_destination_when_new_fence_commits_then_stale_effect_cannot_commit() {
    let fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    fixture
        .create_expiration_target("maintenance_takeover")
        .await
        .expect("create maintenance target");
    let initial_revision = fixture
        .observe()
        .await
        .expect("observe seeded task")
        .progress_revision;
    let paused = fixture
        .pause_before_commit(FaultPoint::MaintenanceCommit)
        .await
        .expect("P01 must connect the destination transaction barrier");
    fixture
        .drive_ready_work()
        .await
        .expect("new owner must drive the same durable destination");
    let committed = fixture.observe().await.expect("observe new owner commit");
    assert_eq!(committed.effect_count, 1);
    assert_eq!(committed.progress_revision, initial_revision + 1);
    paused
        .release()
        .await
        .expect("stale maintenance drive must settle without effects");

    let observed = fixture.observe().await.expect("observe committed state");
    assert_eq!(observed.effect_count, 1);
    assert_eq!(observed.progress_revision, committed.progress_revision);
}

#[tokio::test]
async fn given_committed_page_when_ack_is_lost_then_restart_does_not_repeat_effects() {
    let mut fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    fixture
        .create_expiration_target("maintenance_lost_ack")
        .await
        .expect("create maintenance target");
    fixture
        .lose_commit_ack()
        .expect("P01 must connect commit acknowledgement loss");
    fixture
        .drive_ready_work()
        .await
        .expect("drive the ambiguous page");
    fixture.restart().await.expect("reopen the same backend");
    fixture
        .drive_ready_work()
        .await
        .expect("resume from destination progress");

    let observed = fixture.observe().await.expect("observe resumed state");
    assert_eq!(observed.effect_count, 1);
}

#[tokio::test]
async fn given_paused_old_incarnation_when_table_is_recreated_then_replacement_is_untouched() {
    let fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    fixture
        .create_expiration_target("maintenance_recreate")
        .await
        .expect("create original target");
    let paused = fixture
        .pause_before_commit(FaultPoint::MaintenanceCommit)
        .await
        .expect("P01 must connect the target-incarnation barrier");
    fixture
        .delete()
        .await
        .expect("delete the original target through the public manager");
    fixture
        .create_table_with_stream("maintenance_recreate", true)
        .await
        .expect("create replacement target");
    paused
        .release()
        .await
        .expect("old incarnation drive must settle without effects");

    let observed = fixture.observe().await.expect("observe replacement state");
    assert_eq!(observed.effect_count, 0);
    assert_eq!(observed.generation, 2);
}

#[tokio::test]
async fn given_ready_expiration_page_when_runtime_shuts_down_then_no_page_starts() {
    let fixture = StatefulTestBackend::sqlite()
        .await
        .expect("construct SQLite stateful fixture");
    fixture
        .create_expiration_target("maintenance_shutdown")
        .await
        .expect("create maintenance target");
    let initial_revision = fixture
        .observe()
        .await
        .expect("observe seeded task")
        .progress_revision;
    fixture
        .shutdown_before_next_expiration_page()
        .await
        .expect("shutdown must stop the next expiration page");

    let observed = fixture.observe().await.expect("observe shutdown state");
    assert_eq!(observed.visible_items, ["A"]);
    assert_eq!(observed.effect_count, 0);
    assert_eq!(observed.progress_revision, initial_revision);
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_backend_when_local_maintenance_is_enabled_then_startup_rejects_before_io() {
    let result = DatabaseManager::new_with_config_and_runtime_options(
        StorageConfig {
            backend_type: StorageBackend::Remote,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: Some(RemoteStorageSettings {
                endpoint_urls: vec!["http://127.0.0.1:1".to_owned()],
                region: None,
                tls: false,
                credentials: RemoteCredentialStrategy::DefaultChain,
                timeouts: None,
            }),
        },
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(true)
            .build(),
    )
    .await;

    let error = match result {
        Ok(_) => panic!("remote service owns its database maintenance"),
        Err(error) => error,
    };
    assert!(matches!(
        error.as_ref(),
        StorageEnum::Unsupported { message }
            if message.contains("delegates database maintenance")
    ));
}
