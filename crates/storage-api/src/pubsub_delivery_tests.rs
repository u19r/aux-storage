use std::{sync::Arc, time::Duration};

/// Review D12: file settings reach the runner and both managers. The
/// rules are checked at load (`config` test
/// `pubsub_delivery_rules_reject_at_load`).
#[test]
fn given_pubsub_file_settings_when_prepared_then_runner_and_urls_follow_them() {
    let mut config = config::PubsubConfig {
        public_base_url: Some("https://sns.example.test/pubsub".to_owned()),
        ..config::PubsubConfig::default()
    };
    config.delivery.per_destination = 3;
    config.delivery.max_in_flight = 12;
    config.delivery.request_timeout_seconds = 4;
    config.delivery.lease_seconds = 13;
    config.delivery.idle_probe_millis = 300;
    config.validate().unwrap();
    let storage::messaging::PubsubServiceConfig { delivery, runner } =
        super::service_host::pubsub_delivery(&config);
    assert_eq!(runner.per_destination.get(), 3);
    assert_eq!(runner.max_in_flight.get(), 12);
    assert_eq!(runner.idle_probe, Duration::from_millis(300));
    assert_eq!(delivery.timing.request_timeout(), Duration::from_secs(4));
    assert_eq!(delivery.timing.lease(), Duration::from_secs(13));
    assert_eq!(
        delivery.subscribe_url_base.as_deref(),
        Some("https://sns.example.test/pubsub")
    );
}

#[tokio::test]
async fn given_default_switches_when_state_is_prepared_then_no_messaging_service_exists() {
    let database = Arc::new(storage::DatabaseManager::new_for_test().await.unwrap());
    let state = super::service_host::prepare_app_state(
        database,
        &config::RootConfig::default(),
        "0.0.0.0:3000",
    )
    .unwrap();
    assert!(state.queue_manager.is_none());
    assert!(state.pubsub_manager.is_none());
}

#[cfg(feature = "remote")]
#[tokio::test]
async fn given_remote_backend_when_a_switch_is_on_then_startup_names_the_switch() {
    use storage_provider::{RemoteCredentialStrategy, RemoteStorageSettings};
    let database = Arc::new(
        storage::DatabaseManager::new_for_test_with_remote(RemoteStorageSettings {
            endpoint_urls: vec!["http://127.0.0.1:1".to_string()],
            region: None,
            tls: false,
            credentials: RemoteCredentialStrategy::DefaultChain,
            timeouts: None,
        })
        .await
        .unwrap(),
    );
    let off = super::service_host::prepare_app_state(
        Arc::clone(&database),
        &config::RootConfig::default(),
        "0.0.0.0:3000",
    )
    .unwrap();
    assert!(off.queue_manager.is_none() && off.pubsub_manager.is_none());
    let mut root = config::RootConfig::default();
    root.queue.enabled = true;
    let error =
        super::service_host::prepare_app_state(Arc::clone(&database), &root, "0.0.0.0:3000")
            .err()
            .expect("queue.enabled must fail on a backend without a queue provider");
    assert!(error.to_string().contains("queue.enabled"), "{error}");
    root.queue.enabled = false;
    root.pubsub.enabled = true;
    let error = super::service_host::prepare_app_state(database, &root, "0.0.0.0:3000")
        .err()
        .expect("pubsub.enabled must fail on a backend without a pubsub provider");
    assert!(error.to_string().contains("pubsub.enabled"), "{error}");
}
