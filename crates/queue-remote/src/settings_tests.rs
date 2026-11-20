use std::time::Duration;

use super::settings::{
    RemoteCredentialStrategy, RemoteQueueSettings, RemoteSigv4Settings, RemoteTimeoutOverrides,
};

fn settings() -> RemoteQueueSettings {
    RemoteQueueSettings {
        endpoint_urls: vec!["http://127.0.0.1:9".to_string()],
        region: None,
        tls: false,
        credentials: RemoteCredentialStrategy::DefaultChain,
        timeouts: None,
        sigv4: RemoteSigv4Settings::default(),
    }
}

#[test]
fn given_no_endpoint_when_validated_then_rejected() {
    let mut settings = settings();
    settings.endpoint_urls.clear();

    let error = settings
        .validate()
        .expect_err("an empty endpoint list is rejected");

    assert!(
        format!("{error:?}").contains("remote queue requires at least one endpoint URL"),
        "{error:?}"
    );
}

#[test]
fn given_sigv4_without_region_when_validated_then_rejected() {
    for region in [None, Some(String::new())] {
        let mut settings = settings();
        settings.sigv4.enabled = true;
        settings.region = region;

        let error = settings.validate().expect_err("SigV4 needs a region");

        assert!(
            format!("{error:?}").contains("remote queue with SigV4 enabled requires a region"),
            "{error:?}"
        );
    }
}

#[test]
fn given_sigv4_with_region_when_validated_then_accepted() {
    let mut settings = settings();
    settings.sigv4.enabled = true;
    settings.region = Some("eu-west-2".to_string());

    settings.validate().expect("SigV4 with a region is valid");
}

#[test]
fn given_default_sigv4_settings_then_signing_is_off_for_the_queue_service() {
    let sigv4 = RemoteSigv4Settings::default();

    assert!(!sigv4.enabled);
    assert_eq!(sigv4.service_name, "queue");
}

fn overrides(connect_ms: Option<u64>, request_ms: Option<u64>) -> RemoteTimeoutOverrides {
    RemoteTimeoutOverrides {
        connect_timeout_ms: connect_ms,
        request_timeout_ms: request_ms,
    }
}

#[test]
fn given_request_timeout_when_connect_timeout_defaulted_then_quarter_capped_at_two_seconds() {
    assert_eq!(
        overrides(None, Some(400)).connect_timeout(),
        Duration::from_millis(100)
    );
    assert_eq!(
        overrides(None, Some(30_000)).connect_timeout(),
        Duration::from_secs(2)
    );
}

#[test]
fn given_no_request_timeout_when_connect_timeout_defaulted_then_two_seconds() {
    assert_eq!(
        overrides(None, None).connect_timeout(),
        Duration::from_secs(2)
    );
}

#[test]
fn given_explicit_connect_timeout_when_read_then_value_is_kept() {
    assert_eq!(
        overrides(Some(7_000), Some(400)).connect_timeout(),
        Duration::from_secs(7)
    );
    assert_eq!(
        overrides(Some(50), None).connect_timeout(),
        Duration::from_millis(50)
    );
}
