use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use http_request::reqwest::Client;
use storage_provider::StorageProvider;
use storage_types::{AttributeValue, StorageEnum, StorageError, TableName};

use crate::{
    constants::BASE_BACKOFF_MS,
    provider::{
        NO_ENDPOINT, RemoteStorageProvider,
        implementation::WriteCostTally,
        provider_helpers::{
            RetryTokenBucket, build_endpoints, compute_backoff, full_jitter_delay,
            retry_backoff_cap,
        },
    },
};

fn provider_with_urls(urls: &[&str]) -> RemoteStorageProvider {
    let endpoints = build_endpoints(
        &urls.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
        false,
    )
    .unwrap();
    RemoteStorageProvider {
        client: Client::new(),
        endpoints,
        signer: None,
        credential_source: "test",
        primary_endpoint: AtomicUsize::new(0),
        probation_endpoint: AtomicUsize::new(NO_ENDPOINT),
        pressure_signals: AtomicUsize::new(0),
        retry_budget: RetryTokenBucket::new(),
        request_timeout: None,
    }
}

#[test]
fn select_initial_endpoint_prefers_primary_without_probation() {
    let provider = provider_with_urls(&["http://a.test", "http://b.test"]);
    for _ in 0..32 {
        let (idx, probation) = provider.select_initial_endpoint();
        assert_eq!(idx, 0);
        assert!(!probation);
    }
}

#[test]
fn select_initial_endpoint_samples_probation() {
    let provider = provider_with_urls(&["http://a.test", "http://b.test"]);
    provider.probation_endpoint.store(1, Ordering::Relaxed);

    let mut probation_hits = 0;
    let samples = 1_000;
    for _ in 0..samples {
        let (idx, probation) = provider.select_initial_endpoint();
        if probation {
            probation_hits += 1;
            assert_eq!(idx, 1);
        } else {
            assert_eq!(idx, 0);
        }
    }

    assert!(probation_hits > 30);
    assert!(probation_hits < 200);
}

#[test]
fn promote_primary_updates_probation() {
    let provider = provider_with_urls(&["http://a.test", "http://b.test", "http://c.test"]);
    provider.promote_primary(1, 0);
    assert_eq!(provider.primary_endpoint.load(Ordering::Relaxed), 1);
    assert_eq!(provider.probation_endpoint.load(Ordering::Relaxed), 0);

    provider.promote_primary(2, 1);
    assert_eq!(provider.primary_endpoint.load(Ordering::Relaxed), 2);
    assert_eq!(provider.probation_endpoint.load(Ordering::Relaxed), 1);
}

#[test]
fn restore_primary_clears_probation() {
    let provider = provider_with_urls(&["http://a.test", "http://b.test"]);
    provider.probation_endpoint.store(1, Ordering::Relaxed);
    provider.restore_primary_if_match(1);
    assert_eq!(provider.primary_endpoint.load(Ordering::Relaxed), 1);
    assert_eq!(
        provider.probation_endpoint.load(Ordering::Relaxed),
        NO_ENDPOINT
    );
}

#[test]
fn compute_backoff_behaviour() {
    assert_eq!(compute_backoff(0), Duration::ZERO);
    assert!(compute_backoff(1) <= retry_backoff_cap(1));
    assert_eq!(full_jitter_delay(1, 0), Duration::ZERO);
    assert_eq!(
        full_jitter_delay(1, BASE_BACKOFF_MS),
        Duration::from_millis(BASE_BACKOFF_MS)
    );
}

#[test]
fn retry_token_bucket_refills_with_a_fake_monotonic_clock() {
    let bucket = RetryTokenBucket::new();
    let now = std::time::Instant::now();
    for _ in 0..100 {
        assert!(bucket.try_take_at(now));
    }
    assert!(!bucket.try_take_at(now));
    assert!(bucket.try_take_at(now + Duration::from_secs(1)));
}

#[test]
fn admission_pressure_signal_is_empty_without_underflow() {
    let provider = provider_with_urls(&["http://a.test"]);
    assert!(!provider.take_admission_pressure_signal());

    provider.pressure_signals.fetch_add(1, Ordering::Relaxed);
    assert!(provider.take_admission_pressure_signal());
    assert!(!provider.take_admission_pressure_signal());
}

#[test]
fn admission_pressure_signal_drains_all_retry_markers_at_the_boundary() {
    let provider = provider_with_urls(&["http://a.test"]);
    provider.pressure_signals.store(5, Ordering::Relaxed);

    assert!(provider.take_admission_pressure_signal());
    assert_eq!(provider.pressure_signals.load(Ordering::Acquire), 0);
    assert!(!provider.take_admission_pressure_signal());
}

#[test]
fn suppresses_describe_table_not_found_warnings() {
    let error = StorageError::table_not_found("tenant_missing");
    assert!(RemoteStorageProvider::should_suppress_operation_warning(
        "DescribeTable",
        &error
    ));
    assert!(!RemoteStorageProvider::should_suppress_operation_warning(
        "CreateTable",
        &error
    ));
}

#[test]
fn suppresses_conditional_check_failed_warnings_for_all_operations() {
    let error = StorageError::Base(StorageEnum::ConditionalCheckFailed);
    assert!(RemoteStorageProvider::should_suppress_operation_warning(
        "PutItem", &error
    ));
    assert!(RemoteStorageProvider::is_normal_operation_error(&error));
}

#[test]
fn treats_conditional_check_failed_with_item_as_normal_operation_error() {
    let error = StorageError::Base(StorageEnum::ConditionalCheckFailedWithItem {
        item: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]).into(),
    });

    assert!(RemoteStorageProvider::should_suppress_operation_warning(
        "UpdateItem",
        &error
    ));
    assert!(RemoteStorageProvider::is_normal_operation_error(&error));
}

#[test]
fn retries_expected_point_in_time_recovery_errors() {
    let pitr_unavailable = StorageError::Base(StorageEnum::AwsService {
        code: Some("ContinuousBackupsUnavailableException".to_string()),
        message: "not ready".to_string(),
    });
    let access_denied = StorageError::Base(StorageEnum::AccessDenied {
        message: "denied".to_string(),
    });
    assert!(RemoteStorageProvider::should_retry_point_in_time_recovery_error(&pitr_unavailable));
    assert!(!RemoteStorageProvider::should_retry_point_in_time_recovery_error(&access_denied));
}

#[test]
fn write_cost_tally_tracks_batch_puts_and_deletes() {
    let mut tally = WriteCostTally::default();
    tally.record_write_request(&storage_types::WriteRequest {
        put_request: Some(storage_types::PutRequest {
            item: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]),
            indexers: None,
            aux_item_stream_ttl_hours: None,
        }),
        delete_request: None,
    });
    tally.record_write_request(&storage_types::WriteRequest {
        put_request: None,
        delete_request: Some(storage_types::DeleteRequest {
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))])
                .into(),
            aux_item_stream_ttl_hours: None,
        }),
    });

    assert_eq!(tally.put_ops, 1);
    assert_eq!(tally.delete_ops, 1);
    assert!(tally.put_bytes > 0);
    assert!(tally.delete_bytes > 0);
}

#[test]
fn write_cost_tally_tracks_transact_item_kinds() {
    let mut tally = WriteCostTally::default();
    tally.record_transact_item(&storage_types::TransactWriteItem {
        put: Some(storage_types::TransactPutRequest {
            table_name: TableName::new("tenant_t1"),
            item: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))]),
            indexers: None,
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        update: Some(storage_types::TransactUpdateRequest {
            table_name: TableName::new("tenant_t1"),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#1".to_string()))])
                .into(),
            update_expression: "SET #v = :v".to_string(),
            indexers: None,
            condition_expression: None,
            expression_attribute_names: Some(HashMap::from([(
                "#v".to_string(),
                "value".to_string(),
            )])),
            expression_attribute_values: Some(HashMap::from([(
                ":v".to_string(),
                AttributeValue::S("next".to_string()),
            )])),
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        delete: Some(storage_types::TransactDeleteRequest {
            table_name: TableName::new("tenant_t1"),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#2".to_string()))])
                .into(),
            condition_expression: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        condition_check: Some(storage_types::TransactConditionCheckRequest {
            table_name: TableName::new("tenant_t1"),
            key: HashMap::from([("pk".to_string(), AttributeValue::S("tenant#3".to_string()))])
                .into(),
            condition_expression: "attribute_exists(pk)".to_string(),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
        }),
    });

    assert_eq!(tally.put_ops, 1);
    assert_eq!(tally.update_ops, 1);
    assert_eq!(tally.delete_ops, 1);
    assert_eq!(tally.condition_check_ops, 1);
    assert!(tally.update_bytes > 0);
    assert!(tally.condition_check_bytes > 0);
}
