use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use http_request::reqwest::Client;
use storage_common::WriteCostTally;
use storage_provider::StorageProvider;
use storage_types::{
    AttributeValue, GlobalSecondaryIndexDescription, IndexName, IndexStatus, Projection,
    StorageEnum, StorageError, TableName, context::WrappedError as _,
};

use crate::{
    constants::{BASE_BACKOFF_MS, NO_ENDPOINT},
    provider::{
        RemoteStorageProvider,
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
        redirect_policy: super::provider_helpers::RemoteRedirectPolicy::Disabled,
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

fn legacy_refund_one(state: &Mutex<usize>) {
    let mut state = state.lock().expect("legacy retry state lock");
    *state = (*state).saturating_add(1).min(100);
    std::hint::black_box(Instant::now());
}

#[test]
fn given_retry_budget_refunds_when_full_or_concurrent_then_fast_path_preserves_capacity() {
    const REFUND_ITERATIONS: usize = 50_000;
    let now = Instant::now();
    let bucket = RetryTokenBucket::new();
    let legacy_state = Mutex::new(100usize);

    let legacy_started = Instant::now();
    for _ in 0..REFUND_ITERATIONS {
        legacy_refund_one(&legacy_state);
    }
    let legacy_elapsed = legacy_started.elapsed();

    let candidate_started = Instant::now();
    for _ in 0..REFUND_ITERATIONS {
        bucket.refund_one();
    }
    let candidate_elapsed = candidate_started.elapsed();
    println!(
        "RETRY_REFUND_HEALTHY baseline_us={} candidate_us={}",
        legacy_elapsed.as_micros(),
        candidate_elapsed.as_micros()
    );

    for _ in 0..32 {
        assert!(bucket.try_take_at(now));
    }
    let concurrent = Arc::new(bucket);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let bucket = Arc::clone(&concurrent);
            scope.spawn(move || {
                for _ in 0..4 {
                    bucket.refund_one();
                }
            });
        }
    });
    for _ in 0..100 {
        assert!(concurrent.try_take_at(now));
    }
    assert!(!concurrent.try_take_at(now));
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
fn remote_gsi_readiness_preserves_creating_index_status() {
    let index_name = IndexName::new("ByGroup");
    let indexes = vec![GlobalSecondaryIndexDescription {
        index_name: index_name.clone(),
        key_schema: Vec::new(),
        projection: Projection {
            projection_type: None,
            non_key_attributes: None,
        },
        index_status: Some(IndexStatus::Creating),
        backfilling: Some(true),
        provisioned_throughput: None,
        index_size_bytes: None,
        item_count: None,
        index_arn: None,
    }];

    let readiness = crate::provider::provider_helpers::gsi_index_readiness(&indexes, &[index_name])
        .expect("creating index metadata");
    assert!(matches!(
        &readiness[0].1.index_status,
        IndexStatus::Creating
    ));
    assert!(readiness[0].1.backfilling);
}

#[test]
fn given_requested_gsi_is_absent_when_readiness_is_computed_then_returns_fixed_backend_error() {
    let requested_index = IndexName::new("ByGroup");

    let error = crate::provider::provider_helpers::gsi_index_readiness(
        &[],
        std::slice::from_ref(&requested_index),
    )
    .err()
    .expect("missing requested index must fail closed");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message }
            if message == "remote GSI index readiness metadata is incomplete"
    ));
}

#[test]
fn given_gsi_status_is_absent_when_readiness_is_computed_then_returns_fixed_backend_error() {
    let index_name = IndexName::new("ByGroup");
    let indexes = vec![GlobalSecondaryIndexDescription {
        index_name: index_name.clone(),
        key_schema: Vec::new(),
        projection: Projection {
            projection_type: None,
            non_key_attributes: None,
        },
        index_status: None,
        backfilling: Some(false),
        provisioned_throughput: None,
        index_size_bytes: None,
        item_count: None,
        index_arn: None,
    }];

    let error = crate::provider::provider_helpers::gsi_index_readiness(&indexes, &[index_name])
        .err()
        .expect("missing index status must fail closed");

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message }
            if message == "remote GSI index readiness metadata is incomplete"
    ));
}

#[test]
fn given_active_gsi_when_readiness_is_computed_then_preserves_active_status() {
    let index_name = IndexName::new("ByGroup");
    let indexes = vec![GlobalSecondaryIndexDescription {
        index_name: index_name.clone(),
        key_schema: Vec::new(),
        projection: Projection {
            projection_type: None,
            non_key_attributes: None,
        },
        index_status: Some(IndexStatus::Active),
        backfilling: None,
        provisioned_throughput: None,
        index_size_bytes: None,
        item_count: None,
        index_arn: None,
    }];

    let readiness = crate::provider::provider_helpers::gsi_index_readiness(&indexes, &[index_name])
        .expect("active index metadata");

    assert!(matches!(readiness[0].1.index_status, IndexStatus::Active));
    assert!(!readiness[0].1.backfilling);
}

#[test]
fn suppresses_conditional_check_failed_warnings_for_all_operations() {
    let error = StorageError::from(StorageEnum::ConditionalCheckFailed);
    assert!(RemoteStorageProvider::should_suppress_operation_warning(
        "PutItem", &error
    ));
    assert!(RemoteStorageProvider::is_normal_operation_error(&error));
}

#[test]
fn treats_conditional_check_failed_with_item_as_normal_operation_error() {
    let error = StorageError::from(StorageEnum::ConditionalCheckFailedWithItem {
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
    let pitr_unavailable = StorageError::from(StorageEnum::AwsService {
        code: Some("ContinuousBackupsUnavailableException".to_string()),
        message: "not ready".to_string(),
    });
    let access_denied = StorageError::from(StorageEnum::AccessDenied {
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
            item: storage_types::WireItem::try_from(&HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )]))
            .expect("valid item"),
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
            item: storage_types::WireItem::try_from(&HashMap::from([(
                "pk".to_string(),
                AttributeValue::S("tenant#1".to_string()),
            )]))
            .expect("valid item"),
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
            expression_attribute_names: Some(
                HashMap::from([("#v".to_string(), "value".to_string())]).into(),
            ),
            expression_attribute_values: Some(
                HashMap::from([(":v".to_string(), AttributeValue::S("next".to_string()))]).into(),
            ),
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

#[test]
fn given_transaction_cancellation_when_logging_then_preserves_health_policy() {
    for reasons in [
        vec![],
        vec!["ConditionalCheckFailed".into()],
        vec!["TransactionConflict".into()],
    ] {
        let error = StorageError::from(StorageEnum::TransactionCanceled { reasons });
        assert!(RemoteStorageProvider::should_suppress_operation_warning(
            "TransactWriteItems",
            &error
        ));
        assert!(!RemoteStorageProvider::is_normal_operation_error(&error));
    }
    let error = StorageError::from(StorageEnum::AwsService {
        code: Some("InternalServerError".into()),
        message: "unexpected failure".into(),
    });
    assert!(!RemoteStorageProvider::should_suppress_operation_warning(
        "TransactWriteItems",
        &error
    ));
}

#[tokio::test]
async fn given_remote_provider_when_snapshot_context_is_requested_then_returns_unsupported() {
    let provider = provider_with_urls(&["http://127.0.0.1:1"]);
    let error = provider
        .begin_read_sequence_read_context(storage_types::ReadSequenceConsistency::Transactional)
        .await
        .err()
        .unwrap();
    assert!(matches!(error.to_enum(), StorageEnum::Unsupported { .. }));
}

#[tokio::test]
async fn given_remote_provider_when_atomic_transform_is_requested_then_returns_unsupported() {
    let provider = provider_with_urls(&["http://127.0.0.1:1"]);
    let result = provider
        .atomic_item_read_modify_write(storage_provider::AtomicItemReadModifyWriteRequest {
            table_name: TableName::new("unsupported_table"),
            key: Default::default(),
            transform: Arc::new(|_| panic!("unsupported operation must not invoke the transform")),
        })
        .await;
    assert_unsupported_default(result);
}

fn bind_guarded_write<R>(
    request: R,
) -> storage_provider::PhysicalWrite<R, storage_provider::TtlWriteContext> {
    let table = TableName::new("unsupported_table");
    storage_provider::PhysicalWrite::try_new(
        0,
        table.clone(),
        request,
        storage_provider::TtlWriteContext::direct(table).unwrap(),
    )
    .unwrap()
}

fn unsupported_guard() -> storage_types::DurablePointReadGuard {
    storage_types::DurablePointReadGuard::Absent(storage_types::DurableAbsenceProof::new(vec![]))
}

fn assert_unsupported_default<T>(result: storage_types::StorageResult<T>) {
    let error = result
        .err()
        .expect("unsupported provider operation must return an error");
    assert!(matches!(error.to_enum(), StorageEnum::Unsupported { .. }));
}

#[tokio::test]
async fn given_remote_provider_when_guarded_writes_are_requested_then_returns_unsupported() {
    use storage_types::{
        GuardedDeleteItemRequest, GuardedPutItemRequest, GuardedTransactWriteItemsRequest,
        GuardedUpdateItemRequest,
    };
    let provider = provider_with_urls(&["http://127.0.0.1:1"]);
    let table_name = TableName::new("unsupported_table");
    assert!(!provider.supports_guarded_writes());
    assert!(!provider.supports_guarded_transaction_writes());
    assert_unsupported_default(
        provider
            .guarded_put_item(bind_guarded_write(GuardedPutItemRequest {
                table_name: table_name.clone(),
                item: Default::default(),
                indexers: vec![],
                guard: unsupported_guard(),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values: None,
            }))
            .await,
    );
    assert_unsupported_default(
        provider
            .guarded_delete_item(bind_guarded_write(GuardedDeleteItemRequest {
                table_name: table_name.clone(),
                key: Default::default(),
                guard: unsupported_guard(),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
            }))
            .await,
    );
    assert_unsupported_default(
        provider
            .guarded_update_item(bind_guarded_write(GuardedUpdateItemRequest {
                request: storage_types::UpdateItemRequest::builder()
                    .table_name(table_name)
                    .key(storage_types::KeyAttributes::default())
                    .build(),
                guard: unsupported_guard(),
            }))
            .await,
    );
    assert_unsupported_default(
        provider
            .guarded_transact_write_items(
                GuardedTransactWriteItemsRequest {
                    request: Default::default(),
                    guards: vec![],
                },
                storage_provider::OrderedOperationContexts::try_new(vec![]).unwrap(),
            )
            .await,
    );
}
