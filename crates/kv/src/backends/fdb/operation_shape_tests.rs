use std::collections::HashMap;

use pubsub_provider::{
    ClaimDeliveryRecordsRequest, CreateTopicRequest, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordKind, DeliveryStatus, DeliveryTarget, PubsubMessageId, PubsubProvider,
    SubscribeRequest, SubscriptionProtocol, TopicName,
};
use queue_provider::{Queue, QueueMessage, QueueProvider, QueueResult, ReceiptHandle};
use storage_common::provider_perf::emit_runtime_report;
use storage_provider::StorageProvider;
use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCommitMetadata, SyncLogId, SyncMutationId,
    SyncMutationResponse, SyncPutMutation,
};
use storage_types::{
    AttributeDefinition, BillingMode, CreateTableRequest, DurationSeconds, ItemStreamVersion,
    KeyAttributeType, KeySchemaElement, KeyType, StreamSpecification, StreamViewType, TableName,
    TimestampMillis,
};
use stream_provider::StreamProvider;
use uuid::Uuid;

use crate::{
    SortedKvDbStorageProvider,
    backends::fdb::{
        fdb_support_tests::connect_fdb_store,
        foundationdb_operation_metrics_reset, foundationdb_operation_metrics_snapshot,
        range_read::{DYNAMODB_RANGE_TARGET_BYTES, dynamodb_range_option},
    },
    sorted_kv_store::{DirectWriteOperation, SortedKvStore},
};

#[test]
fn foundationdb_dynamodb_range_option_requests_one_mebibyte_want_all_pages() {
    let option = dynamodb_range_option(
        foundationdb::KeySelector::first_greater_or_equal(b"start".as_slice()),
        foundationdb::KeySelector::first_greater_than(b"end".as_slice()),
        101,
        false,
    );

    assert_eq!(option.limit, Some(101));
    assert_eq!(option.target_bytes, 1024 * 1024);
    assert_eq!(option.target_bytes, DYNAMODB_RANGE_TARGET_BYTES);
    assert!(matches!(
        option.mode,
        foundationdb::options::StreamingMode::WantAll
    ));
    assert!(!option.reverse);
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_unchecked_check_then_put_same_key_commits() {
    let Some(store) = connect_fdb_store("fdb-unchecked-check-put").await else {
        eprintln!(
            "Skipping FoundationDB unchecked transaction test: unable to connect to local cluster"
        );
        return;
    };

    let key = b"check-then-put".to_vec();
    store
        .transact_write_unchecked(vec![
            DirectWriteOperation::CheckValue {
                key: key.clone(),
                expected_value: None,
            },
            DirectWriteOperation::Put {
                key: key.clone(),
                value: b"written".to_vec(),
            },
        ])
        .await
        .expect("check-then-put transaction should commit");

    assert_eq!(
        store.get(&key, true).await.expect("read key"),
        Some(b"written".to_vec())
    );
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_queue_and_pubsub_operation_shape_tests() {
    let Some(store) = connect_fdb_store("fdb-operation-shape").await else {
        eprintln!("Skipping FoundationDB operation-shape test: unable to connect to local cluster");
        return;
    };
    let provider = SortedKvDbStorageProvider::new(store);
    QueueProvider::initialize(&provider).await.unwrap();
    PubsubProvider::initialize(&provider).await.unwrap();

    let queue_url = format!(
        "https://queue.example.test/000000000000/fdb-shape-{}",
        Uuid::now_v7()
    );
    provider
        .create_queue(Queue {
            queue_name: "fdb-shape".to_string(),
            queue_url: queue_url.clone(),
            attributes: HashMap::new(),
            created_at: TimestampMillis::from(1_000),
        })
        .await
        .unwrap();

    let topic = provider
        .create_topic(CreateTopicRequest {
            name: TopicName::new(format!("fdb-shape-{}", Uuid::now_v7())).unwrap(),
            attributes: HashMap::new(),
        })
        .await
        .unwrap();
    let subscription = provider
        .create_subscription(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_url.clone(),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();

    foundationdb_operation_metrics_reset();

    let batch_messages = (0..32)
        .map(|index| QueueMessage {
            queue_url: queue_url.clone(),
            body: format!("message-{index}"),
            created_at: TimestampMillis::from(2_000 + index),
            visibility_timestamp: Some(TimestampMillis::from(2_000)),
            ..Default::default()
        })
        .collect();
    let send_results = provider.send_messages(batch_messages).await.unwrap();
    assert_eq!(send_results.len(), 32);
    assert!(
        send_results.iter().all(Result::is_ok),
        "batch send should succeed: {send_results:?}"
    );
    let mut messages = Vec::new();
    for _ in 0..32 {
        if messages.len() >= 10 {
            break;
        }
        let mut received = provider
            .receive_messages(
                &queue_url,
                10 - u32::try_from(messages.len()).unwrap_or(10),
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .unwrap();
        messages.append(&mut received);
    }
    assert_eq!(messages.len(), 10);
    let (visibility_messages, delete_messages): (Vec<_>, Vec<_>) = messages
        .into_iter()
        .enumerate()
        .partition(|(index, _)| *index < 5);
    let visibility_receipts = visibility_messages
        .iter()
        .map(|(_, message)| {
            (
                ReceiptHandle(message.receipt_handle.clone()),
                DurationSeconds::from(45),
            )
        })
        .collect::<Vec<_>>();
    let visibility_results = provider
        .change_message_visibilities(&queue_url, visibility_receipts)
        .await
        .unwrap();
    assert_all_batch_results_ok(&visibility_results, "batch visibility change");

    let delete_receipts = delete_messages
        .into_iter()
        .map(|(_, message)| ReceiptHandle(message.receipt_handle))
        .collect();
    let delete_results = provider
        .delete_messages(&queue_url, delete_receipts)
        .await
        .unwrap();
    assert_all_batch_results_ok(&delete_results, "batch delete");
    let cleaned_payloads = provider.cleanup_queue_payload_orphans(128).await.unwrap();
    assert_eq!(cleaned_payloads, 5);

    let queue_metrics = foundationdb_operation_metrics_snapshot();
    eprintln!(
        "foundationdb queue cleanup after ledger: queue_send_commit={} unchecked_commit={} \
         unchecked_set={} unchecked_clear={} unchecked_range_clear={} range_reads={} multi_gets={}",
        operation_metric(&queue_metrics, "queue_send", "commit"),
        operation_metric(&queue_metrics, "transact_write_unchecked", "commit"),
        operation_metric(&queue_metrics, "transact_write_unchecked", "set"),
        operation_metric(&queue_metrics, "transact_write_unchecked", "clear"),
        operation_metric(&queue_metrics, "transact_write_unchecked", "range_clear"),
        operation_metric(&queue_metrics, "range", "snapshot_range_read"),
        operation_metric(&queue_metrics, "multi_get", "get"),
    );

    let records = (0..4)
        .map(|index| delivery_record(subscription.subscription_arn.clone(), index))
        .collect::<Vec<_>>();
    provider.put_delivery_records(records).await.unwrap();
    let claim = provider
        .claim_delivery_records(ClaimDeliveryRecordsRequest {
            owner: "shape-worker".to_string(),
            now: TimestampMillis::from(10_000),
            lease_expires_at: TimestampMillis::from(20_000),
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(claim.records.len(), 2);

    assert_metric_at_most(&queue_metrics, "queue_claim", "snapshot_range_read", 64);
    assert_metric_at_least(&queue_metrics, "queue_claim", "ordinary_point_read", 10);
    assert_metric_at_least(&queue_metrics, "queue_claim", "snapshot_point_read", 10);
    assert_metric_at_least(&queue_metrics, "queue_claim", "read_modify_write", 20);
    assert_metric_at_most(&queue_metrics, "queue_send", "ordinary_point_read", 0);
    assert_metric_at_most(&queue_metrics, "queue_send", "commit", 1);
    assert_metric_at_least(&queue_metrics, "queue_send", "blind_write", 64);
    assert_metric_at_most(&queue_metrics, "transact_write_unchecked", "commit", 3);
    assert_metric_at_least(
        &queue_metrics,
        "transact_write_unchecked",
        "range_clear",
        15,
    );
    let metrics = foundationdb_operation_metrics_snapshot();
    assert_metric_at_least(
        &metrics,
        "transact_write_unchecked",
        "ordinary_point_read",
        12,
    );
    assert_metric_at_least(&metrics, "range", "snapshot_range_read", 1);
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_resolved_sync_apply_batches_group_into_one_commit_tests() {
    let Some(store) = connect_fdb_store("fdb-sync-apply-shape").await else {
        eprintln!(
            "Skipping FoundationDB sync apply operation-shape test: unable to connect to local \
             cluster"
        );
        return;
    };
    let provider = SortedKvDbStorageProvider::new(store).with_immediate_gsi_consistency(true);
    provider.initialize_storage().await.unwrap();
    provider.initialize_stream().await.unwrap();

    let separate_table = TableName::new("FdbSyncApplySeparateShape");
    let grouped_table = TableName::new("FdbSyncApplyGroupedShape");
    provider
        .create_table(&create_hash_table_request(separate_table.clone()))
        .await
        .unwrap();
    provider
        .create_table(&create_hash_table_request(grouped_table.clone()))
        .await
        .unwrap();

    foundationdb_operation_metrics_reset();
    let separate_started = std::time::Instant::now();
    for index in 0..8 {
        provider
            .apply_resolved_sync_mutations(
                SyncCommitMetadata {
                    log_id: SyncLogId::new(1, index as u64 + 1),
                    committed_at: TimestampMillis::from(1_700_000_000_000),
                    leader_node_id: "node-1".to_string(),
                },
                ResolvedSyncMutationBatch::new(vec![sync_put(&separate_table, index)]),
            )
            .await
            .unwrap();
    }
    let separate_elapsed = separate_started.elapsed();
    let separate_metrics = foundationdb_operation_metrics_snapshot();

    foundationdb_operation_metrics_reset();
    let grouped_started = std::time::Instant::now();

    provider
        .apply_resolved_sync_mutations(
            SyncCommitMetadata {
                log_id: SyncLogId::new(2, 1),
                committed_at: TimestampMillis::from(1_700_000_000_000),
                leader_node_id: "node-1".to_string(),
            },
            ResolvedSyncMutationBatch::new(
                (0..8)
                    .map(|index| sync_put(&grouped_table, index))
                    .collect::<Vec<_>>(),
            ),
        )
        .await
        .unwrap();
    let grouped_elapsed = grouped_started.elapsed();

    let metrics = foundationdb_operation_metrics_snapshot();
    eprintln!(
        "foundationdb sync apply grouped batch: separate_commit={} grouped_commit={} \
         grouped_set={} grouped_read={}",
        operation_metric(&separate_metrics, "transact_write_unchecked", "commit"),
        operation_metric(&metrics, "transact_write_unchecked", "commit"),
        operation_metric(&metrics, "transact_write_unchecked", "set"),
        operation_metric(&metrics, "transact_write_unchecked", "ordinary_point_read")
    );
    emit_runtime_report(
        module_path!(),
        "foundationdb_resolved_sync_apply_batches_group_into_one_commit_tests",
        "separate_puts_8x1",
        8,
        separate_elapsed,
    );
    emit_runtime_report(
        module_path!(),
        "foundationdb_resolved_sync_apply_batches_group_into_one_commit_tests",
        "grouped_puts_1x8",
        8,
        grouped_elapsed,
    );
    assert_metric_at_least(&separate_metrics, "transact_write_unchecked", "commit", 8);
    assert_metric_at_most(&metrics, "transact_write_unchecked", "commit", 1);
    assert_metric_at_least(&metrics, "transact_write_unchecked", "set", 8);
    assert!(
        grouped_elapsed < separate_elapsed,
        "grouped apply should beat separate applies: grouped={grouped_elapsed:?} \
         separate={separate_elapsed:?}"
    );
}

fn assert_all_batch_results_ok<T: std::fmt::Debug>(results: &[QueueResult<T>], label: &str) {
    assert!(
        results.iter().all(Result::is_ok),
        "{label} should succeed: {results:?}"
    );
}

fn delivery_record(
    subscription_arn: pubsub_provider::SubscriptionArn,
    index: usize,
) -> DeliveryRecord {
    DeliveryRecord {
        id: DeliveryRecordId(format!("fdb-shape-record-{index}")),
        kind: DeliveryRecordKind::Notification,
        message_id: PubsubMessageId::new_from_string(format!("fdb-shape-message-{index}")).unwrap(),
        subscription_arn,
        message_body: Some("body".to_string()),
        subject: None,
        message_attributes: HashMap::new(),
        target: DeliveryTarget::BuiltIn,
        status: DeliveryStatus::Pending,
        attempts: 0,
        next_attempt_at: None,
        lease_owner: None,
        lease_expires_at: None,
        last_error: None,
        created_at: TimestampMillis::from(3_000),
        updated_at: TimestampMillis::from(3_000),
    }
}

fn create_hash_table_request(table_name: TableName) -> CreateTableRequest {
    CreateTableRequest::new(
        table_name,
        vec![attr("pk", KeyAttributeType::S)],
        vec![hash_key("pk")],
        BillingMode::PayPerRequest,
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }))
}

fn sync_put(table_name: &TableName, index: usize) -> ResolvedSyncMutation {
    let key = format!("item#{index}");
    ResolvedSyncMutation::Put(SyncPutMutation {
        mutation_id: SyncMutationId::new(format!("fdb-sync-apply-{table_name}-{index}")).unwrap(),
        table_name: table_name.clone(),
        key_json: format!(r#"{{"pk":{{"S":"{key}"}}}}"#),
        item_json: format!(r#"{{"pk":{{"S":"{key}"}},"status":{{"S":"open"}}}}"#),
        old_item_json: None,
        target_item_stream_version: ItemStreamVersion::new(index as u64 + 1),
        response: SyncMutationResponse {
            response_json: Some(format!(r#"{{"index":{index}}}"#)),
        },
    })
}

fn attr(name: &str, attribute_type: KeyAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type,
    }
}

fn hash_key(name: &str) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type: KeyType::Hash,
    }
}

fn assert_metric_at_least(metrics: &str, path: &str, operation: &str, expected: u64) {
    let actual = operation_metric(metrics, path, operation);
    assert!(
        actual >= expected,
        "expected {path}/{operation} >= {expected}, got {actual}\n{metrics}"
    );
}

fn assert_metric_at_most(metrics: &str, path: &str, operation: &str, expected: u64) {
    let actual = operation_metric(metrics, path, operation);
    assert!(
        actual <= expected,
        "expected {path}/{operation} <= {expected}, got {actual}\n{metrics}"
    );
}

fn operation_metric(metrics: &str, path: &str, operation: &str) -> u64 {
    let needle = format!("path=\"{path}\",operation=\"{operation}\"");
    metrics
        .lines()
        .find(|line| line.contains(&needle))
        .and_then(|line| line.rsplit_once(' '))
        .and_then(|(_, value)| value.parse::<u64>().ok())
        .unwrap_or(0)
}
