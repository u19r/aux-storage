#![cfg(feature = "foundationdb-backend")]

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use queue_provider::{
    MessageId, MessageResponse, Queue, QueueMessage, QueueProvider, ReceiptHandle,
};
use storage_types::{DurationSeconds, TimestampMillis};
use uuid::Uuid;

use crate::{
    SortedKvDbStorageProvider,
    backends::fdb::fdb_support_tests::{connect_fdb_store, metrics_handle, parse_metric_value},
    constants::{PARTITION_CONTROLLER_LOW_STREAK_TARGET, PARTITION_LOAD_SAMPLE_WINDOW_SECONDS},
    key_template::{KeyTemplate, PlaceholderBinding},
    keyspace::compact::QueueStorageId,
    partition_family::{
        PartitionFamilyKind, PartitionLoadSample, PartitionLoadSampleRecord,
        ResolvedPartitionFamily, ordered_log_hash, partition_load_sample_bytes,
        partition_load_sample_key, partition_load_sample_prefix, partition_sample_window_start_ms,
        queue_family_component, queue_state_key_with_slot,
    },
    partition_reconcile::{QueueReconcileAction, apply_queue_action},
    sorted_kv_store::{SortedKvStore, TransactWriteOperation},
};

fn queue_definition(queue_url: &str) -> Queue {
    Queue {
        queue_name: queue_url.to_string(),
        queue_url: queue_url.to_string(),
        attributes: HashMap::new(),
        created_at: TimestampMillis::now(),
    }
}

fn new_message(queue_url: &str, body: &str) -> QueueMessage {
    QueueMessage {
        message_id: MessageId::default(),
        queue_url: queue_url.to_string(),
        body: body.to_string(),
        message_attributes: None,
        receipt_handle: None,
        created_at: TimestampMillis::now(),
        visibility_timestamp: Some(TimestampMillis::now()),
    }
}

fn new_message_with_id(queue_url: &str, body: &str, message_id: MessageId) -> QueueMessage {
    QueueMessage {
        message_id,
        ..new_message(queue_url, body)
    }
}

fn receipt_handle_from_response(response: &MessageResponse) -> ReceiptHandle {
    ReceiptHandle(response.receipt_handle.clone())
}

async fn receive_until_message(
    provider: &SortedKvDbStorageProvider<crate::FoundationDbKvStore>,
    queue_url: &str,
) -> Vec<MessageResponse> {
    for _ in 0..64 {
        let messages = provider
            .receive_messages(
                queue_url,
                1,
                DurationSeconds::from(5),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive messages");
        if !messages.is_empty() {
            return messages;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Vec::new()
}

fn routed_partition_for_message(
    queue_url: &str,
    message_id: &MessageId,
    family: &ResolvedPartitionFamily,
) -> (u16, u16) {
    let writable: Vec<_> = family
        .partitions
        .iter()
        .filter(|partition| partition.is_writable())
        .collect();
    let primary_index = usize::try_from(ordered_log_hash(
        &[queue_url.as_bytes(), message_id.as_bytes()].concat(),
    ))
    .unwrap_or(0)
        % writable.len();
    let primary = writable[primary_index];
    (primary.partition_id, primary.placement_slot)
}

async fn write_hot_queue_sample(
    provider: &SortedKvDbStorageProvider<crate::FoundationDbKvStore>,
    queue_url: &str,
    partition_id: u16,
    writes: u64,
) {
    let family_component = queue_family_component(queue_url);
    let window_start_ms = partition_sample_window_start_ms(
        storage_types::TimestampMillis::now().timestamp_millis(),
        PARTITION_LOAD_SAMPLE_WINDOW_SECONDS,
    );
    let publisher_id = format!("test-{}", Uuid::now_v7());
    let sample = PartitionLoadSampleRecord {
        partition_id,
        window_start_ms,
        publisher_id: publisher_id.clone(),
        sample: PartitionLoadSample {
            writes,
            ..Default::default()
        },
    };
    provider
        .kv_store
        .put(
            &partition_load_sample_key(
                PartitionFamilyKind::StandardQueue,
                &family_component,
                partition_id,
                window_start_ms,
                &publisher_id,
            ),
            &partition_load_sample_bytes(&sample).expect("encode queue load sample"),
            None,
        )
        .await
        .expect("persist queue load sample");
}

async fn queue_compact_record_count_for_partition(
    provider: &SortedKvDbStorageProvider<crate::FoundationDbKvStore>,
    placement_slot: u16,
    partition_id: u16,
) -> usize {
    let queue_id = QueueStorageId::new(1).expect("first queue id");
    provider
        .kv_store
        .get_prefix(
            &queue_state_key_with_slot(queue_id, placement_slot, partition_id, ""),
            true,
            None,
            true,
        )
        .await
        .expect("scan queue compact record prefix for partition")
        .items
        .len()
}

async fn prepare_queue_scale_in(
    provider: &SortedKvDbStorageProvider<crate::FoundationDbKvStore>,
    queue_url: &str,
    min_open_partitions: u16,
) {
    let family_component = queue_family_component(queue_url);
    let mut family = provider
        .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
        .await
        .expect("load queue family for scale-in preparation")
        .expect("queue family exists for scale-in preparation");
    family.config.min_open_partitions = min_open_partitions;
    family.config.cooldown_until_ms = None;
    family.config.controller.low_streak = PARTITION_CONTROLLER_LOW_STREAK_TARGET;
    family.config.controller.ewma_pressure = 0.0;
    family.config.controller.integral = -1.0;
    provider
        .save_partition_family_state(
            PartitionFamilyKind::StandardQueue,
            &family_component,
            &family,
        )
        .await
        .expect("save queue family scale-in preparation");
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster"]
async fn given_prewarmed_queue_when_reading_attributes_then_marker_is_not_counted() {
    let Some(store) = connect_fdb_store("fdb-queue-attributes").await else {
        eprintln!("Skipping FoundationDB queue test: unable to connect to local cluster");
        return;
    };
    let provider = SortedKvDbStorageProvider::new(store);
    provider
        .initialize()
        .await
        .expect("initialize queue provider");
    let queue_url = format!("fdb-queue-attributes-{}", Uuid::now_v7());
    let queue = provider
        .create_queue(queue_definition(&queue_url))
        .await
        .expect("create queue");

    let (stored, counts) = provider
        .get_queue_with_message_counts(&queue.queue_url)
        .await
        .expect("read queue attributes")
        .expect("queue exists");

    assert_eq!(stored.queue_url, queue.queue_url);
    assert_eq!(counts.visible, 0);
    assert_eq!(counts.not_visible, 0);
    assert_eq!(counts.delayed, 0);

    provider
        .send_message(new_message(&queue.queue_url, "visible"))
        .await
        .expect("send visible message");
    let mut delayed = new_message(&queue.queue_url, "delayed");
    delayed.visibility_timestamp = Some(TimestampMillis::from(
        TimestampMillis::now().timestamp_millis() + 120_000,
    ));
    provider
        .send_message(delayed)
        .await
        .expect("send delayed message");
    let received = receive_until_message(&provider, &queue.queue_url).await;
    assert_eq!(received.len(), 1);

    let (_, counts) = provider
        .get_queue_with_message_counts(&queue.queue_url)
        .await
        .expect("read populated queue attributes")
        .expect("queue exists");
    assert_eq!(counts.visible, 0);
    assert_eq!(counts.not_visible, 1);
    assert_eq!(counts.delayed, 1);
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_queue_send_receive_extend_delete() {
    let Some(store) = connect_fdb_store("fdb-queues").await else {
        eprintln!("Skipping FoundationDB queue test: unable to connect to local cluster");
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());

    let test_future = async move {
        provider
            .initialize()
            .await
            .expect("initialize queue provider");

        let queue_url = format!("fdb-queue-{}", Uuid::now_v7());
        let queue_url = provider
            .create_queue(queue_definition(&queue_url))
            .await
            .expect("create queue")
            .queue_url;

        let message_body = "hello-foundationdb";
        let sent_message_id = provider
            .send_message(new_message(&queue_url, message_body))
            .await
            .expect("send message");

        let received = receive_until_message(&provider, &queue_url).await;

        assert_eq!(received.len(), 1, "expected one message after send");

        let first = &received[0];
        let receipt_handle = receipt_handle_from_response(first);
        let parsed_message_id = first
            .message_id
            .parse::<MessageId>()
            .expect("message id should be hex");
        assert_eq!(parsed_message_id, sent_message_id);

        provider
            .change_message_visibility(&queue_url, receipt_handle, DurationSeconds::from(2))
            .await
            .expect("change visibility");

        let invisible = provider
            .receive_messages(
                &queue_url,
                1,
                DurationSeconds::from(5),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive while invisible");
        assert!(
            invisible.is_empty(),
            "message should remain invisible after extension"
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        let second_batch = receive_until_message(&provider, &queue_url).await;
        assert_eq!(second_batch.len(), 1, "message should become visible again");

        let receipt_handle = receipt_handle_from_response(&second_batch[0]);

        provider
            .delete_message(&queue_url, receipt_handle)
            .await
            .expect("delete message");

        let after_delete = provider
            .receive_messages(
                &queue_url,
                1,
                DurationSeconds::from(5),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive after delete");
        assert!(
            after_delete.is_empty(),
            "queue should be empty after deleting the only message"
        );
    };

    if tokio::time::timeout(Duration::from_secs(90), test_future)
        .await
        .is_err()
    {
        eprintln!("Skipping FoundationDB queue test: timed out while exercising queue operations");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_shared_backend_concurrent_receives_do_not_duplicate_claims() {
    let Some(store) = connect_fdb_store("fdb-queue-concurrent-receive").await else {
        eprintln!(
            "Skipping FoundationDB concurrent queue test: unable to connect to local cluster"
        );
        return;
    };

    let writer = SortedKvDbStorageProvider::new(store.clone());
    let receiver_a = SortedKvDbStorageProvider::new(store.clone());
    let receiver_b = SortedKvDbStorageProvider::new(store.clone());

    let test_future = async move {
        writer.initialize().await.expect("initialize queue writer");
        receiver_a
            .initialize()
            .await
            .expect("initialize queue receiver a");
        receiver_b
            .initialize()
            .await
            .expect("initialize queue receiver b");

        let queue_url = format!("fdb-queue-concurrent-{}", Uuid::now_v7());
        let queue_url = writer
            .create_queue(queue_definition(&queue_url))
            .await
            .expect("create queue")
            .queue_url;

        let mut expected_ids = HashSet::new();
        for index in 0..40usize {
            let message_id = MessageId::random();
            assert!(expected_ids.insert(message_id));
            writer
                .send_message(new_message_with_id(
                    &queue_url,
                    &format!("concurrent-{index}"),
                    message_id,
                ))
                .await
                .expect("send concurrent message");
        }

        let receive_a = receiver_a.receive_messages(
            &queue_url,
            10,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        );
        let receive_b = receiver_b.receive_messages(
            &queue_url,
            10,
            DurationSeconds::from(30),
            DurationSeconds::from(0),
        );
        let (batch_a, batch_b) = tokio::join!(receive_a, receive_b);
        let batch_a = batch_a.expect("receiver a batch");
        let batch_b = batch_b.expect("receiver b batch");

        assert!(!batch_a.is_empty(), "receiver a should claim messages");
        assert!(!batch_b.is_empty(), "receiver b should claim messages");

        let mut received_ids = HashSet::new();
        for message in batch_a.iter().chain(batch_b.iter()) {
            let parsed_message_id = message
                .message_id
                .parse::<MessageId>()
                .expect("message id should be hex");
            assert!(
                expected_ids.contains(&parsed_message_id),
                "received unexpected message id"
            );
            assert!(
                received_ids.insert(parsed_message_id),
                "concurrent receivers must not claim the same message before visibility expiry"
            );
        }

        let invisible = writer
            .receive_messages(
                &queue_url,
                40,
                DurationSeconds::from(30),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive invisible check");
        for message in &invisible {
            let parsed_message_id = message
                .message_id
                .parse::<MessageId>()
                .expect("message id should be hex");
            assert!(
                !received_ids.contains(&parsed_message_id),
                "claimed messages must remain invisible before timeout"
            );
        }

        for message in batch_a.iter().chain(batch_b.iter()) {
            writer
                .delete_message(&queue_url, receipt_handle_from_response(message))
                .await
                .expect("delete concurrently claimed message");
        }
    };

    if tokio::time::timeout(Duration::from_secs(90), test_future)
        .await
        .is_err()
    {
        eprintln!("Skipping FoundationDB concurrent queue test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_template_uses_versionstamp_not_fallback() {
    let Some(store) = connect_fdb_store("fdb-templates").await else {
        eprintln!("Skipping FoundationDB template test: unable to connect to local cluster");
        return;
    };

    let test_future = async move {
        let fallback = b"fallback-literal".to_vec();
        let binding = PlaceholderBinding::unique(fallback.clone());
        let binding_id = binding.id();
        let prefix = format!("tests/fdb/templates/{}/", Uuid::now_v7()).into_bytes();
        let template = KeyTemplate::placeholder(prefix.clone(), Vec::new(), binding);
        let payload = b"payload".to_vec();

        let output = store
            .transact_write(vec![TransactWriteOperation::PutTemplate {
                template,
                value: payload.clone(),
                condition: None,
            }])
            .await
            .expect("transact write");

        let resolved = *output
            .placeholder_versions
            .get(&binding_id)
            .expect("versionstamp returned");

        let range = store
            .get_prefix(&prefix, true, Some(1), true)
            .await
            .expect("scan prefix");
        assert_eq!(range.items.len(), 1, "expected one stored item");
        let (key, value) = &range.items[0];
        assert_eq!(value.as_ref(), payload.as_slice());

        let suffix = &key[key.len() - resolved.len()..];
        assert_ne!(suffix, fallback.as_slice(), "fallback bytes were persisted");
        assert_eq!(
            suffix, resolved,
            "suffix should match committed versionstamp"
        );
    };

    if tokio::time::timeout(Duration::from_secs(90), test_future)
        .await
        .is_err()
    {
        eprintln!(
            "Skipping FoundationDB template test: timed out while verifying versionstamp writes"
        );
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_queue_reconcile_scales_out_hot_family_tests() {
    let Some(store) = connect_fdb_store("fdb-queue-reconcile").await else {
        eprintln!("Skipping FoundationDB queue reconcile test: unable to connect to local cluster");
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());
    let metrics = metrics_handle().clone();
    let test_future = async move {
        let add_partition_before = parse_metric_value(
            &metrics,
            "partition_reconcile_actions_total",
            &["family_kind=\"standard_queue\"", "action=\"add_partition\""],
        );
        provider
            .initialize()
            .await
            .expect("initialize queue provider");

        let queue_url = format!("fdb-queue-reconcile-{}", Uuid::now_v7());
        let queue_url = provider
            .create_queue(queue_definition(&queue_url))
            .await
            .expect("create queue")
            .queue_url;

        let family_component = queue_family_component(&queue_url);
        let family_before = provider
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("load queue family")
            .expect("queue family exists");
        let open_before = family_before
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        let hottest_partition = family_before
            .partitions
            .iter()
            .find(|partition| partition.is_writable())
            .expect("open queue partition");

        write_hot_queue_sample(
            &provider,
            &queue_url,
            hottest_partition.partition_id,
            family_before
                .config
                .target_writes_per_second
                .saturating_mul(4),
        )
        .await;

        for _ in 0..3 {
            provider
                .run_partition_reconcile()
                .await
                .expect("run queue reconcile");
        }

        let family_after = provider
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("reload queue family")
            .expect("queue family exists after reconcile");
        let open_after = family_after
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        assert!(open_after > open_before, "expected queue scale out");
        assert!(
            parse_metric_value(
                &metrics,
                "partition_reconcile_actions_total",
                &["family_kind=\"standard_queue\"", "action=\"add_partition\""],
            ) >= add_partition_before + 1.0,
            "expected queue add-partition metric to increment"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_family_hot_families",
                &["family_kind=\"standard_queue\""],
            ) >= 1.0,
            "expected queue hot-family gauge"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_family_managed_families",
                &["family_kind=\"standard_queue\""],
            ) >= 1.0,
            "expected queue managed-family gauge"
        );

        let mut sent_message_ids = std::collections::HashSet::new();
        for index in 0..16 {
            sent_message_ids.insert(
                provider
                    .send_message(new_message(&queue_url, &format!("after-scale-out-{index}")))
                    .await
                    .expect("send message after queue scale-out"),
            );
        }

        let mut received_after_scale_out = false;
        for _ in 0..5 {
            let received = provider
                .receive_messages(
                    &queue_url,
                    10,
                    DurationSeconds::from(5),
                    DurationSeconds::from(0),
                )
                .await
                .expect("receive after queue scale-out");
            for message in received {
                let parsed_message_id = message
                    .message_id
                    .parse::<MessageId>()
                    .expect("message id should be hex");
                received_after_scale_out |= sent_message_ids.contains(&parsed_message_id);
            }
            if received_after_scale_out {
                break;
            }
        }
        assert!(
            received_after_scale_out,
            "expected sampled receives to find at least one post-scale-out message"
        );
    };

    if tokio::time::timeout(Duration::from_secs(90), test_future)
        .await
        .is_err()
    {
        eprintln!("Skipping FoundationDB queue reconcile test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_partitioned_queue_send_retries_after_topology_change_tests() {
    let Some(store) = connect_fdb_store("fdb-queue-routing-fence").await else {
        eprintln!(
            "Skipping FoundationDB queue routing fence test: unable to connect to local cluster"
        );
        return;
    };

    let writer = SortedKvDbStorageProvider::new(store.clone());
    let updater = SortedKvDbStorageProvider::new(store.clone());
    let metrics = metrics_handle().clone();
    let test_future = async move {
        let retries_before = parse_metric_value(
            &metrics,
            "partition_routing_retries_total",
            &[
                "family_kind=\"standard_queue\"",
                "operation=\"send\"",
                "reason=\"stale_topology\"",
            ],
        );
        writer
            .initialize()
            .await
            .expect("initialize writer queue provider");
        updater
            .initialize()
            .await
            .expect("initialize updater queue provider");

        let queue_url = format!("fdb-queue-routing-fence-{}", Uuid::now_v7());
        let queue_url = writer
            .create_queue(queue_definition(&queue_url))
            .await
            .expect("create queue")
            .queue_url;

        let family_component = queue_family_component(&queue_url);
        let cached_family = writer
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("load cached queue family")
            .expect("queue family exists");
        let message_id = MessageId::random();
        let (stale_partition_id, stale_slot) =
            routed_partition_for_message(&queue_url, &message_id, &cached_family);

        let mut updated_family = updater
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("load queue family for update")
            .expect("queue family exists for update");
        let partition = updated_family
            .partitions
            .iter_mut()
            .find(|partition| partition.partition_id == stale_partition_id)
            .expect("stale routed partition");
        let changed = partition.begin_draining().is_ok();
        assert!(
            changed,
            "stale routed partition must be open before draining"
        );
        updated_family.config.family_epoch = updated_family.config.family_epoch.saturating_add(1);
        updater
            .save_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                &family_component,
                &updated_family,
            )
            .await
            .expect("save updated queue family");

        let sent_id = writer
            .send_message(new_message_with_id(
                &queue_url,
                "rerouted-after-topology-change",
                message_id,
            ))
            .await
            .expect("send rerouted queue message");
        assert_eq!(sent_id, message_id);

        let stale_key = queue_state_key_with_slot(
            QueueStorageId::new(1).expect("first queue id"),
            stale_slot,
            stale_partition_id,
            &message_id.to_string(),
        );
        assert!(
            writer
                .kv_store
                .get(&stale_key, true)
                .await
                .expect("read stale partition key")
                .is_none(),
            "message should not land in a write-closed partition"
        );

        let received = writer
            .receive_messages(
                &queue_url,
                1,
                DurationSeconds::from(5),
                DurationSeconds::from(0),
            )
            .await
            .expect("receive rerouted queue message");
        assert_eq!(received.len(), 1);
        let parsed_message_id = received[0]
            .message_id
            .parse::<MessageId>()
            .expect("message id should be hex");
        assert_eq!(parsed_message_id, message_id);
        assert!(
            parse_metric_value(
                &metrics,
                "partition_routing_retries_total",
                &[
                    "family_kind=\"standard_queue\"",
                    "operation=\"send\"",
                    "reason=\"stale_topology\"",
                ],
            ) >= retries_before + 1.0,
            "expected stale-topology queue routing retry metric to increment"
        );
    };

    if tokio::time::timeout(Duration::from_secs(90), test_future)
        .await
        .is_err()
    {
        eprintln!("Skipping FoundationDB queue routing fence test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689 and is intentionally expensive"]
async fn foundationdb_queue_scale_churn_stress_preserves_messages_tests() {
    let Some(store) = connect_fdb_store("fdb-queue-scale-churn").await else {
        eprintln!(
            "Skipping FoundationDB queue churn stress test: unable to connect to local cluster"
        );
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());
    let scaler = SortedKvDbStorageProvider::new(store.clone());
    let metrics = metrics_handle().clone();
    let test_future = async move {
        let begin_drain_before = parse_metric_value(
            &metrics,
            "partition_reconcile_actions_total",
            &["family_kind=\"standard_queue\"", "action=\"begin_drain\""],
        );
        let retire_before = parse_metric_value(
            &metrics,
            "partition_reconcile_actions_total",
            &["family_kind=\"standard_queue\"", "action=\"retire\""],
        );

        provider
            .initialize()
            .await
            .expect("initialize queue provider");
        scaler
            .initialize()
            .await
            .expect("initialize queue scaler provider");

        let queue_url = format!("fdb-queue-scale-churn-{}", Uuid::now_v7());
        let queue_url = provider
            .create_queue(queue_definition(&queue_url))
            .await
            .expect("create queue")
            .queue_url;

        let family_component = queue_family_component(&queue_url);
        for _ in 0..2 {
            let mut family = provider
                .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
                .await
                .expect("load queue family for scale-out")
                .expect("queue family exists for scale-out");
            family.config.cooldown_until_ms = None;
            let hottest_partition = family
                .partitions
                .iter()
                .find(|partition| partition.is_writable())
                .expect("open partition for queue scale-out")
                .partition_id;
            provider
                .save_partition_family_state(
                    PartitionFamilyKind::StandardQueue,
                    &family_component,
                    &family,
                )
                .await
                .expect("save queue family before scale-out");
            write_hot_queue_sample(
                &provider,
                &queue_url,
                hottest_partition,
                family.config.target_writes_per_second.saturating_mul(4),
            )
            .await;
            for _ in 0..3 {
                provider
                    .run_partition_reconcile()
                    .await
                    .expect("run queue scale-out reconcile");
            }
        }

        let scaled_family = provider
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("load scaled queue family")
            .expect("scaled queue family exists");
        let open_after_scale_out = scaled_family
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        assert!(
            open_after_scale_out >= 4,
            "expected repeated queue scale-out to create additional open partitions"
        );

        let mut expected_ids = HashSet::new();
        for index in 0..80usize {
            let message_id = MessageId::random();
            let inserted = expected_ids.insert(message_id);
            assert!(inserted, "stress test message ids should be unique");
            provider
                .send_message(new_message_with_id(
                    &queue_url,
                    &format!("before-drain-{index}"),
                    message_id,
                ))
                .await
                .expect("send queue churn message before drain");
        }

        let scaled_family = provider
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("reload scaled queue family")
            .expect("scaled queue family exists after sends");
        let mut draining_target = None;
        for partition in scaled_family
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
        {
            let compact_record_count = queue_compact_record_count_for_partition(
                &provider,
                partition.placement_slot,
                partition.partition_id,
            )
            .await;
            if compact_record_count > 0 {
                draining_target = Some((
                    partition.partition_id,
                    partition.placement_slot,
                    compact_record_count,
                ));
                break;
            }
        }
        let (draining_partition_id, draining_slot, draining_compact_record_count_before) =
            draining_target.expect("expected at least one populated open partition");

        let mut family = provider
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("load queue family for manual drain")
            .expect("queue family exists for manual drain");
        let changed = apply_queue_action(
            &mut family,
            QueueReconcileAction::BeginDrain {
                partition_id: draining_partition_id,
            },
            TimestampMillis::now().timestamp_millis(),
        );
        assert!(changed, "manual drain transition should apply");
        provider
            .save_partition_family_state(
                PartitionFamilyKind::StandardQueue,
                &family_component,
                &family,
            )
            .await
            .expect("save queue family after manual drain");

        for index in 0..20usize {
            let message_id = MessageId::random();
            let inserted = expected_ids.insert(message_id);
            assert!(inserted, "post-drain message ids should be unique");
            provider
                .send_message(new_message_with_id(
                    &queue_url,
                    &format!("after-drain-{index}"),
                    message_id,
                ))
                .await
                .expect("send queue churn message after drain");
        }

        let draining_compact_record_count_after = queue_compact_record_count_for_partition(
            &provider,
            draining_slot,
            draining_partition_id,
        )
        .await;
        assert_eq!(
            draining_compact_record_count_after, draining_compact_record_count_before,
            "new sends must not land in a draining partition"
        );

        let mut received_ids = HashSet::new();
        for _ in 0..40 {
            let batch = provider
                .receive_messages(
                    &queue_url,
                    10,
                    DurationSeconds::from(30),
                    DurationSeconds::from(0),
                )
                .await
                .expect("receive queue churn batch");
            if batch.is_empty() {
                if received_ids.len() == expected_ids.len() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            for message in batch {
                let parsed_message_id = message
                    .message_id
                    .parse::<MessageId>()
                    .expect("message id should be hex");
                assert!(
                    expected_ids.contains(&parsed_message_id),
                    "received unexpected message during queue churn"
                );
                let inserted = received_ids.insert(parsed_message_id);
                assert!(
                    inserted,
                    "queue churn should not duplicate deliveries in this test"
                );
                provider
                    .delete_message(&queue_url, receipt_handle_from_response(&message))
                    .await
                    .expect("delete queue churn message");
            }

            if received_ids.len() == expected_ids.len() {
                break;
            }
        }
        assert_eq!(
            received_ids, expected_ids,
            "queue churn should not lose messages across draining transitions"
        );

        scaler
            .kv_store
            .delete_prefix(partition_load_sample_prefix(
                PartitionFamilyKind::StandardQueue,
                &family_component,
            ))
            .await
            .expect("clear persisted queue load samples before scale-in");

        for _ in 0..160 {
            prepare_queue_scale_in(&scaler, &queue_url, 1).await;
            scaler
                .run_partition_reconcile()
                .await
                .expect("run queue scale-in reconcile");

            let family = scaler
                .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
                .await
                .expect("load queue family during scale-in")
                .expect("queue family exists during scale-in");
            let open_count = family
                .partitions
                .iter()
                .filter(|partition| partition.is_writable())
                .count();
            let draining_count = family
                .partitions
                .iter()
                .filter(|partition| partition.is_draining())
                .count();
            if open_count == 1 && draining_count == 0 {
                break;
            }
        }

        let final_family = scaler
            .load_partition_family_state(PartitionFamilyKind::StandardQueue, &family_component)
            .await
            .expect("load final queue family")
            .expect("final queue family exists");
        let final_open_count = final_family
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        let final_draining_count = final_family
            .partitions
            .iter()
            .filter(|partition| partition.is_draining())
            .count();
        assert_eq!(
            final_open_count, 1,
            "queue should scale back to one open partition"
        );
        assert_eq!(
            final_draining_count, 0,
            "queue should not leave draining partitions behind"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_reconcile_actions_total",
                &["family_kind=\"standard_queue\"", "action=\"begin_drain\""],
            ) >= begin_drain_before + 1.0,
            "expected queue begin-drain metric to increment"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_reconcile_actions_total",
                &["family_kind=\"standard_queue\"", "action=\"retire\""],
            ) >= retire_before + 1.0,
            "expected queue retire metric to increment"
        );
    };

    if tokio::time::timeout(Duration::from_secs(180), test_future)
        .await
        .is_err()
    {
        eprintln!("Skipping FoundationDB queue churn stress test: timed out");
    }
}
