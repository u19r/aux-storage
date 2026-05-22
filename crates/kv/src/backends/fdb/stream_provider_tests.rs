#![cfg(feature = "foundationdb-backend")]

use std::{collections::HashMap, time::Duration};

use storage_provider::StorageProvider;
use storage_types::{
    AttributeDefinition, AttributeValue, CreateTableRequest, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, StreamName, StreamSpecification, StreamViewType, TableName,
    UserStreamName,
};
use stream_provider::{StreamPartitioningMode, StreamProvider};
use tokio::time::timeout;
use uuid::Uuid;

use crate::{
    FoundationDbKvStore, SortedKvDbStorageProvider,
    backends::fdb::fdb_support_tests::{connect_fdb_store, metrics_handle, parse_metric_value},
    constants::PARTITION_LOAD_SAMPLE_WINDOW_SECONDS,
    partition_family::{
        PartitionFamilyKind, PartitionLoadSample, PartitionLoadSampleRecord,
        find_partition_for_hash, ordered_log_family_component, ordered_log_hash,
        ordered_log_partition_prefix_with_slot, partition_load_sample_bytes,
        partition_load_sample_key, partition_sample_window_start_ms, routing_key_bucket_bit,
    },
    sorted_kv_store::SortedKvStore,
};

fn simple_item(pk: &str, sk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
        ("value".to_string(), AttributeValue::S(value.to_string())),
    ])
}

async fn create_stream_enabled_table(
    provider: &SortedKvDbStorageProvider<FoundationDbKvStore>,
    table_name: &TableName,
) {
    let request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        storage_types::BillingMode::PayPerRequest,
    )
    .with_stream_specification(Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: Some(StreamViewType::NewAndOldImages),
    }));

    provider
        .create_table(&request)
        .await
        .expect("create stream-enabled table");
}

async fn write_hot_ordered_log_sample(
    provider: &SortedKvDbStorageProvider<FoundationDbKvStore>,
    stream_name: &StreamName,
    partition_id: u16,
    writes: u64,
) {
    let family_component = ordered_log_family_component(stream_name);
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
            routing_key_bucket_bitmap: routing_key_bucket_bit(0) | routing_key_bucket_bit(1),
            ..Default::default()
        },
    };
    provider
        .kv_store
        .put(
            &partition_load_sample_key(
                PartitionFamilyKind::OrderedLog,
                &family_component,
                partition_id,
                window_start_ms,
                &publisher_id,
            ),
            &partition_load_sample_bytes(&sample).expect("encode ordered log load sample"),
            None,
        )
        .await
        .expect("persist ordered log load sample");
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_key_ordered_stream_split_keeps_page_tokens_tests() {
    let Some(store) = connect_fdb_store("fdb-streams").await else {
        eprintln!("Skipping FoundationDB stream split test: unable to connect to local cluster");
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());
    let test_future = async move {
        provider
            .initialize_storage()
            .await
            .expect("initialize storage");
        provider
            .initialize_stream()
            .await
            .expect("initialize stream provider");

        let stream_name = provider
            .create_stream(
                UserStreamName::new(&format!("fdb-key-ordered-{}", Uuid::now_v7())),
                None,
                StreamPartitioningMode::KeyOrdered,
            )
            .await
            .expect("create key ordered stream");
        let partition_key = "customer-1";

        let item1 = provider
            .append_item(stream_name.clone(), b"item-1", Some(partition_key))
            .await
            .expect("append item 1");
        let item2 = provider
            .append_item(stream_name.clone(), b"item-2", Some(partition_key))
            .await
            .expect("append item 2");
        let first_page = provider
            .read_forward(stream_name.clone(), None, 1)
            .await
            .expect("read first page");
        assert_eq!(first_page.items.len(), 1);
        assert_eq!(first_page.items[0].id, item1);

        let family = provider
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("load partition family")
            .expect("partition family exists");
        let parent = find_partition_for_hash(
            &family.partitions,
            ordered_log_hash(partition_key.as_bytes()),
        )
        .expect("partition for key");
        let parent_prefix = ordered_log_partition_prefix_with_slot(
            &stream_name,
            parent.placement_slot,
            parent.partition_id,
        );
        let parent_before = provider
            .kv_store
            .get_prefix(&parent_prefix, true, None, true)
            .await
            .expect("scan parent prefix")
            .items
            .len();
        assert_eq!(parent_before, 2);

        provider
            .split_ordered_log_partition(&stream_name, parent.partition_id)
            .await
            .expect("split partition");

        let item3 = provider
            .append_item(stream_name.clone(), b"item-3", Some(partition_key))
            .await
            .expect("append item 3");
        let item4 = provider
            .append_item(stream_name.clone(), b"item-4", Some(partition_key))
            .await
            .expect("append item 4");

        let family_after = provider
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("load family after split")
            .expect("partition family after split");
        let sealed_parent = family_after
            .partitions
            .iter()
            .find(|partition| partition.partition_id == parent.partition_id)
            .expect("sealed parent after split");
        let child = find_partition_for_hash(
            &family_after.partitions,
            ordered_log_hash(partition_key.as_bytes()),
        )
        .expect("child partition");
        assert_ne!(child.partition_id, parent.partition_id);
        let split_boundary = sealed_parent
            .sealed_after_id
            .expect("exact split boundary recorded");
        assert_eq!(
            sealed_parent.state,
            crate::partition_family::PartitionState::WriteClosed
        );
        assert_eq!(child.opened_after_id, Some(split_boundary));

        let parent_after = provider
            .kv_store
            .get_prefix(&parent_prefix, true, None, true)
            .await
            .expect("scan parent prefix after split")
            .items
            .len();
        assert_eq!(parent_after, parent_before);

        let child_prefix = ordered_log_partition_prefix_with_slot(
            &stream_name,
            child.placement_slot,
            child.partition_id,
        );
        let child_count = provider
            .kv_store
            .get_prefix(&child_prefix, true, None, true)
            .await
            .expect("scan child prefix")
            .items
            .len();
        assert_eq!(child_count, 2);

        let next_page = provider
            .read_forward(stream_name, first_page.last_evaluated_key, 10)
            .await
            .expect("read second page");
        let ids: Vec<_> = next_page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![item2, item3, item4]);
    };

    if timeout(Duration::from_secs(90), test_future).await.is_err() {
        eprintln!("Skipping FoundationDB stream split test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_system_stream_split_reroutes_pointer_writes_tests() {
    let Some(store) = connect_fdb_store("fdb-pointer-streams").await else {
        eprintln!("Skipping FoundationDB pointer stream test: unable to connect to local cluster");
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());
    let test_future = async move {
        provider
            .initialize_storage()
            .await
            .expect("initialize storage");
        provider
            .initialize_stream()
            .await
            .expect("initialize stream provider");

        let table_name = TableName::new(&format!("fdb-stream-table-{}", Uuid::now_v7()));
        create_stream_enabled_table(&provider, &table_name).await;

        let item_key = ItemKey::table_key(
            table_name.clone(),
            AttributeValue::S("ORG#1".to_string()),
            Some(AttributeValue::S("ITEM#1".to_string())),
        );
        let item_stream =
            StreamName::table_item_stream(&table_name, &item_key).expect("build item stream name");

        provider
            .put_item(
                table_name.clone(),
                simple_item("ORG#1", "ITEM#1", "before-split"),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("put item before split");

        let system_stream = StreamName::system_table_stream();
        let family = provider
            .load_ordered_log_family_state(&system_stream)
            .await
            .expect("load system stream family")
            .expect("system stream family exists");
        let parent =
            find_partition_for_hash(&family.partitions, ordered_log_hash(item_stream.as_ref()))
                .expect("system stream partition");
        let parent_prefix = ordered_log_partition_prefix_with_slot(
            &system_stream,
            parent.placement_slot,
            parent.partition_id,
        );
        let parent_before = provider
            .kv_store
            .get_prefix(&parent_prefix, true, None, true)
            .await
            .expect("scan system parent prefix")
            .items
            .len();
        assert_eq!(parent_before, 1);

        provider
            .split_ordered_log_partition(&system_stream, parent.partition_id)
            .await
            .expect("split system stream partition");

        provider
            .put_item(
                table_name.clone(),
                simple_item("ORG#1", "ITEM#1", "after-split"),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("put item after split");

        let family_after = provider
            .load_ordered_log_family_state(&system_stream)
            .await
            .expect("load system family after split")
            .expect("system stream family after split");
        let sealed_parent = family_after
            .partitions
            .iter()
            .find(|partition| partition.partition_id == parent.partition_id)
            .expect("sealed system parent after split");
        let child = find_partition_for_hash(
            &family_after.partitions,
            ordered_log_hash(item_stream.as_ref()),
        )
        .expect("child partition");
        assert_ne!(child.partition_id, parent.partition_id);
        assert_eq!(
            sealed_parent.state,
            crate::partition_family::PartitionState::WriteClosed
        );
        assert_eq!(child.opened_after_id, sealed_parent.sealed_after_id);

        let parent_after = provider
            .kv_store
            .get_prefix(&parent_prefix, true, None, true)
            .await
            .expect("scan system parent prefix after split")
            .items
            .len();
        assert_eq!(parent_after, parent_before);

        let child_prefix = ordered_log_partition_prefix_with_slot(
            &system_stream,
            child.placement_slot,
            child.partition_id,
        );
        let child_count = provider
            .kv_store
            .get_prefix(&child_prefix, true, None, true)
            .await
            .expect("scan system child prefix")
            .items
            .len();
        assert_eq!(child_count, 1);

        let page = provider
            .read_forward(system_stream, None, 10)
            .await
            .expect("read system stream");
        assert_eq!(page.items.len(), 2);
    };

    if timeout(Duration::from_secs(90), test_future).await.is_err() {
        eprintln!("Skipping FoundationDB pointer stream test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_ordered_log_reconcile_splits_hot_family_tests() {
    let Some(store) = connect_fdb_store("fdb-stream-reconcile").await else {
        eprintln!(
            "Skipping FoundationDB ordered-log reconcile test: unable to connect to local cluster"
        );
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());
    let metrics = metrics_handle().clone();
    let test_future = async move {
        let splits_before = parse_metric_value(
            &metrics,
            "partition_reconcile_actions_total",
            &["family_kind=\"ordered_log\"", "action=\"split\""],
        );
        provider
            .initialize_storage()
            .await
            .expect("initialize storage");
        provider
            .initialize_stream()
            .await
            .expect("initialize stream provider");

        let stream_name = provider
            .create_stream(
                UserStreamName::new(&format!("fdb-reconcile-{}", Uuid::now_v7())),
                None,
                StreamPartitioningMode::KeyOrdered,
            )
            .await
            .expect("create key ordered stream");

        let family_before = provider
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("load ordered-log family")
            .expect("ordered-log family exists");
        let open_before = family_before
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        let parent = family_before
            .partitions
            .iter()
            .find(|partition| partition.is_writable())
            .expect("open partition");

        write_hot_ordered_log_sample(
            &provider,
            &stream_name,
            parent.partition_id,
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
                .expect("run ordered-log reconcile");
        }

        let family_after = provider
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("reload ordered-log family")
            .expect("ordered-log family after reconcile");
        let open_after = family_after
            .partitions
            .iter()
            .filter(|partition| partition.is_writable())
            .count();
        assert!(open_after > open_before, "expected ordered-log split");
        assert!(
            parse_metric_value(
                &metrics,
                "partition_reconcile_actions_total",
                &["family_kind=\"ordered_log\"", "action=\"split\""],
            ) >= splits_before + 1.0,
            "expected ordered-log split metric to increment"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_family_transition_partitions",
                &["family_kind=\"ordered_log\"", "state=\"write_closed\""],
            ) >= 1.0,
            "expected ordered-log write_closed transition gauge"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_family_hot_families",
                &["family_kind=\"ordered_log\""],
            ) >= 1.0,
            "expected ordered-log hot-family gauge"
        );
        assert!(
            parse_metric_value(
                &metrics,
                "partition_family_managed_families",
                &["family_kind=\"ordered_log\""],
            ) >= 1.0,
            "expected ordered-log managed-family gauge"
        );

        provider
            .append_item(stream_name.clone(), b"after-reconcile", Some("customer-1"))
            .await
            .expect("append after ordered-log split");
        let page = provider
            .read_forward(stream_name, None, 10)
            .await
            .expect("read after ordered-log split");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].data, b"after-reconcile");
    };

    if timeout(Duration::from_secs(90), test_future).await.is_err() {
        eprintln!("Skipping FoundationDB ordered-log reconcile test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689 and is intentionally expensive"]
async fn foundationdb_key_ordered_stream_repeated_split_stress_preserves_all_items_tests() {
    let Some(store) = connect_fdb_store("fdb-stream-split-stress").await else {
        eprintln!("Skipping FoundationDB split stress test: unable to connect to local cluster");
        return;
    };

    let provider = SortedKvDbStorageProvider::new(store.clone());
    let test_future = async move {
        provider
            .initialize_storage()
            .await
            .expect("initialize storage");
        provider
            .initialize_stream()
            .await
            .expect("initialize stream provider");

        let stream_name = provider
            .create_stream(
                UserStreamName::new(&format!("fdb-split-stress-{}", Uuid::now_v7())),
                None,
                StreamPartitioningMode::KeyOrdered,
            )
            .await
            .expect("create key ordered stream");

        let routing_keys = ["tenant-hot", "tenant-a", "tenant-b", "tenant-c"];
        let mut expected_ids = Vec::new();
        let mut split_rounds = 0usize;

        for index in 0..96usize {
            let routing_key = match index % 4 {
                0 | 1 => routing_keys[0],
                2 => routing_keys[1],
                _ => routing_keys[2],
            };
            let payload = format!("{routing_key}:{index}");
            let item_id = provider
                .append_item(stream_name.clone(), payload.as_bytes(), Some(routing_key))
                .await
                .expect("append split-stress item");
            expected_ids.push(item_id);

            if index > 0 && index % 16 == 0 {
                let family = provider
                    .load_ordered_log_family_state(&stream_name)
                    .await
                    .expect("load split-stress family")
                    .expect("split-stress family exists");
                let hot_partition = find_partition_for_hash(
                    &family.partitions,
                    ordered_log_hash(routing_keys[0].as_bytes()),
                )
                .expect("hot partition during split-stress");
                provider
                    .split_ordered_log_partition(&stream_name, hot_partition.partition_id)
                    .await
                    .expect("split hot partition during split-stress");
                split_rounds = split_rounds.saturating_add(1);

                let page = provider
                    .read_forward(
                        stream_name.clone(),
                        None,
                        u32::try_from(expected_ids.len() + 8).unwrap_or(u32::MAX),
                    )
                    .await
                    .expect("read split-stress stream after split");
                let actual_ids: Vec<_> = page.items.iter().map(|item| item.id).collect();
                assert_eq!(
                    actual_ids, expected_ids,
                    "repeated splits must preserve exact logical stream order"
                );
            }
        }

        let final_page = provider
            .read_forward(
                stream_name.clone(),
                None,
                u32::try_from(expected_ids.len() + 8).unwrap_or(u32::MAX),
            )
            .await
            .expect("read split-stress stream");
        let final_ids: Vec<_> = final_page.items.iter().map(|item| item.id).collect();
        assert_eq!(
            final_ids, expected_ids,
            "split-stress stream should not lose or duplicate records"
        );

        let family_after = provider
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("load split-stress family after churn")
            .expect("split-stress family exists after churn");
        let write_closed = family_after
            .partitions
            .iter()
            .filter(|partition| {
                partition.state == crate::partition_family::PartitionState::WriteClosed
            })
            .count();
        assert_eq!(
            write_closed, split_rounds,
            "each split round should leave one sealed parent behind"
        );
    };

    if timeout(Duration::from_secs(180), test_future)
        .await
        .is_err()
    {
        eprintln!("Skipping FoundationDB split stress test: timed out");
    }
}

#[tokio::test]
#[ignore = "requires a local FoundationDB cluster on 127.0.0.1:4689"]
async fn foundationdb_key_ordered_stream_split_invalidates_cached_routing_tests() {
    let Some(store) = connect_fdb_store("fdb-stream-routing").await else {
        eprintln!("Skipping FoundationDB routing cache test: unable to connect to local cluster");
        return;
    };

    let writer = SortedKvDbStorageProvider::new(store.clone());
    let splitter = SortedKvDbStorageProvider::new(store.clone());
    let test_future = async move {
        writer
            .initialize_storage()
            .await
            .expect("initialize storage");
        writer
            .initialize_stream()
            .await
            .expect("initialize stream provider");
        splitter
            .initialize_stream()
            .await
            .expect("initialize secondary stream provider");

        let stream_name = writer
            .create_stream(
                UserStreamName::new(&format!("fdb-key-ordered-routing-{}", Uuid::now_v7())),
                None,
                StreamPartitioningMode::KeyOrdered,
            )
            .await
            .expect("create key ordered stream");
        let partition_key = "customer-2";

        let item1 = writer
            .append_item(stream_name.clone(), b"before-split", Some(partition_key))
            .await
            .expect("append item before split");
        let cached_family = writer
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("load cached family")
            .expect("cached family exists");
        let parent = find_partition_for_hash(
            &cached_family.partitions,
            ordered_log_hash(partition_key.as_bytes()),
        )
        .expect("parent partition");
        let parent_prefix = ordered_log_partition_prefix_with_slot(
            &stream_name,
            parent.placement_slot,
            parent.partition_id,
        );

        splitter
            .split_ordered_log_partition(&stream_name, parent.partition_id)
            .await
            .expect("split ordered log partition");

        let item2 = writer
            .append_item(stream_name.clone(), b"after-split", Some(partition_key))
            .await
            .expect("append item after split");

        let split_family = splitter
            .load_ordered_log_family_state(&stream_name)
            .await
            .expect("load split family")
            .expect("split family exists");
        let child = find_partition_for_hash(
            &split_family.partitions,
            ordered_log_hash(partition_key.as_bytes()),
        )
        .expect("child partition");
        assert_ne!(child.partition_id, parent.partition_id);

        let parent_count = writer
            .kv_store
            .get_prefix(&parent_prefix, true, None, true)
            .await
            .expect("scan parent partition after split")
            .items
            .len();
        assert_eq!(parent_count, 1);

        let child_prefix = ordered_log_partition_prefix_with_slot(
            &stream_name,
            child.placement_slot,
            child.partition_id,
        );
        let child_count = writer
            .kv_store
            .get_prefix(&child_prefix, true, None, true)
            .await
            .expect("scan child partition after split")
            .items
            .len();
        assert_eq!(child_count, 1);

        let mut writer_view = None;
        for _ in 0..20 {
            let family = writer
                .load_ordered_log_family_state(&stream_name)
                .await
                .expect("reload writer family")
                .expect("writer family exists");
            let routed_partition = find_partition_for_hash(
                &family.partitions,
                ordered_log_hash(partition_key.as_bytes()),
            )
            .expect("writer routed partition");
            if routed_partition.partition_id == child.partition_id {
                writer_view = Some(family);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            writer_view.is_some(),
            "writer cache should refresh after partition family changes"
        );

        let page = writer
            .read_forward(stream_name, None, 10)
            .await
            .expect("read merged stream after split");
        let ids: Vec<_> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![item1, item2]);
    };

    if timeout(Duration::from_secs(90), test_future).await.is_err() {
        eprintln!("Skipping FoundationDB routing cache test: timed out");
    }
}
