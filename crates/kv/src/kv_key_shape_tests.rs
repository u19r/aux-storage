use queue_provider::MessageId;
use storage_common::ttl;
use storage_types::{
    AttributeValue, IndexName, ItemKey, SerializesToKey, StreamItemId, StreamName, TableKey,
    TableName, TimestampMillis,
};

use crate::{
    keyspace::compact::{self, PubsubRecordKind, QueueStorageId, U48},
    newtypes::MessageVisibilityKey,
    partition_family::{
        ordered_log_partition_prefix_with_slot, queue_body_key_with_slot,
        queue_checkpoint_key_with_slot, queue_ready_key_with_slot, queue_state_key_with_slot,
    },
    queue_provider::queue_delete_ledger_key,
};

const TABLE_NAME: &str = "orders-by-region2026";
const INDEX_NAME: &str = "gsi1";
const KEY_HASH: &str = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
const KEY_RANGE: &str = "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr";
const GSI_HASH: &str = "ssssssssssssssssssssssssssssssssssssssssssssssssss";
const GSI_RANGE: &str = "tttttttttttttttttttttttttttttttttttttttttttttttttt";
const QUEUE_URL: &str = "https://sqs.us-east-1.localhost/000000000000/orders-primaryq";
const TOPIC_ARN: &str =
    "arn:aws:sns:us-east-1:000000000000:orders-primary-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SUBSCRIPTION_ARN: &str =
    "arn:aws:sns:us-east-1:000000000000:orders-primary-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const MESSAGE_ID_HEX: &str = "000102030405060708090a0b";
const STREAM_ITEM_ID_BYTES: [u8; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

fn legacy_table_metadata_key(table_name: &TableName) -> Vec<u8> {
    format!("tables/{table_name}").into_bytes()
}

fn legacy_gsi_backfill_key(table_name: &TableName, index_name: &IndexName) -> Vec<u8> {
    format!("tables/{table_name}/gsi-backfill/{index_name}").into_bytes()
}

fn legacy_gsi_tombstone_prefix(table_name: &TableName, index_name: &IndexName) -> Vec<u8> {
    let mut prefix = table_name.sanitized_name().as_bytes().to_vec();
    prefix.extend(b"/index-tombstone/");
    prefix.extend(index_name.as_ref().as_bytes());
    prefix.extend(b"/data/");
    prefix
}

fn legacy_queue_message_storage_key(queue_url: &str, message_id: &MessageId) -> Vec<u8> {
    let mut key = format!("sys/queues/{queue_url}/messages/").into_bytes();
    key.extend_from_slice(message_id.as_bytes());
    key
}

#[derive(Debug)]
struct CurrentKeyShape {
    name: &'static str,
    owner: &'static str,
    key: Vec<u8>,
    compact_len: usize,
}

impl CurrentKeyShape {
    fn new(name: &'static str, owner: &'static str, key: Vec<u8>, compact_len: usize) -> Self {
        Self {
            name,
            owner,
            key,
            compact_len,
        }
    }

    fn current_len(&self) -> usize {
        self.key.len()
    }

    fn saved_bytes(&self) -> usize {
        self.current_len().saturating_sub(self.compact_len)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactKeyDebug {
    family: char,
    resource: &'static str,
    id: u64,
    suffix: Option<&'static str>,
}

impl std::fmt::Display for CompactKeyDebug {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.suffix {
            Some(suffix) => write!(
                formatter,
                "{}({}={},suffix={suffix})",
                self.family, self.resource, self.id
            ),
            None => write!(formatter, "{}({}={})", self.family, self.resource, self.id),
        }
    }
}

#[test]
fn key_shape_inventory_has_owner_for_each_current_sorted_kv_family() {
    let inventory = key_shape_inventory();

    assert!(
        inventory.len() >= 20,
        "phase-1 inventory should cover the representative hot key families"
    );
    for shape in inventory {
        assert!(
            !shape.owner.is_empty(),
            "missing owner for key shape {}",
            shape.name
        );
        assert!(
            shape.current_len() >= shape.compact_len,
            "compact target for {} should not exceed current length",
            shape.name
        );
    }
}

#[test]
fn current_key_shapes_record_representative_lengths_and_savings() {
    let inventory = key_shape_inventory();

    assert_shape(&inventory, "primary item", 128, 107, 21);
    assert_shape(&inventory, "gsi item", 243, 211, 32);
    assert_shape(&inventory, "gsi tombstone", 253, 213, 40);
    assert_shape(&inventory, "table metadata", 27, 5, 22);
    assert_shape(&inventory, "gsi backfill", 45, 7, 38);
    assert_shape(&inventory, "ttl due index", 156, 115, 41);
    assert_shape(&inventory, "table stream row", 46, 17, 29);
    assert_shape(&inventory, "item stream row", 150, 119, 31);
    assert_shape(&inventory, "ordered log table stream prefix", 10, 10, 0);
    assert_shape(&inventory, "partitioned queue data prefix", 10, 10, 0);
    assert_shape(&inventory, "legacy queue message", 93, 19, 74);
    assert_shape(&inventory, "pubsub subscription-by-topic", 14, 14, 0);
}

#[test]
fn current_hot_key_shapes_still_expose_customer_names_and_path_segments() {
    let inventory = key_shape_inventory();
    let forbidden = [
        TABLE_NAME.as_bytes(),
        INDEX_NAME.as_bytes(),
        QUEUE_URL.as_bytes(),
        TOPIC_ARN.as_bytes(),
        SUBSCRIPTION_ARN.as_bytes(),
        b"sys/",
        b"tables/",
        b"/data/",
        b"/index/",
        b"pqueue/",
        b"plog/",
    ];

    let exposed = inventory
        .iter()
        .filter(|shape| {
            forbidden
                .iter()
                .any(|needle| contains_bytes(&shape.key, needle))
        })
        .count();

    assert!(
        exposed > 0,
        "remaining legacy fixtures should prove why compact key guards are still needed"
    );
}

#[test]
fn compact_debug_contract_is_stable_for_later_codec_tests() {
    let examples = [
        (
            CompactKeyDebug {
                family: 'p',
                resource: "table",
                id: 42,
                suffix: Some("key=006f72646572"),
            },
            "p(table=42,suffix=key=006f72646572)",
        ),
        (
            CompactKeyDebug {
                family: 'g',
                resource: "table",
                id: 42,
                suffix: Some("index=3,gsi_key=...,table_key=..."),
            },
            "g(table=42,suffix=index=3,gsi_key=...,table_key=...)",
        ),
        (
            CompactKeyDebug {
                family: 'q',
                resource: "queue",
                id: 7,
                suffix: Some("partition=2,kind=ready"),
            },
            "q(queue=7,suffix=partition=2,kind=ready)",
        ),
    ];

    for (debug, expected) in examples {
        assert_eq!(debug.to_string(), expected);
    }
}

#[test]
fn compact_hot_key_guard_rejects_names_and_old_path_segments() {
    let forbidden = [
        TABLE_NAME.as_bytes(),
        INDEX_NAME.as_bytes(),
        QUEUE_URL.as_bytes(),
        TOPIC_ARN.as_bytes(),
        SUBSCRIPTION_ARN.as_bytes(),
        b"sys/",
        b"tables/",
        b"/data/",
        b"/index/",
        b"pqueue/",
        b"plog/",
    ];
    let compact_fixtures = [
        b"p\0\0\0*encoded-user-key".to_vec(),
        b"g\0\0\0*\0\x03encoded-gsi-keyencoded-table-key".to_vec(),
        b"q\x01\0\0\0\0\0\x07\0\x02rmessage".to_vec(),
        b"j\0\0\0\0\0\x07\0\0\0\0\0\x09".to_vec(),
    ];

    for key in compact_fixtures {
        for needle in forbidden {
            assert!(
                !contains_bytes(&key, needle),
                "compact fixture leaked forbidden segment {} in {}",
                String::from_utf8_lossy(needle),
                hex_debug(&key)
            );
        }
    }
}

#[test]
fn kv_production_sources_do_not_reintroduce_legacy_hot_prefix_construction() {
    let forbidden = [
        "format!(\"sys/queues",
        "format!(\"sys/partition-control",
        "format!(\"sys/pubsub",
        "format!(\"sys/sync/item-revisions",
        "format!(\"tables/",
        "format!(\"pqueue/",
        "format!(\"plog/",
        "b\"sys/queues",
        "b\"sys/partition-control",
        "b\"sys/pubsub",
        "b\"sys/sync/item-revisions",
        "b\"tables/",
        "b\"pqueue/",
        "b\"plog/",
    ];
    let mut violations = Vec::new();

    for path in kv_source_files() {
        let file_name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .expect("source file should have utf8 filename");
        if file_name.ends_with("_tests.rs") || file_name == "kv_key_shape_tests.rs" {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("source file should be readable");
        for needle in forbidden {
            if source.contains(needle) {
                violations.push(format!("{} contains {needle}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "legacy hot-prefix construction returned in production kv sources:\n{}",
        violations.join("\n")
    );
}

fn key_shape_inventory() -> Vec<CurrentKeyShape> {
    let table_name = TableName::new(TABLE_NAME);
    let index_name = IndexName::new(INDEX_NAME);
    let item_key = representative_item_key(&table_name);
    let index_key = representative_index_key(&table_name, &index_name);
    let primary_key = item_key.serialize_to_bytes().expect("primary key");
    let gsi_key = index_key.serialize_to_bytes().expect("gsi key");
    let gsi_suffix = gsi_key
        .strip_prefix(ItemKey::index_prefix_from_name(&table_name, &index_name).as_slice())
        .expect("gsi prefix")
        .to_vec();
    let stream_item_id = StreamItemId::from(STREAM_ITEM_ID_BYTES);
    let table_stream_name = StreamName::table_stream(&table_name);
    let table_stream_row = stream_row_key(&table_stream_name, &stream_item_id);
    let item_stream_name = StreamName::table_item_stream(&table_name, &item_key)
        .expect("item stream name should encode");
    let item_stream_row = stream_row_key(&item_stream_name, &stream_item_id);
    let ttl_token = "x".repeat(102);
    let ttl_due_index = ttl::ttl_index_key(&table_name, 1_700_000_000, &ttl_token);
    let visibility = MessageVisibilityKey(format!(
        "{:013}:{}",
        TimestampMillis::from(1_700_000_000_000).timestamp_millis(),
        MessageId::default()
    ));
    let queue_id = QueueStorageId::new(7).expect("representative queue id");
    let topic_id = U48::new(11).expect("topic id");
    let subscription_id = U48::new(12).expect("subscription id");
    let delivery_id = U48::new(13).expect("delivery id");
    let delivery_status_id = U48::new(4).expect("delivery status id");
    let mut delivery_claim_suffix = 1_700_000_000_i64.to_be_bytes().to_vec();
    delivery_claim_suffix.extend_from_slice(&[0, 0, 0, 0, 0, 13]);
    let delivery_claim_key = compact::pubsub_record_key(
        PubsubRecordKind::DeliveryClaim,
        delivery_status_id,
        Some(delivery_id),
        &delivery_claim_suffix,
    );

    vec![
        CurrentKeyShape::new(
            "primary item",
            "storage-types/src/item_key_rocksdb.rs",
            primary_key,
            107,
        ),
        CurrentKeyShape::new(
            "gsi item",
            "storage-types/src/item_key_rocksdb.rs",
            gsi_key,
            211,
        ),
        CurrentKeyShape::new(
            "gsi tombstone",
            "crates/kv/src/keyspace/table_keys.rs",
            {
                let mut key = legacy_gsi_tombstone_prefix(&table_name, &index_name);
                key.extend_from_slice(&gsi_suffix);
                key
            },
            213,
        ),
        CurrentKeyShape::new(
            "table metadata",
            "crates/kv/src/keyspace/compact.rs",
            legacy_table_metadata_key(&table_name),
            5,
        ),
        CurrentKeyShape::new(
            "gsi backfill",
            "crates/kv/src/keyspace/compact.rs",
            legacy_gsi_backfill_key(&table_name, &index_name),
            7,
        ),
        CurrentKeyShape::new(
            "ttl config",
            "crates/kv/src/storage_ops/ttl.rs",
            {
                let mut key = legacy_table_metadata_key(&table_name);
                key.extend_from_slice(b"/ttl-config");
                key
            },
            5,
        ),
        CurrentKeyShape::new(
            "ttl due index",
            "storage-common/src/ttl.rs",
            ttl_due_index,
            115,
        ),
        CurrentKeyShape::new(
            "table stream row",
            "storage-types/src/stream_name.rs",
            table_stream_row,
            17,
        ),
        CurrentKeyShape::new(
            "item stream row",
            "storage-types/src/stream_name.rs",
            item_stream_row,
            119,
        ),
        CurrentKeyShape::new(
            "stream pointer table index",
            "crates/kv/src/storage_ops/stream_duration.rs",
            stream_pointer_table_index_key(&table_name, &stream_item_id),
            17,
        ),
        CurrentKeyShape::new(
            "stream pointer item index",
            "crates/kv/src/storage_ops/stream_duration.rs",
            stream_pointer_item_index_key(&table_name, &item_stream_name, &stream_item_id),
            119,
        ),
        CurrentKeyShape::new(
            "ordered log table stream prefix",
            "crates/kv/src/partition_family/model.rs",
            ordered_log_partition_prefix_with_slot(&table_stream_name, 2, 2),
            10,
        ),
        CurrentKeyShape::new(
            "partitioned queue data prefix",
            "crates/kv/src/partition_family/model.rs",
            crate::partition_family::queue_partition_prefix_with_slot(queue_id, 2, 2),
            10,
        ),
        CurrentKeyShape::new(
            "partitioned queue ready",
            "crates/kv/src/partition_family/model.rs",
            queue_ready_key_with_slot(queue_id, 2, 2, &visibility),
            34,
        ),
        CurrentKeyShape::new(
            "partitioned queue body",
            "crates/kv/src/partition_family/model.rs",
            queue_body_key_with_slot(queue_id, 2, 2, MESSAGE_ID_HEX),
            33,
        ),
        CurrentKeyShape::new(
            "partitioned queue state",
            "crates/kv/src/partition_family/model.rs",
            queue_state_key_with_slot(queue_id, 2, 2, MESSAGE_ID_HEX),
            33,
        ),
        CurrentKeyShape::new(
            "partitioned queue checkpoint",
            "crates/kv/src/partition_family/model.rs",
            queue_checkpoint_key_with_slot(queue_id, 2, 2, MESSAGE_ID_HEX),
            33,
        ),
        CurrentKeyShape::new(
            "legacy queue message",
            "crates/kv/src/queue_provider.rs",
            legacy_queue_message_storage_key(QUEUE_URL, &MessageId::default()),
            19,
        ),
        CurrentKeyShape::new(
            "queue delete ledger",
            "crates/kv/src/queue_provider.rs",
            queue_delete_ledger_key(queue_id, 2, 2, MESSAGE_ID_HEX),
            39,
        ),
        CurrentKeyShape::new(
            "pubsub topic by arn",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_global_record_key(PubsubRecordKind::Topic, &[0; 8]),
            16,
        ),
        CurrentKeyShape::new(
            "pubsub topic by name",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_global_record_key(PubsubRecordKind::TopicName, &[0; 8]),
            16,
        ),
        CurrentKeyShape::new(
            "pubsub subscription by arn",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_global_record_key(PubsubRecordKind::Subscription, &[0; 8]),
            16,
        ),
        CurrentKeyShape::new(
            "pubsub subscription-by-topic",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_record_key(
                PubsubRecordKind::SubscriptionTopic,
                topic_id,
                Some(subscription_id),
                b"",
            ),
            14,
        ),
        CurrentKeyShape::new(
            "pubsub subscription dedupe",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_record_key(
                PubsubRecordKind::SubscriptionDedupe,
                topic_id,
                None,
                &[0; 8],
            ),
            16,
        ),
        CurrentKeyShape::new(
            "pubsub delivery by id",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_global_record_key(PubsubRecordKind::Delivery, &[0; 8]),
            16,
        ),
        CurrentKeyShape::new(
            "pubsub delivery by subscription",
            "crates/kv/src/pubsub/provider.rs",
            compact::pubsub_record_key(
                PubsubRecordKind::DeliverySubscription,
                subscription_id,
                Some(delivery_id),
                b"",
            ),
            13,
        ),
        CurrentKeyShape::new(
            "pubsub delivery claim",
            "crates/kv/src/pubsub/provider.rs",
            delivery_claim_key,
            15,
        ),
        CurrentKeyShape::new(
            "idempotency token",
            "crates/kv/src/storage_ops/idempotency.rs",
            b"idempotency_token:request-token-000000000001".to_vec(),
            17,
        ),
        CurrentKeyShape::new(
            "sync apply mutation",
            "crates/kv/src/storage_ops/resolved_sync_apply.rs",
            b"sys/sync/apply/mutation/orders-by-region-2026/000000000001".to_vec(),
            17,
        ),
    ]
}

fn kv_source_files() -> Vec<std::path::PathBuf> {
    fn visit(path: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).expect("source directory should be readable") {
            let entry = entry.expect("source directory entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                visit(&path, files);
            } else if path.extension().and_then(std::ffi::OsStr::to_str) == Some("rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    files
}

fn representative_item_key(table_name: &TableName) -> ItemKey {
    ItemKey::table_key(
        table_name.clone(),
        AttributeValue::S(KEY_HASH.to_string()),
        Some(AttributeValue::S(KEY_RANGE.to_string())),
    )
}

fn representative_index_key(table_name: &TableName, index_name: &IndexName) -> ItemKey {
    let table_key = TableKey::new(
        table_name.clone(),
        AttributeValue::S(KEY_HASH.to_string()),
        Some(AttributeValue::S(KEY_RANGE.to_string())),
    );
    ItemKey::index_key(
        table_name.clone(),
        index_name.clone(),
        AttributeValue::S(GSI_HASH.to_string()),
        Some(AttributeValue::S(GSI_RANGE.to_string())),
        table_key,
    )
}

fn stream_row_key(stream_name: &StreamName, stream_item_id: &StreamItemId) -> Vec<u8> {
    let mut key = stream_name.as_ref().to_vec();
    key.push(b'/');
    key.extend_from_slice(stream_item_id.as_bytes());
    key
}

fn stream_pointer_table_index_key(
    table_name: &TableName,
    stream_item_id: &StreamItemId,
) -> Vec<u8> {
    let mut key = b"sys/stream-duration/pointer/".to_vec();
    key.extend_from_slice(table_name.as_ref().as_bytes());
    key.extend_from_slice(b"/table/");
    key.extend_from_slice(stream_item_id.as_bytes());
    key
}

fn stream_pointer_item_index_key(
    table_name: &TableName,
    stream_name: &StreamName,
    stream_item_id: &StreamItemId,
) -> Vec<u8> {
    let mut key = b"sys/stream-duration/pointer/".to_vec();
    key.extend_from_slice(table_name.as_ref().as_bytes());
    key.push(b'/');
    key.extend_from_slice(b"kv-stream:");
    key.extend_from_slice(hex_debug(stream_name.as_ref()).as_bytes());
    key.push(b'/');
    key.extend_from_slice(stream_item_id.as_bytes());
    key
}

fn assert_shape(
    inventory: &[CurrentKeyShape],
    name: &'static str,
    current_len: usize,
    compact_len: usize,
    saved_bytes: usize,
) {
    let shape = inventory
        .iter()
        .find(|shape| shape.name == name)
        .unwrap_or_else(|| panic!("missing shape {name}"));
    assert_eq!(shape.current_len(), current_len, "{name} current length");
    assert_eq!(shape.compact_len, compact_len, "{name} compact length");
    assert_eq!(shape.saved_bytes(), saved_bytes, "{name} saved bytes");
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn hex_debug(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}
