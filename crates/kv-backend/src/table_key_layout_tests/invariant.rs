use std::num::{NonZeroU32, NonZeroUsize};

use serde_json::json;
use storage_provider::{
    DynamoOperations as _, MaintenanceOperations as _, TableAppendOperations as _, TableReclaimStep,
};
use storage_types::{AttemptPlan, DeadlineClass};
use stream_provider::{
    StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt, StreamAttempt as _,
    StreamOperations as _, SystemStreamPageRequest, TableAppendRequest,
};

use crate::{
    AttemptMode, KvAttempt as _, KvBackend, KvMutation, KvRange, KvStore, ReadProtection,
    table_key_layout::*,
};

/// `[T id, upper(T id))`: every key one table incarnation owns.
/// Pseudo-random (splitmix64) symbols: LZ4 finds almost no matches, so the
/// stored record stays above the native value limit and is chunked.
fn incompressible_body(len: u32) -> String {
    (0..u64::from(len))
        .map(|i| {
            let mut z = i.wrapping_add(1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[(z % 62) as usize]
                as char
        })
        .collect()
}

fn root_range(table: u32) -> (Vec<u8>, Vec<u8>) {
    let begin = [&[TABLE_ROOT][..], &table.to_be_bytes()].concat();
    let end = crate::range::prefix_upper_bound(&begin).unwrap();
    (begin, end)
}

/// Pages of `[begin, end)`: physical rows when `native`, hydrated rows
/// otherwise.
async fn scan_pairs<B: KvBackend>(
    backend: &B,
    begin: Vec<u8>,
    end: Vec<u8>,
    native: bool,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::new();
    let mut begin = begin;
    loop {
        let range = KvRange {
            entry_bounds: None,
            begin: begin.clone(),
            end: end.clone(),
            reverse: false,
            limit: NonZeroU32::new(500).unwrap(),
            max_bytes: NonZeroUsize::new(8 << 20).unwrap(),
        };
        let page = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        if native {
                            attempt.native_scan(range, ReadProtection::Snapshot).await
                        } else {
                            attempt.scan(range, ReadProtection::Snapshot).await
                        }
                    })
                },
            )
            .await
            .unwrap();
        let Some(last) = page.entries.last() else {
            break;
        };
        begin = [last.key.as_slice(), &[0]].concat();
        let more = page.more;
        rows.extend(
            page.entries
                .into_iter()
                .map(|entry| (entry.key, entry.value)),
        );
        if !more {
            break;
        }
    }
    rows
}

/// The history of every feed reference (`7f "s/r/"`) with its key.
async fn feed_histories<B: KvBackend>(
    backend: &B,
) -> Vec<(Vec<u8>, storage_types::TableHistoryId)> {
    let begin = crate::constants::SYSTEM_STREAM_RECORD_PREFIX.to_vec();
    let end = crate::range::prefix_upper_bound(&begin).unwrap();
    scan_pairs(backend, begin, end, false)
        .await
        .into_iter()
        .map(|(key, value)| {
            let reference = crate::stream_write::record::FeedReference::decode(&value).unwrap();
            (key, reference.history_id)
        })
        .collect()
}

/// Every record of the system stream, in feed order.
async fn read_records<B: KvBackend + 'static>(
    store: &KvStore<B>,
) -> Vec<storage_types::StreamRecord> {
    let scope = store.system_stream_scope().await.unwrap();
    let mut records = Vec::new();
    let mut token = None;
    for _ in 0..64 {
        let page = store
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), token, 100).unwrap(),
            )
            .await
            .unwrap();
        records.extend_from_slice(page.records());
        if page.terminal() {
            return records;
        }
        token = Some(page.issued_checkpoint().token().clone());
    }
    panic!("stream paging must reach the captured high");
}

/// Every logical key of the store.
async fn all_keys<B: KvBackend>(backend: &B) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let mut begin = Vec::new();
    loop {
        let range = KvRange {
            entry_bounds: None,
            begin: begin.clone(),
            end: vec![0xff],
            reverse: false,
            limit: NonZeroU32::new(500).unwrap(),
            max_bytes: NonZeroUsize::new(8 << 20).unwrap(),
        };
        let page = backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(
                        async move { attempt.native_scan(range, ReadProtection::Snapshot).await },
                    )
                },
            )
            .await
            .unwrap();
        let Some(last) = page.entries.last() else {
            break;
        };
        begin = [last.key.as_slice(), &[0]].concat();
        let more = page.more;
        keys.extend(page.entries.into_iter().map(|entry| entry.key));
        if !more {
            break;
        }
    }
    keys
}

fn is_global(key: &[u8]) -> bool {
    [
        &b"M"[..],
        b"S",
        b"d",
        b"f",
        b"aT",
        b"aL",
        b"\0system-retired/",
        b"\0backend-health",
        b"\x7fs/r/",
        b"\x7fs/l/",
        b"\x7fs/s",
        b"\x7fs/v",
        b"\x7fs/c",
        b"\x7fs/t/",
        b"\x7fs/i/",
        b"\x7fm/t/",
        b"\x7ft/",
        b"\x7fu/",
        b"\x7fq/",
        b"\x7fp/",
        b"\x7fm/",
        b"\x7fi/",
        b"\x7fj/",
    ]
    .iter()
    .any(|family| key.starts_with(family))
}

async fn create(store: &KvStore<impl KvBackend + 'static>, name: &str) -> u32 {
    let created = store
        .create_table(
            serde_json::from_value(json!({
                "TableName": name,
                "StreamSpecification": {
                    "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"
                },
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "g", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "by_g",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }]
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let target = storage_types::TableHistoryTarget::try_from(&created.table_description).unwrap();
    store
        .update_time_to_live(
            serde_json::from_value(json!({
                "TableName": name,
                "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    for _ in 0..64 {
        let step = store
            .advance_gsi_build(storage_provider::maintenance::GsiBuildRequest {
                destination: b"aux-storage:table-layout:gsi:v1"
                    .to_vec()
                    .try_into()
                    .unwrap(),
                job: [9; 16].into(),
                fence: std::num::NonZeroU64::MIN,
                page_limit: storage_backfill::GsiPageLimit::try_from(16usize).unwrap(),
                captured_at: storage_types::TimestampMillis::from_timestamp(1_800_000_000_000),
            })
            .await
            .unwrap();
        if matches!(step, storage_types::maintenance::StepResult::Complete) {
            break;
        }
    }
    let large = "x".repeat(150_000);
    for (sk, value) in [
        ("a", "small"),
        ("b", large.as_str()),
        ("c", "small"),
        ("d", "small"),
    ] {
        store
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": name,
                    "Item": {
                        "pk": {"S": "p"}, "sk": {"S": sk}, "g": {"S": format!("g-{sk}")},
                        "expires": {"N": "4102444800"}, "value": {"S": value}
                    }
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    store
        .update_item(
            serde_json::from_value(json!({
                "TableName": name, "Key": {"pk": {"S": "p"}, "sk": {"S": "a"}},
                "UpdateExpression": "SET g = :g", "ExpressionAttributeValues": {":g": {"S": "moved"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .delete_item(
            serde_json::from_value(json!({
                "TableName": name, "Key": {"pk": {"S": "p"}, "sk": {"S": "c"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .append_table_item(
            StreamAppendOperationId::fresh(),
            target.clone(),
            large.as_bytes(),
        )
        .await
        .unwrap();
    store
        .append_table_item(StreamAppendOperationId::fresh(), target.clone(), b"small")
        .await
        .unwrap();
    // No production path writes a receipt; store one as the reclaim tests do.
    let receipt_target = target.clone();
    storage_provider::DynamoPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let request = TableAppendRequest::new(
                    StreamAppendOperationId::fresh(),
                    receipt_target,
                    b"payload",
                );
                let receipt = StreamAppendReceipt::new(
                    &request,
                    stream_provider::StreamAppendProof::try_new(vec![1]).unwrap(),
                    StreamAppendAllocation::AttemptAllocated(storage_types::StreamItemId::from(
                        [1; 12],
                    )),
                );
                attempt.store_append_receipt(&receipt).await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    crate::catalog::table_id_from_history(target.history_id).unwrap()
}

/// The table incarnation behind a live table name.
async fn target_of(
    store: &KvStore<impl KvBackend + 'static>,
    name: &str,
) -> storage_types::TableHistoryTarget {
    let described = store
        .describe_table(serde_json::from_value(json!({"TableName": name})).unwrap())
        .await
        .unwrap();
    storage_types::TableHistoryTarget::try_from(&described.table).unwrap()
}

fn families_of(keys: &[Vec<u8>], table: u32) -> Vec<TableSubFamily> {
    let mut families = keys
        .iter()
        .filter_map(|key| table_data_family(key))
        .filter(|(id, _)| *id == table)
        .map(|(_, family)| family)
        .collect::<Vec<_>>();
    families.dedup();
    families
}

fn stream_families_of(keys: &[Vec<u8>], table: u32) -> Vec<TableStreamFamily> {
    let mut families = Vec::new();
    for (id, family) in keys.iter().filter_map(|key| table_stream_family(key)) {
        if id == table && !families.contains(&family) {
            families.push(family);
        }
    }
    families
}

async fn check_table_ranges<B: KvBackend + 'static>(backend: B, chunks: bool) -> KvStore<B> {
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let a = create(&store, "layout_a").await;
    let b = create(&store, "layout_b").await;
    // One queue with a message that needs value chunks on FoundationDB.
    {
        use queue_provider::{
            Queue, QueueOperations as _, SendMessageRequest, queue_message_for_send,
        };
        let url = store
            .create_queue(Queue {
                queue_name: "layout-queue".into(),
                queue_url: "http://localhost/queue/layout-queue".into(),
                attributes: Default::default(),
                created_at: storage_types::TimestampMillis::now(),
                ..Default::default()
            })
            .await
            .unwrap()
            .queue_url;
        let body = incompressible_body(150_000);
        store
            .send_message(queue_message_for_send(
                SendMessageRequest {
                    queue_url: url.clone(),
                    message_body: body.clone(),
                    delay_seconds: None,
                    message_attributes: None,
                    ..Default::default()
                },
                storage_types::TimestampMillis::now(),
            ))
            .await
            .unwrap();
        let received = store.receive_messages(&url, 1, None, None).await.unwrap();
        assert_eq!(received.messages.len(), 1);
        assert!(received.messages[0].body == body);
    }
    let ranges = [a, b].map(root_range);
    let keys = all_keys(store.backend.as_ref()).await;
    for key in &keys {
        let tables = ranges
            .iter()
            .filter(|(begin, end)| begin.as_slice() <= key.as_slice() && key < end)
            .count();
        assert!(
            tables == 1 || (tables == 0 && is_global(key)),
            "key outside every table root and named global family: {:?}",
            key.first()
        );
        assert!(
            key.first() != Some(&TABLE_ROOT) || tables == 1,
            "other table range"
        );
    }
    for moved in [
        &b"\0chunk/"[..],
        b"\0stream-append-receipt/",
        b"\x7fs/e/",
        b"\x7fs/h/",
        b"r",
    ] {
        assert!(
            !keys.iter().any(|key| key.starts_with(moved)),
            "key of a moved family remains: {moved:?}"
        );
    }
    assert!(
        !keys
            .iter()
            .any(|key| key.starts_with(b"\x7fu/q/") && key[7..].starts_with(b"history/")),
        "table append sequence remains global"
    );
    let queue_prefixes = keys
        .iter()
        .filter(|key| key.starts_with(b"\x7fq/"))
        .map(|key| String::from_utf8_lossy(&key[..key.len().min(6)]).into_owned())
        .collect::<std::collections::BTreeSet<_>>();
    eprintln!("queue key prefixes: {queue_prefixes:?}");
    assert_eq!(
        keys.iter().any(|key| key.starts_with(b"\x7fq/k/")),
        chunks,
        "queue message chunks live in the queue home"
    );
    for table in [a, b] {
        let families = families_of(&keys, table);
        for family in [
            TableSubFamily::Item,
            TableSubFamily::GsiEntry,
            TableSubFamily::TtlIndex,
            TableSubFamily::Catalog,
        ] {
            assert!(
                families.contains(&family),
                "{family:?} missing from table {table}"
            );
        }
        assert_eq!(families.contains(&TableSubFamily::Chunk), chunks);
        let streams = stream_families_of(&keys, table);
        for family in [
            TableStreamFamily::Entry,
            TableStreamFamily::AppendRow,
            TableStreamFamily::AppendReceipt,
            TableStreamFamily::AppendSequence,
            TableStreamFamily::TrimState,
        ] {
            assert!(
                streams.contains(&family),
                "{family:?} missing from table {table}"
            );
        }
        assert_eq!(streams.contains(&TableStreamFamily::Chunk), chunks);
    }
    store
        .delete_table(serde_json::from_value(json!({"TableName": "layout_a"})).unwrap())
        .await
        .unwrap();
    for _ in 0..100 {
        if store.reclaim_table_page().await.unwrap() == TableReclaimStep::Complete {
            break;
        }
    }
    let keys = all_keys(store.backend.as_ref()).await;
    let (begin, end) = &ranges[0];
    assert!(
        !keys.iter().any(|key| begin <= key && key < end),
        "reclaimed table root keeps keys"
    );
    let history_a = storage_types::TableHistoryId::from(a);
    assert!(
        !feed_histories(store.backend.as_ref())
            .await
            .iter()
            .any(|(_, history)| *history == history_a),
        "reclaimed table keeps feed records"
    );
    assert!(families_of(&keys, b).contains(&TableSubFamily::Item));
    assert!(stream_families_of(&keys, b).contains(&TableStreamFamily::Entry));
    store
}

/// Items and GSI queries of one table as JSON.
async fn read_view(store: &KvStore<impl KvBackend + 'static>, name: &str) -> serde_json::Value {
    let mut view = Vec::new();
    for sk in ["a", "b", "c", "d"] {
        let response = store
            .get_item(
                serde_json::from_value(json!({
                    "TableName": name, "Key": {"pk": {"S": "p"}, "sk": {"S": sk}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        view.push(serde_json::to_value(&response).unwrap());
    }
    for g in ["g-b", "g-c", "moved"] {
        let response = store
            .query(
                serde_json::from_value(json!({
                    "TableName": name, "IndexName": "by_g",
                    "KeyConditionExpression": "g = :g",
                    "ExpressionAttributeValues": {":g": {"S": g}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        view.push(serde_json::to_value(&response).unwrap());
    }
    serde_json::Value::Array(view)
}

fn records_of(records: &[storage_types::StreamRecord], name: &str) -> Vec<serde_json::Value> {
    records
        .iter()
        .filter(|record| record.source_table_name.as_ref().unwrap().as_ref() == name)
        .map(|record| serde_json::to_value(record).unwrap())
        .collect()
}

/// Restores table A from a raw copy of its root range and re-derives its global
/// records.
async fn restore_by_hand<B: KvBackend + 'static>(backend: B, clears_ranges: bool) -> KvStore<B> {
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let a = create(&store, "layout_a").await;
    create(&store, "layout_b").await;
    let history_a = storage_types::TableHistoryId::from(a);
    let target_a = target_of(&store, "layout_a").await;
    let (root_begin, root_end) = root_range(a);
    let view_before = read_view(&store, "layout_a").await;
    let records_before = records_of(&read_records(&store).await, "layout_a");
    let b_records_before = records_of(&read_records(&store).await, "layout_b").len();
    let copy = scan_pairs(
        store.backend.as_ref(),
        root_begin.clone(),
        root_end.clone(),
        true,
    )
    .await;
    let runnable_begin = [&b"f"[..], &a.to_be_bytes()].concat();
    let runnable_end = crate::range::prefix_upper_bound(&runnable_begin).unwrap();
    let runnable_before = scan_pairs(
        store.backend.as_ref(),
        runnable_begin.clone(),
        runnable_end.clone(),
        false,
    )
    .await;
    assert!(!records_before.is_empty() && !copy.is_empty());

    // Later writes that the restore must remove.
    for sk in ["e", "f"] {
        store
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": "layout_a",
                    "Item": {"pk": {"S": "p"}, "sk": {"S": sk}, "g": {"S": "later"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    store
        .update_item(
            serde_json::from_value(json!({
                "TableName": "layout_a", "Key": {"pk": {"S": "p"}, "sk": {"S": "c"}},
                "UpdateExpression": "SET g = :g", "ExpressionAttributeValues": {":g": {"S": "later"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .append_table_item(StreamAppendOperationId::fresh(), target_a.clone(), b"later")
        .await
        .unwrap();
    store
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": "layout_b",
                "Item": {"pk": {"S": "p"}, "sk": {"S": "z"}, "g": {"S": "b-later"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    // Clear the root range and the feed records of table A.
    if clears_ranges {
        let (begin, end) = (root_begin.clone(), root_end.clone());
        store
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(store.backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt.native_stage(vec![KvMutation::DeleteRange { begin, end }])
                    })
                },
            )
            .await
            .unwrap();
    } else {
        let live = scan_pairs(
            store.backend.as_ref(),
            root_begin.clone(),
            root_end.clone(),
            true,
        )
        .await;
        for page in live.chunks(500) {
            let changes = page
                .iter()
                .map(|(key, _)| KvMutation::Delete { key: key.clone() })
                .collect::<Vec<_>>();
            store
                .backend
                .with_attempt(
                    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                        .start(store.backend.attempt_deadlines()),
                    move |attempt| Box::pin(async move { attempt.native_stage(changes) }),
                )
                .await
                .unwrap();
        }
    }
    let feed_keys = feed_histories(store.backend.as_ref())
        .await
        .into_iter()
        .filter(|(_, history)| *history == history_a)
        .map(|(key, _)| KvMutation::Delete { key })
        .collect::<Vec<_>>();
    assert!(!feed_keys.is_empty());
    store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            move |attempt| Box::pin(async move { attempt.stage(feed_keys) }),
        )
        .await
        .unwrap();

    // Write the copy back unchanged.
    for page in copy.chunks(500) {
        let changes = page
            .iter()
            .map(|(key, value)| KvMutation::Put {
                key: key.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        store
            .backend
            .with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(store.backend.attempt_deadlines()),
                move |attempt| Box::pin(async move { attempt.native_stage(changes) }),
            )
            .await
            .unwrap();
    }

    // Derive the global records again, in one attempt.
    let entry_keys = copy
        .iter()
        .map(|(key, _)| key.clone())
        .filter(|key| table_stream_family(key) == Some((a, TableStreamFamily::Entry)))
        .collect::<Vec<_>>();
    assert!(!entry_keys.is_empty());
    let lineages = entry_keys
        .iter()
        .map(|key| {
            let mut parts = key[6..].split(|byte| *byte == 0);
            parts.next().unwrap();
            String::from_utf8(parts.next().unwrap().to_vec()).unwrap()
        })
        .collect::<Vec<_>>();
    let trim_key = table_stream_prefix(a, TableStreamFamily::TrimState).to_vec();
    let scope = storage_provider::StreamTrimScope::table(
        storage_provider::table_retention_scope_id(history_a),
        storage_types::TableName::try_from("layout_a").unwrap(),
    );
    let builds_begin = table_prefix(a, TableSubFamily::GsiBuild).to_vec();
    let builds_end = crate::range::prefix_upper_bound(&builds_begin).unwrap();
    let lookup_key = [&b"M"[..], b"layout_a"].concat();
    store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    for key in &entry_keys {
                        let value = attempt
                            .get(key, ReadProtection::Conflict)
                            .await?
                            .expect("copied entry is readable");
                        let created_at =
                            stream_provider::PositionedStreamEntry::decode_created_at(&value)?;
                        attempt.stage(vec![KvMutation::Put {
                            key: [crate::constants::SYSTEM_STREAM_RECORD_PREFIX, &key[6..]]
                                .concat(),
                            value: crate::stream_write::record::FeedReference::encode(
                                history_a, created_at, &value,
                            )?,
                        }])?;
                    }
                    if let Some(bytes) = attempt.get(&trim_key, ReadProtection::Conflict).await? {
                        let state = crate::retention::decode_trim_state(&bytes, &scope)?;
                        if let Some((key, value)) = crate::retention::derived_due_entry(&state)? {
                            attempt.stage(vec![KvMutation::Put { key, value }])?;
                        }
                    }
                    let builds = attempt
                        .scan(
                            KvRange {
                                entry_bounds: None,
                                begin: builds_begin,
                                end: builds_end,
                                reverse: false,
                                limit: NonZeroU32::new(500).unwrap(),
                                max_bytes: NonZeroUsize::new(8 << 20).unwrap(),
                            },
                            ReadProtection::Conflict,
                        )
                        .await?;
                    for entry in builds.entries {
                        let build: storage_backfill::GsiBuild<Vec<u8>> =
                            storage_types::storage_serde::from_bytes(&entry.value)?;
                        if build.state.is_runnable() {
                            let index_id = u16::from_be_bytes([entry.key[6], entry.key[7]]);
                            attempt.stage(vec![KvMutation::Put {
                                key: crate::gsi::gsi_runnable_key(a, index_id),
                                value: entry.value,
                            }])?;
                        }
                    }
                    let metadata = attempt
                        .get(&crate::catalog::metadata_key(a), ReadProtection::Conflict)
                        .await?
                        .expect("copied catalog record");
                    let lookup = attempt.get(&lookup_key, ReadProtection::Conflict).await?;
                    let allocator = attempt
                        .get(
                            crate::constants::TABLE_ID_ALLOCATOR_KEY,
                            ReadProtection::Conflict,
                        )
                        .await?;
                    attempt.stage(crate::catalog::restored_catalog_mutations(
                        &metadata,
                        lookup.as_deref(),
                        allocator.as_deref(),
                    )?)?;
                    attempt.native_record_catalog_change().await?;
                    Ok(())
                })
            },
        )
        .await
        .unwrap();
    let scope = store.system_stream_scope().await.unwrap();
    let catalog = store.system_stream_lineage_catalog(scope).await.unwrap();
    for lineage in &lineages {
        assert!(
            catalog
                .iter()
                .any(|known| known.lineage_id().as_str() == lineage),
            "lineage {lineage} of a restored entry is not in the catalog"
        );
    }

    // The copy is what table A returns, and both tables keep working.
    assert_eq!(read_view(&store, "layout_a").await, view_before);
    let records = read_records(&store).await;
    assert_eq!(records_of(&records, "layout_a"), records_before);
    assert_eq!(records_of(&records, "layout_b").len(), b_records_before + 1);
    assert_eq!(
        scan_pairs(store.backend.as_ref(), runnable_begin, runnable_end, false).await,
        runnable_before
    );
    store
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": "layout_a",
                "Item": {"pk": {"S": "p"}, "sk": {"S": "after"}, "g": {"S": "after"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .append_table_item(StreamAppendOperationId::fresh(), target_a, b"after")
        .await
        .unwrap();
    let records = read_records(&store).await;
    assert_eq!(
        records_of(&records, "layout_a").len(),
        records_before.len() + 1
    );
    store
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_tables_when_written_and_reclaimed_then_each_key_is_in_its_table_range() {
    let root = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        root.path().to_path_buf(),
        Default::default(),
        256,
    )
    .unwrap();
    check_table_ranges(backend, false).await;
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_tables_when_written_and_reclaimed_then_each_key_is_in_its_table_range() {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let prefix = storage_types::ambient::uuid_v4().to_string();
    let backend = crate::foundationdb::FoundationDbBackend::initialize(
        crate::foundationdb::FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = check_table_ranges(backend, true).await;
    store.backend.delete_namespace_keys().await.unwrap();
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_rocks_table_when_restored_by_hand_from_its_root_then_items_gsi_and_streams_return_the_copy()
 {
    let root = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        root.path().to_path_buf(),
        Default::default(),
        256,
    )
    .unwrap();
    restore_by_hand(backend, false).await;
}

#[cfg(feature = "rocksdb")]
async fn due_entries<B: KvBackend>(backend: &B) -> Vec<(Vec<u8>, Vec<u8>)> {
    let begin = vec![b'd'];
    let end = crate::range::prefix_upper_bound(&begin).unwrap();
    scan_pairs(backend, begin, end, false).await
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_two_scheduled_tables_when_one_due_entry_is_rederived_then_both_are_discovered_and_the_other_is_unchanged()
 {
    let root = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        root.path().to_path_buf(),
        Default::default(),
        256,
    )
    .unwrap();
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    let a = create(&store, "layout_a").await;
    let b = create(&store, "layout_b").await;
    for name in ["layout_a", "layout_b"] {
        store
            .update_table(
                serde_json::from_value(json!({
                    "TableName": name, "AuxStreamDurationHours": 48
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let due_before = storage_types::TimestampMillis::from_timestamp(
        *storage_types::TimestampMillis::now() + 49 * 60 * 60 * 1000,
    );
    let entries = due_entries(store.backend.as_ref()).await;
    let owned_by = |table: u32| {
        entries
            .iter()
            .filter(|(key, _)| key[9..13] == table.to_be_bytes())
            .cloned()
            .collect::<Vec<_>>()
    };
    let a_entries = owned_by(a);
    let b_before = owned_by(b);
    assert_eq!(a_entries.len(), 1, "table A has one due entry");
    assert_eq!(b_before.len(), 1, "table B has one due entry");
    let a_key = a_entries[0].0.clone();

    let history_a = storage_types::TableHistoryId::from(a);
    let scope = storage_provider::StreamTrimScope::table(
        storage_provider::table_retention_scope_id(history_a),
        storage_types::TableName::try_from("layout_a").unwrap(),
    );
    let trim_key = table_stream_prefix(a, TableStreamFamily::TrimState).to_vec();
    let expected = store
        .backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(store.backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![KvMutation::Delete { key: a_key }])?;
                    let bytes = attempt
                        .get(&trim_key, ReadProtection::Conflict)
                        .await?
                        .expect("table A has a trim state");
                    let state = crate::retention::decode_trim_state(&bytes, &scope)?;
                    let (key, value) = crate::retention::derived_due_entry(&state)?
                        .expect("table A schedules a trim");
                    attempt.stage(vec![KvMutation::Put { key, value }])?;
                    Ok(state.scheduled_marker().expect("table A schedules a trim"))
                })
            },
        )
        .await
        .unwrap();

    let markers = store
        .list_due_stream_trim_markers(due_before, 100)
        .await
        .unwrap();
    let marker_of = |name: &str| {
        markers
            .iter()
            .filter(|marker| marker.scope.table_name.as_ref() == name)
            .collect::<Vec<_>>()
    };
    let [found_a] = marker_of("layout_a")[..] else {
        panic!("table A must be discovered once after its entry is re-derived");
    };
    assert!(found_a.due_at == expected.due_at);
    assert!(found_a.scope == expected.scope);
    assert_eq!(found_a.policy_version, expected.policy_version);
    assert_eq!(marker_of("layout_b").len(), 1, "table B is discovered");
    let after = due_entries(store.backend.as_ref()).await;
    assert_eq!(
        after
            .iter()
            .filter(|(key, _)| key[9..13] == b.to_be_bytes())
            .cloned()
            .collect::<Vec<_>>(),
        b_before,
        "table B's due entry is unchanged"
    );
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires owned FoundationDB: just test-fdb-owned"]
async fn given_fdb_table_when_restored_by_hand_from_its_root_then_items_gsi_and_streams_return_the_copy()
 {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let prefix = storage_types::ambient::uuid_v4().to_string();
    let backend = crate::foundationdb::FoundationDbBackend::initialize(
        crate::foundationdb::FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = restore_by_hand(backend, true).await;
    store.backend.delete_namespace_keys().await.unwrap();
}
