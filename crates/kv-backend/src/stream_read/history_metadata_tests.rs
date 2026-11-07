use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use storage_provider::{DynamoOperations, DynamoPort, maintenance::ItemHistoryMetadataAttempt};
use storage_types::{
    AttemptMode, AttributeValue, ItemHistoryAddress, ItemHistoryChange, ItemHistoryReference,
    ItemKey, ItemStreamVersion, KeyAttributes, StorageError, StreamName, TableHistoryTarget,
    TableName,
};
use stream_provider::{HistoryRowDelete, HistoryRowDeleteAttempt as _, ItemHistoryReadAttempt};

use crate::{KvBackend, KvFaultCall, KvFaultOperation, KvFaultProbe, KvStore, KvTopology};

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_baseline_history_when_replaced_or_deleted_then_exact_anchors_follow_table_authority()
{
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(directory.path().to_owned()).unwrap();
    let store = KvStore::initialize_format(backend).await.unwrap();
    storage_provider::baseline_test_support::qualify_baseline_history(store).await;
}

#[derive(Default)]
struct MetadataReadCalls {
    gets: AtomicUsize,
    scans: AtomicUsize,
}

impl KvFaultProbe for MetadataReadCalls {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError> {
        match call.operation {
            KvFaultOperation::GetBatch => {
                self.gets.fetch_add(1, Ordering::Relaxed);
            }
            KvFaultOperation::ScanBatch => {
                self.scans.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        None
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_local_histories_when_metadata_is_read_then_native_heads_supply_the_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let calls = Arc::new(MetadataReadCalls::default());
    let backend = crate::rocksdb::RocksDbBackend::initialize(directory.path().to_owned())
        .unwrap()
        .with_fault_probe(calls.clone());
    qualify_metadata_reads(backend, calls).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires an owned FoundationDB fixture (FDB_CLUSTER_FILE)"]
async fn given_chunked_histories_when_metadata_is_read_then_first_chunks_are_batched_and_committed()
{
    let cluster = std::env::var("FDB_CLUSTER_FILE").expect("owned FoundationDB fixture");
    let prefix = format!("metadata-prefix/{}", uuid::Uuid::new_v4());
    let calls = Arc::new(MetadataReadCalls::default());
    let backend = crate::foundationdb::FoundationDbBackend::initialize(
        crate::foundationdb::FoundationDbOptions {
            cluster_file: Some(&cluster),
            subspace_prefix: Some(prefix.as_bytes()),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_fault_probe(calls.clone());
    qualify_metadata_reads(backend, calls).await;
}

async fn qualify_metadata_reads<B: KvBackend + 'static>(backend: B, calls: Arc<MetadataReadCalls>) {
    let topology = backend.topology();
    let store = KvStore::initialize_format(backend).await.unwrap();
    let mut request: storage_types::CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "metadata", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap();
    let schema = request.key_schema.clone();
    request.max_indexers = storage_types::MaxIndexers::try_new(1).unwrap();
    let created = store.create_table(request).await.unwrap();
    let target = TableHistoryTarget::try_from(
        created
            .table_description
            .latest_stream_arn
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    let mut histories = Vec::new();
    for (key, bytes, writes) in [("large", 200_000, 3), ("small", 10, 2)] {
        let attributes = KeyAttributes::from([("pk".into(), AttributeValue::S(key.into()))]);
        let item_key =
            ItemKey::from_key_schema(TableName::new("metadata"), &schema, &attributes).unwrap();
        histories.push(ItemHistoryAddress {
            history_id: target.history_id,
            stream_name: StreamName::table_item_stream(&TableName::new("metadata"), &item_key)
                .unwrap(),
        });
        for value in &(["a", "b", "c"][..writes]) {
            let body = if key == "large" {
                format!("{value}{}", "\u{1}".repeat(bytes))
            } else {
                value.repeat(bytes)
            };
            let mut put: storage_types::PutItemRequest =
                serde_json::from_value(serde_json::json!({
                    "TableName": "metadata", "Item": {"pk": {"S": key}, "body": {"S": body}}
                }))
                .unwrap();
            put.indexers = Some(vec!["pk".to_owned()]);
            store.put_item(put).await.unwrap();
        }
    }
    let pointer = store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let states = attempt
                    .load_item_history_table_states(&[
                        histories[0].history_id,
                        u32::MAX.into(),
                        histories[0].history_id,
                    ])
                    .await?;
                let [
                    Some(storage_types::TableHistoryState::Active(first)),
                    None,
                    Some(storage_types::TableHistoryState::Active(second)),
                ] = states.as_slice()
                else {
                    panic!("history authority order")
                };
                assert_eq!(first, second);
                assert_eq!(first.source_table_name.as_str(), "metadata");
                assert_eq!(first.key_schema.len(), 1);
                assert_eq!(first.key_schema[0].attribute_name, "pk");
                assert_eq!(first.key_schema[0].key_type, storage_types::KeyType::Hash);
                calls.gets.store(0, Ordering::Relaxed);
                calls.scans.store(0, Ordering::Relaxed);
                let pages = attempt.load_item_history_metadata_pages(&histories).await?;
                assert_eq!(pages.len(), 2);
                for ((page, history), reached_tail) in
                    pages.iter().zip(&histories).zip([false, true])
                {
                    assert_eq!(page.reached_tail, reached_tail);
                    assert_eq!(
                        page.entries
                            .iter()
                            .map(|entry| entry.version().get())
                            .collect::<Vec<_>>(),
                        vec![1, 2]
                    );
                    assert!(page.entries.iter().all(|entry| entry.address() == history));
                }
                assert_eq!(
                    calls.scans.load(Ordering::Relaxed),
                    match topology {
                        KvTopology::Local => 2,
                        KvTopology::Distributed => 1,
                    },
                    "inline heads use bounded groups; distributed heads batch both histories"
                );
                let expected_gets = match topology {
                    KvTopology::Local => 0,
                    KvTopology::Distributed => 2,
                };
                assert_eq!(
                    calls.gets.load(Ordering::Relaxed),
                    expected_gets,
                    "distributed values use one head batch and one first-chunk batch; local heads \
                     are already inline"
                );
                if topology == KvTopology::Distributed {
                    for entry in pages.iter().flat_map(|page| &page.entries) {
                        let storage_types::ItemHistoryPublication::Event { table_position, .. } =
                            entry.publication()
                        else {
                            panic!("published event")
                        };
                        let stamp = table_position.offset().versionstamp().unwrap();
                        assert_ne!(
                            &stamp[..10],
                            &[u8::MAX; 10],
                            "metadata must contain the committed native stamp"
                        );
                    }
                }
                let references = [(0, 3), (1, 1), (0, 99)]
                    .into_iter()
                    .map(|(index, version)| {
                        ItemHistoryReference::try_new(
                            histories[index].clone(),
                            ItemStreamVersion::new(version),
                        )
                    })
                    .collect::<storage_types::StorageResult<Vec<_>>>()?;
                let read = stream_provider::ItemHistoryReadBatch {
                    references: &references,
                    maximum_bytes: std::num::NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
                };
                let headers = attempt.load_item_history_headers(read).await?;
                let entries = attempt.load_item_history_entries(read).await?;
                assert_eq!(headers.len(), references.len());
                assert_eq!(entries.len(), references.len());
                for ((header, entry), reference) in headers.iter().zip(&entries).zip(&references) {
                    if reference.version().get() == 99 {
                        assert!(header.is_none() && entry.is_none());
                        continue;
                    }
                    let header = header.as_ref().unwrap();
                    let entry = entry.as_ref().unwrap();
                    assert_eq!(header.metadata().reference(), reference);
                    assert_eq!(entry.metadata(), header.metadata());
                    assert_eq!(entry.encode()?.len(), header.record_bytes());
                    let ItemHistoryChange::Image { item, indexers } = entry.change() else {
                        panic!("snapshot")
                    };
                    assert_eq!(indexers.names(), ["pk".to_owned()]);
                    let expected = if reference.version().get() == 3 {
                        format!("c{}", "\u{1}".repeat(200_000))
                    } else {
                        "a".repeat(10)
                    };
                    assert_eq!(
                        item.attribute_value("body")?,
                        Some(AttributeValue::S(expected))
                    );
                }
                let current = headers[0].as_ref().unwrap();
                // The wide b/c snapshots differ by one equal-width character.
                let pointer = storage_types::ItemHistoryPointer::try_new(
                    current.metadata().clone(),
                    current.record_bytes(),
                    Some(current.payload_bytes()),
                )?;
                let resolved = stream_provider::resolve_item_history(
                    attempt,
                    &[pointer.clone(), pointer.clone()],
                    stream_provider::ItemHistoryImages::WithPredecessor,
                    read.maximum_bytes,
                )
                .await?;
                let first = resolved[0].as_ref().unwrap();
                let second = resolved[1].as_ref().unwrap();
                assert!(std::sync::Arc::ptr_eq(&first.current, &second.current));
                assert!(std::sync::Arc::ptr_eq(&first.table, &second.table));
                assert_eq!(first.table.source_table_name.as_str(), "metadata");
                let storage_types::ItemHistoryPublication::Event {
                    source_scope,
                    table_position,
                    ..
                } = pointer.metadata().publication()
                else {
                    panic!("published pointer")
                };
                let lane = storage_types::TableHistoryLane::try_from((
                    pointer.metadata().address().history_id,
                    source_scope,
                    table_position,
                ))?;
                let through =
                    storage_types::TableStreamSequence::try_from(table_position.offset())?;
                let read_index = stream_provider::TableHistoryIndexRead {
                    lane,
                    exclusive_start: None,
                    through,
                    direction: stream_provider::ReadDirection::Forward,
                    limit: std::num::NonZeroU32::MIN,
                };
                let after = stream_provider::TableHistoryIndexRead {
                    exclusive_start: Some(through),
                    ..read_index.clone()
                };
                let peer = stream_provider::TableHistoryIndexRead {
                    lane: storage_types::TableHistoryLane::try_from((
                        u32::MAX.into(),
                        source_scope,
                        table_position,
                    ))?,
                    ..read_index.clone()
                };
                let backward = stream_provider::TableHistoryIndexRead {
                    direction: stream_provider::ReadDirection::Backward,
                    ..read_index.clone()
                };
                let backward_after = stream_provider::TableHistoryIndexRead {
                    exclusive_start: Some(through),
                    ..backward.clone()
                };
                let mut reverse_pages = stream_provider::read_table_history_indexes(
                    attempt,
                    &[backward, backward_after],
                )
                .await?;
                let preceding = reverse_pages.pop().unwrap();
                assert_eq!(
                    preceding.pointers[0].metadata().version().get(),
                    2,
                    "backward continuation excludes current revision"
                );
                assert!(preceding.more);
                assert!(reverse_pages.pop().unwrap().pointers == [pointer.clone()]);
                let mut index_pages = stream_provider::read_table_history_indexes(
                    attempt,
                    &[read_index, after, peer],
                )
                .await?;
                assert!(
                    index_pages.pop().unwrap().pointers.is_empty(),
                    "another ID cannot share this index"
                );
                let exhausted = index_pages.pop().unwrap();
                assert!(exhausted.pointers.is_empty() && !exhausted.more);
                let indexed = index_pages.pop().unwrap();
                assert_eq!(indexed.pointers.len(), 1);
                assert_eq!(indexed.pointers[0].metadata().version().get(), 1);
                assert!(
                    indexed
                        .last_position
                        .as_ref()
                        .unwrap()
                        .offset()
                        .compare_order(table_position.offset())
                        .unwrap()
                        .is_lt()
                );
                assert!(indexed.more);
                let snapshot = storage_types::StreamRecord::try_from(first)?;
                assert_eq!(
                    snapshot.source_table_name.as_ref().unwrap().as_ref(),
                    "metadata"
                );
                assert_eq!(snapshot.keys.len(), 1);
                assert_eq!(
                    snapshot.keys.get("pk"),
                    Some(&AttributeValue::S("large".into()))
                );
                assert!(snapshot.system_position.is_some());
                assert!(snapshot.item.is_some());

                assert!(std::sync::Arc::ptr_eq(
                    first.predecessor.as_ref().unwrap(),
                    second.predecessor.as_ref().unwrap()
                ));
                assert_eq!(first.current.metadata().version().get(), 3);
                assert_eq!(
                    first
                        .predecessor
                        .as_ref()
                        .unwrap()
                        .metadata()
                        .version()
                        .get(),
                    2
                );
                let metadata = attempt
                    .load_item_history_metadata_pages(std::slice::from_ref(&histories[0]))
                    .await?
                    .pop()
                    .unwrap();
                let mut deletes = Vec::new();
                for entry in metadata.entries {
                    let storage_types::ItemHistoryPublication::Event {
                        source_scope,
                        table_position,
                        ..
                    } = entry.publication()
                    else {
                        panic!("published head")
                    };
                    deletes.push(HistoryRowDelete::Table {
                        lane: storage_types::TableHistoryLane::try_from((
                            entry.address().history_id,
                            source_scope,
                            table_position,
                        ))?,
                        sequence: storage_types::TableStreamSequence::try_from(
                            table_position.offset(),
                        )?,
                    });
                    deletes.push(HistoryRowDelete::Item(entry.reference().clone()));
                }
                attempt.delete_history_rows(&deletes).await?;
                let remaining = attempt.load_item_history_metadata_pages(&histories).await?;
                assert_eq!(
                    remaining[0]
                        .entries
                        .iter()
                        .map(|entry| entry.version().get())
                        .collect::<Vec<_>>(),
                    [3]
                );
                assert!(remaining[0].reached_tail);
                assert_eq!(
                    remaining[1]
                        .entries
                        .iter()
                        .map(|entry| entry.version().get())
                        .collect::<Vec<_>>(),
                    [1, 2]
                );
                Ok(pointer)
            })
        })
        .await
        .unwrap();
    store
        .delete_table(storage_types::DeleteTableRequest {
            table_name: TableName::new("metadata"),
        })
        .await
        .unwrap();
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let states = attempt
                    .load_item_history_table_states(&[target.history_id, u32::MAX.into()])
                    .await?;
                assert_eq!(
                    states,
                    vec![Some(storage_types::TableHistoryState::Deleting), None]
                );
                let resolved = stream_provider::resolve_item_history(
                    attempt,
                    &[pointer],
                    stream_provider::ItemHistoryImages::WithPredecessor,
                    std::num::NonZeroUsize::MIN,
                )
                .await?;
                assert!(
                    resolved[0].is_none(),
                    "deletion authority must suppress payload reads before admission"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
