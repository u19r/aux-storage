use storage_provider::{DynamoOperations, DynamoPort, maintenance::ItemHistoryMetadataAttempt};
use storage_types::{
    AttemptMode, AttributeValue, ItemHistoryAddress, ItemHistoryChange, ItemHistoryKind,
    ItemHistoryReference, ItemKey, ItemStreamVersion, KeyAttributes, StreamName,
    TableHistoryTarget, TableName,
};
use stream_provider::{HistoryRowDelete, HistoryRowDeleteAttempt as _, ItemHistoryReadAttempt};

use crate::{ReadProtection, SqlAttempt, SqlStore, SqlValue, sqlite::SqliteBackend};

#[tokio::test]
async fn given_baseline_history_when_replaced_or_deleted_then_exact_anchors_follow_table_authority()
{
    let store = SqlStore::initialize_schema(SqliteBackend::initialize(":memory:").await.unwrap())
        .await
        .unwrap();
    storage_provider::baseline_test_support::qualify_baseline_history(store).await;
}

#[tokio::test]
async fn given_large_history_when_metadata_is_scanned_then_only_bounded_prefixes_and_exact_tail_are_returned()
 {
    let store = SqlStore::initialize_schema(SqliteBackend::initialize(":memory:").await.unwrap())
        .await
        .unwrap();
    let mut request: storage_types::CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "metadata", "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap();
    let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
    let item_key =
        ItemKey::from_key_schema(request.table_name.clone(), &request.key_schema, &key).unwrap();
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
    let history = ItemHistoryAddress {
        history_id: target.history_id,
        stream_name: StreamName::table_item_stream(&TableName::new("metadata"), &item_key).unwrap(),
    };
    for value in ["a", "b", "c"] {
        let body = if value == "c" {
            "\u{1}".repeat(200_000)
        } else {
            value.repeat(100_000)
        };
        let mut put: storage_types::PutItemRequest = serde_json::from_value(serde_json::json!({
            "TableName": "metadata", "Item": {"pk": {"S": "key"}, "body": {"S": body}}
        }))
        .unwrap();
        put.indexers = Some(vec!["pk".to_owned()]);
        store.put_item(put).await.unwrap();
    }
    store
        .delete_item(
            serde_json::from_value(serde_json::json!({"TableName": "metadata", "Key": key}))
                .unwrap(),
        )
        .await
        .unwrap();
    let first = store
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            TableName::new("metadata"),
            None,
            Some(2),
        ))
        .await
        .unwrap();
    assert_eq!(first.records().len(), 2);
    let checkpoint = first.last_evaluated_key().unwrap().to_owned();
    let consumed_sequences = first
        .records()
        .iter()
        .map(|record| {
            record
                .sequence_number
                .parse::<storage_types::TableStreamSequence>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let (pointer, missing_predecessor) = store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let states = attempt.load_item_history_table_states(&[history.history_id, u32::MAX.into(), history.history_id]).await?;
                let [Some(storage_types::TableHistoryState::Active(first)), None, Some(storage_types::TableHistoryState::Active(second))] = states.as_slice() else { panic!("history authority order") };
                assert_eq!(first, second);
                assert_eq!(first.source_table_name.as_str(), "metadata");
                assert_eq!(first.key_schema.len(), 1);
                assert_eq!(first.key_schema[0].attribute_name, "pk");
                assert_eq!(first.key_schema[0].key_type, storage_types::KeyType::Hash);
                let statement =
                    super::sql_statements::read_history_metadata(attempt.engine, &history)?;
                let raw = attempt
                    .attempt
                    .query(
                        statement,
                        ReadProtection::Conflict,
                        crate::constants::STREAM_READ_BYTES,
                    )
                    .await?;
                assert_eq!(raw.rows.len(), 3, "one native lookahead");
                for row in raw.rows {
                    let [_, length, prefix] = row.into_values().try_into().unwrap();
                    let (SqlValue::Integer(length), SqlValue::Blob(prefix)) = (length, prefix)
                    else {
                        panic!("metadata columns")
                    };
                    assert_eq!(
                        prefix.len(),
                        stream_provider::StreamRowValue::history_metadata_prefix_bytes(&history)?
                            .get()
                    );
                    assert!(
                        prefix.len() < usize::try_from(length).unwrap(),
                        "native SQL must not return the full image"
                    );
                }
                let mut pages = attempt
                    .load_item_history_metadata_pages(std::slice::from_ref(&history))
                    .await?;
                let page = pages.pop().unwrap();
                assert!(!page.reached_tail);
                assert_eq!(
                    page.entries
                        .iter()
                        .map(|entry| entry.version().get())
                        .collect::<Vec<_>>(),
                    vec![1, 2]
                );
                assert_eq!(page.entries[0].previous_version(), None);
                assert_eq!(
                    page.entries[1].previous_version(),
                    Some(ItemStreamVersion::new(1))
                );
                let references = [3, 1, 4, 99].into_iter().map(|version| {
                    ItemHistoryReference::try_new(history.clone(), ItemStreamVersion::new(version))
                }).collect::<storage_types::StorageResult<Vec<_>>>()?;
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
                    if reference.version().get() == 4 {
                        assert!(matches!(entry.change(), ItemHistoryChange::Deleted { key: deleted } if deleted == &key));
                    } else {
                        let ItemHistoryChange::Image { item, indexers } = entry.change() else { panic!("snapshot") };
                        assert_eq!(indexers.names(), ["pk".to_owned()]);
                        let expected = if reference.version().get() == 1 { "a".repeat(100_000) } else { "\u{1}".repeat(200_000) };
                        assert_eq!(item.to_attribute_map()?.get("body"), Some(&AttributeValue::S(expected)));
                    }
                }
                let current = headers[2].as_ref().unwrap();
                let predecessor = headers[0].as_ref().unwrap();
                let pointer = storage_types::ItemHistoryPointer::try_new(current.metadata().clone(), current.record_bytes(), Some(predecessor.payload_bytes()))?;
                let missing_predecessor = storage_types::ItemHistoryPointer::try_new(predecessor.metadata().clone(), predecessor.record_bytes(), Some(headers[1].as_ref().unwrap().payload_bytes()))?;
                let resolved = stream_provider::resolve_item_history(attempt, &[pointer.clone(), pointer.clone()], stream_provider::ItemHistoryImages::WithPredecessor, read.maximum_bytes).await?;
                let first = resolved[0].as_ref().unwrap();
                let second = resolved[1].as_ref().unwrap();
                assert!(std::sync::Arc::ptr_eq(&first.current, &second.current));
                assert!(std::sync::Arc::ptr_eq(&first.table, &second.table));
                assert_eq!(first.table.source_table_name.as_str(), "metadata");
                let storage_types::ItemHistoryPublication::Event { source_scope, table_position, .. } = pointer.metadata().publication() else { panic!("published pointer") };
                let lane = storage_types::TableHistoryLane::try_from((pointer.metadata().address().history_id, source_scope, table_position))?;
                let through = storage_types::TableStreamSequence::try_from(table_position.offset())?;
                let read_index = stream_provider::TableHistoryIndexRead { lane: lane.clone(), exclusive_start: None, through, direction: stream_provider::ReadDirection::Forward, limit: std::num::NonZeroU32::MIN };
                let after = stream_provider::TableHistoryIndexRead { exclusive_start: Some(through), ..read_index.clone() };
                let peer = stream_provider::TableHistoryIndexRead {
                    lane: storage_types::TableHistoryLane::try_from((u32::MAX.into(), source_scope, table_position))?, ..read_index.clone()
                };
                let backward = stream_provider::TableHistoryIndexRead { direction: stream_provider::ReadDirection::Backward, ..read_index.clone() };
                let backward_after = stream_provider::TableHistoryIndexRead { exclusive_start: Some(through), ..backward.clone() };
                let mut reverse_pages = stream_provider::read_table_history_indexes(attempt, &[backward, backward_after]).await?;
                let preceding = reverse_pages.pop().unwrap();
                assert_eq!(preceding.pointers[0].metadata().version().get(), 3, "backward continuation excludes the delete revision");
                assert!(preceding.more);
                assert!(reverse_pages.pop().unwrap().pointers == [pointer.clone()]);
                let mut index_pages = stream_provider::read_table_history_indexes(attempt, &[read_index, after, peer]).await?;
                assert!(index_pages.pop().unwrap().pointers.is_empty(), "another ID cannot share this index");
                let exhausted = index_pages.pop().unwrap();
                assert!(exhausted.pointers.is_empty() && !exhausted.more);
                let indexed = index_pages.pop().unwrap();
                assert_eq!(indexed.pointers.len(), 1);
                assert_eq!(indexed.pointers[0].metadata().version().get(), 1);
                assert!(indexed.last_position.as_ref().unwrap().offset().compare_order(table_position.offset()).unwrap().is_lt());
                assert!(indexed.more);
                let snapshot = storage_types::StreamRecord::try_from(first)?;
                assert_eq!(snapshot.source_table_name.as_ref().unwrap().as_ref(), "metadata");
                assert_eq!(snapshot.keys.len(), 1);
                assert_eq!(snapshot.keys.get("pk"), Some(&AttributeValue::S("key".into())));
                assert!(snapshot.system_position.is_some());
                assert!(snapshot.item.is_none());

                assert!(std::sync::Arc::ptr_eq(first.predecessor.as_ref().unwrap(), second.predecessor.as_ref().unwrap()));
                assert_eq!(first.current.metadata().kind(), ItemHistoryKind::Deleted);
                assert_eq!(first.predecessor.as_ref().unwrap().metadata().version().get(), 3);
                let mut deletes = [1, 2].into_iter().map(|version| ItemHistoryReference::try_new(history.clone(), ItemStreamVersion::new(version)).map(HistoryRowDelete::Item)).collect::<storage_types::StorageResult<Vec<_>>>()?;
                deletes.extend(consumed_sequences.into_iter().map(|sequence| HistoryRowDelete::Table { lane: lane.clone(), sequence }));
                attempt.delete_history_rows(&deletes).await?;
                let mut pages = attempt.load_item_history_metadata_pages(&[history]).await?;
                let page = pages.pop().unwrap();
                assert!(page.reached_tail);
                assert_eq!(
                    page.entries
                        .iter()
                        .map(|entry| entry.version().get())
                        .collect::<Vec<_>>(),
                    vec![3, 4]
                );
                assert_eq!(page.entries[1].kind(), ItemHistoryKind::Deleted);
                assert_eq!(
                    page.entries[1].previous_version(),
                    Some(ItemStreamVersion::new(3))
                );
                Ok((pointer, missing_predecessor))
            })
        })
        .await
        .unwrap();
    let resumed = store
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            TableName::new("metadata"),
            Some(checkpoint),
            Some(2),
        ))
        .await
        .unwrap();
    assert_eq!(resumed.records().len(), 2);
    assert_eq!(
        resumed.records()[0].item.as_ref().unwrap().get("body"),
        Some(&AttributeValue::S("\u{1}".repeat(200_000)))
    );
    assert!(resumed.records()[1].item.is_none());
    for record in resumed.records() {
        assert_eq!(
            record.keys.get("pk"),
            Some(&AttributeValue::S("key".into()))
        );
    }
    let snapshot = missing_predecessor.clone();
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let resolved = stream_provider::resolve_item_history(
                    attempt,
                    &[snapshot],
                    stream_provider::ItemHistoryImages::Snapshot,
                    std::num::NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
                )
                .await?;
                let record = resolved[0].as_ref().unwrap();
                assert_eq!(record.current.metadata().version().get(), 3);
                assert!(record.predecessor.is_none());
                Ok(())
            })
        })
        .await
        .unwrap();
    let error = store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                stream_provider::resolve_item_history(
                    attempt,
                    &[missing_predecessor],
                    stream_provider::ItemHistoryImages::WithPredecessor,
                    std::num::NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
                )
                .await
                .map(|_| ())
            })
        })
        .await
        .expect_err("required exact predecessor is missing");
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::HistoryUnavailable
    ));
    let oversized = pointer.clone();
    let error = store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                stream_provider::resolve_item_history(
                    attempt,
                    &[oversized],
                    stream_provider::ItemHistoryImages::WithPredecessor,
                    std::num::NonZeroUsize::MIN,
                )
                .await
                .map(|_| ())
            })
        })
        .await
        .expect_err("body budget must be admitted before reads");
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::InfrastructureResult(
            storage_types::InfrastructureResultKind::NativeCapacityRejected
        )
    ));
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
