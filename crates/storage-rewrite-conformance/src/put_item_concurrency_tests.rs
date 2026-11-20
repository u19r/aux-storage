//! Concurrent PutItem commits through the public replacement operation.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    sync::Arc,
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, GetItemRequest, PutItemRequest};
use tokio::sync::Barrier;

use super::test_support::{TestStore, unique_table_name};

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires an owned FoundationDB provider"]
async fn given_observed_absence_when_another_writer_puts_and_deletes_then_retry_preserves_exact_history()
 {
    use std::time::Duration;

    use super::{
        dynamodb_stream_history_tests::{
            create_stream_table, describe_shards, iterator, put_value,
        },
        native_census_test_support::{CensusWave, NativeCensus},
    };

    assert_eq!(TestStore::selected_backend(), "foundationdb");
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = AssertUnwindSafe(async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        // Warm request metadata before holding the item commit, so a separate
        // metadata-only attempt cannot consume the one-time hold.
        put_value(&store, &table, "warm", "seed").await;
        census.reset();
        let held = census.hold(|wave| matches!(wave, CensusWave::Kv(wave) if wave.kind == kv_backend::KvWaveKind::Commit));
        let operations = Arc::clone(&store.operations);
        let request = PutItemRequest::try_from(json!({"TableName": table, "Item": {
            "pk": {"S": "absent"}, "value": {"S": "retried"}
        }})).unwrap();
        let pending = tokio::spawn(async move { operations.put_item(request).await });
        held.reached().await;
        let interference = tokio::time::timeout(Duration::from_secs(10), async {
            put_value(&store, &table, "absent", "temporary").await;
            store.operations.delete_item(storage_types::DeleteItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "absent"}}
            })).unwrap()).await.unwrap();
        }).await;
        held.release();
        let completed = tokio::time::timeout(Duration::from_secs(20), pending).await;
        interference.expect("the other writer can commit its put and delete while the first commit waits");
        completed.expect("the held write retries").unwrap().unwrap();
        let counts = census.snapshot();
        // Native commit conflicts consume an attempt without emitting the
        // explicit-abort event counted as a rollback.
        assert!(counts.begins > counts.commits + counts.rollbacks,
            "returning to absence must require a fresh attempt: {counts}");
        let mut changes = Vec::new();
        for shard in describe_shards(&store, &arn).await {
            let mut token = iterator(&store, &arn, &shard.shard_id, "TRIM_HORIZON", None).await;
            loop {
                let page = store.operations.get_records(storage_types::GetRecordsRequest { shard_iterator: token, limit: Some(2) }).await.unwrap();
                let empty = page.records.is_empty();
                changes.extend(page.records.into_iter().filter(|record|
                    record.dynamodb.keys.get("pk") == Some(&storage_types::AttributeValue::S("absent".to_owned()))));
                let Some(next) = page.next_shard_iterator else { break; };
                if empty { break; }
                token = next;
            }
        }
        assert_eq!(changes.len(), 3, "the aborted attempt publishes nothing");
        assert_eq!(changes.iter().map(|record| record.event_name.as_str()).collect::<Vec<_>>(), ["INSERT", "REMOVE", "INSERT"]);
        let mut previous = None;
        for record in &changes {
            assert_eq!(record.dynamodb.old_image, previous, "old image follows the actual committed state");
            previous = record.dynamodb.new_image.clone();
        }
        assert_eq!(previous.unwrap().get("value"), Some(&storage_types::AttributeValue::S("retried".to_owned())));
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Business rule from legacy KV write tests: same-key writers all commit, with
// no lost CDC effects.
#[tokio::test]
async fn given_eight_concurrent_puts_to_one_key_when_all_commit_then_eight_changes_are_visible() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let table = unique_table_name();
        let create = CreateTableRequest::try_from(json!({
            "TableName": table.as_ref(),
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }))
        .expect("valid stream-enabled table");
        store
            .operations
            .create_table(create)
            .await
            .expect("create table");

        let barrier = Arc::new(Barrier::new(8));
        let mut writers = Vec::new();
        for writer in 0..8 {
            let operations = Arc::clone(&store.operations);
            let barrier = Arc::clone(&barrier);
            let table = table.clone();
            writers.push(tokio::spawn(async move {
                let request = PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "hot"}, "writer": {"N": writer.to_string()}}
                }))
                .expect("valid concurrent PutItem");
                barrier.wait().await;
                observe(operations.put_item(request)).await
            }));
        }
        for writer in writers {
            let observed = writer.await.expect("writer task completes");
            observed.assert_no_warning_or_error();
            assert_eq!(
                serde_json::to_value(observed.result.expect("concurrent PutItem commits"))
                    .expect("serialize PutItem response"),
                json!({}),
            );
        }

        let read = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(),
            "Key": {"pk": {"S": "hot"}}
        }))
        .expect("valid GetItem");
        let found = store
            .operations
            .get_item(read)
            .await
            .expect("read final item");
        let found = serde_json::to_value(found).expect("serialize final item");
        let winner = found["Item"]["writer"]["N"]
            .as_str()
            .expect("writer number");
        assert!((0..8).any(|writer| winner == writer.to_string()));

        store.wait_until_changes_are_readable().await;
        let changes = super::cdc_stream_tests::system_records_with_count(&store, 2, 8)
            .await
            .unwrap();
        assert_eq!(
            changes.len(),
            8,
            "one durable CDC pointer per committed write"
        );
        assert!(changes.windows(2).all(|pair| {
            pair[0]
                .system_position
                .as_ref()
                .unwrap()
                .offset()
                .compare_order(pair[1].system_position.as_ref().unwrap().offset())
                == Ok(std::cmp::Ordering::Less)
        }));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// N4a experiment (c): a held native attempt must not publish behind a reader.
#[tokio::test]
#[ignore = "requires an explicitly selected Postgres or FoundationDB provider"]
async fn given_held_writer_when_eight_writers_replace_two_keys_then_images_and_cursor_preserve_every_commit()
 {
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::Duration,
    };

    use storage_types::{
        GetRecordsRequest, GetStreamRecordsRequest, StreamRecord, TableStreamCheckpoint,
        TableStreamSequence,
    };

    use super::{
        dynamodb_stream_history_tests::{
            create_stream_table, describe_shards, iterator, put_value,
        },
        native_census_test_support::{CensusWave, NativeCensus},
    };

    let backend = std::env::var("STORAGE_TEST_BACKEND").unwrap();
    assert!(matches!(backend.as_str(), "postgres" | "foundationdb"));
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let outcome = AssertUnwindSafe(async {
        let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let entries = store.stream_operations.system_stream_lineage_catalog(scope.clone()).await.unwrap();
        let catalog = stream_provider::SystemStreamLineageCatalog::try_new(scope, entries).unwrap();
        let schema = super::test_support::hash_table_request(table.clone()).key_schema;
        let mut roots = BTreeMap::new();
        for candidate in 0..256 {
            let key = format!("held-key-{candidate}");
            let attrs = serde_json::from_value(json!({"pk": {"S": key}})).unwrap();
            let route = stream_provider::DynamoMutationRoute::try_new(&table, &schema, &attrs).unwrap();
            let root = catalog.writer_lineage(route.routing_key()).unwrap().lineage_id().as_str().to_owned();
            roots.entry(root).or_insert(key);
            if roots.len() == 2 { break; }
        }
        assert_eq!(roots.len(), 2, "experiment requires two real writer roots");
        let keys = roots.into_values().collect::<Vec<_>>();
        for key in &keys { put_value(&store, &table, key, &format!("seed-{key}")).await; }
        let held = census.hold(|wave| match wave {
            CensusWave::Sql(_) => wave.sql_contains("hashtextextended"),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            CensusWave::Kv(wave) => wave.kind == kv_backend::KvWaveKind::Commit,
        });
        let operations = Arc::clone(&store.operations);
        let request = PutItemRequest::try_from(json!({"TableName": table.as_ref(), "Item": {"pk": {"S": keys[0]}, "value": {"S": "writer-0"}}})).unwrap();
        let first = tokio::spawn(async move { operations.put_item(request).await });
        held.reached().await;
        let mut writers = Vec::new();
        let barrier = Arc::new(Barrier::new(7));
        for writer in 1..8 {
            let operations = Arc::clone(&store.operations);
            let barrier = Arc::clone(&barrier);
            let request = PutItemRequest::try_from(json!({"TableName": table.as_ref(), "Item": {"pk": {"S": keys[writer % 2]}, "value": {"S": format!("writer-{writer}")}}})).unwrap();
            writers.push(tokio::spawn(async move { barrier.wait().await; operations.put_item(request).await }));
        }
        // All seven acknowledgements precede the checkpoint: a concrete overlap
        // witness, including on FDB where the held observation must be retried.
        for writer in writers {
            tokio::time::timeout(Duration::from_secs(20), writer).await.expect("unheld writers must commit").unwrap().unwrap();
        }
        assert!(!first.is_finished());
        let assert_cursor_order = |input: Option<&TableStreamCheckpoint>, records: &[StreamRecord]| {
            if let Some(input) = input {
                for record in records {
                    let route = stream_provider::DynamoMutationRoute::try_new(&table, &schema, &record.keys).unwrap();
                    let lane = catalog.writer_lineage(route.routing_key()).unwrap();
                    let saved = input.positions().iter().find(|position| {
                        position.shard_id() == lane.shard_id() && position.lineage_id() == lane.lineage_id()
                    }).expect("input checkpoint qualifies the record's lane");
                    let sequence: TableStreamSequence = record.sequence_number.parse().unwrap();
                    assert!(sequence > TableStreamSequence::try_from(saved.offset()).unwrap(), "record must be above its input cursor");
                }
            }
        };
        let mut checkpoint: Option<String> = None;
        let mut snapshots = Vec::new();
        for _ in 0..12 {
            let input = checkpoint.as_ref().map(|token| token.parse::<TableStreamCheckpoint>().unwrap());
            let page = store.operations.get_stream_records(GetStreamRecordsRequest::table(table.clone(), checkpoint, Some(2))).await.unwrap();
            assert_cursor_order(input.as_ref(), page.records());
            checkpoint = page.last_evaluated_key().map(str::to_owned);
            let empty = page.records().is_empty();
            snapshots.extend(page.into_parts().0);
            if empty { break; }
        }
        let prefix_len = snapshots.len();
        assert_eq!(prefix_len, 9, "all seven unheld writes and both seeds must be visible before release");
        assert!(snapshots.iter().all(|record| !matches!(
            record.item.as_ref().and_then(|item| item.get("value")),
            Some(storage_types::AttributeValue::S(value)) if value == "writer-0"
        )), "held write must be absent from the captured prefix");
        assert!(checkpoint.is_some(), "held reader must obtain a resumable cursor");
        held.release();
        tokio::time::timeout(Duration::from_secs(20), first).await.expect("held writer completes").unwrap().unwrap();
        for _ in 0..12 {
            let input = checkpoint.as_ref().map(|token| token.parse::<TableStreamCheckpoint>().unwrap());
            let page = store.operations.get_stream_records(GetStreamRecordsRequest::table(table.clone(), checkpoint, Some(2))).await.unwrap();
            assert_cursor_order(input.as_ref(), page.records());
            checkpoint = page.last_evaluated_key().map(str::to_owned);
            let empty = page.records().is_empty();
            snapshots.extend(page.into_parts().0);
            if empty { break; }
        }
        let snapshot_values = snapshots.iter().map(|record| serde_json::to_value(&record.item).unwrap()["value"]["S"].as_str().unwrap().to_owned()).collect::<BTreeSet<_>>();
        let expected = (0..8).map(|writer| format!("writer-{writer}")).chain(keys.iter().map(|key| format!("seed-{key}"))).collect::<BTreeSet<_>>();
        assert_eq!(snapshots.len(), 10, "prefix plus resumed cursor loses or repeats no commit");
        assert_eq!(snapshot_values, expected);
        let mut chains = BTreeMap::new();
        let mut previous_images = BTreeMap::new();
        let mut standard_values = BTreeSet::new();
        for shard in describe_shards(&store, &arn).await {
            let mut token = iterator(&store, &arn, &shard.shard_id, "TRIM_HORIZON", None).await;
            for _ in 0..12 {
                let page = store.operations.get_records(GetRecordsRequest { shard_iterator: token, limit: Some(2) }).await.unwrap();
                let empty = page.records.is_empty();
                for record in page.records {
                    let wire = serde_json::to_value(&record.dynamodb).unwrap();
                    let key = wire["Keys"]["pk"]["S"].as_str().unwrap().to_owned();
                    let old_image = previous_images.insert(key.clone(), wire["NewImage"].clone()).unwrap_or(serde_json::Value::Null);
                    assert_eq!(wire["OldImage"], old_image, "complete old image must equal the preceding new image");
                    assert_eq!(record.event_name, if old_image.is_null() { "INSERT" } else { "MODIFY" });
                    let previous = chains.entry(key).or_insert((None, 0));
                    assert_eq!(wire["OldImage"]["value"]["S"].as_str(), previous.0.as_deref());
                    let value = wire["NewImage"]["value"]["S"].as_str().unwrap().to_owned();
                    assert!(standard_values.insert(value.clone()));
                    previous.0 = Some(value);
                    previous.1 += 1;
                }
                let Some(next) = page.next_shard_iterator else { break; };
                token = next;
                if empty { break; }
            }
        }
        assert_eq!(standard_values, expected);
        for key in keys {
            let (last, count) = chains.get(&key).unwrap();
            assert_eq!(*count, 5);
            let read = GetItemRequest::try_from(json!({"TableName": table.as_ref(), "Key": {"pk": {"S": key}}, "ConsistentRead": true})).unwrap();
            let final_item = serde_json::to_value(store.operations.get_item(read).await.unwrap()).unwrap();
            assert_eq!(final_item["Item"]["value"]["S"].as_str(), last.as_deref());
        }
        eprintln!("N4a held-writer backend={backend} roots=2 acknowledgements_while_held=7 prefix={prefix_len} total=10 exact_chains=2x5 cursor_resume=complete");
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
