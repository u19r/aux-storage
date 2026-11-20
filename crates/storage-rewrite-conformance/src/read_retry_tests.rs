//! Complete public reads replay known native aborts, retaining request
//! identity.

use std::sync::{Arc, Mutex};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_types::{StorageEnum, StorageError, StorageResult, TableName};

use super::test_support::TestStore;

#[derive(Clone, Copy)]
enum Failure {
    Conflict,
    Permanent,
    Unknown,
}

#[derive(Default)]
struct ReadFaultState {
    target: Option<usize>,
    remaining: usize,
    calls: usize,
    fired: usize,
    begins: usize,
    completed_queries: usize,
    failed_after_completed: Option<usize>,
    failure: Option<Failure>,
}

#[derive(Default)]
struct ReadFault(Mutex<ReadFaultState>);

impl ReadFault {
    fn arm(&self, target: Option<usize>, remaining: usize, failure: Failure) {
        *self.0.lock().unwrap() = ReadFaultState {
            target,
            remaining,
            failure: Some(failure),
            ..ReadFaultState::default()
        };
    }

    fn read(&self) -> Option<StorageError> {
        let mut state = self.0.lock().unwrap();
        let ordinal = state.calls;
        state.calls += 1;
        if state.remaining == 0 || state.target.is_some_and(|target| target != ordinal) {
            return None;
        }
        state.failed_after_completed = Some(state.completed_queries);
        state.fired += 1;
        state.remaining -= 1;
        Some(match state.failure.unwrap() {
            Failure::Conflict => StorageEnum::Attempt(storage_types::AttemptFailure::Conflict(
                storage_types::ConflictCause::Logical("synthetic aborted read"),
            ))
            .into(),
            Failure::Permanent => StorageError::internal("synthetic permanent read failure"),
            Failure::Unknown => StorageError::commit_unknown("synthetic unknown read outcome"),
        })
    }

    fn begins(&self) -> usize {
        self.0.lock().unwrap().begins
    }

    fn counts(&self) -> (usize, usize) {
        let state = self.0.lock().unwrap();
        (state.calls, state.fired)
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl sql_backend::SqlFaultProbe for ReadFault {
    fn observe(&self, event: sql_backend::SqlNativeEvent) {
        match event {
            sql_backend::SqlNativeEvent::Begin => self.0.lock().unwrap().begins += 1,
            sql_backend::SqlNativeEvent::Statement {
                operation: sql_backend::SqlFaultOperation::Query,
                ..
            } => self.0.lock().unwrap().completed_queries += 1,
            _ => {}
        }
    }
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        (call.operation == sql_backend::SqlFaultOperation::Query)
            .then(|| self.read())
            .flatten()
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for ReadFault {
    fn observe(&self, event: kv_backend::KvNativeEvent) {
        if matches!(event, kv_backend::KvNativeEvent::Begin) {
            self.0.lock().unwrap().begins += 1;
        }
    }
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }
    fn before_wave(
        &self,
        wave: kv_backend::KvWave,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if matches!(
                wave.kind,
                kv_backend::KvWaveKind::GetBatch | kv_backend::KvWaveKind::Scan
            ) && let Some(error) = self.read()
            {
                return Err(error);
            }
            Ok(())
        })
    }
}

async fn fixture(probe: &Arc<ReadFault>) -> TestStore {
    TestStore::new_with_native_probes(
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await
}

/// The stream ARN no longer carries the incarnation id; the table does.
async fn current_target(store: &TestStore, table: &TableName) -> storage_types::TableHistoryTarget {
    let described = store
        .operations
        .describe_table(
            storage_types::DescribeTableRequest::try_from(json!({"TableName": table})).unwrap(),
        )
        .await
        .unwrap();
    storage_types::TableHistoryTarget::try_from(&described.table).unwrap()
}

async fn read(
    store: &TestStore,
    family: &str,
    table: &TableName,
    arn: &str,
) -> StorageResult<Value> {
    let target = match family {
        "checkpoint" | "stream_records" => Some(current_target(store, table).await),
        _ => None,
    };
    read_with_target(store, family, table, arn, target).await
}

/// `target` is resolved by the caller so its describe is not counted as part of
/// the measured read.
async fn read_with_target(
    store: &TestStore,
    family: &str,
    table: &TableName,
    arn: &str,
    target: Option<storage_types::TableHistoryTarget>,
) -> StorageResult<Value> {
    let key = json!({"pk": {"S": "customer-1"}});
    macro_rules! request {
        ($method:ident, $value:expr) => {
            serde_json::to_value(
                store
                    .operations
                    .$method(serde_json::from_value($value).unwrap())
                    .await?,
            )
            .unwrap()
        };
    }
    Ok(match family {
        "get" => request!(get_item, json!({"TableName": table, "Key": key})),
        "batch" => request!(
            batch_get_item,
            json!({"RequestItems": {table.as_ref(): {"Keys": [key]}}})
        ),
        "transact" => request!(
            transact_get_items,
            json!({"TransactItems": [{"Get": {"TableName": table, "Key": key}}]})
        ),
        "query" => request!(
            query,
            json!({"TableName": table, "KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": {"S": "customer-1"}}})
        ),
        "scan" => request!(scan, json!({"TableName": table})),
        "sequence" => request!(
            read_sequence,
            json!({"Nodes": [
            {"Name": "item", "Operation": {"Get": {"TableName": table, "Key": key}}},
            {"Name": "batch", "Operation": {"BatchGet": {"RequestItems": {table.as_ref(): {"Keys": [key]}}}}},
            {"Name": "query", "Operation": {"Query": {"TableName": table, "KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": {"S": "customer-1"}}}}}
        ], "Outputs": ["item", "batch", "query"]})
        ),
        "describe" => request!(describe_table, json!({"TableName": table})),
        "ttl" => request!(describe_time_to_live, json!({"TableName": table})),
        "tables" => request!(list_tables, json!({})),
        "streams" => request!(list_streams, json!({"TableName": table})),
        "describe_stream" => request!(describe_stream, json!({"StreamArn": arn})),
        "checkpoint" => serde_json::to_value(
            store
                .operations
                .table_stream_checkpoint(target.expect("checkpoint target"))
                .await?,
        )
        .unwrap(),
        "stream_records" => serde_json::to_value(
            store
                .operations
                .get_stream_records(
                    storage_types::GetStreamRecordsRequest::table_at_incarnation(
                        target.expect("stream records target"),
                        None,
                        Some(10),
                    ),
                )
                .await?,
        )
        .unwrap(),
        "table_info" => {
            serde_json::to_value(store.operations.get_table_info(table.clone()).await?).unwrap()
        }
        other => panic!("unknown read family: {other}"),
    })
}

fn assert_checkpoint_progress(
    actual: &storage_types::TableStreamCheckpoint,
    expected: &storage_types::TableStreamCheckpoint,
) {
    assert_eq!(actual.history_id(), expected.history_id());
    assert_eq!(actual.source_scope(), expected.source_scope());
    assert_eq!(actual.next_lane(), expected.next_lane());
    assert_eq!(
        actual.initial_visibility_cutoff(),
        expected.initial_visibility_cutoff()
    );
    assert_eq!(actual.positions().len(), expected.positions().len());
    for (actual, expected) in actual.positions().iter().zip(expected.positions()) {
        assert_eq!(actual.shard_id(), expected.shard_id());
        assert_eq!(actual.lineage_id(), expected.lineage_id());
        assert!(actual.offset().order().unwrap() >= expected.offset().order().unwrap());
    }
}

fn assert_read_response(family: &str, mut actual: Value, expected: &Value) {
    if family == "checkpoint" {
        assert_checkpoint_progress(
            &serde_json::from_value(actual).unwrap(),
            &serde_json::from_value(expected.clone()).unwrap(),
        );
        return;
    }
    for field in ["NextShardIterator", "ShardIterator", "LastEvaluatedKey"] {
        if let (Some(actual_token), Some(expected_token)) =
            (actual[field].as_str(), expected[field].as_str())
        {
            if field == "LastEvaluatedKey" {
                assert_checkpoint_progress(
                    &actual_token.parse().unwrap(),
                    &expected_token.parse().unwrap(),
                );
            } else {
                let actual_parts = actual_token.split('|').collect::<Vec<_>>();
                let expected_parts = expected_token.split('|').collect::<Vec<_>>();
                assert_eq!(actual_parts.len(), 4);
                assert_eq!(&actual_parts[..3], &expected_parts[..3]);
                if actual_parts[3].is_empty() || expected_parts[3].is_empty() {
                    assert_eq!(actual_parts[3], expected_parts[3]);
                } else {
                    assert_checkpoint_progress(
                        &actual_parts[3].parse().unwrap(),
                        &expected_parts[3].parse().unwrap(),
                    );
                }
            }
            // A new Postgres snapshot may advance an empty page's native
            // checkpoint.
            actual[field] = expected[field].clone();
        }
    }
    assert_eq!(
        &actual, expected,
        "{family} must replay the complete response"
    );
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_native_read_conflicts_when_public_reads_run_then_each_complete_snapshot_retries() {
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (table, arn) =
            super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_IMAGE").await;
        store.operations.put_item(storage_types::PutItemRequest::try_from(json!({
        "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "current"}}
    })).unwrap()).await.unwrap();
        for family in [
            "get",
            "batch",
            "transact",
            "query",
            "scan",
            "sequence",
            "describe",
            "ttl",
            "tables",
            "streams",
            "describe_stream",
            "checkpoint",
            "stream_records",
            "table_info",
        ] {
            read(&store, family, &table, &arn).await.unwrap();
            let target = matches!(family, "checkpoint" | "stream_records")
                .then(|| current_target(&store, &table));
            let target = match target {
                Some(target) => Some(target.await),
                None => None,
            };
            // Recreate the stale catalog-version path for each first attempt;
            // success otherwise retags metadata and can remove a measured read.
            // ListTables scans names without resolving cached table candidates,
            // and creating a table would change its complete expected response.
            probe.arm(None, 0, Failure::Conflict);
            if family != "tables" {
                store.create_standard_table().await;
            }
            probe.arm(None, 0, Failure::Conflict);
            let expected = read_with_target(&store, family, &table, &arn, target.clone())
                .await
                .unwrap();
            let calls = probe.counts().0;
            assert!(calls > 0, "{family} must exercise the real native port");
            for ordinal in 0..calls {
                probe.arm(None, 0, Failure::Conflict);
                if family != "tables" {
                    store.create_standard_table().await;
                }
                probe.arm(Some(ordinal), 1, Failure::Conflict);
                let result = read_with_target(&store, family, &table, &arn, target.clone()).await;
                let counts = probe.counts();
                assert_eq!(
                    counts.1, 1,
                    "{family} native read {ordinal} must fail once; baseline calls={calls}, \
                     actual calls={}",
                    counts.0
                );
                assert_read_response(family, result.unwrap(), &expected);
                assert_eq!(
                    probe.begins(),
                    2,
                    "{family} must open a new native snapshot"
                );
            }
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_native_read_failures_when_get_runs_then_retries_are_bounded_and_unknown_is_preserved()
 {
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        for (failure, expected_calls) in [
            (Failure::Conflict, 12),
            (Failure::Permanent, 1),
            (Failure::Unknown, 1),
        ] {
            probe.arm(None, usize::MAX, failure);
            let error = read(&store, "get", &table, "").await.unwrap_err();
            assert_eq!(probe.counts(), (expected_calls, expected_calls));
            match failure {
                Failure::Conflict => assert_eq!(
                    error.attempt_kind(),
                    Some(storage_types::AttemptKind::Conflict)
                ),
                Failure::Permanent => assert!(
                    !error
                        .attempt_kind()
                        .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
                ),
                Failure::Unknown => assert!(error.is_commit_unknown()),
            }
        }
        probe.arm(None, 0, Failure::Conflict);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_cold_metadata_when_last_read_conflicts_then_failed_attempt_does_not_publish_metadata()
 {
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let first = store.create_standard_table().await;
        probe.arm(None, 0, Failure::Conflict);
        read(&store, "get", &first, "").await.unwrap();
        let cold_calls = probe.counts().0;
        assert!(
            cold_calls > 1,
            "cold read includes metadata and item access"
        );
        let second = store.create_standard_table().await;
        probe.arm(Some(cold_calls - 1), 1, Failure::Conflict);
        assert_eq!(read(&store, "get", &second, "").await.unwrap(), json!({}));
        assert_eq!(
            probe.counts(),
            (cold_calls * 2, 1),
            "retry must load unpublished metadata again"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_aborted_read_when_item_changes_before_retry_then_new_snapshot_returns_current_item()
{
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let put = |value| {
            storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": value}}
            }))
            .unwrap()
        };
        store.operations.put_item(put("before")).await.unwrap();
        // Fail the last read after earlier graph nodes already assembled old
        // data. Keep catalog-version checks identical before measuring and
        // arming, since the successful baseline publishes its version into the
        // metadata cache.
        read(&store, "sequence", &table, "").await.unwrap();
        change_catalog_version(&store).await;
        probe.arm(None, 0, Failure::Conflict);
        read(&store, "sequence", &table, "").await.unwrap();
        let last_read = probe.counts().0 - 1;
        change_catalog_version(&store).await;
        probe.arm(Some(last_read), 1, Failure::Conflict);
        let pause = store.pause_next_retry();
        let (result, write_begins) = tokio::join!(read(&store, "sequence", &table, ""), async {
            tokio::time::timeout(std::time::Duration::from_secs(10), pause.arrived.notified())
                .await
                .unwrap();
            let before_write = probe.begins();
            store.operations.put_item(put("current")).await.unwrap();
            let write_begins = probe.begins() - before_write;
            pause.released.notify_one();
            write_begins
        });
        let response = result.unwrap();
        let serialized = response.to_string();
        assert!(
            serialized.contains("current"),
            "retry must load the new image"
        );
        assert!(
            !serialized.contains("before"),
            "failed graph node results must be discarded"
        );
        assert_eq!(probe.counts().1, 1);
        assert_eq!(
            probe.begins() - write_begins,
            2,
            "retry opens a fresh read snapshot"
        );
        probe.arm(None, 0, Failure::Conflict);
        let expected = read(&store, "sequence", &table, "").await.unwrap();
        assert_read_response("sequence", response, &expected);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_pinned_read_when_table_is_recreated_before_retry_then_new_incarnation_is_rejected() {
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        // A cold Get observes and pins the catalog before its item access. A
        // warm combined SQL Get can fail before observing any authority at all.
        probe.arm(None, 0, Failure::Conflict);
        read(&store, "get", &table, "").await.unwrap();
        let item_read = probe.counts().0 - 1;
        let table = store.create_standard_table().await;
        probe.arm(Some(item_read), 1, Failure::Conflict);
        let pause = store.pause_next_retry();
        let (result, ()) = tokio::join!(read(&store, "get", &table, ""), async {
            tokio::time::timeout(std::time::Duration::from_secs(10), pause.arrived.notified())
                .await
                .unwrap();
            store
                .operations
                .delete_table(storage_types::DeleteTableRequest {
                    table_name: table.clone(),
                })
                .await
                .unwrap();
            store.create_hash_table(table.clone()).await;
            pause.released.notify_one();
        });
        assert!(matches!(
            result.unwrap_err().as_ref(),
            StorageEnum::ResourceNotFound { .. }
        ));
        if TestStore::selected_backend() == "postgres" {
            assert!(
                probe.0.lock().unwrap().failed_after_completed.unwrap() >= 2,
                "cold authority and metadata completed before the item failure"
            );
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_native_stream_read_conflicts_when_iterators_and_records_are_read_then_results_replay()
 {
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let (_, arn) =
            super::dynamodb_stream_history_tests::create_stream_table(&store, "NEW_IMAGE").await;
        let shards = super::dynamodb_stream_history_tests::describe_shards(&store, &arn).await;
        let shard = &shards[0].shard_id;
        let token = super::dynamodb_stream_history_tests::iterator(
            &store,
            &arn,
            shard,
            "TRIM_HORIZON",
            None,
        )
        .await;
        for records in [false, true] {
            let operation = || async {
                if records {
                    serde_json::to_value(
                        store
                            .operations
                            .get_records(storage_types::GetRecordsRequest {
                                shard_iterator: token.clone(),
                                limit: Some(10),
                            })
                            .await?,
                    )
                    .map_err(|error| StorageError::internal(&error))
                } else {
                    serde_json::to_value(
                        store
                            .operations
                            .get_shard_iterator(storage_types::GetShardIteratorRequest {
                                stream_arn: arn.clone(),
                                shard_id: shard.clone(),
                                shard_iterator_type: "TRIM_HORIZON".into(),
                                sequence_number: None,
                            })
                            .await?,
                    )
                    .map_err(|error| StorageError::internal(&error))
                }
            };
            operation().await.unwrap();
            // A catalog publication makes the cached stream table version
            // stale. Count that path consistently: successful reads publish the
            // new version and would otherwise make a later fault ordinal
            // unreachable.
            probe.arm(None, 0, Failure::Conflict);
            change_catalog_version(&store).await;
            probe.arm(None, 0, Failure::Conflict);
            let expected = operation().await.unwrap();
            let calls = probe.counts().0;
            assert!(calls > 0);
            for ordinal in 0..calls {
                probe.arm(None, 0, Failure::Conflict);
                change_catalog_version(&store).await;
                probe.arm(Some(ordinal), 1, Failure::Conflict);
                assert_read_response("aws_stream", operation().await.unwrap(), &expected);
                assert_eq!(
                    probe.counts().1,
                    1,
                    "records={records} ordinal={ordinal} baseline_calls={calls} actual_calls={}",
                    probe.counts().0
                );
                assert_eq!(probe.begins(), 2);
            }
        }
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires owned native fixture"
)]
async fn given_stale_table_cache_when_other_store_recreates_before_binding_then_reads_use_new_table()
 {
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let reader = store.independent_operations.as_ref().unwrap();
        let table = store.create_standard_table().await;
        reader
            .get_item(
                serde_json::from_value(json!({
                    "TableName": table, "Key": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        store
            .operations
            .delete_table(storage_types::DeleteTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        store.create_hash_table(table.clone()).await;
        store.operations.put_item(storage_types::PutItemRequest::try_from(json!({
            "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "recreated"}}
        })).unwrap()).await.unwrap();
        let response = reader
            .get_item(
                serde_json::from_value(json!({
                    "TableName": table, "Key": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            serde_json::to_string(&response)
                .unwrap()
                .contains("recreated")
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_warm_get_without_observations_when_table_is_recreated_before_retry_then_current_incarnation_is_used()
 {
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(ReadFault::default());
    let store = fixture(&probe).await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        read(&store, "get", &table, "").await.unwrap();
        probe.arm(Some(0), 1, Failure::Conflict);
        let pause = store.pause_next_retry();
        let (result, ()) = tokio::join!(read(&store, "get", &table, ""), async {
            tokio::time::timeout(std::time::Duration::from_secs(10), pause.arrived.notified())
                .await
                .unwrap();
            assert_eq!(
                probe.0.lock().unwrap().failed_after_completed,
                Some(0),
                "candidate alone supplied no authority observation"
            );
            store
                .operations
                .delete_table(storage_types::DeleteTableRequest {
                    table_name: table.clone(),
                })
                .await
                .unwrap();
            store.create_hash_table(table.clone()).await;
            pause.released.notify_one();
        });
        assert_eq!(result.unwrap(), json!({}));
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// CreateTable keeps the catalog version; DeleteTable changes it.
async fn change_catalog_version(store: &TestStore) {
    let table = store.create_standard_table().await;
    store
        .operations
        .delete_table(storage_types::DeleteTableRequest { table_name: table })
        .await
        .unwrap();
}
