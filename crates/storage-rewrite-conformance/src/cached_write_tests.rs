//! Public ordinary writes keep decisions inside native settlement.

use std::sync::{
    Arc,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};

use futures::future::BoxFuture;
use serde_json::json;
use storage_types::{StorageError, StorageResult};

use super::test_support::{TestStore, unique_table_name};

#[cfg(feature = "foundationdb")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RequestAttempt {
    nested: bool,
    mode: storage_types::AttemptMode,
    policy: storage_types::ReadVersionPolicy,
}

#[derive(Default)]
struct WriteProbe {
    before: AtomicUsize,
    after: AtomicUsize,
    failure: AtomicU8,
    #[cfg(feature = "foundationdb")]
    before_commit: std::sync::Mutex<Option<BoxFuture<'static, ()>>>,
    #[cfg(feature = "foundationdb")]
    mutations: AtomicUsize,
    #[cfg(feature = "foundationdb")]
    nested: AtomicU8,
    #[cfg(feature = "foundationdb")]
    attempts: std::sync::Mutex<Vec<RequestAttempt>>,
    #[cfg(feature = "foundationdb")]
    commit_failures:
        std::sync::Mutex<Vec<(RequestAttempt, Option<storage_types::AttemptKind>, usize)>>,
    #[cfg(feature = "foundationdb")]
    acknowledged_attempt: std::sync::Mutex<Option<usize>>,
}

impl WriteProbe {
    fn boundary(&self, after: bool) -> StorageResult<()> {
        if after {
            self.after.fetch_add(1, Ordering::SeqCst);
            #[cfg(feature = "foundationdb")]
            {
                let nested = self.nested.load(Ordering::SeqCst) != 0;
                *self.acknowledged_attempt.lock().unwrap() = self
                    .attempts
                    .lock()
                    .unwrap()
                    .iter()
                    .rposition(|attempt| attempt.nested == nested);
            }
        } else {
            self.before.fetch_add(1, Ordering::SeqCst);
        }
        let point = if after { 2 } else { 1 };
        if self
            .failure
            .compare_exchange(point, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            Err(StorageError::internal("synthetic write settlement failure"))
        } else {
            Ok(())
        }
    }

    fn reset(&self, failure: u8) {
        self.before.store(0, Ordering::SeqCst);
        self.after.store(0, Ordering::SeqCst);
        self.failure.store(failure, Ordering::SeqCst);
        #[cfg(feature = "foundationdb")]
        {
            self.attempts.lock().unwrap().clear();
            self.commit_failures.lock().unwrap().clear();
            *self.acknowledged_attempt.lock().unwrap() = None;
            self.nested.store(0, Ordering::SeqCst);
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl sql_backend::SqlFaultProbe for WriteProbe {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }
    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        let result =
            self.boundary(boundary == sql_backend::SqlCommitBoundary::AfterDurableAcknowledgement);
        Box::pin(async move { result })
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for WriteProbe {
    #[cfg(feature = "foundationdb")]
    fn observe(&self, event: kv_backend::KvNativeEvent) {
        let nested = self.nested.load(Ordering::SeqCst) != 0;
        if let kv_backend::KvNativeEvent::AttemptRequested { mode, read_version } = event {
            self.attempts.lock().unwrap().push(RequestAttempt {
                nested,
                mode,
                policy: read_version,
            });
        }
        if let kv_backend::KvNativeEvent::CommitFailed { kind, .. } = event {
            let attempt = *self
                .attempts
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|attempt| attempt.nested == nested)
                .expect("native commit must belong to an observed request attempt");
            self.commit_failures.lock().unwrap().push((
                attempt,
                kind,
                self.after.load(Ordering::SeqCst),
            ));
        }
        if let kv_backend::KvNativeEvent::Mutations {
            puts,
            deletes,
            range_deletes,
        } = event
        {
            self.mutations
                .fetch_add(puts + deletes + range_deletes, Ordering::SeqCst);
        }
    }
    fn fail(&self, call: kv_backend::KvFaultCall) -> Option<StorageError> {
        if call.operation == kv_backend::KvFaultOperation::ReadCommittedVersion
            && self
                .failure
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            Some(StorageError::internal(
                "synthetic local committed-version lookup failure",
            ))
        } else {
            None
        }
    }
    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        let after = boundary == kv_backend::KvCommitBoundary::AfterDurableAcknowledgement;
        #[cfg(feature = "foundationdb")]
        let action = if after
            || self.attempts.lock().unwrap().last().is_none_or(|attempt| {
                attempt.mode != storage_types::AttemptMode::ReadWriteItems
                    || attempt.policy != storage_types::ReadVersionPolicy::Cached
            }) {
            None
        } else {
            self.before_commit.lock().unwrap().take()
        };
        Box::pin(async move {
            #[cfg(feature = "foundationdb")]
            if let Some(action) = action {
                action.await;
            }
            self.boundary(after)
        })
    }
}

fn assert_settlements(
    before: usize,
    acknowledgements: usize,
    conflicts: usize,
    expected_acknowledgements: usize,
    synthetic_failures: usize,
) {
    assert_eq!(
        acknowledgements, expected_acknowledgements,
        "unexpected native acknowledgements, including read-only settlements"
    );
    assert_eq!(
        before,
        expected_acknowledgements + synthetic_failures + conflicts
    );
}

impl WriteProbe {
    fn count_conflicts(&self) -> usize {
        #[cfg(feature = "foundationdb")]
        {
            let failures = self.commit_failures.lock().unwrap();
            assert!(
                failures
                    .iter()
                    .all(|(_, kind, _)| *kind == Some(storage_types::AttemptKind::Conflict)),
                "unexpected actual native commit failure: {failures:?}"
            );
            failures.len()
        }
        #[cfg(not(feature = "foundationdb"))]
        {
            0
        }
    }

    fn assert_unknown_settlement(&self) {
        let conflicts = self.count_conflicts();
        assert_settlements(
            self.before.load(Ordering::SeqCst),
            self.after.load(Ordering::SeqCst),
            conflicts,
            1,
            0,
        );
        assert_eq!(
            self.failure.load(Ordering::SeqCst),
            0,
            "unknown-outcome fault must be consumed exactly once"
        );
        #[cfg(feature = "foundationdb")]
        if TestStore::selected_backend() == "foundationdb" {
            self.assert_callback_policies(false, conflicts > 0);
            assert!(
                self.commit_failures
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(_, _, acknowledgements)| *acknowledgements == 0),
                "known conflicts must precede the eventual unknown acknowledgement"
            );
            let attempts = self.attempts.lock().unwrap();
            assert_eq!(
                *self.acknowledged_attempt.lock().unwrap(),
                Some(attempts.len() - 1),
                "no attempt may open after the acknowledgement becomes unknown"
            );
            eprintln!(
                "unknown settlement: preceding_conflicts={conflicts}, acknowledged_attempt={:?}",
                attempts.last().unwrap()
            );
        }
    }

    #[cfg(feature = "foundationdb")]
    fn requested_policies(&self) -> Vec<bool> {
        self.attempts
            .lock()
            .unwrap()
            .iter()
            .map(|attempt| attempt.policy == storage_types::ReadVersionPolicy::Cached)
            .collect()
    }

    #[cfg(feature = "foundationdb")]
    fn assert_callback_policies(&self, nested: bool, requires_retry: bool) {
        let attempts = self.attempts.lock().unwrap();
        let policies: Vec<_> = attempts
            .iter()
            .filter(|attempt| {
                attempt.nested == nested
                    && attempt.mode == storage_types::AttemptMode::ReadWriteItems
            })
            .map(|attempt| attempt.policy)
            .collect();
        assert_eq!(
            policies.first(),
            Some(&storage_types::ReadVersionPolicy::Cached),
            "warmed interleaving fixture must reach its initial Cached callback: {attempts:?}"
        );
        assert!(
            policies[1..]
                .iter()
                .all(|policy| *policy == storage_types::ReadVersionPolicy::Fresh),
            "retries must request Fresh: {attempts:?}"
        );
        if requires_retry {
            assert!(
                policies.len() > 1,
                "fixture must exercise Fresh redecision: {attempts:?}"
            );
        }
    }
}

// Successful public requests cannot supply these invalid diagnostic
// combinations.
#[cfg(feature = "foundationdb")]
#[test]
fn given_invalid_settlement_observations_when_checked_then_oracle_rejects_them() {
    use kv_backend::KvFaultProbe;
    use storage_types::{AttemptKind, AttemptMode, ReadVersionPolicy};

    for kind in [
        None,
        Some(AttemptKind::Unavailable),
        Some(AttemptKind::Capacity),
        Some(AttemptKind::FreshValidation),
    ] {
        let probe = WriteProbe::default();
        probe.observe(kv_backend::KvNativeEvent::AttemptRequested {
            mode: AttemptMode::ReadWriteItems,
            read_version: ReadVersionPolicy::Cached,
        });
        probe.observe(kv_backend::KvNativeEvent::CommitFailed {
            native_code: 0,
            kind,
        });
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| probe.count_conflicts()))
                .is_err(),
            "invalid native failure must fail the oracle: {kind:?}"
        );
    }
    // Even a balanced extra BeforeCommit/acknowledgement pair is a replay.
    assert!(std::panic::catch_unwind(|| assert_settlements(3, 2, 0, 1, 1)).is_err());
}

async fn store(probe: &Arc<WriteProbe>) -> TestStore {
    TestStore::new_with_cached_write_probes(
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await
}

fn no_mutation_attempts() -> usize {
    if TestStore::selected_backend() == "foundationdb" {
        2
    } else {
        1
    }
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_absent_item_when_deleted_then_cached_no_mutation_requires_fresh_decision() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "warm", "warm").await;
    probe.reset(0);
    store
        .operations
        .delete_item(
            storage_types::DeleteItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "absent"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(probe.before.load(Ordering::SeqCst), no_mutation_attempts());
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_condition_rejection_when_settled_then_old_item_is_returned_without_mutation() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "before").await;
    probe.reset(0);
    let error = store.operations.put_item(storage_types::PutItemRequest::try_from(json!({
        "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"S": "after"}},
        "ConditionExpression": "attribute_not_exists(pk)", "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
    })).unwrap()).await.unwrap_err();
    assert_eq!(probe.before.load(Ordering::SeqCst), no_mutation_attempts());
    assert!(
        matches!(error.as_ref(), storage_types::StorageEnum::ConditionalCheckFailedWithItem { item } if serde_json::to_value(item).unwrap()["value"]["S"] == "before"),
        "{error:?}"
    );
    let item = store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap()
            ["value"]["S"],
        "before"
    );
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_batch_known_abort_when_retried_then_control_reaches_group_owner() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "warm", "warm").await;
    probe.reset(1);
    let response = store.operations.batch_write_item(storage_types::BatchWriteItemRequest::try_from(json!({
        "RequestItems": {table.to_string(): [{"PutRequest": {"Item": {"pk": {"S": "one"}}}}, {"PutRequest": {"Item": {"pk": {"S": "two"}}}}]}
    })).unwrap()).await.unwrap();
    assert!(
        response
            .unprocessed_items
            .is_none_or(|items| items.is_empty())
    );
    let before = probe.before.load(Ordering::SeqCst);
    let after = probe.after.load(Ordering::SeqCst);
    let conflicts = probe.count_conflicts();
    assert_eq!(
        probe.failure.load(Ordering::SeqCst),
        0,
        "synthetic failure must be consumed"
    );
    for key in ["one", "two"] {
        let item = store
            .operations
            .get_item(
                storage_types::GetItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": key}}, "ConsistentRead": true
                }))
                .unwrap(),
            )
            .await
            .unwrap()
            .item
            .unwrap();
        assert_eq!(
            serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap(),
            json!({"pk": {"S": key}})
        );
    }
    eprintln!(
        "batch public response and exact committed items verified before attempt-count oracle"
    );
    assert_settlements(before, after, conflicts, 1, 1);
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_lost_commit_acknowledgement_when_writing_then_unknown_is_returned_once() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "warm", "warm").await;
    probe.reset(2);
    let error = store
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "one"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.is_commit_unknown());
    probe.assert_unknown_settlement();
    let item = store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap(),
        json!({"pk": {"S": "one"}})
    );
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_receipt_hit_when_table_is_recreated_then_receipt_keeps_priority() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    let request = json!({"ClientRequestToken": "cached-receipt", "TransactItems": [{"Put": {
        "TableName": table, "Item": {"pk": {"S": "one"}}
    }}]});
    store
        .operations
        .transact_write_items(
            storage_types::TransactWriteItemsRequest::try_from(request.clone()).unwrap(),
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
    super::dynamodb_stream_history_tests::put_value(&store, &table, "warm", "warm").await;
    probe.reset(0);
    store
        .operations
        .transact_write_items(storage_types::TransactWriteItemsRequest::try_from(request).unwrap())
        .await
        .unwrap();
    assert_eq!(probe.before.load(Ordering::SeqCst), no_mutation_attempts());
    let item = store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item;
    assert!(item.is_none());
    store.finish().await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_cached_condition_rejection_when_old_item_changes_then_fresh_decision_can_accept() {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "before").await;
    let independent = store.independent_operations.as_ref().unwrap().clone();
    let changed = table.clone();
    probe.mutations.store(0, Ordering::SeqCst);
    let observed = Arc::clone(&probe);
    *probe.before_commit.lock().unwrap() = Some(Box::pin(async move {
        assert_eq!(
            observed.requested_policies().first().copied(),
            Some(true),
            "first callback must request Cached; this does not prove a native cache hit"
        );
        assert_eq!(
            observed.mutations.load(Ordering::SeqCst),
            0,
            "initial cached condition must reject before staging"
        );
        observed.nested.store(1, Ordering::SeqCst);
        independent
            .delete_item(
                storage_types::DeleteItemRequest::try_from(json!({
                    "TableName": changed, "Key": {"pk": {"S": "one"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        observed.nested.store(0, Ordering::SeqCst);
    }));
    probe.reset(0);
    store
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"S": "after"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    // One concurrent delete, discarded cached rejection, and accepted fresh
    // write.
    let before = probe.before.load(Ordering::SeqCst);
    let after = probe.after.load(Ordering::SeqCst);
    let conflicts = probe.count_conflicts();
    probe.assert_callback_policies(false, true);
    probe.assert_callback_policies(true, false);
    assert!(
        probe.before_commit.lock().unwrap().is_none(),
        "interleaving action must fire"
    );
    let item = store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap()
            ["value"]["S"],
        "after"
    );
    eprintln!("accepted public write and stored value verified before attempt-count oracle");
    assert_settlements(before, after, conflicts, 3, 0);
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_invalid_later_batch_member_when_request_is_rejected_then_no_earlier_member_is_written()
 {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    let error = store.operations.batch_write_item(storage_types::BatchWriteItemRequest::try_from(json!({
        "RequestItems": {table.to_string(): [{"PutRequest": {"Item": {"pk": {"S": "one"}}}}, {"PutRequest": {"Item": {"other": {"S": "invalid"}}}}]}
    })).unwrap()).await;
    let error = error.unwrap_err();
    assert!(
        matches!(
            error.as_ref(),
            storage_types::StorageEnum::Validation { .. }
                | storage_types::StorageEnum::RawValidation { .. }
                | storage_types::StorageEnum::KeyValidation(_)
        ),
        "{error:?}"
    );
    let item = store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item;
    assert!(item.is_none());
    store.finish().await;
}

async fn read_item(
    store: &TestStore,
    table: &storage_types::TableName,
    key: &str,
) -> Option<serde_json::Value> {
    store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": key}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item
        .map(|item| {
            serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap()
        })
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_cached_condition_acceptance_when_old_item_changes_then_fresh_rejection_returns_new_old_item()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "one", "before").await;
    let independent = store.independent_operations.as_ref().unwrap().clone();
    let changed = table.clone();
    probe.mutations.store(0, Ordering::SeqCst);
    let observed = probe.clone();
    *probe.before_commit.lock().unwrap() =
        Some(Box::pin(async move {
            assert_eq!(observed.requested_policies(), vec![true]);
            assert!(
                observed.mutations.load(Ordering::SeqCst) > 0,
                "accepted Cached callback must stage its write before concurrent replacement"
            );
            observed.nested.store(1, Ordering::SeqCst);
            independent.put_item(storage_types::PutItemRequest::try_from(json!({
            "TableName": changed, "Item": {"pk": {"S": "one"}, "value": {"S": "concurrent"}}
        })).unwrap()).await.unwrap();
            observed.nested.store(0, Ordering::SeqCst);
        }));
    probe.reset(0);
    let error = store.operations.put_item(storage_types::PutItemRequest::try_from(json!({
        "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"S": "after"}},
        "ConditionExpression": "#v = :before", "ExpressionAttributeNames": {"#v": "value"},
        "ExpressionAttributeValues": {":before": {"S": "before"}}, "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
    })).unwrap()).await.unwrap_err();
    let expected = json!({"pk": {"S": "one"}, "value": {"S": "concurrent"}});
    assert!(
        matches!(error.as_ref(), storage_types::StorageEnum::ConditionalCheckFailedWithItem { item } if serde_json::to_value(item).unwrap() == expected),
        "{error:?}"
    );
    let conflicts = probe.count_conflicts();
    probe.assert_callback_policies(false, true);
    probe.assert_callback_policies(true, false);
    assert!(
        probe.before_commit.lock().unwrap().is_none(),
        "interleaving action must fire"
    );
    assert!(
        probe
            .commit_failures
            .lock()
            .unwrap()
            .iter()
            .any(|(attempt, _, _)| !attempt.nested
                && attempt.mode == storage_types::AttemptMode::ReadWriteItems
                && attempt.policy == storage_types::ReadVersionPolicy::Cached),
        "original staged Cached attempt must encounter an actual conflict"
    );
    let after = probe.after.load(Ordering::SeqCst);
    let before = probe.before.load(Ordering::SeqCst);
    assert_eq!(read_item(&store, &table, "one").await, Some(expected));
    eprintln!(
        "conditional rejection image and exact stored item verified before policy/count oracles"
    );
    assert_settlements(before, after, conflicts, 2, 0);
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_warmed_table_when_recreated_before_request_then_put_binds_new_table() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "old", "old").await;
    let independent = store.independent_operations.as_ref().unwrap();
    independent
        .delete_table(storage_types::DeleteTableRequest {
            table_name: table.clone(),
        })
        .await
        .unwrap();
    independent
        .create_table(super::test_support::hash_table_request(table.clone()))
        .await
        .unwrap();
    probe.reset(0);
    super::dynamodb_stream_history_tests::put_value(&store, &table, "new", "new").await;
    assert_eq!(
        read_item(&store, &table, "new").await,
        Some(json!({"pk": {"S": "new"}, "value": {"S": "new"}}))
    );
    assert_eq!(read_item(&store, &table, "old").await, None);
    store.finish().await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_cold_fresh_preparation_when_table_recreated_during_cached_commit_then_request_cannot_retarget()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    let independent = store.independent_operations.as_ref().unwrap().clone();
    // Only the independent metadata cache knows this table; this request must
    // prepare cold.
    independent
        .create_table(super::test_support::hash_table_request(table.clone()))
        .await
        .unwrap();
    let changed = table.clone();
    let observed = probe.clone();
    *probe.before_commit.lock().unwrap() = Some(Box::pin(async move {
        assert_eq!(
            observed.requested_policies(),
            vec![false, true],
            "cold preparation is genuinely Fresh before the Cached writer"
        );
        independent
            .delete_table(storage_types::DeleteTableRequest {
                table_name: changed.clone(),
            })
            .await
            .unwrap();
        independent
            .create_table(super::test_support::hash_table_request(changed.clone()))
            .await
            .unwrap();
        independent
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": changed, "Item": {"pk": {"S": "sentinel"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }));
    probe.reset(0);
    let error = store
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "new"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.as_ref(),
            storage_types::StorageEnum::ResourceNotFound { .. }
        ),
        "{error:?}"
    );
    assert_eq!(probe.requested_policies().last(), Some(&false));
    assert_eq!(read_item(&store, &table, "new").await, None);
    assert_eq!(
        read_item(&store, &table, "sentinel").await,
        Some(json!({"pk": {"S": "sentinel"}}))
    );
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_receipt_mismatch_when_table_recreated_then_mismatch_precedes_table_confirmation() {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    let request = json!({"ClientRequestToken": "cached-mismatch", "TransactItems": [{"Put": {
        "TableName": table, "Item": {"pk": {"S": "one"}}
    }}]});
    store
        .operations
        .transact_write_items(
            storage_types::TransactWriteItemsRequest::try_from(request.clone()).unwrap(),
        )
        .await
        .unwrap();
    let independent = store.independent_operations.as_ref().unwrap();
    independent
        .delete_table(storage_types::DeleteTableRequest {
            table_name: table.clone(),
        })
        .await
        .unwrap();
    independent
        .create_table(super::test_support::hash_table_request(table.clone()))
        .await
        .unwrap();
    independent
        .put_item(
            storage_types::PutItemRequest::try_from(
                json!({"TableName": table, "Item": {"pk": {"S": "sentinel"}}}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let mut mismatch = request;
    mismatch["TransactItems"][0]["Put"]["Item"]["pk"]["S"] = json!("two");
    probe.reset(0);
    let error = store
        .operations
        .transact_write_items(storage_types::TransactWriteItemsRequest::try_from(mismatch).unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.as_ref(),
            storage_types::StorageEnum::IdempotentParameterMismatch
        ),
        "{error:?}"
    );
    assert_eq!(probe.before.load(Ordering::SeqCst), no_mutation_attempts());
    assert_eq!(read_item(&store, &table, "one").await, None);
    assert_eq!(read_item(&store, &table, "two").await, None);
    assert_eq!(
        read_item(&store, &table, "sentinel").await,
        Some(json!({"pk": {"S": "sentinel"}}))
    );
    store.finish().await;
}

#[tokio::test]
#[cfg_attr(
    any(feature = "postgres", feature = "foundationdb"),
    ignore = "requires selected native provider fixture"
)]
async fn given_later_transaction_condition_rejection_when_settled_then_ordinals_images_and_all_storage_stay_unchanged()
 {
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.operations.create_table(storage_types::CreateTableRequest::try_from(json!({
        "TableName": table, "BillingMode": "PAY_PER_REQUEST", "AuxStreamDurationHours": 48,
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "status", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{"IndexName": "ByStatus", "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}]
    })).unwrap()).await.unwrap();
    for key in ["one", "two"] {
        store
            .operations
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": table, "Item": {"pk": {"S": key}, "status": {"S": "before"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    store.wait_until_changes_are_readable().await;
    let history_reads = super::image_retention_tests::canonical_history_reads(&store, &table)
        .await
        .unwrap();
    let before_history = (store.canonical_history_read)(history_reads.clone())
        .await
        .unwrap();
    let before_positions: Vec<_> = before_history
        .iter()
        .flat_map(|page| page.entries.iter().map(|entry| entry.position.clone()))
        .collect();
    assert_eq!(before_positions.len(), 2);
    #[cfg(feature = "foundationdb")]
    probe.mutations.store(0, Ordering::SeqCst);
    probe.reset(0);
    let error = store.operations.transact_write_items(storage_types::TransactWriteItemsRequest::try_from(json!({
        "TransactItems": [
            {"Put": {"TableName": table, "Item": {"pk": {"S": "one"}, "status": {"S": "after"}}}},
            {"Delete": {"TableName": table, "Key": {"pk": {"S": "two"}}, "ConditionExpression": "attribute_not_exists(pk)", "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}}
        ]
    })).unwrap()).await.unwrap_err();
    assert_eq!(probe.before.load(Ordering::SeqCst), no_mutation_attempts());
    #[cfg(feature = "foundationdb")]
    assert_eq!(
        probe.mutations.load(Ordering::SeqCst),
        0,
        "all members must be accepted before any native mutation"
    );
    let storage_types::StorageEnum::TransactionCanceled { reasons } = error.as_ref() else {
        panic!("{error:?}");
    };
    assert_eq!(reasons.len(), 2);
    assert_eq!(reasons[0], storage_types::CancellationReason::none());
    assert!(error.is_canceled_condition_check_failed(1));
    let old_item = reasons[1]
        .item
        .as_ref()
        .expect("ALL_OLD is required at rejected member ordinal");
    assert_eq!(
        serde_json::to_value(old_item).unwrap(),
        json!({"pk": {"S": "two"}, "status": {"S": "before"}})
    );
    for key in ["one", "two"] {
        assert_eq!(
            read_item(&store, &table, key).await,
            Some(json!({"pk": {"S": key}, "status": {"S": "before"}}))
        );
    }
    for (status, expected) in [("before", 2), ("after", 0)] {
        let response = store.operations.query(storage_types::QueryRequest::try_from(json!({
            "TableName": table, "IndexName": "ByStatus", "KeyConditionExpression": "#s = :s",
            "ExpressionAttributeNames": {"#s": "status"}, "ExpressionAttributeValues": {":s": {"S": status}}
        })).unwrap()).await.unwrap();
        assert_eq!(response.count, expected);
    }
    store.wait_until_changes_are_readable().await;
    let history_reads = super::image_retention_tests::canonical_history_reads(&store, &table)
        .await
        .unwrap();
    let after_history = (store.canonical_history_read)(history_reads).await.unwrap();
    let after_positions: Vec<_> = after_history
        .iter()
        .flat_map(|page| page.entries.iter().map(|entry| entry.position.clone()))
        .collect();
    assert_eq!(
        before_positions, after_positions,
        "rejected transaction must add no canonical history"
    );
    store.finish().await;
}

#[tokio::test]
#[cfg(feature = "foundationdb")]
#[ignore = "requires selected FoundationDB fixture"]
async fn given_unavailable_committed_version_when_cached_write_is_durable_then_unknown_returns_once_without_replay()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let probe = Arc::new(WriteProbe::default());
    let store = store(&probe).await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    super::dynamodb_stream_history_tests::put_value(&store, &table, "warm", "warm").await;
    probe.reset(3);
    let error = store
        .operations
        .put_item(
            storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "one"}, "value": {"S": "durable"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.is_commit_unknown());
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    probe.assert_unknown_settlement();
    let item = store
        .operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "one"}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap(),
        json!({"pk": {"S": "one"}, "value": {"S": "durable"}})
    );
    store.finish().await;
}

#[cfg(feature = "foundationdb")]
struct ConcurrentWriteProbe {
    armed: std::sync::atomic::AtomicBool,
    has_mutations: std::sync::atomic::AtomicBool,
    window: AtomicUsize,
    barriers: Arc<Vec<(tokio::sync::Barrier, tokio::sync::Barrier)>>,
    events: std::sync::Mutex<Vec<(usize, bool, kv_backend::KvNativeEvent)>>,
}

#[cfg(feature = "foundationdb")]
impl kv_backend::KvFaultProbe for ConcurrentWriteProbe {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe(&self, event: kv_backend::KvNativeEvent) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        match event {
            kv_backend::KvNativeEvent::Begin => self.has_mutations.store(false, Ordering::SeqCst),
            kv_backend::KvNativeEvent::Mutations {
                puts,
                deletes,
                range_deletes,
            } if puts + deletes + range_deletes > 0 => {
                self.has_mutations.store(true, Ordering::SeqCst);
            }
            _ => {}
        }
        self.events.lock().unwrap().push((
            self.window.load(Ordering::SeqCst),
            self.has_mutations.load(Ordering::SeqCst),
            event,
        ));
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        let window = if self.armed.load(Ordering::SeqCst)
            && boundary == kv_backend::KvCommitBoundary::BeforeCommit
            && self.has_mutations.load(Ordering::SeqCst)
        {
            Some(self.window.fetch_add(1, Ordering::SeqCst))
        } else {
            None
        };
        Box::pin(async move {
            if let Some(window) = window
                && let Some((arrived, released)) = self.barriers.get(window)
            {
                arrived.wait().await;
                released.wait().await;
            }
            Ok(())
        })
    }
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_twelve_cached_clients_when_their_table_catalogs_change_twice_then_fresh_retries_commit_once()
 {
    verify_concurrent_cached_publications(false).await;
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_twelve_cached_clients_when_third_staged_window_has_publication_then_additional_conflict_commits_once()
 {
    verify_concurrent_cached_publications(true).await;
}

#[cfg(feature = "foundationdb")]
async fn verify_concurrent_cached_publications(has_third_publication: bool) {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    // An unexpected precommit failure must report a missing barrier
    // participant.
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        const CLIENTS: usize = 12;
        let barriers = Arc::new(
            (0..3)
                .map(|_| {
                    (
                        tokio::sync::Barrier::new(CLIENTS + 1),
                        tokio::sync::Barrier::new(CLIENTS + 1),
                    )
                })
                .collect::<Vec<_>>(),
        );
        let mut clients = Vec::new();
        for _ in 0..CLIENTS {
            let probe = Arc::new(ConcurrentWriteProbe {
                armed: false.into(),
                has_mutations: false.into(),
                window: AtomicUsize::new(0),
                barriers: Arc::clone(&barriers),
                events: Default::default(),
            });
            let store = TestStore::new_with_cached_write_probes(
                #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
                Arc::new(WriteProbe::default()),
                probe.clone(),
            )
            .await;
            let table = unique_table_name();
            store.create_hash_table(table.clone()).await;
            super::dynamodb_stream_history_tests::put_value(&store, &table, "warm", "warm").await;
            clients.push((store, table, probe));
        }
        // Each publication changes every client table's own catalog stamp
        // (a stream toggle); a change to another table no longer conflicts.
        let publish = |enabled: bool| {
            let clients = &clients;
            async move {
                for (_, _, probe) in clients {
                    probe.armed.store(false, Ordering::SeqCst);
                }
                for (store, table, _) in clients {
                    let specification = if enabled {
                        json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"})
                    } else {
                        json!({"StreamEnabled": false})
                    };
                    store
                        .independent_operations
                        .as_ref()
                        .unwrap()
                        .update_table(
                            storage_types::UpdateTableRequest::try_from(json!({
                                "TableName": table, "StreamSpecification": specification
                            }))
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                }
                for (_, _, probe) in clients {
                    probe.armed.store(true, Ordering::SeqCst);
                }
            }
        };
        let mut writes = tokio::task::JoinSet::new();
        for (store, table, probe) in &clients {
            probe.armed.store(true, Ordering::SeqCst);
            let operations = Arc::clone(&store.operations);
            let table = table.clone();
            writes.spawn(async move {
                operations.put_item(storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "new"}, "value": {"S": "committed"}}
            })).unwrap()).await
            });
        }
        for (round, (arrived, released)) in barriers.iter().enumerate() {
            arrived.wait().await;
            if round < 2 {
                publish(round == 0).await;
            } else {
                // The independent store clone shares the backend probe. All
                // writes are held, so suspend observation while these separate
                // Fresh read attempts run.
                for (_, _, probe) in &clients {
                    probe.armed.store(false, Ordering::SeqCst);
                }
                for (store, table, _) in &clients {
                    // This separate public read uses Fresh and must see no
                    // staged primary item.
                    let item = store.independent_operations.as_ref().unwrap().get_item(
                    storage_types::GetItemRequest::try_from(json!({
                        "TableName": table, "Key": {"pk": {"S": "new"}}, "ConsistentRead": true
                    })).unwrap(),
                ).await.unwrap().item;
                    assert!(item.is_none());
                }
                for (_, _, probe) in &clients {
                    probe.armed.store(true, Ordering::SeqCst);
                }
                if has_third_publication {
                    publish(true).await;
                }
            }
            released.wait().await;
        }
        while let Some(write) = writes.join_next().await {
            let response = write.unwrap().unwrap();
            assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        }
        for (index, (store, table, probe)) in clients.into_iter().enumerate() {
            probe.armed.store(false, Ordering::SeqCst);
            let events = probe.events.lock().unwrap().clone();
            let policies = events
                .iter()
                .filter_map(|(_, _, event)| match event {
                    kv_backend::KvNativeEvent::AttemptRequested {
                        mode: storage_types::AttemptMode::ReadWriteItems,
                        read_version,
                    } => Some(*read_version),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let failures = events
                .iter()
                .filter_map(|(window, has_mutations, event)| match event {
                    kv_backend::KvNativeEvent::CommitFailed { native_code, kind } => {
                        Some((*window, *has_mutations, *native_code, *kind))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(
                policies.first(),
                Some(&storage_types::ReadVersionPolicy::Cached)
            );
            assert!(
                policies[1..]
                    .iter()
                    .all(|policy| *policy == storage_types::ReadVersionPolicy::Fresh)
            );
            assert!(
                (3..=13).contains(&policies.len()),
                "native ReadWriteItems callback requests={policies:?}"
            );
            // Each controlled publication must reject its own staged precommit
            // window.
            assert_eq!(
                failures.get(..2),
                Some(
                    [
                        (1, true, 1020, Some(storage_types::AttemptKind::Conflict)),
                        (2, true, 1020, Some(storage_types::AttemptKind::Conflict))
                    ]
                    .as_slice()
                )
            );
            assert!(
                failures
                    .iter()
                    .skip(2)
                    .all(|(window, has_mutations, _, kind)| *window >= 3
                        && *has_mutations
                        && *kind == Some(storage_types::AttemptKind::Conflict)),
                "only additional staged known conflicts after the controlled windows are allowed: \
                 {failures:?}"
            );
            if has_third_publication {
                assert_eq!(
                    failures.get(2),
                    Some(&(3, true, 1020, Some(storage_types::AttemptKind::Conflict))),
                    "third real publication must reject every staged third window"
                );
            }
            let mutation_windows = probe.window.load(Ordering::SeqCst);
            assert_eq!(
                mutation_windows,
                1 + failures.len(),
                "one final mutation acknowledgement plus each actual known conflict"
            );
            // ReadOnly preparation acknowledgements are excluded from durable
            // write acknowledgements.
            let mut has_mutations = false;
            let mut durable_writes = 0;
            for (_, _, event) in &events {
                match event {
                    kv_backend::KvNativeEvent::Begin => has_mutations = false,
                    kv_backend::KvNativeEvent::Mutations {
                        puts,
                        deletes,
                        range_deletes,
                    } => has_mutations |= puts + deletes + range_deletes > 0,
                    kv_backend::KvNativeEvent::Commit if has_mutations => durable_writes += 1,
                    _ => {}
                }
            }
            assert_eq!(durable_writes, 1);
            let item = store
                .operations
                .get_item(
                    storage_types::GetItemRequest::try_from(json!({
                        "TableName": table, "Key": {"pk": {"S": "new"}}, "ConsistentRead": true
                    }))
                    .unwrap(),
                )
                .await
                .unwrap()
                .item
                .unwrap();
            assert_eq!(
                serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap())
                    .unwrap(),
                json!({"pk": {"S": "new"}, "value": {"S": "committed"}})
            );
            println!(
                "client={index} third_publication={has_third_publication} process_scope=one \
                 independent_store=true native_write_callback_policies={policies:?} \
                 native_failures={failures:?} mutation_windows={mutation_windows} \
                 durable_writes=1 absent_before_third_release=true exact_item=true"
            );
            store.finish().await;
        }
    })
    .await
    .expect(
        "controlled concurrent writes did not complete all three commit barriers within 30 seconds",
    );
}

/// A probe that holds the first mutating commit of `store` at one barrier pair.
#[cfg(feature = "foundationdb")]
async fn held_commit_store() -> (TestStore, Arc<ConcurrentWriteProbe>) {
    let probe = Arc::new(ConcurrentWriteProbe {
        armed: false.into(),
        has_mutations: false.into(),
        window: AtomicUsize::new(0),
        barriers: Arc::new(vec![(
            tokio::sync::Barrier::new(2),
            tokio::sync::Barrier::new(2),
        )]),
        events: Default::default(),
    });
    let store = TestStore::new_with_cached_write_probes(
        #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
        Arc::new(WriteProbe::default()),
        probe.clone(),
    )
    .await;
    (store, probe)
}

#[cfg(feature = "foundationdb")]
impl ConcurrentWriteProbe {
    fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Commit conflicts and attempts of the held operation: events of the
    /// other node's work in `other` are left out.
    fn conflicts_and_attempts(&self, other: std::ops::Range<usize>) -> (usize, usize) {
        let events = self.events.lock().unwrap();
        let held = events
            .iter()
            .enumerate()
            .filter(|(index, _)| !other.contains(index))
            .map(|(_, event)| event)
            .collect::<Vec<_>>();
        let conflicts = held
            .iter()
            .filter(|(_, _, event)| matches!(event, kv_backend::KvNativeEvent::CommitFailed { .. }))
            .count();
        let attempts = held
            .iter()
            .filter(|(_, _, event)| {
                matches!(event, kv_backend::KvNativeEvent::AttemptRequested { .. })
            })
            .count();
        (conflicts, attempts)
    }
}

#[cfg(feature = "foundationdb")]
fn put(table: &storage_types::TableName, key: &str, value: &str) -> storage_types::PutItemRequest {
    storage_types::PutItemRequest::try_from(json!({
        "TableName": table, "Item": {"pk": {"S": key}, "value": {"S": value}}
    }))
    .unwrap()
}

#[cfg(feature = "foundationdb")]
async fn consistent_value(
    operations: &dyn storage_provider::DynamoOperations,
    table: &storage_types::TableName,
    key: &str,
) -> Option<serde_json::Value> {
    operations
        .get_item(
            storage_types::GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": key}}, "ConsistentRead": true
            }))
            .unwrap(),
        )
        .await
        .unwrap()
        .item
        .map(|item| {
            serde_json::to_value(storage_types::AttributeMap::try_from(&item).unwrap()).unwrap()
        })
}

// RK2: node A (independent cache) creates a table while node B holds a write
// on another table before its commit. B commits without a conflict, binds the
// new table, and after A recreates it writes only the new incarnation.
#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_create_table_on_other_node_when_write_is_in_flight_then_write_commits_without_conflict()
 {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let (store, probe) = held_commit_store().await;
    let node_a = Arc::clone(store.independent_operations.as_ref().unwrap());
    let warm = unique_table_name();
    store.create_hash_table(warm.clone()).await;
    store
        .operations
        .put_item(put(&warm, "warm", "warm"))
        .await
        .unwrap();
    let created = unique_table_name();
    probe.armed.store(true, Ordering::SeqCst);
    let operations = Arc::clone(&store.operations);
    let written = warm.clone();
    let write =
        tokio::spawn(async move { operations.put_item(put(&written, "held", "held")).await });
    let (arrived, released) = &probe.barriers[0];
    tokio::time::timeout(std::time::Duration::from_secs(30), arrived.wait())
        .await
        .expect("held write reached its commit");
    let other_begin = probe.event_count();
    node_a
        .create_table(super::test_support::hash_table_request(created.clone()))
        .await
        .unwrap();
    let other = other_begin..probe.event_count();
    released.wait().await;
    write.await.unwrap().unwrap();
    let (conflicts, attempts) = probe.conflicts_and_attempts(other);
    probe.armed.store(false, Ordering::SeqCst);
    println!("rk2_create_during_write conflicts={conflicts} native_attempts={attempts}");
    assert_eq!(
        conflicts, 0,
        "CreateTable on another node must not abort an in-flight write"
    );
    assert_eq!(
        consistent_value(node_a.as_ref(), &warm, "held").await,
        Some(json!({"pk": {"S": "held"}, "value": {"S": "held"}}))
    );
    // B binds the new table, which it never cached.
    store
        .operations
        .put_item(put(&created, "first", "first-incarnation"))
        .await
        .unwrap();
    assert_eq!(
        consistent_value(node_a.as_ref(), &created, "first").await,
        Some(json!({"pk": {"S": "first"}, "value": {"S": "first-incarnation"}}))
    );
    // A recreates the table; B's cached binding is the old incarnation.
    node_a
        .delete_table(storage_types::DeleteTableRequest {
            table_name: created.clone(),
        })
        .await
        .unwrap();
    node_a
        .create_table(super::test_support::hash_table_request(created.clone()))
        .await
        .unwrap();
    store
        .operations
        .put_item(put(&created, "second", "second-incarnation"))
        .await
        .unwrap();
    assert_eq!(
        consistent_value(node_a.as_ref(), &created, "second").await,
        Some(json!({"pk": {"S": "second"}, "value": {"S": "second-incarnation"}}))
    );
    assert_eq!(
        consistent_value(node_a.as_ref(), &created, "first").await,
        None,
        "the new incarnation holds no row of the old one"
    );
    assert_eq!(
        consistent_value(store.operations.as_ref(), &created, "second").await,
        Some(json!({"pk": {"S": "second"}, "value": {"S": "second-incarnation"}}))
    );
    store.finish().await;
}

// RK4: a SystemStream trim page scans with a snapshot, so appends at the live
// tail during the page do not abort it.
#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_appends_on_lineage_when_trim_page_commits_then_trim_has_no_conflict() {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let (store, probe) = held_commit_store().await;
    let node_a = Arc::clone(store.independent_operations.as_ref().unwrap());
    let table = unique_table_name();
    store.operations.create_table(storage_types::CreateTableRequest::try_from(json!({
        "TableName": table.as_ref(),
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    for key in ["one", "two", "three"] {
        store
            .operations
            .put_item(put(&table, key, "before"))
            .await
            .unwrap();
    }
    store.wait_until_changes_are_readable().await;
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let cutoff = store.now() + 60_000;
    probe.armed.store(true, Ordering::SeqCst);
    let repository = Arc::clone(&store.maintenance);
    let trim = tokio::spawn(async move {
        storage_provider::SystemStreamTrimExecutor::new(repository.as_ref(), 1)
            .run_page(scope, cutoff, None)
            .await
    });
    let (arrived, released) = &probe.barriers[0];
    tokio::time::timeout(std::time::Duration::from_secs(30), arrived.wait())
        .await
        .expect("trim page reached its commit");
    let other_begin = probe.event_count();
    for key in ["four", "five", "six", "seven"] {
        node_a.put_item(put(&table, key, "during")).await.unwrap();
    }
    let other = other_begin..probe.event_count();
    released.wait().await;
    let page = trim.await.unwrap().unwrap();
    let (conflicts, attempts) = probe.conflicts_and_attempts(other);
    probe.armed.store(false, Ordering::SeqCst);
    println!(
        "rk4_trim_page_with_appends conflicts={conflicts} native_attempts={attempts} reclaimed={}",
        page.reclaimed_records()
    );
    assert!(page.reclaimed_records() >= 1);
    assert_eq!(
        conflicts, 0,
        "appends at the live tail must not abort a trim page"
    );
    store.finish().await;
}

// RK4: the transact receipt sweep scans with a snapshot; new receipts written
// during the page do not abort it, and the expired receipt is deleted.
#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_new_receipts_when_receipt_sweep_page_commits_then_sweep_has_no_conflict() {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let (store, probe) = held_commit_store().await;
    let node_a = Arc::clone(store.independent_operations.as_ref().unwrap());
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    let transact = |token: &str, key: &str| {
        storage_types::TransactWriteItemsRequest::try_from(json!({
        "ClientRequestToken": token, "TransactItems": [{"Put": {"TableName": table, "Item": {"pk": {"S": key}}}}]
    })).unwrap()
    };
    store
        .operations
        .transact_write_items(transact("sweep-a-expired", "expired"))
        .await
        .unwrap();
    store.advance_time(std::time::Duration::from_secs(3600));
    probe.armed.store(true, Ordering::SeqCst);
    let repository = Arc::clone(&store.maintenance);
    let sweep = tokio::spawn(async move {
        repository
            .trim_transact_receipts_page(None, std::num::NonZeroU32::new(100).unwrap())
            .await
    });
    let (arrived, released) = &probe.barriers[0];
    tokio::time::timeout(std::time::Duration::from_secs(30), arrived.wait())
        .await
        .expect("receipt sweep reached its commit");
    let other_begin = probe.event_count();
    for token in ["sweep-b-new", "sweep-c-new", "sweep-d-new"] {
        node_a
            .transact_write_items(transact(token, token))
            .await
            .unwrap();
    }
    let other = other_begin..probe.event_count();
    released.wait().await;
    sweep.await.unwrap().unwrap();
    let (conflicts, attempts) = probe.conflicts_and_attempts(other);
    probe.armed.store(false, Ordering::SeqCst);
    let deletes = probe
        .events
        .lock()
        .unwrap()
        .iter()
        .map(|(_, _, event)| match event {
            kv_backend::KvNativeEvent::Mutations { deletes, .. } => *deletes,
            _ => 0,
        })
        .sum::<usize>();
    println!(
        "rk4_receipt_sweep_with_writes conflicts={conflicts} native_attempts={attempts} \
         deletes={deletes}"
    );
    assert_eq!(
        conflicts, 0,
        "new receipts must not abort a receipt sweep page"
    );
    assert!(deletes >= 1, "the sweep deletes the expired receipt");
    store.finish().await;
}

// RK1: one BatchGetItem of 100 keys submits every point read in one wave.
#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore = "requires selected native provider fixture"]
async fn given_hundred_keys_when_batch_get_runs_then_item_reads_are_one_wave() {
    if TestStore::selected_backend() != "foundationdb" {
        return;
    }
    let (store, probe) = held_commit_store().await;
    let table = unique_table_name();
    store.create_hash_table(table.clone()).await;
    let keys = (0..100)
        .map(|index| json!({"pk": {"S": format!("key-{index:03}")}}))
        .collect::<Vec<_>>();
    let request = || {
        storage_types::BatchGetItemRequest::try_from(json!({
            "RequestItems": {table.as_ref(): {"Keys": keys}}
        }))
        .unwrap()
    };
    store.operations.batch_get_item(request()).await.unwrap();
    probe.armed.store(true, Ordering::SeqCst);
    store.operations.batch_get_item(request()).await.unwrap();
    probe.armed.store(false, Ordering::SeqCst);
    let waves = probe
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(_, _, event)| match event {
            kv_backend::KvNativeEvent::Wave { kind, keys, .. } => Some((*kind, *keys)),
            _ => None,
        })
        .collect::<Vec<_>>();
    println!("rk1_batch_get_100 waves={} detail={waves:?}", waves.len());
    let item_waves = waves
        .iter()
        .filter(|(kind, keys)| *kind == kv_backend::KvWaveKind::GetBatch && *keys > 1)
        .collect::<Vec<_>>();
    // PERF-FDB-1: the catalog-version key rides in the item wave.
    assert_eq!(
        item_waves,
        vec![&(kv_backend::KvWaveKind::GetBatch, 101)],
        "all 100 item reads and the catalog version in one wave: {waves:?}"
    );
    assert_eq!(waves.len(), 2, "read version and one read wave: {waves:?}");
    store.finish().await;
}
