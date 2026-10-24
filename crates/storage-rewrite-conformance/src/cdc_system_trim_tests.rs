//! Bounded SystemStream reclamation through the replacement provider.

use std::{
    collections::BTreeSet,
    panic::{AssertUnwindSafe, resume_unwind},
};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_provider::SystemStreamTrimExecutor;
use storage_types::{
    CreateTableRequest, PutItemRequest, SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, StreamName,
    SystemStreamScope,
};
use stream_provider::{
    StreamRowKind, SystemStreamPageRequest, SystemStreamTrimContinuation,
    SystemStreamTrimOperationId,
};

use super::test_support::{TestStore, assert_dynamo_error_response_exact, unique_table_name};

// Boundary and implementation rules from the legacy SQL SystemStream trim
// tests: bounded reclamation replays receipts and preserves issued checkpoints.
#[tokio::test]
async fn given_issued_cdc_checkpoint_when_system_stream_is_trimmed_then_pages_replay_and_checkpoint_survives()
 {
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
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        for key in ["first", "second", "third"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .unwrap();
            let written = observe(store.operations.put_item(put)).await;
            written.assert_no_warning_or_error();
            written.result.unwrap();
        }

        let issued = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 3).unwrap(),
        ))
        .await;
        issued.assert_no_warning_or_error();
        let issued = issued.result.unwrap();
        assert_eq!(issued.records().len(), 3);
        assert!(issued.terminal());
        let checkpoint = issued.issued_checkpoint().token().clone();
        let cutoff = store.now() + 60_000;
        let worker = SystemStreamTrimExecutor::new(store.system_stream_trim_repository.as_ref(), 1);

        let first = observe(worker.run_page(scope.clone(), cutoff, None)).await;
        first.assert_no_warning_or_error();
        let first = first.result.unwrap();
        assert_eq!(first.reclaimed_records(), 1);
        let continuation = first.next_continuation().cloned().unwrap();

        let replay = observe(worker.run_page(scope.clone(), cutoff, None)).await;
        replay.assert_no_warning_or_error();
        assert_eq!(replay.result.unwrap(), first);

        let second = observe(worker.run_page(scope.clone(), cutoff, Some(continuation))).await;
        second.assert_no_warning_or_error();
        let second = second.result.unwrap();
        assert_eq!(second.reclaimed_records(), 1);
        let continuation = second.next_continuation().cloned().unwrap();

        let terminal = observe(worker.run_page(scope.clone(), cutoff, Some(continuation))).await;
        terminal.assert_no_warning_or_error();
        let terminal = terminal.result.unwrap();
        assert_eq!(terminal.reclaimed_records(), 0);
        assert!(terminal.next_continuation().is_none());

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint.clone()), 3).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert!(resumed.records().is_empty());
        assert!(resumed.terminal());
        assert_eq!(resumed.issued_checkpoint().token(), &checkpoint);
        let parent = store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap()
            .into_iter()
            .find(|lineage| !lineage.sealed())
            .unwrap();
        let children = ["trim-left", "trim-right"].map(|id| {
            stream_provider::SystemStreamSplitChild::try_new(
                stream_provider::SystemStreamLineage::try_new(
                    id,
                    id,
                    Some(parent.lineage_id().as_str().to_owned()),
                    false,
                )
                .unwrap(),
                1,
            )
            .unwrap()
        });
        let split = stream_provider::SystemStreamSplitRequest::try_new(
            scope.clone(),
            parent,
            children,
            stream_provider::SystemStreamSplitOperationId::try_new("trim-split").unwrap(),
        )
        .unwrap();
        store
            .stream_operations
            .split_system_stream(split)
            .await
            .unwrap();
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(), "Item": {"pk": {"S": "after-trim-split"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        let page = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope, Some(checkpoint), 3).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(page.records().len(), 1);
        assert!(page.terminal());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Adversarial and negative rules: a continuation is valid only when it is the
// exact issued output of the preceding committed trim page.
#[tokio::test]
async fn given_tampered_trim_continuation_when_page_is_requested_then_validation_rejects_and_rows_survive()
 {
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
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        for key in ["first", "second"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        let issued = store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ).await.unwrap();
        assert_eq!(issued.records().len(), 2);

        let cutoff = store.now() + 60_000;
        let worker = SystemStreamTrimExecutor::new(
            store.system_stream_trim_repository.as_ref(),
            1,
        );
        let first = worker.run_page(scope.clone(), cutoff, None).await.unwrap();
        assert_eq!(first.reclaimed_records(), 1);
        let continuation = first.next_continuation().cloned().unwrap();
        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(stream.clone(), None, 10).await.unwrap();
        assert_eq!(before.items.len(), 1);

        let tampered = SystemStreamTrimContinuation::try_new(
            SystemStreamTrimOperationId::new(&scope, cutoff),
            continuation.sequence(),
            b"tampered-cursor",
        )
        .unwrap();
        let failure = observe(worker.run_page(scope.clone(), cutoff, Some(tampered))).await;
        assert!(!format!("{:?}{:?}", failure.events, failure.spans).contains("tampered-cursor"));
        assert_dynamo_error_response_exact(
            failure.result.unwrap_err(),
            axum::http::StatusCode::BAD_REQUEST,
            r#"{"__type":"com.amazon.coral.validate#ValidationException","message":"system stream trim continuation does not match preceding page"}"#,
        );

        let after = store.stream_operations.read_forward(stream, None, 10).await.unwrap();
        assert_eq!(after.items.len(), before.items.len());
        assert_eq!(after.items[0].id, before.items[0].id);
        assert_eq!(after.items[0].data, before.items[0].data);
        assert_eq!(after.items[0].data_type, before.items[0].data_type);
        let valid = worker.run_page(scope, cutoff, Some(continuation)).await.unwrap();
        assert_eq!(valid.reclaimed_records(), 0);
        assert!(valid.next_continuation().is_none());
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Validation and negative rules: a destructive trim request cannot claim a
// different logical source scope and must leave every committed row available.
#[tokio::test]
async fn given_wrong_source_scope_when_system_cdc_trim_runs_then_history_is_not_reclaimed() {
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
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        for key in ["first", "second"] {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": key}}
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        let issued = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ))
        .await;
        issued.assert_no_warning_or_error();
        assert_eq!(issued.result.unwrap().records().len(), 2);

        let stream = StreamRowKind::System(StreamName::system_table_stream());
        let before = store.stream_operations.read_forward(stream.clone(), None, 10).await.unwrap();
        assert_eq!(before.items.len(), 2);
        let worker = SystemStreamTrimExecutor::new(
            store.system_stream_trim_repository.as_ref(),
            1,
        );
        let cutoff = store.now() + 60_000;
        let wrong_scope = SystemStreamScope::try_new("foreign-source-scope", scope.generation()).unwrap();
        let failure = observe(worker.run_page(wrong_scope, cutoff, None)).await;
        assert!(!format!("{:?}{:?}", failure.events, failure.spans).contains("foreign-source-scope"));
        assert_dynamo_error_response_exact(
            failure.result.unwrap_err(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
        );

        let after = store.stream_operations.read_forward(stream, None, 10).await.unwrap();
        assert_eq!(after.items.len(), before.items.len());
        assert_eq!(after.items[0].id, before.items[0].id);
        assert_eq!(after.items[1].id, before.items[1].id);
        let valid = observe(worker.run_page(scope, cutoff, None)).await;
        valid.assert_no_warning_or_error();
        assert_eq!(valid.result.unwrap().reclaimed_records(), 1);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Boundary rule from the legacy SQL trim test: a large number of issued
// receipts must not cap protected-frontier derivation or invalidate a resume.
#[tokio::test]
async fn given_more_than_checkpoint_entry_bound_when_system_cdc_is_trimmed_then_latest_receipt_replays()
 {
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
        .unwrap();
        store.operations.create_table(create).await.unwrap();
        let scope = store.stream_operations.system_stream_scope().await.unwrap();

        for index in 0..2 {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": format!("checkpoint-{index}")}}
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        let first = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), None, 2).unwrap(),
        ))
        .await;
        first.assert_no_warning_or_error();
        let first = first.result.unwrap();
        assert_eq!(first.records().len(), 2);
        assert!(first.terminal());
        let first_checkpoint = first.issued_checkpoint().token().clone();
        let mut checkpoint = first_checkpoint.clone();
        let mut issued = BTreeSet::from([checkpoint.as_str().to_owned()]);

        for index in 2..SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES + 2 {
            let put = PutItemRequest::try_from(json!({
                "TableName": table.as_ref(),
                "Item": {"pk": {"S": format!("checkpoint-{index}")}}
            }))
            .unwrap();
            store.operations.put_item(put).await.unwrap();
        }
        for _ in 0..SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
            let page = store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope.clone(), Some(checkpoint), 1).unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(page.records().len(), 1);
            checkpoint = page.issued_checkpoint().token().clone();
            assert!(issued.insert(checkpoint.as_str().to_owned()));
        }
        assert_eq!(issued.len(), SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES + 1);

        let worker = SystemStreamTrimExecutor::new(store.system_stream_trim_repository.as_ref(), 1);
        let cutoff = store.now() + 60_000;
        let first_trim = observe(worker.run_page(scope.clone(), cutoff, None)).await;
        first_trim.assert_no_warning_or_error();
        let first_trim = first_trim.result.unwrap();
        assert_eq!(first_trim.reclaimed_records(), 1);
        let mut continuation = first_trim.next_continuation().cloned();
        let mut reclaimed = first_trim.reclaimed_records();
        let mut pages = 1;
        while let Some(next) = continuation {
            let trimmed = observe(worker.run_page(scope.clone(), cutoff, Some(next))).await;
            trimmed.assert_no_warning_or_error();
            let trimmed = trimmed.result.unwrap();
            reclaimed += trimmed.reclaimed_records();
            continuation = trimmed.next_continuation().cloned();
            pages += 1;
            assert!(pages <= SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES + 2);
        }
        assert_eq!(reclaimed, 1, "later checkpoint frontiers remain protected");

        let early = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope.clone(), Some(first_checkpoint), 1).unwrap(),
        ))
        .await;
        early.assert_no_warning_or_error();
        let early = early.result.unwrap();
        assert_eq!(early.records().len(), 1);
        assert_eq!(
            serde_json::to_value(&early.records()[0].keys).unwrap(),
            json!({"pk": {"S": "checkpoint-2"}}),
        );

        let resumed = observe(store.stream_operations.get_system_stream_page(
            SystemStreamPageRequest::try_new(scope, Some(checkpoint.clone()), 1).unwrap(),
        ))
        .await;
        resumed.assert_no_warning_or_error();
        let resumed = resumed.result.unwrap();
        assert!(resumed.records().is_empty());
        assert!(resumed.terminal());
        assert_eq!(resumed.issued_checkpoint().token(), &checkpoint);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
