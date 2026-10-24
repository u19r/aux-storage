//! Native SQL failures observed through complete customer operations.

use std::{
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{Arc, Mutex},
};

use futures::FutureExt as _;
use serde_json::json;
use sql_backend::{SqlFaultCall, SqlFaultOperation, SqlFaultProbe, SqlQueryPage, SqlRow, SqlValue};
use storage_observability_test_support::observation::observe;
use storage_types::{
    DeleteItemRequest, GetItemRequest, PutItemRequest, StorageError, StorageResult,
    UpdateItemRequest,
};

use super::test_support::{TestStore, assert_dynamo_error_response_exact};

#[derive(Default)]
struct FaultState {
    target: Option<usize>,
    calls: Vec<(SqlFaultOperation, String)>,
    fired: usize,
    corrupt_catalog: bool,
    corrupt_image: bool,
}

#[derive(Default)]
struct NativeFaultProbe(Mutex<FaultState>);

impl NativeFaultProbe {
    fn arm(&self, target: Option<usize>) {
        *self.0.lock().unwrap() = FaultState {
            target,
            ..FaultState::default()
        };
    }

    fn disarm(&self) -> FaultState {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl SqlFaultProbe for NativeFaultProbe {
    fn fail(&self, call: SqlFaultCall<'_>) -> Option<StorageError> {
        let mut state = self.0.lock().unwrap();
        let ordinal = state.calls.len();
        state.calls.push((call.operation, call.sql.to_owned()));
        if state.target == Some(ordinal) {
            state.fired += 1;
            Some(StorageError::internal("synthetic native SQL failure"))
        } else {
            None
        }
    }

    fn alter_item_image(
        &self,
        _protection: sql_backend::ReadProtection,
        item: storage_types::WireItem,
    ) -> storage_types::WireItem {
        let mut state = self.0.lock().unwrap();
        if !state.corrupt_image {
            return item;
        }
        state.corrupt_image = false;
        state.fired += 1;
        storage_types::WireItem::dynamo_json(br#"{"pk":{"S":"foreign"}}"#.to_vec())
    }

    fn alter_query_page(&self, call: SqlFaultCall<'_>, mut page: SqlQueryPage) -> SqlQueryPage {
        let mut state = self.0.lock().unwrap();
        if state.corrupt_catalog
            && call
                .sql
                .starts_with("SELECT id, catalog_revision FROM tables")
        {
            state.corrupt_catalog = false;
            state.fired += 1;
            let row = page.rows.first_mut().expect("catalog observation exists");
            let mut values = std::mem::replace(row, SqlRow::new(Vec::new())).into_values();
            values[1] = SqlValue::Text("invalid catalog revision".into());
            *row = SqlRow::new(values);
        }
        page
    }
}

async fn perform_operation(store: &TestStore, action: &str, table: &str) -> StorageResult<()> {
    match action {
        "get_missing" | "get_existing" => {
            let request = GetItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "customer-1"}}
            }))
            .unwrap();
            store.operations.get_item(request).await.map(|_| ())
        }
        "put_new" | "put_overwrite" => {
            let request = PutItemRequest::try_from(json!({
                "TableName": table,
                "Item": {"pk": {"S": "customer-1"}, "v": {"S": "private-current-value"}}
            }))
            .unwrap();
            store.operations.put_item(request).await.map(|_| ())
        }
        "delete_existing" | "delete_missing" => {
            let request = DeleteItemRequest::try_from(json!({
                "TableName": table, "Key": {"pk": {"S": "customer-1"}}
            }))
            .unwrap();
            store.operations.delete_item(request).await.map(|_| ())
        }
        "update_existing" | "update_missing" => {
            let request = UpdateItemRequest::try_from(json!({
                "TableName": table,
                "Key": {"pk": {"S": "customer-1"}},
                "UpdateExpression": "SET #v = :value",
                "ExpressionAttributeNames": {"#v": "v"},
                "ExpressionAttributeValues": {":value": {"S": "private-current-value"}}
            }))
            .unwrap();
            store.operations.update_item(request).await.map(|_| ())
        }
        other => panic!("unknown fault action: {other}"),
    }
}

async fn prepare_action(store: &TestStore, action: &str) -> storage_types::TableName {
    let table = store.create_standard_table().await;
    if has_existing_item(action) {
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"S": "customer-1"}, "v": {"S": "private-before-value"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    table
}

fn has_existing_item(action: &str) -> bool {
    matches!(
        action,
        "delete_existing" | "get_existing" | "put_overwrite" | "update_existing"
    )
}

async fn assert_action_faults(action: &str) {
    let probe = Arc::new(NativeFaultProbe::default());
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = prepare_action(&store, action).await;
        probe.arm(None);
        perform_operation(&store, action, table.as_ref()).await.unwrap();
        let successful = probe.disarm().calls;
        assert!(!successful.is_empty(), "operation must exercise native calls: {action}");
        for (ordinal, (expected_operation, _)) in successful.into_iter().enumerate() {
            let table = prepare_action(&store, action).await;
            probe.arm(Some(ordinal));
            let observed = observe(perform_operation(&store, action, table.as_ref())).await;
            let recorded = probe.disarm();
            assert_eq!(recorded.fired, 1, "fault must fire: {action} call {ordinal}");
            assert_eq!(recorded.calls[ordinal].0, expected_operation, "native call order changed: {action}");
            let trace = format!("{:?} {:?}", observed.events, observed.spans);
            for private in ["customer-1", "private-before-value", "private-current-value"] {
                assert!(!trace.contains(private), "private customer content in trace: {action}");
            }
            assert_dynamo_error_response_exact(
                observed.result.expect_err(&format!("native failure was ignored: {action} call {ordinal}: {:?}", recorded.calls[ordinal])),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
            );
            let read = GetItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"S": "customer-1"}}
            })).unwrap();
            let readback = store.operations.get_item(read).await.unwrap();
            let expected = if has_existing_item(action) {
                json!({"Item": {"pk": {"S": "customer-1"}, "v": {"S": "private-before-value"}}})
            } else { json!({}) };
            assert_eq!(serde_json::to_value(readback).unwrap(), expected, "atomic customer state: {action} call {ordinal}");
        }
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

macro_rules! fault_flow {
    ($name:ident, $action:literal) => {
        #[tokio::test]
        async fn $name() {
            assert_action_faults($action).await;
        }
    };
}

fault_flow!(
    given_missing_item_when_get_item_reads_fail_then_errors_and_state_are_exact,
    "get_missing"
);
fault_flow!(
    given_existing_item_when_get_item_reads_fail_then_errors_and_state_are_exact,
    "get_existing"
);
fault_flow!(
    given_new_item_when_put_item_calls_fail_then_errors_and_state_are_exact,
    "put_new"
);
fault_flow!(
    given_existing_item_when_put_item_calls_fail_then_errors_and_state_are_exact,
    "put_overwrite"
);
fault_flow!(
    given_missing_item_when_delete_item_calls_fail_then_errors_and_state_are_exact,
    "delete_missing"
);
fault_flow!(
    given_existing_item_when_delete_item_calls_fail_then_errors_and_state_are_exact,
    "delete_existing"
);
fault_flow!(
    given_missing_item_when_update_item_calls_fail_then_errors_and_state_are_exact,
    "update_missing"
);
fault_flow!(
    given_existing_item_when_update_item_calls_fail_then_errors_and_state_are_exact,
    "update_existing"
);

#[tokio::test]
async fn given_corrupt_catalog_observation_when_get_item_runs_then_error_and_state_are_exact() {
    let probe = Arc::new(NativeFaultProbe::default());
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = prepare_action(&store, "get_existing").await;
        probe.arm(None);
        probe.0.lock().unwrap().corrupt_catalog = true;
        let result = perform_operation(&store, "get_existing", table.as_ref()).await;
        assert_eq!(probe.disarm().fired, 1, "catalog corruption must reach decoding");
        assert_dynamo_error_response_exact(result.unwrap_err(), axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#);
        let request = GetItemRequest::try_from(json!({"TableName": table.as_ref(), "Key": {"pk": {"S": "customer-1"}}})).unwrap();
        assert_eq!(serde_json::to_value(store.operations.get_item(request).await.unwrap()).unwrap(),
            json!({"Item": {"pk": {"S": "customer-1"}, "v": {"S": "private-before-value"}}}));
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

#[tokio::test]
async fn given_a_hydrated_sql_image_when_primary_key_disagrees_then_read_fails_without_mutation() {
    let probe = Arc::new(NativeFaultProbe::default());
    let store = TestStore::new_sql_with_fault_probe(probe.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let table = prepare_action(&store, "get_existing").await;
        probe.arm(None);
        probe.0.lock().unwrap().corrupt_image = true;
        let result = perform_operation(&store, "get_existing", table.as_ref()).await;
        assert_eq!(probe.disarm().fired, 1, "fault must reach hydrated content");
        assert_dynamo_error_response_exact(result.unwrap_err(), axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#);
        let request = GetItemRequest::try_from(json!({"TableName": table.as_ref(), "Key": {"pk": {"S": "customer-1"}}})).unwrap();
        assert_eq!(serde_json::to_value(store.operations.get_item(request).await.unwrap()).unwrap(),
            json!({"Item": {"pk": {"S": "customer-1"}, "v": {"S": "private-before-value"}}}));
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
