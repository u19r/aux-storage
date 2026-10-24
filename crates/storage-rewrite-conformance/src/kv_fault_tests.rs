//! Customer-visible KV faults through the real replacement provider stack.

use std::sync::{Arc, Mutex};

use kv_backend::{KvFaultCall, KvFaultOperation, KvFaultProbe};
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{GetItemRequest, PutItemRequest, StorageError};
use tracing::Level;

use super::test_support::{TestStore, assert_dynamo_error_response_exact};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultEffect {
    Error,
    WrongCount,
    MissingMetadata,
    CorruptMetadata,
    StageError,
}

#[derive(Default)]
struct ReadFaultProbe(Mutex<(Option<FaultEffect>, usize, usize)>);

impl ReadFaultProbe {
    fn arm(&self, effect: FaultEffect) {
        *self.0.lock().unwrap() = (Some(effect), 0, 0);
    }

    fn fired_once(&self) {
        assert_eq!(self.0.lock().unwrap().0, None);
        assert_eq!(self.0.lock().unwrap().1, 1);
    }
}

impl KvFaultProbe for ReadFaultProbe {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError> {
        if call.operation != KvFaultOperation::GetBatch && call.operation != KvFaultOperation::Stage
        {
            return None;
        }
        let mut state = self.0.lock().unwrap();
        let triggered = matches!(
            (state.0, call.operation),
            (Some(FaultEffect::Error), KvFaultOperation::GetBatch)
                | (Some(FaultEffect::StageError), KvFaultOperation::Stage)
        );
        if !triggered {
            return None;
        }
        state.0 = None;
        state.1 += 1;
        Some(StorageError::internal(
            if call.operation == KvFaultOperation::Stage {
                "synthetic native KV write failure"
            } else {
                "synthetic native KV read failure"
            },
        ))
    }

    fn alter_get_batch(
        &self,
        call: KvFaultCall,
        values: Vec<Option<Vec<u8>>>,
    ) -> Vec<Option<Vec<u8>>> {
        if call.operation != KvFaultOperation::GetBatch {
            return values;
        }
        let mut state = self.0.lock().unwrap();
        state.2 += 1;
        let triggered = match state.0 {
            Some(FaultEffect::WrongCount) if state.2 == 1 => Some(Vec::new()),
            Some(FaultEffect::MissingMetadata) if state.2 == 2 => Some(vec![None]),
            Some(FaultEffect::CorruptMetadata) if state.2 == 2 => {
                Some(vec![Some(b"corrupt catalog metadata\0".to_vec())])
            }
            _ => None,
        };
        if let Some(replacement) = triggered {
            state.0 = None;
            state.1 += 1;
            replacement
        } else {
            values
        }
    }
}

#[tokio::test]
async fn given_standard_table_when_native_kv_read_fails_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(FaultEffect::Error, "synthetic native KV read failure").await;
}

#[tokio::test]
async fn given_standard_table_when_native_kv_read_has_wrong_count_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(
        FaultEffect::WrongCount,
        "native KV batch returned the wrong count",
    )
    .await;
}

#[tokio::test]
async fn given_standard_table_when_kv_metadata_is_missing_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(
        FaultEffect::MissingMetadata,
        "KV name lookup points to missing metadata",
    )
    .await;
}

#[tokio::test]
async fn given_standard_table_when_kv_metadata_is_corrupt_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(FaultEffect::CorruptMetadata, "KV table metadata is corrupt").await;
}

#[tokio::test]
async fn given_standard_table_when_native_kv_stage_fails_then_customer_error_is_logged_and_write_rolls_back()
 {
    let probe = Arc::new(ReadFaultProbe::default());
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let table = store.create_standard_table().await;
    probe.arm(FaultEffect::StageError);
    let request = PutItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Item": {"pk": {"S": "private-customer-key"}, "v": {"S": "private-value"}}
    }))
    .unwrap();
    let observed = observe(store.operations.put_item(request)).await;
    let warning =
        observed.only_warning_or_error_in_span("dynamo_operation", &[("operation", "PutItem")]);
    assert_eq!(warning.level, Level::WARN);
    assert_eq!(warning.fields.get("operation").unwrap(), "kv.stage");
    assert_eq!(
        warning.fields.get("error_code").unwrap(),
        "InternalServerError"
    );
    assert_eq!(
        warning.fields.get("diagnostic").unwrap(),
        "synthetic native KV write failure"
    );
    assert!(!format!("{:?}", observed.events).contains("private-customer-key"));
    assert!(!format!("{:?}", observed.events).contains("private-value"));
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
    );
    probe.fired_once();
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let readback = observe(store.operations.get_item(request)).await;
    readback.assert_no_warning_or_error();
    assert_eq!(
        serde_json::to_value(readback.result.unwrap()).unwrap(),
        json!({})
    );
    store.finish().await;
}

async fn assert_kv_fault(effect: FaultEffect, diagnostic: &str) {
    let probe = Arc::new(ReadFaultProbe::default());
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let table = store.create_standard_table().await;
    probe.arm(effect);
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let observed = observe(store.operations.get_item(request)).await;
    let warning =
        observed.only_warning_or_error_in_span("dynamo_operation", &[("operation", "GetItem")]);
    assert_eq!(warning.level, Level::WARN);
    let backend = match std::env::var("STORAGE_TEST_BACKEND").ok().as_deref() {
        Some("foundationdb") => "foundationdb",
        Some("rocksdb") => "rocksdb",
        _ if cfg!(feature = "rocksdb") => "rocksdb",
        _ => "foundationdb",
    };
    assert_eq!(warning.fields.get("backend").unwrap(), backend);
    let operation = match effect {
        FaultEffect::MissingMetadata => "catalog.read_table",
        FaultEffect::CorruptMetadata => "catalog.bind_metadata",
        _ => "kv.get_batch",
    };
    assert_eq!(warning.fields.get("operation").unwrap(), operation);
    assert_eq!(
        warning.fields.get("error_code").unwrap(),
        "InternalServerError"
    );
    assert_eq!(warning.fields.get("diagnostic").unwrap(), diagnostic);
    assert_eq!(
        warning.fields.get("message").unwrap(),
        "native storage operation failed"
    );
    assert_eq!(
        warning.fields.len(),
        5,
        "no key or item data in the warning"
    );
    assert!(!format!("{:?}", observed.events).contains("private-customer-key"));
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
    );
    probe.fired_once();
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let readback = observe(store.operations.get_item(request)).await;
    readback.assert_no_warning_or_error();
    assert_eq!(
        serde_json::to_value(readback.result.unwrap()).unwrap(),
        json!({})
    );
    store.finish().await;
}
