use async_trait::async_trait;
use serde_json::json;
use storage_observability_test_support::observation::{Observation, observe};
use storage_provider::{CachePublication, ReadSequenceStepReader, read_sequence_with_reader};
use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetItemResponse, GetItemRequest, GetItemResponse,
    KeyAttributes, QueryRequest, QueryResponse, ReadSequenceNode, ReadSequenceNodeOperation,
    ReadSequenceRequest, StorageEnum, StorageError, StorageResult, TableName,
    context::WrappedError as _,
};
use tracing::Level;

#[derive(Clone, Copy)]
enum MockFailure {
    NativeIo,
    BusyTransaction,
}

impl MockFailure {
    fn error(self) -> StorageError {
        match self {
            Self::NativeIo => StorageError::internal("SQLite native operation failed"),
            Self::BusyTransaction => StorageEnum::TransactionConflict {
                message: "SQLite transaction was busy or locked".to_owned(),
            }
            .into(),
        }
    }
}

struct FailingReader {
    calls: usize,
    failure: MockFailure,
}

#[async_trait]
impl ReadSequenceStepReader for FailingReader {
    async fn get(
        &mut self,
        _request: GetItemRequest,
    ) -> StorageResult<(GetItemResponse, Option<CachePublication>)> {
        self.calls += 1;
        Err(self.failure.error())
    }

    async fn batch_get_once(
        &mut self,
        _request: BatchGetItemRequest,
    ) -> StorageResult<(BatchGetItemResponse, Vec<CachePublication>)> {
        self.calls += 1;
        Err(self.failure.error())
    }

    async fn query(
        &mut self,
        _request: QueryRequest,
    ) -> StorageResult<(
        QueryResponse,
        Option<Vec<KeyAttributes>>,
        Option<CachePublication>,
    )> {
        self.calls += 1;
        Err(self.failure.error())
    }
}

fn get_request() -> ReadSequenceRequest {
    ReadSequenceRequest::new(vec![ReadSequenceNode::new(
        "root",
        ReadSequenceNodeOperation::Get(Box::new(GetItemRequest::new(
            TableName::new("items"),
            [("pk".to_owned(), AttributeValue::S("one".to_owned()))],
        ))),
    )])
}

async fn run_failure(failure: MockFailure) -> (usize, Observation<StorageError>) {
    run_request_failure(failure, get_request()).await
}

async fn run_request_failure(
    failure: MockFailure,
    request: ReadSequenceRequest,
) -> (usize, Observation<StorageError>) {
    let mut reader = FailingReader { calls: 0, failure };
    let observed = observe(async {
        read_sequence_with_reader(&mut reader, request, Default::default())
            .await
            .err()
            .unwrap()
    })
    .await;
    (reader.calls, observed)
}

/// Provider failures retain bounded diagnostics without customer identifiers.
#[tokio::test]
async fn given_batch_get_provider_failure_when_read_sequence_runs_then_trace_omits_identifiers_and_keys()
 {
    let request = ReadSequenceRequest::try_from(json!({
        "Nodes": [{"Name": "batch", "Operation": {"BatchGet": {
            "RequestItems": {
                "items": {"Keys": [{"pk": {"S": "private-key-one"}}]},
                "other_items": {"Keys": [{"pk": {"S": "private-key-two"}}]}
            }
        }}}],
        "Outputs": ["batch"]
    }))
    .unwrap();
    let (calls, observed) = run_request_failure(MockFailure::NativeIo, request).await;

    assert_eq!(calls, 1);
    observed.assert_database_calls_exact(&[("read_sequence.batch_get_item", 1)]);
    assert_eq!(
        observed.result.to_string(),
        "DynamoDB could not process your request."
    );
    observed.assert_only_event_exact(
        Level::WARN,
        &[
            ("operation", "read_sequence.batch_get_item"),
            ("error_kind", "internal_server_error"),
            ("message", "read sequence provider read failed"),
        ],
    );
}

/// Provider failures retain bounded diagnostics without customer identifiers.
#[tokio::test]
async fn given_query_provider_failure_when_read_sequence_runs_then_trace_omits_identifiers_and_values()
 {
    let request = ReadSequenceRequest::try_from(json!({
        "Nodes": [{"Name": "query", "Operation": {"Query": {
            "TableName": "items",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "private-partition"}}
        }}}],
        "Outputs": ["query"]
    }))
    .unwrap();
    let (calls, observed) = run_request_failure(MockFailure::BusyTransaction, request).await;

    assert_eq!(calls, 1);
    observed.assert_database_calls_exact(&[("read_sequence.query_table", 1)]);
    assert_eq!(
        observed.result.to_string(),
        "Operation was rejected because there is an ongoing transaction for the item."
    );
    observed.assert_only_event_exact(
        Level::WARN,
        &[
            ("operation", "read_sequence.query_table"),
            ("error_kind", "transaction_conflict"),
            ("message", "read sequence provider read failed"),
        ],
    );
}

/// Negative and implementation rules: one native failure has one metered call
/// and safe trace.
#[tokio::test]
async fn given_provider_io_failure_when_read_sequence_runs_then_metric_and_trace_identify_failure()
{
    let (calls, observed) = run_failure(MockFailure::NativeIo).await;

    assert_eq!(calls, 1);
    observed.assert_database_calls_exact(&[("read_sequence.get_item", 1)]);
    assert_eq!(
        observed.result.to_string(),
        "DynamoDB could not process your request."
    );
    assert!(
        matches!(observed.result.to_enum(), StorageEnum::InternalServerError { message }
        if message == "SQLite native operation failed")
    );
    observed.assert_only_event_exact(
        Level::WARN,
        &[
            ("operation", "read_sequence.get_item"),
            ("error_kind", "internal_server_error"),
            ("message", "read sequence provider read failed"),
        ],
    );
}

/// Provider failures retain bounded diagnostics without customer identifiers.
#[tokio::test]
async fn given_sqlite_busy_failure_when_read_sequence_runs_then_trace_distinguishes_conflict() {
    let (calls, observed) = run_failure(MockFailure::BusyTransaction).await;

    assert_eq!(calls, 1);
    observed.assert_database_calls_exact(&[("read_sequence.get_item", 1)]);
    assert_eq!(
        observed.result.to_string(),
        "Operation was rejected because there is an ongoing transaction for the item."
    );
    observed.assert_only_event_exact(
        Level::WARN,
        &[
            ("operation", "read_sequence.get_item"),
            ("error_kind", "transaction_conflict"),
            ("message", "read sequence provider read failed"),
        ],
    );
}

/// Implementation and negative rules: a spawned read owns its own observation
/// scope.
#[tokio::test]
async fn given_background_read_failure_when_tasks_run_then_each_observation_has_its_own_cost_and_trace()
 {
    let (foreground, background) = tokio::join!(
        run_failure(MockFailure::NativeIo),
        tokio::spawn(run_failure(MockFailure::BusyTransaction)),
    );
    let (foreground_calls, foreground) = foreground;
    let (background_calls, background) = background.unwrap();

    assert_eq!(foreground_calls, 1);
    assert_eq!(background_calls, 1);
    foreground.assert_database_calls_exact(&[("read_sequence.get_item", 1)]);
    background.assert_database_calls_exact(&[("read_sequence.get_item", 1)]);
    assert_eq!(
        foreground.result.to_string(),
        "DynamoDB could not process your request."
    );
    assert_eq!(
        background.result.to_string(),
        "Operation was rejected because there is an ongoing transaction for the item."
    );
    foreground.assert_only_event_exact(
        Level::WARN,
        &[
            ("operation", "read_sequence.get_item"),
            ("error_kind", "internal_server_error"),
            ("message", "read sequence provider read failed"),
        ],
    );
    background.assert_only_event_exact(
        Level::WARN,
        &[
            ("operation", "read_sequence.get_item"),
            ("error_kind", "transaction_conflict"),
            ("message", "read sequence provider read failed"),
        ],
    );
}
