//! DescribeTable public errors from the authoritative DynamoDB fixture suite.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{
    CreateTableRequest, DeleteTableRequest, DescribeTableRequest, ListTablesRequest, QueryRequest,
    StorageResult, UpdateTableRequest,
};

use super::test_support::{ConformanceFixtures, TestStore, assert_conformance_body};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

/// Runs the `steps` of a table-shape case (from the AWS P13 probe log) and
/// compares each body exactly after the volatile fields are replaced:
/// `<<table-id>>`, `<<creation-time>>`, `<<table-arn>>`, `<<stream-label>>`.
/// The aux extension `MaxIndexers` is removed. Capacity is intentionally
/// unsupported, so the expected bodies carry no `ProvisionedThroughput` and
/// the fixed on-demand `BillingModeSummary`; AWS `WarmThroughput` is out of
/// scope and absent.
pub(crate) async fn run_table_steps(store: &TestStore, case: &Value) {
    let source = case["source"].as_str().expect("fixture source");
    for (position, step) in case["steps"].as_array().expect("steps").iter().enumerate() {
        let label = format!("{source} step {position}");
        let request = step["request"].clone();
        let outcome = match step["operation"].as_str().expect("step operation") {
            "CreateTable" => match CreateTableRequest::try_from(request) {
                Ok(request) => response_body(
                    observe(async {
                        json(
                            store.operations.create_table(request).await,
                            serde_json::to_value,
                        )
                    })
                    .await,
                ),
                Err(message) => request_error(message),
            },
            "UpdateTable" => match UpdateTableRequest::try_from(request) {
                Ok(request) => response_body(
                    observe(async {
                        json(
                            store.operations.update_table(request).await,
                            serde_json::to_value,
                        )
                    })
                    .await,
                ),
                Err(message) => request_error(message),
            },
            "DeleteTable" => {
                let request = DeleteTableRequest::try_from(request).expect("delete request");
                response_body(
                    observe(async {
                        json(
                            store.operations.delete_table(request).await,
                            serde_json::to_value,
                        )
                    })
                    .await,
                )
            }
            "DescribeTable" => {
                let request = DescribeTableRequest::try_from(request).expect("describe request");
                response_body(
                    observe(async {
                        json(
                            store.operations.describe_table(request).await,
                            serde_json::to_value,
                        )
                    })
                    .await,
                )
            }
            "Query" => {
                let request = QueryRequest::try_from(request).expect("query request");
                response_body(
                    observe(async {
                        json(store.operations.query(request).await, serde_json::to_value)
                    })
                    .await,
                )
            }
            other => panic!("{label}: unsupported operation {other}"),
        };
        let (status, mut body) = outcome;
        assert_eq!(
            status,
            step["expected"]["status"]
                .as_u64()
                .expect("expected status"),
            "{label}: {body}"
        );
        for key in ["TableDescription", "Table"] {
            if let Some(description) = body.get_mut(key) {
                normalize_description(description, &label);
            }
        }
        assert_conformance_body(&body, &step["expected"]["body"], &label);
    }
}

fn json<T>(
    response: StorageResult<T>,
    encode: fn(T) -> serde_json::Result<Value>,
) -> StorageResult<Value> {
    response.map(|response| encode(response).expect("serialize response"))
}

fn response_body(
    observed: storage_observability_test_support::observation::Observation<StorageResult<Value>>,
) -> (u64, Value) {
    observed.assert_no_warning_or_error();
    match observed.result {
        Ok(response) => (200, response),
        Err(error) => {
            let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                http_error::HttpApiError::from(error).into();
            (
                u64::from(status.as_u16()),
                serde_json::to_value(body).expect("serialize error"),
            )
        }
    }
}

fn request_error(message: String) -> (u64, Value) {
    (
        400,
        json!({"__type": "com.amazon.coral.validate#ValidationException", "message": message}),
    )
}

fn normalize_description(description: &mut Value, label: &str) {
    let object = description.as_object_mut().expect("table description");
    object.remove("MaxIndexers");
    let name = object["TableName"].as_str().expect("TableName").to_owned();
    let table_arn = format!("arn:aws:dynamodb:us-east-1:123456789012:table/{name}");
    assert_eq!(object["TableArn"], json!(table_arn), "{label}");
    object.insert("TableArn".into(), json!("<<table-arn>>"));
    assert!(
        object["TableId"].as_str().is_some_and(|id| id.len() == 36),
        "{label}"
    );
    object.insert("TableId".into(), json!("<<table-id>>"));
    // DeleteTable leaves `CreationDateTime` out (AWS P13).
    if let Some(created) = object.get("CreationDateTime") {
        assert!(created.is_f64(), "{label}");
        object.insert("CreationDateTime".into(), json!("<<creation-time>>"));
    }
    for index in object
        .get_mut("GlobalSecondaryIndexes")
        .and_then(Value::as_array_mut)
        .into_iter()
        .flatten()
    {
        let index_name = index["IndexName"].as_str().expect("IndexName").to_owned();
        assert_eq!(
            index["IndexArn"],
            json!(format!("{table_arn}/index/{index_name}")),
            "{label}"
        );
        index["IndexArn"] = json!(format!("<<table-arn>>/index/{index_name}"));
    }
    if let Some(label_value) = object.get("LatestStreamLabel").cloned() {
        let stream_label = label_value.as_str().expect("LatestStreamLabel");
        assert_iso_stream_label(stream_label, label);
        assert_eq!(
            object["LatestStreamArn"],
            json!(format!("{table_arn}/stream/{stream_label}")),
            "{label}"
        );
        object.insert("LatestStreamLabel".into(), json!("<<stream-label>>"));
        object.insert(
            "LatestStreamArn".into(),
            json!("<<table-arn>>/stream/<<stream-label>>"),
        );
    }
}

macro_rules! table_shape_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = FIXTURES.case(
                include_str!("../fixtures/describe_table_cases.jsonl"),
                $source,
            );
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(run_table_steps(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// AWS P13: CreateTable answers CREATING with CREATING GSIs and no
// `Backfilling`; DescribeTable then shows ACTIVE.
table_shape_case!(
    given_table_with_gsi_when_created_then_response_is_creating_with_aws_index_fields,
    "local/ddb-par-5/gsi_create_describe.json"
);

// Negative and format rules: a missing table returns the exact AWS body without
// changing the catalog.
#[tokio::test]
async fn given_missing_table_when_described_then_exact_not_found_and_empty_catalog_match_aws() {
    let case = FIXTURES.case(
        include_str!("../fixtures/describe_table_cases.jsonl"),
        "400/missing_table.json",
    );
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let parsed =
            observe(async { DescribeTableRequest::try_from(case["request"].clone()) }).await;
        parsed.assert_no_warning_or_error();
        parsed.assert_database_calls_exact(&[]);
        let request = parsed.result.unwrap();

        let observed = observe(store.operations.describe_table(request)).await;
        observed.assert_no_warning_or_error();
        let span = observed.one_span_with("dynamo_operation", &[("operation", "DescribeTable")]);
        assert_eq!(span.fields.len(), 1);
        let error = observed.result.unwrap_err();
        let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
            http_error::HttpApiError::from(error).into();
        assert_eq!(
            status.as_u16(),
            case["expected"]["status"].as_u64().unwrap() as u16
        );
        assert_conformance_body(
            &serde_json::to_value(body).unwrap(),
            &case["expected"]["body"],
            case["source"].as_str().unwrap(),
        );

        let listed = store
            .operations
            .list_tables(ListTablesRequest {
                exclusive_start_table_name: None,
                limit: None,
            })
            .await
            .unwrap();
        assert!(
            listed.table_names.is_empty(),
            "missing describe must not create a table"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

/// AWS labels a stream with its ISO timestamp and nothing else, such as
/// `2026-11-13T15:37:27.939` (P13 DeleteTable probe).
pub(crate) fn assert_iso_stream_label(stream_label: &str, label: &str) {
    let bytes = stream_label.as_bytes();
    let shape = b"dddd-dd-ddTdd:dd:dd.ddd";
    assert!(
        bytes.len() == shape.len()
            && bytes
                .iter()
                .zip(shape)
                .all(|(byte, shape)| if *shape == b'd' {
                    byte.is_ascii_digit()
                } else {
                    byte == shape
                }),
        "{label}: stream label is not an ISO timestamp: {stream_label}"
    );
}
