//! DescribeTable public errors from the authoritative DynamoDB fixture suite.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use storage_observability_test_support::observation::observe;
use storage_types::{DescribeTableRequest, ListTablesRequest};

use super::test_support::{ConformanceFixtures, TestStore, assert_conformance_body};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

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
