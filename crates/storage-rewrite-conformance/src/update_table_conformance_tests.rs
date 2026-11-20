//! UpdateTable public errors from the authoritative DynamoDB fixture suite.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::Value;
use storage_observability_test_support::observation::observe;
use storage_types::{ListTablesRequest, UpdateTableRequest};

use super::test_support::{ConformanceFixtures, TestStore, assert_conformance_body};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

macro_rules! update_table_shape_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = FIXTURES.case(
                include_str!("../fixtures/update_table_cases.jsonl"),
                $source,
            );
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(
                super::describe_table_conformance_tests::run_table_steps(&store, &case),
            )
            .catch_unwind()
            .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// AWS P13: a GSI create answers UPDATING with the index CREATING and
// `Backfilling: false`; a Query of that index gets the missing-index text.
update_table_shape_case!(
    given_gsi_create_when_table_updated_then_response_is_updating,
    "local/ddb-par-5/gsi_create_updating.json"
);

// AWS P13: enabling an enabled stream again, with the same or another view
// type, is rejected with the AWS text.
update_table_shape_case!(
    given_stream_table_when_stream_is_enabled_again_then_aws_text,
    "local/ddb-par-5/stream_rejections.json"
);

// Negative and format rules: a missing table returns the exact AWS error and
// creates nothing.
#[tokio::test]
async fn given_missing_table_when_update_table_runs_then_exact_not_found_and_empty_catalog_match_aws()
 {
    let case: Value = FIXTURES.case(
        include_str!("../fixtures/update_table_cases.jsonl"),
        "400/missing_table.json",
    );
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let parsed = observe(async { UpdateTableRequest::try_from(case["request"].clone()) }).await;
        parsed.assert_no_warning_or_error();
        parsed.assert_database_calls_exact(&[]);
        let request = parsed.result.unwrap();

        let observed = observe(store.operations.update_table(request)).await;
        observed.assert_no_warning_or_error();
        let span = observed.one_span_with("dynamo_operation", &[("operation", "UpdateTable")]);
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
            "missing update must not create a table"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// AWS P13: the DeleteTable response is DELETING and leaves out
// `AttributeDefinitions`, `KeySchema` and `CreationDateTime`; the stream label
// is an ISO timestamp.
update_table_shape_case!(
    given_stream_table_when_deleted_then_response_is_deleting_without_schema,
    "local/ddb-par-5b/delete_table_response.json"
);

// A GSI delete answers UPDATING with the index DELETING; DescribeTable shows
// the index gone.
update_table_shape_case!(
    given_gsi_when_deleted_by_update_table_then_response_shows_deleting_index,
    "local/ddb-par-5b/gsi_delete_deleting.json"
);

// AWS accepts a view-type change when the stream was disabled first.
update_table_shape_case!(
    given_disabled_stream_when_enabled_with_another_view_then_view_changes,
    "local/ddb-par-5b/stream_view_change_after_disable.json"
);
