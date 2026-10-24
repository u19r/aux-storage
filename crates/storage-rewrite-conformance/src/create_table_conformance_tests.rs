//! CreateTable public errors from the authoritative DynamoDB fixture suite.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_observability_test_support::observation::observe;
use storage_types::{CreateTableRequest, DescribeTableRequest, ListTablesRequest, TableName};

use super::test_support::{ConformanceFixtures, TestStore, assert_conformance_body};

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

async fn assert_create_table_case(store: &TestStore, case: &Value) {
    assert_eq!(case["operation"], "CreateTable");
    let source = case["source"].as_str().expect("fixture source");
    let table = TableName::new(case["request"]["TableName"].as_str().expect("table name"));
    let duplicate = source == "400/duplicate_table_name.json";
    let before = if duplicate {
        store
            .create_conformance_table("basic_pk_sk", table.clone())
            .await;
        let described = store
            .operations
            .describe_table(DescribeTableRequest {
                table_name: table.clone(),
            })
            .await
            .expect("describe existing table");
        Some(serde_json::to_value(described).expect("serialize existing table"))
    } else {
        None
    };

    let parsed = observe(async { CreateTableRequest::try_from(case["request"].clone()) }).await;
    parsed.assert_no_warning_or_error();
    parsed.assert_database_calls_exact(&[]);
    let (status, body) = match parsed.result {
        Err(message) => (
            400,
            json!({
                "__type": "com.amazon.coral.validate#ValidationException",
                "message": message,
            }),
        ),
        Ok(request) => {
            let observed = observe(store.operations.create_table(request)).await;
            observed.assert_no_warning_or_error();
            let error = observed.result.expect_err("CreateTable fixture must fail");
            let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                http_error::HttpApiError::from(error).into();
            (
                status.as_u16(),
                serde_json::to_value(body).expect("serialize CreateTable error"),
            )
        }
    };
    assert_eq!(status, case["expected"]["status"].as_u64().unwrap() as u16);
    assert_conformance_body(&body, &case["expected"]["body"], source);

    let listed = store
        .operations
        .list_tables(ListTablesRequest {
            exclusive_start_table_name: None,
            limit: None,
        })
        .await
        .expect("list tables after rejected CreateTable");
    assert_eq!(
        listed.table_names,
        if duplicate {
            vec![table.clone()]
        } else {
            vec![]
        }
    );
    if let Some(before) = before {
        let after = store
            .operations
            .describe_table(DescribeTableRequest { table_name: table })
            .await
            .expect("describe existing table after rejection");
        assert_conformance_body(
            &serde_json::to_value(after).expect("serialize unchanged table"),
            &before,
            source,
        );
    }
}

macro_rules! create_table_case {
    ($name:ident, $source:literal) => {
        #[tokio::test]
        async fn $name() {
            let case = FIXTURES.case(
                include_str!("../fixtures/create_table_cases.jsonl"),
                $source,
            );
            let store = TestStore::new().await;
            let outcome = AssertUnwindSafe(assert_create_table_case(&store, &case))
                .catch_unwind()
                .await;
            store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Validation rule: table names must contain at least three characters.
create_table_case!(
    given_short_name_when_create_table_runs_then_exact_validation_and_no_table_match_aws,
    "400/table_name_too_short.json"
);

// Validation rule: table names reject characters outside the AWS character
// class.
create_table_case!(
    given_slash_in_name_when_create_table_runs_then_exact_validation_and_no_table_match_aws,
    "400/table_name_invalid_character.json"
);

// Negative rule: an existing table rejects a second create without replacing
// its description.
create_table_case!(
    given_existing_name_when_create_table_runs_then_exact_error_and_original_table_match_aws,
    "400/duplicate_table_name.json"
);
