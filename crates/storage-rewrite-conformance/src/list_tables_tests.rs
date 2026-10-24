//! Customer-visible ListTables ordering, pagination, and validation rules.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{ListTablesRequest, TableName};

use super::test_support::{TestStore, unique_table_name};

// Boundary and format rules: pages are ordered, exclusive, and only carry a
// cursor when more remain.
#[tokio::test]
async fn given_three_tables_when_listed_in_pages_then_names_and_cursor_are_exact() {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let prefix = unique_table_name();
        let a = TableName::new(&format!("{prefix}_a"));
        let c = TableName::new(&format!("{prefix}_c"));
        let e = TableName::new(&format!("{prefix}_e"));
        for table in [&e, &a, &c] {
            store.create_hash_table(table.clone()).await;
        }

        let first = ListTablesRequest::try_from(json!({"Limit": 2})).unwrap();
        let first = observe(store.operations.list_tables(first)).await;
        first.assert_no_warning_or_error();
        assert_eq!(
            first
                .one_span_with("dynamo_operation", &[("operation", "ListTables")])
                .fields
                .len(),
            1,
        );
        assert_eq!(
            serde_json::to_value(first.result.unwrap()).unwrap(),
            json!({
                "TableNames": [a.as_ref(), c.as_ref()],
                "LastEvaluatedTableName": c.as_ref(),
            })
        );

        let last = ListTablesRequest::try_from(json!({
            "Limit": 2,
            "ExclusiveStartTableName": c.as_ref(),
        }))
        .unwrap();
        let last = observe(store.operations.list_tables(last)).await;
        last.assert_no_warning_or_error();
        assert_eq!(
            last.one_span_with("dynamo_operation", &[("operation", "ListTables")])
                .fields
                .len(),
            1,
        );
        assert_eq!(
            serde_json::to_value(last.result.unwrap()).unwrap(),
            json!({
                "TableNames": [e.as_ref()],
            })
        );

        let between = TableName::new(&format!("{prefix}_b"));
        let after_missing = observe(store.operations.list_tables(ListTablesRequest {
            exclusive_start_table_name: Some(between),
            limit: None,
        }))
        .await;
        after_missing.assert_no_warning_or_error();
        assert_eq!(
            after_missing
                .one_span_with("dynamo_operation", &[("operation", "ListTables")])
                .fields
                .len(),
            1,
        );
        assert_eq!(
            serde_json::to_value(after_missing.result.unwrap()).unwrap(),
            json!({
                "TableNames": [c.as_ref(), e.as_ref()],
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

// Validation and negative rules: a limit above the public maximum fails without
// creating a table.
#[tokio::test]
async fn given_limit_above_max_when_tables_are_listed_then_exact_validation_and_empty_catalog_result()
 {
    let store = TestStore::new().await;
    let outcome = AssertUnwindSafe(async {
        let parsed = observe(async { ListTablesRequest::try_from(json!({"Limit": 101})) }).await;
        parsed.assert_no_warning_or_error();
        parsed.assert_database_calls_exact(&[]);
        assert_eq!(
            parsed.result.unwrap_err(),
            "Limit must be between 1 and 100",
        );

        let listed = observe(store.operations.list_tables(ListTablesRequest {
            exclusive_start_table_name: None,
            limit: None,
        }))
        .await;
        listed.assert_no_warning_or_error();
        assert_eq!(
            listed
                .one_span_with("dynamo_operation", &[("operation", "ListTables")])
                .fields
                .len(),
            1,
        );
        assert_eq!(
            serde_json::to_value(listed.result.unwrap()).unwrap(),
            json!({
                "TableNames": [],
            })
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}
