//! W6F N3 experiments: a SQL GSI read uses the GSI partial index on the item
//! relation (f), and a `KEYS_ONLY` GSI applies its projection before the
//! FilterExpression and consumed capacity (h, always zero).

use serde_json::{Value, json};
use storage_types::{CreateTableRequest, PutItemRequest, QueryRequest, ScanRequest};

use super::test_support::TestStore;

fn request<T>(value: Value) -> T
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value).expect("valid GSI request")
}

/// Table `table` with GSI `by_g` (`g` HASH, `s` RANGE, `KEYS_ONLY`).
async fn create_keys_only_table(store: &TestStore, table: &str) {
    store
        .operations
        .create_table(request::<CreateTableRequest>(json!({
            "TableName": table,
            "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"},
                {"AttributeName": "s", "AttributeType": "S"}
            ],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "GlobalSecondaryIndexes": [{
                "IndexName": "by_g",
                "KeySchema": [
                    {"AttributeName": "g", "KeyType": "HASH"},
                    {"AttributeName": "s", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "KEYS_ONLY"}
            }]
        })))
        .await
        .expect("create KEYS_ONLY GSI table");
}

async fn put(store: &TestStore, table: &str, item: Value) {
    store
        .operations
        .put_item(request::<PutItemRequest>(
            json!({"TableName": table, "Item": item}),
        ))
        .await
        .expect("put GSI item");
}

/// AWS probe (2026-11-13,
/// `docs/plans/incomplete/handover/gsi-filter-aws-probe.md`): a Scan filter on
/// an attribute the index does not project sees the projected index item, so
/// the attribute is missing. `payload = :p` returns no items and
/// `attribute_not_exists(payload)` returns the item; both scan the one index
/// item.
#[tokio::test]
async fn given_keys_only_gsi_when_scan_filter_reads_unprojected_attribute_then_attribute_is_missing()
 {
    let store = TestStore::new().await;
    let table = "gsi_keys_only_scan_filter";
    create_keys_only_table(&store, table).await;
    put(
        &store,
        table,
        json!({
            "pk": {"S": "item"}, "g": {"S": "g"}, "s": {"S": "1"},
            "payload": {"S": "x"}
        }),
    )
    .await;
    let scan = |filter: &str, values: Option<Value>| {
        let mut body = json!({
            "TableName": table,
            "IndexName": "by_g",
            "FilterExpression": filter
        });
        if let Some(values) = values {
            body["ExpressionAttributeValues"] = values;
        }
        store.operations.scan(request::<ScanRequest>(body))
    };
    let equal = scan("payload = :p", Some(json!({":p": {"S": "x"}})))
        .await
        .expect("a Scan filter on an unprojected attribute is accepted");
    assert_eq!(equal.count, 0);
    assert_eq!(equal.scanned_count, 1);
    assert_eq!(equal.items.unwrap_or_default().len(), 0);
    let missing = scan("attribute_not_exists(payload)", None)
        .await
        .expect("attribute_not_exists on an unprojected attribute is accepted");
    assert_eq!(missing.count, 1);
    assert_eq!(missing.scanned_count, 1);
    let response = serde_json::to_value(&missing).expect("response json");
    assert_eq!(
        response["Items"],
        json!([{"pk": {"S": "item"}, "g": {"S": "g"}, "s": {"S": "1"}}])
    );
    store.finish().await;
}

/// AWS probe (2026-11-13): a Query filter on an attribute the index does
/// not project is rejected before the read.
#[tokio::test]
async fn given_keys_only_gsi_when_query_filter_reads_unprojected_attribute_then_rejected() {
    let store = TestStore::new().await;
    let table = "gsi_keys_only_query_filter";
    create_keys_only_table(&store, table).await;
    for filter in ["payload = :p", "attribute_not_exists(payload)"] {
        let mut values = json!({":g": {"S": "g"}});
        if filter.contains(":p") {
            values[":p"] = json!({"S": "x"});
        }
        let error = store
            .operations
            .query(request::<QueryRequest>(json!({
                "TableName": table,
                "IndexName": "by_g",
                "KeyConditionExpression": "g = :g",
                "FilterExpression": filter,
                "ExpressionAttributeValues": values
            })))
            .await
            .expect_err("a Query filter on an unprojected attribute is rejected");
        assert_eq!(
            error.to_string(),
            "One or more parameter values were invalid: Secondary index by_g does not project one \
             or more filter attributes: [payload]",
            "{filter}"
        );
    }
    store.finish().await;
}

/// Experiment (h): the GSI page returns the projected item, and the
/// consumed capacity is the zero shape.
#[tokio::test]
async fn given_keys_only_gsi_when_filter_reads_projected_attribute_then_zero_capacity() {
    let store = TestStore::new().await;
    let table = "gsi_keys_only_filter";
    create_keys_only_table(&store, table).await;
    put(
        &store,
        table,
        json!({
            "pk": {"S": "item"}, "g": {"S": "g"}, "s": {"S": "1"},
            "payload": {"S": "x".repeat(10_000)}
        }),
    )
    .await;
    let response = store
        .operations
        .query(request::<QueryRequest>(json!({
            "TableName": table,
            "IndexName": "by_g",
            "KeyConditionExpression": "g = :g",
            "FilterExpression": "s = :other",
            "ExpressionAttributeValues": {":g": {"S": "g"}, ":other": {"S": "2"}},
            "ReturnConsumedCapacity": "TOTAL"
        })))
        .await
        .expect("filtered KEYS_ONLY query");
    assert_eq!(response.count, 0);
    assert_eq!(response.scanned_count, 1);
    let capacity =
        serde_json::to_value(&response).expect("response json")["ConsumedCapacity"].take();
    assert_eq!(
        capacity["CapacityUnits"],
        json!(0.0),
        "consumed capacity is always zero: {capacity}"
    );
    let unfiltered = store
        .operations
        .query(request::<QueryRequest>(json!({
            "TableName": table,
            "IndexName": "by_g",
            "KeyConditionExpression": "g = :g",
            "ExpressionAttributeValues": {":g": {"S": "g"}}
        })))
        .await
        .expect("KEYS_ONLY query");
    let response = serde_json::to_value(&unfiltered).expect("response json");
    assert_eq!(
        response["Items"],
        json!([{"pk": {"S": "item"}, "g": {"S": "g"}, "s": {"S": "1"}}])
    );
    store.finish().await;
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
/// Experiment (f): the compiled page statement of a GSI Query with a
/// row-value continuation uses the GSI partial index. SQLite and Turso
/// need no temporary sort for `ORDER BY`; Postgres names the index.
#[tokio::test]
async fn given_gsi_query_with_continuation_when_explained_then_plan_uses_partial_index() {
    let provider = TestStore::selected_backend();
    if !matches!(provider.as_str(), "sqlite" | "turso" | "postgres") {
        return;
    }
    let census = super::native_census_test_support::NativeCensus::new();
    let store = TestStore::new_with_census(std::sync::Arc::clone(&census)).await;
    let table = "gsi_plan";
    create_keys_only_table(&store, table).await;
    for index in 0..3 {
        put(
            &store,
            table,
            json!({"pk": {"S": format!("p{index}")}, "g": {"S": "g"}, "s": {"S": format!("{index}")}}),
        )
        .await;
    }
    let query = |start: Option<Value>| {
        let mut body = json!({
            "TableName": table,
            "IndexName": "by_g",
            "KeyConditionExpression": "g = :g AND s > :s",
            "ExpressionAttributeValues": {":g": {"S": "g"}, ":s": {"S": ""}},
            "Limit": 1
        });
        if let Some(start) = start {
            body["ExclusiveStartKey"] = start;
        }
        store.operations.query(request::<QueryRequest>(body))
    };
    let first = query(None).await.expect("first GSI page");
    let start = serde_json::to_value(first.last_evaluated_key.expect("continuation"))
        .expect("continuation json");
    census.record();
    let second = query(Some(start)).await.expect("continued GSI page");
    assert_eq!(second.count, 1);
    let statement = census
        .recorded()
        .into_iter()
        .find(|sql| sql.contains("IS NOT NULL") && sql.contains(") > ("))
        .expect("continued GSI page statement");
    let plan = explain(&store, &provider, &statement).await;
    assert!(
        plan.iter().any(|line| line.contains("gsi_")),
        "the page must use the GSI partial index: {plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("TEMP B-TREE")),
        "the index order must serve ORDER BY: {plan:?}"
    );
    store.finish().await;
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
/// The plan lines of `statement`, with each parameter unbound (its value
/// does not change the plan).
async fn explain(store: &TestStore, provider: &str, statement: &str) -> Vec<String> {
    let parameters = statement
        .split(['?', '$'])
        .skip(1)
        .filter_map(|rest| {
            rest.split(|character: char| !character.is_ascii_digit())
                .next()?
                .parse::<usize>()
                .ok()
        })
        .max()
        .unwrap_or(0);
    match provider {
        #[cfg(feature = "postgres")]
        "postgres" => {
            let (client, connection) =
                tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
                    .await
                    .expect("plan connection");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let arguments = vec!["NULL"; parameters].join(", ");
            client
                .batch_execute(&format!(
                    "SET plan_cache_mode = force_generic_plan; PREPARE gsi_page AS {statement}"
                ))
                .await
                .expect("prepare GSI page");
            client
                .query(&format!("EXPLAIN EXECUTE gsi_page({arguments})"), &[])
                .await
                .expect("explain GSI page")
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect()
        }
        #[cfg(any(feature = "sqlite", feature = "turso"))]
        "sqlite" | "turso" => {
            use sql_backend::{SqlAttempt as _, SqlBackend as _};
            let sql = format!("EXPLAIN QUERY PLAN {statement}");
            let binds = (0..parameters)
                .map(|_| sql_backend::SqlValue::Null)
                .collect::<Vec<_>>();
            let page = match provider {
                #[cfg(feature = "sqlite")]
                "sqlite" => {
                    let backend = sql_backend::sqlite::SqliteBackend::initialize(
                        store.sqlite_path().expect("SQLite database path"),
                        storage_types::AttemptDeadlines::default(),
                    )
                    .await
                    .expect("plan connection");
                    backend
                        .with_schema_attempt(move |attempt| {
                            Box::pin(async move {
                                attempt
                                    .query(
                                        sql_backend::SqlStatement::new(sql, binds),
                                        sql_backend::ReadProtection::Snapshot,
                                        std::num::NonZeroUsize::new(1 << 20).expect("budget"),
                                    )
                                    .await
                            })
                        })
                        .await
                }
                #[cfg(feature = "turso")]
                "turso" => {
                    let backend = sql_backend::turso::TursoBackend::initialize(
                        store.turso_path().expect("Turso database path"),
                        storage_types::AttemptDeadlines::default(),
                    )
                    .await
                    .expect("plan connection");
                    backend
                        .with_schema_attempt(move |attempt| {
                            Box::pin(async move {
                                attempt
                                    .query(
                                        sql_backend::SqlStatement::new(sql, binds),
                                        sql_backend::ReadProtection::Snapshot,
                                        std::num::NonZeroUsize::new(1 << 20).expect("budget"),
                                    )
                                    .await
                            })
                        })
                        .await
                }
                _ => unreachable!("SQLite family provider"),
            }
            .expect("explain GSI page");
            page.rows
                .into_iter()
                .map(|row| {
                    row.into_values()
                        .into_iter()
                        .map(|value| match value {
                            sql_backend::SqlValue::Text(text) => text,
                            other => format!("{other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect()
        }
        _ => unreachable!("SQL provider {provider} with {parameters} parameters"),
    }
}
