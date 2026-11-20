//! AWS emulator behavioural suite ports.
//!
//! Query and Scan behaviour: key conditions, ordering, paging, filters,
//! projections and index reads. Expected values are AWS's, taken from the
//! scripts of `behavioral/dynamodb`; capacity values, legacy `QueryFilter` /
//! `ScanFilter` / `KeyConditions` and local secondary indexes are out of scope
//! (`writing/dynamodb_parity.md`, Query, Scan and Local secondary indexes).

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_types::{
    CreateTableRequest, DeleteItemRequest, DeleteTableRequest, DescribeTableRequest,
    GetItemRequest, PutItemRequest, QueryRequest, ScanRequest,
};

use crate::test_support::{TestStore, unique_table_name};

const ONE_MB: usize = 1024 * 1024;

#[derive(Debug)]
struct Failure {
    kind: String,
    message: String,
}

fn failure_from_body(body: &Value) -> Failure {
    let kind = body["__type"].as_str().unwrap_or_default();
    let message = body
        .get("message")
        .or_else(|| body.get("Message"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    Failure {
        kind: kind.rsplit('#').next().unwrap_or_default().to_owned(),
        message: message.to_owned(),
    }
}

fn failure<E>(error: E) -> Failure
where http_error::HttpApiError: From<E> {
    let (_status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::from(error).into();
    failure_from_body(&serde_json::to_value(body).unwrap())
}

fn parse_failure(message: String) -> Failure {
    Failure {
        kind: "ValidationException".to_owned(),
        message,
    }
}

async fn query(store: &TestStore, request: Value) -> Result<Value, Failure> {
    let request = QueryRequest::try_from(request).map_err(parse_failure)?;
    let response = store.operations.query(request).await.map_err(failure)?;
    Ok(serde_json::to_value(response).unwrap())
}

async fn scan(store: &TestStore, request: Value) -> Result<Value, Failure> {
    let request = ScanRequest::try_from(request).map_err(parse_failure)?;
    let response = store.operations.scan(request).await.map_err(failure)?;
    Ok(serde_json::to_value(response).unwrap())
}

/// Every page of a Query or Scan, following `LastEvaluatedKey`.
async fn pages(store: &TestStore, operation: &str, mut request: Value) -> Vec<Value> {
    let mut out = Vec::new();
    loop {
        let page = if operation == "Query" {
            query(store, request.clone()).await
        } else {
            scan(store, request.clone()).await
        }
        .expect("page succeeds");
        let next = page.get("LastEvaluatedKey").cloned();
        out.push(page);
        match next {
            Some(key) => request["ExclusiveStartKey"] = key,
            None => return out,
        }
        assert!(out.len() < 200, "paging did not terminate");
    }
}

fn items(page: &Value) -> Vec<Value> {
    page["Items"].as_array().cloned().unwrap_or_default()
}

fn all_items(pages: &[Value]) -> Vec<Value> {
    pages.iter().flat_map(items).collect()
}

fn strs(items: &[Value], attribute: &str, kind: &str) -> Vec<String> {
    items
        .iter()
        .map(|item| {
            item[attribute][kind]
                .as_str()
                .expect("attribute")
                .to_owned()
        })
        .collect()
}

fn names(item: &Value) -> Vec<String> {
    let mut names: Vec<String> = item.as_object().unwrap().keys().cloned().collect();
    names.sort();
    names
}

fn expect_failure(label: &str, result: Result<Value, Failure>, kind: &str, message: &str) {
    match result {
        Ok(value) => panic!("{label}: expected {kind} but the call succeeded: {value}"),
        Err(found) => assert_eq!(
            (found.kind.as_str(), found.message.as_str()),
            (kind, message),
            "{label}"
        ),
    }
}

fn gsi(name: &str, keys: &[(&str, &str)], projection: Value) -> Value {
    let key_schema: Vec<Value> = keys
        .iter()
        .map(|(attribute, key_type)| json!({"AttributeName": attribute, "KeyType": key_type}))
        .collect();
    json!({"IndexName": name, "KeySchema": key_schema, "Projection": projection})
}

/// `keys` are `(attribute, type, key type)`; `extra` are index key attributes.
async fn make_table(
    store: &TestStore,
    keys: &[(&str, &str, &str)],
    extra: &[(&str, &str)],
    gsis: Vec<Value>,
) -> String {
    let name = unique_table_name().to_string();
    let mut definitions: Vec<Value> = keys
        .iter()
        .map(|(attribute, kind, _)| json!({"AttributeName": attribute, "AttributeType": kind}))
        .collect();
    definitions.extend(
        extra
            .iter()
            .map(|(attribute, kind)| json!({"AttributeName": attribute, "AttributeType": kind})),
    );
    let key_schema: Vec<Value> = keys
        .iter()
        .map(|(attribute, _, key_type)| json!({"AttributeName": attribute, "KeyType": key_type}))
        .collect();
    let mut request = json!({
        "TableName": name,
        "AttributeDefinitions": definitions,
        "KeySchema": key_schema,
        "BillingMode": "PAY_PER_REQUEST"
    });
    if !gsis.is_empty() {
        request["GlobalSecondaryIndexes"] = Value::Array(gsis);
    }
    store
        .operations
        .create_table(CreateTableRequest::try_from(request).expect("valid CreateTable"))
        .await
        .expect("create table");
    name
}

async fn hash_range(store: &TestStore, sort_type: &str) -> String {
    make_table(
        store,
        &[("pk", "S", "HASH"), ("sk", sort_type, "RANGE")],
        &[],
        vec![],
    )
    .await
}

async fn hash_only(store: &TestStore) -> String {
    make_table(store, &[("pk", "S", "HASH")], &[], vec![]).await
}

async fn put(store: &TestStore, table: &str, item: Value) {
    let request =
        PutItemRequest::try_from(json!({"TableName": table, "Item": item})).expect("valid PutItem");
    store.operations.put_item(request).await.expect("put item");
}

fn item_bytes(item: &Value) -> usize {
    item.as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| name.len() + value["S"].as_str().expect("string attribute").len())
        .sum()
}

fn page_bytes(items: &[Value]) -> usize {
    items.iter().map(item_bytes).sum()
}

/// aux deliberately differs from AWS: AWS includes the item that crosses 1 MB
/// in a page, aux (`RangeReadBudget::admit`) stops before it. A truncated page
/// is therefore the longest prefix within 1 MB: adding the next item exceeds
/// it.
fn assert_maximal_prefix(items: &[Value], next: &Value, page: usize) {
    let total = page_bytes(items);
    assert!(
        total <= ONE_MB,
        "page {page} holds {total} bytes, over the 1 MB budget"
    );
    let with_next = total + item_bytes(next);
    assert!(
        with_next > ONE_MB,
        "page {page} stopped early: the next item would still fit ({with_next} bytes)"
    );
}

fn key_of(item: &Value) -> Value {
    json!({"pk": item["pk"], "sk": item["sk"]})
}

macro_rules! store_test {
    (#[ignore = $reason:literal] $name:ident, |$store:ident| $body:block) => {
        #[tokio::test]
        #[ignore = $reason]
        async fn $name() {
            let $store = TestStore::new().await;
            let outcome = AssertUnwindSafe(async $body).catch_unwind().await;
            $store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
    ($name:ident, |$store:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            let $store = TestStore::new().await;
            let outcome = AssertUnwindSafe(async $body).catch_unwind().await;
            $store.finish().await;
            if let Err(panic) = outcome {
                resume_unwind(panic);
            }
        }
    };
}

// Binary values: 0x01, 0x7f, 0x80, 0xff and 0x80 0x00.
const B_01: &str = "AQ==";
const B_7F: &str = "fw==";
const B_80: &str = "gA==";
const B_FF: &str = "/w==";
const B_80_00: &str = "gAA=";

// AWS behavioural:
// check_all_projected_attributes_select_serves_include_index_projection.py,
// check_gsi_projection_include.py,
// check_include_projection_governs_index_reads_on_lsi_and_gsi.py (GSI parts
// only; local secondary indexes are not implemented).
store_test!(
    given_include_and_keys_only_gsis_when_queried_and_scanned_then_only_projected_attributes_return,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
            &[("gi", "S"), ("gk", "S")],
            vec![
                gsi(
                    "include-gsi",
                    &[("gi", "HASH")],
                    json!({"ProjectionType": "INCLUDE", "NonKeyAttributes": ["kept_g"]}),
                ),
                gsi(
                    "keys-only-gsi",
                    &[("gk", "HASH")],
                    json!({"ProjectionType": "KEYS_ONLY"}),
                ),
            ],
        )
        .await;
        put(
            &store,
            &table,
            json!({
                "pk": {"S": "p"}, "sk": {"S": "s"}, "gi": {"S": "g"}, "gk": {"S": "kv"},
                "kept_g": {"S": "KG"}, "excluded": {"S": "X"}
            }),
        )
        .await;
        let base = json!({"TableName": table, "IndexName": "include-gsi"});
        let by_gi = json!({
            "KeyConditionExpression": "gi = :g",
            "ExpressionAttributeValues": {":g": {"S": "g"}}
        });
        let mut selected = base.clone();
        selected["Select"] = json!("ALL_PROJECTED_ATTRIBUTES");
        let mut query_selected = selected.clone();
        query_selected["KeyConditionExpression"] = by_gi["KeyConditionExpression"].clone();
        query_selected["ExpressionAttributeValues"] = by_gi["ExpressionAttributeValues"].clone();
        let mut query_default = base.clone();
        query_default["KeyConditionExpression"] = by_gi["KeyConditionExpression"].clone();
        query_default["ExpressionAttributeValues"] = by_gi["ExpressionAttributeValues"].clone();

        let from_query = items(&query(&store, query_selected).await.unwrap());
        let from_default = items(&query(&store, query_default).await.unwrap());
        let from_scan = items(&scan(&store, selected).await.unwrap());
        for (label, found) in [
            ("Query ALL_PROJECTED_ATTRIBUTES", from_query),
            ("Query default select", from_default),
            ("Scan ALL_PROJECTED_ATTRIBUTES", from_scan),
        ] {
            assert_eq!(found.len(), 1, "{label}");
            assert_eq!(names(&found[0]), ["gi", "kept_g", "pk", "sk"], "{label}");
            assert_eq!(found[0]["kept_g"], json!({"S": "KG"}), "{label}");
        }
        let keys_only = items(
            &query(
                &store,
                json!({
                    "TableName": table, "IndexName": "keys-only-gsi",
                    "KeyConditionExpression": "gk = :g",
                    "ExpressionAttributeValues": {":g": {"S": "kv"}}
                }),
            )
            .await
            .unwrap(),
        );
        assert_eq!(keys_only.len(), 1);
        assert_eq!(names(&keys_only[0]), ["gk", "pk", "sk"]);
    }
);

// AWS behavioural:
// check_scan_all_projected_attributes_rejects_attributes_to_get.py
store_test!(
    given_include_gsi_when_scan_pairs_all_projected_with_attributes_to_get_then_validation_error,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
            &[("gi", "S")],
            vec![gsi(
                "include-gsi",
                &[("gi", "HASH")],
                json!({"ProjectionType": "INCLUDE", "NonKeyAttributes": ["kept"]}),
            )],
        )
        .await;
        put(
            &store,
            &table,
            json!({
                "pk": {"S": "p"}, "sk": {"S": "s"}, "gi": {"S": "g"},
                "kept": {"S": "K"}, "excluded": {"S": "X"}
            }),
        )
        .await;
        let control = scan(
            &store,
            json!({
                "TableName": table, "IndexName": "include-gsi",
                "Select": "ALL_PROJECTED_ATTRIBUTES"
            }),
        )
        .await
        .unwrap();
        assert_eq!(names(&items(&control)[0]), ["gi", "kept", "pk", "sk"]);
        expect_failure(
            "Select ALL_PROJECTED_ATTRIBUTES with AttributesToGet",
            scan(
                &store,
                json!({
                    "TableName": table, "IndexName": "include-gsi",
                    "Select": "ALL_PROJECTED_ATTRIBUTES", "AttributesToGet": ["kept"]
                }),
            )
            .await,
            "ValidationException",
            "1 validation error detected: Cannot specify the AttributesToGet when choosing to get \
             ALL_PROJECTED_ATTRIBUTES",
        );
    }
);

// AWS behavioural:
// check_scan_select_all_attributes_on_partial_projection_index_rejected.py
store_test!(
    given_partial_projection_gsis_when_scan_selects_all_attributes_then_validation_error_names_the_index,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
            &[("gk", "S")],
            vec![
                gsi(
                    "keys-only-index",
                    &[("gk", "HASH")],
                    json!({"ProjectionType": "KEYS_ONLY"}),
                ),
                gsi(
                    "include-index",
                    &[("gk", "HASH")],
                    json!({"ProjectionType": "INCLUDE", "NonKeyAttributes": ["inc"]}),
                ),
            ],
        )
        .await;
        let item = json!({
            "pk": {"S": "P"}, "sk": {"S": "S"}, "gk": {"S": "G"},
            "inc": {"S": "i"}, "body": {"S": "b"}
        });
        put(&store, &table, item.clone()).await;
        for index in ["keys-only-index", "include-index"] {
            expect_failure(
                index,
                scan(
                    &store,
                    json!({"TableName": table, "IndexName": index, "Select": "ALL_ATTRIBUTES"}),
                )
                .await,
                "ValidationException",
                &format!(
                    "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index {index} because its projection type is not ALL"
                ),
            );
        }
        let keys = json!({"pk": {"S": "P"}, "sk": {"S": "S"}, "gk": {"S": "G"}});
        let with_inc = json!({
            "pk": {"S": "P"}, "sk": {"S": "S"}, "gk": {"S": "G"}, "inc": {"S": "i"}
        });
        for (index, expected) in [("keys-only-index", keys), ("include-index", with_inc)] {
            let found = scan(
                &store,
                json!({
                    "TableName": table, "IndexName": index,
                    "Select": "ALL_PROJECTED_ATTRIBUTES"
                }),
            )
            .await
            .unwrap();
            assert_eq!(items(&found), [expected], "{index}");
        }
        let full = scan(
            &store,
            json!({"TableName": table, "Select": "ALL_ATTRIBUTES", "ConsistentRead": true}),
        )
        .await
        .unwrap();
        assert_eq!(items(&full), [item]);
    }
);

// AWS behavioural: check_binary_sort_key_orders_by_unsigned_bytes.py
store_test!(
    given_binary_sort_keys_when_queried_then_unsigned_byte_order_holds_in_both_directions,
    |store| {
        let table = hash_range(&store, "B").await;
        for sk in [B_FF, B_80, B_7F, B_01] {
            put(
                &store,
                &table,
                json!({"pk": {"S": "SPAN"}, "sk": {"B": sk}}),
            )
            .await;
        }
        let run = |partition: &'static str,
                   condition: &'static str,
                   forward: bool,
                   high: Option<&'static str>| {
            let mut values = json!({":p": {"S": partition}});
            if let Some(high) = high {
                values[":hi"] = json!({"B": high});
            }
            json!({
                "TableName": table, "KeyConditionExpression": condition,
                "ExpressionAttributeValues": values, "ScanIndexForward": forward,
                "ConsistentRead": true
            })
        };
        let order = [B_01, B_7F, B_80, B_FF];
        let found = query(&store, run("SPAN", "pk = :p", true, None))
            .await
            .unwrap();
        assert_eq!(strs(&items(&found), "sk", "B"), order);
        let found = query(&store, run("SPAN", "pk = :p", false, None))
            .await
            .unwrap();
        let mut reversed = order;
        reversed.reverse();
        assert_eq!(strs(&items(&found), "sk", "B"), reversed);
        let found = query(
            &store,
            run("SPAN", "pk = :p AND sk <= :hi", true, Some(B_7F)),
        )
        .await
        .unwrap();
        assert_eq!(strs(&items(&found), "sk", "B"), [B_01, B_7F]);
        for sk in [B_80_00, B_80] {
            put(
                &store,
                &table,
                json!({"pk": {"S": "PREFIX"}, "sk": {"B": sk}}),
            )
            .await;
        }
        let found = query(&store, run("PREFIX", "pk = :p", true, None))
            .await
            .unwrap();
        assert_eq!(strs(&items(&found), "sk", "B"), [B_80, B_80_00]);
    }
);

// AWS behavioural: check_e2e_event_sourcing.py
store_test!(
    given_ten_events_when_queried_and_paged_then_order_holds_and_deleted_table_is_not_found,
    |store| {
        let table = make_table(
            &store,
            &[("aggregate_id", "S", "HASH"), ("event_id", "S", "RANGE")],
            &[],
            vec![],
        )
        .await;
        let event_types = [
            "order_created",
            "item_added",
            "item_added",
            "item_removed",
            "address_set",
            "item_added",
            "payment_submitted",
            "payment_confirmed",
            "shipped",
            "delivered",
        ];
        for (index, event_type) in event_types.iter().enumerate() {
            put(
                &store,
                &table,
                json!({
                    "aggregate_id": {"S": "order-abc-123"},
                    "event_id": {"S": format!("evt-{index:04}")},
                    "event_type": {"S": event_type},
                    "seq": {"N": index.to_string()}
                }),
            )
            .await;
        }
        let request = json!({
            "TableName": table, "KeyConditionExpression": "aggregate_id = :agg",
            "ExpressionAttributeValues": {":agg": {"S": "order-abc-123"}},
            "ScanIndexForward": true, "ConsistentRead": true
        });
        let found = query(&store, request.clone()).await.unwrap();
        assert_eq!(found["Count"], 10);
        let ids = strs(&items(&found), "event_id", "S");
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted);
        let mut paged = request;
        paged["Limit"] = json!(3);
        let walked = pages(&store, "Query", paged).await;
        assert!(walked.len() >= 3, "got {} pages", walked.len());
        assert_eq!(strs(&all_items(&walked), "event_type", "S"), event_types);

        store
            .operations
            .delete_table(DeleteTableRequest::try_from(json!({"TableName": table})).unwrap())
            .await
            .expect("delete table");
        let described = store
            .operations
            .describe_table(DescribeTableRequest::try_from(json!({"TableName": table})).unwrap())
            .await
            .map(|_| Value::Null)
            .map_err(failure);
        expect_failure(
            "DescribeTable after delete",
            described,
            "ResourceNotFoundException",
            &format!("Requested resource not found: Table: {table} not found"),
        );
        let written = store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table,
                    "Item": {"aggregate_id": {"S": "order-abc-123"}, "event_id": {"S": "evt-post-delete"}}
                }))
                .unwrap(),
            )
            .await
            .map(|_| Value::Null)
            .map_err(failure);
        expect_failure(
            "PutItem after delete",
            written,
            "ResourceNotFoundException",
            "Requested resource not found",
        );
    }
);

// AWS behavioural: check_e2e_paginated_export.py
store_test!(
    given_thirty_items_when_scanned_with_limit_ten_then_every_item_appears_once,
    |store| {
        let table = hash_only(&store).await;
        for index in 0..30 {
            put(
                &store,
                &table,
                json!({"pk": {"S": format!("export-{index:03}")}, "seq": {"N": index.to_string()}}),
            )
            .await;
        }
        let walked = pages(
            &store,
            "Scan",
            json!({"TableName": table, "Limit": 10, "ConsistentRead": true}),
        )
        .await;
        assert!(walked.len() >= 3, "got {} pages", walked.len());
        assert!(walked.iter().all(|page| items(page).len() <= 10));
        let mut keys = strs(&all_items(&walked), "pk", "S");
        keys.sort();
        let expected: Vec<String> = (0..30).map(|index| format!("export-{index:03}")).collect();
        assert_eq!(keys, expected);
    }
);

// AWS behavioural:
// check_empty_value_filter_expression_and_legacy_scan_filter_selection.py
// (legacy ScanFilter half dropped: writing/dynamodb_parity.md, Scan row).
store_test!(
    given_empty_string_and_binary_attributes_when_filtered_then_table_and_gsi_select_the_same_items,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH")],
            &[("gpk", "S")],
            vec![gsi(
                "by-gpk",
                &[("gpk", "HASH")],
                json!({"ProjectionType": "ALL"}),
            )],
        )
        .await;
        for item in [
            json!({"pk": {"S": "both-empty"}, "gpk": {"S": "g1"}, "s": {"S": ""}, "b": {"B": ""}}),
            json!({"pk": {"S": "s-empty"}, "gpk": {"S": "g2"}, "s": {"S": ""}, "b": {"B": "eg=="}}),
            json!({"pk": {"S": "b-empty"}, "gpk": {"S": "g3"}, "s": {"S": "w"}, "b": {"B": ""}}),
            json!({"pk": {"S": "neither-empty"}, "gpk": {"S": "g4"}, "s": {"S": "x"}, "b": {"B": "eQ=="}}),
        ] {
            put(&store, &table, item).await;
        }
        let selected = |index: Option<&str>, filter: &str, values: Value| {
            let mut request = json!({
                "TableName": table, "FilterExpression": filter,
                "ExpressionAttributeValues": values
            });
            match index {
                Some(index) => request["IndexName"] = json!(index),
                None => request["ConsistentRead"] = json!(true),
            }
            request
        };
        for index in [None, Some("by-gpk")] {
            for (filter, values, expected) in [
                (
                    "s = :e",
                    json!({":e": {"S": ""}}),
                    vec!["both-empty", "s-empty"],
                ),
                (
                    "begins_with(s, :e)",
                    json!({":e": {"S": ""}}),
                    vec!["b-empty", "both-empty", "neither-empty", "s-empty"],
                ),
                (
                    "size(b) = :z",
                    json!({":z": {"N": "0"}}),
                    vec!["b-empty", "both-empty"],
                ),
            ] {
                let walked = pages(&store, "Scan", selected(index, filter, values)).await;
                let mut keys = strs(&all_items(&walked), "pk", "S");
                keys.sort();
                assert_eq!(keys, expected, "{filter} on index {index:?}");
            }
        }
    }
);

// AWS behavioural:
// check_exclusive_start_key_outside_range_and_missing_attribute_rejected.py
// (the missing-sort-key halves are covered by query_cases.jsonl and
// scan_cases.jsonl).
store_test!(
    given_range_key_condition_when_start_key_is_outside_the_range_then_validation_error_in_both_directions,
    |store| {
        let table = hash_range(&store, "S").await;
        for index in 0..10 {
            put(
                &store,
                &table,
                json!({"pk": {"S": "x"}, "sk": {"S": format!("{index:03}")}}),
            )
            .await;
        }
        let request = |start: &str, forward: bool| {
            json!({
                "TableName": table,
                "KeyConditionExpression": "pk = :p AND sk BETWEEN :lo AND :hi",
                "ExpressionAttributeValues": {
                    ":p": {"S": "x"}, ":lo": {"S": "003"}, ":hi": {"S": "006"}
                },
                "ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": start}},
                "ScanIndexForward": forward, "ConsistentRead": true
            })
        };
        let found = query(&store, request("004", true)).await.unwrap();
        assert_eq!(strs(&items(&found), "sk", "S"), ["005", "006"]);
        let found = query(&store, request("005", false)).await.unwrap();
        assert_eq!(strs(&items(&found), "sk", "S"), ["004", "003"]);
        for (label, start, forward) in [("forward", "020", true), ("backward", "002", false)] {
            expect_failure(
                label,
                query(&store, request(start, forward)).await,
                "ValidationException",
                "The provided starting key does not match the range key predicate",
            );
        }
        let resumed = scan(
            &store,
            json!({
                "TableName": table,
                "ExclusiveStartKey": {"pk": {"S": "x"}, "sk": {"S": "003"}}
            }),
        )
        .await
        .unwrap();
        assert!(resumed.get("Items").is_some());
        let all = scan(&store, json!({"TableName": table, "ConsistentRead": true}))
            .await
            .unwrap();
        assert_eq!(items(&all).len(), 10);
    }
);

// AWS behavioural:
// check_filter_and_key_condition_expression_size_limit_rejected.py
store_test!(
    given_expressions_over_4096_bytes_when_run_then_validation_error_and_exact_size_is_accepted,
    |store| {
        let table = hash_only(&store).await;
        let item = json!({"pk": {"S": "k1"}, "foo": {"S": "a"}});
        put(&store, &table, item.clone()).await;
        let path = |total: usize| format!("a{}", "z".repeat(total - 1));
        let suffix = " = :v";
        let over_filter = format!("{}{suffix}", path(4097 - suffix.len()));
        let at_filter = format!("{}{suffix}", path(4096 - suffix.len()));
        let over_key = format!(
            "pk = :p AND {} = :q",
            path(4097 - "pk = :p AND  = :q".len())
        );
        assert_eq!(
            (over_filter.len(), at_filter.len(), over_key.len()),
            (4097, 4096, 4097)
        );
        let filter_message =
            "Invalid FilterExpression: Expression size has exceeded the maximum allowed size;";
        expect_failure(
            "Scan 4097-byte FilterExpression",
            scan(
                &store,
                json!({
                    "TableName": table, "FilterExpression": over_filter,
                    "ExpressionAttributeValues": {":v": {"S": "x"}}
                }),
            )
            .await,
            "ValidationException",
            filter_message,
        );
        expect_failure(
            "Query 4097-byte FilterExpression",
            query(
                &store,
                json!({
                    "TableName": table, "KeyConditionExpression": "pk = :p",
                    "FilterExpression": over_filter,
                    "ExpressionAttributeValues": {":p": {"S": "k1"}, ":v": {"S": "x"}}
                }),
            )
            .await,
            "ValidationException",
            filter_message,
        );
        expect_failure(
            "Query 4097-byte KeyConditionExpression",
            query(
                &store,
                json!({
                    "TableName": table, "KeyConditionExpression": over_key,
                    "ExpressionAttributeValues": {":p": {"S": "k1"}, ":q": {"S": "x"}}
                }),
            )
            .await,
            "ValidationException",
            "Invalid KeyConditionExpression: Expression size has exceeded the maximum allowed \
             size;",
        );
        let accepted = scan(
            &store,
            json!({
                "TableName": table, "FilterExpression": at_filter,
                "ExpressionAttributeValues": {":v": {"S": "x"}}, "ConsistentRead": true
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            (accepted["Count"].clone(), accepted["ScannedCount"].clone()),
            (json!(0), json!(1))
        );
        let found = query(
            &store,
            json!({
                "TableName": table, "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "k1"}}, "ConsistentRead": true
            }),
        )
        .await
        .unwrap();
        assert_eq!(items(&found), [item]);
    }
);

// AWS behavioural: check_filter_expression_operator_limit_in_counts_as_one.py
store_test!(
    given_filter_with_301_operators_when_scanned_then_rejected_and_in_counts_as_one_operator,
    |store| {
        let table = hash_only(&store).await;
        put(&store, &table, json!({"pk": {"S": "x"}, "a": {"S": "v"}})).await;
        let chain = |count: usize| vec!["#a = :v"; count].join(" AND ");
        let chain_with_in = |count: usize, operands: usize| {
            let mut values = json!({":v": {"S": "v"}});
            let mut clauses = vec!["#a = :v".to_owned(); count - 1];
            let placeholders: Vec<String> =
                (0..operands).map(|index| format!(":w{index}")).collect();
            clauses.push(format!("#a IN ({})", placeholders.join(", ")));
            for (index, placeholder) in placeholders.iter().enumerate() {
                values[placeholder] = json!({"S": format!("other{index}")});
            }
            (clauses.join(" AND "), values)
        };
        let request = |filter: String, values: Value| {
            json!({
                "TableName": table, "FilterExpression": filter,
                "ExpressionAttributeNames": {"#a": "a"}, "ExpressionAttributeValues": values
            })
        };
        let message = "Invalid FilterExpression: The expression contains too many operators; \
                       operator count: 301";
        expect_failure(
            "301 operators",
            scan(&store, request(chain(151), json!({":v": {"S": "v"}}))).await,
            "ValidationException",
            message,
        );
        let accepted = scan(&store, request(chain(150), json!({":v": {"S": "v"}})))
            .await
            .unwrap();
        assert_eq!(accepted["Count"], 1);
        let (filter, values) = chain_with_in(151, 100);
        expect_failure(
            "151 clauses with a 100-operand IN",
            scan(&store, request(filter, values)).await,
            "ValidationException",
            message,
        );
        let (filter, values) = chain_with_in(150, 100);
        let accepted = scan(&store, request(filter, values)).await.unwrap();
        assert_eq!(accepted["Count"], 0);
    }
);

// AWS behavioural:
// check_get_item_query_unaliased_dotted_name_is_a_nested_path.py
store_test!(
    given_attribute_named_with_a_dot_when_projected_then_only_an_alias_reaches_it,
    |store| {
        let table = hash_only(&store).await;
        let meta = json!({"M": {"x": {"S": "nested-value"}, "y": {"S": "other"}}});
        put(
            &store,
            &table,
            json!({"pk": {"S": "rec1"}, "Safety.Warning": {"S": "flat-value"}, "meta": meta}),
        )
        .await;
        let get = |extra: Value| {
            let mut request = json!({
                "TableName": table, "Key": {"pk": {"S": "rec1"}}, "ConsistentRead": true
            });
            for (name, value) in extra.as_object().unwrap() {
                request[name] = value.clone();
            }
            let request = GetItemRequest::try_from(request).expect("valid GetItem");
            let operations = store.operations.clone();
            async move {
                serde_json::to_value(operations.get_item(request).await.expect("get item")).unwrap()
            }
        };
        assert_eq!(
            get(json!({"ProjectionExpression": "Safety.Warning"})).await["Item"],
            json!({})
        );
        assert_eq!(
            get(json!({
                "ProjectionExpression": "#sw",
                "ExpressionAttributeNames": {"#sw": "Safety.Warning"}
            }))
            .await["Item"],
            json!({"Safety.Warning": {"S": "flat-value"}})
        );
        assert_eq!(
            get(json!({"AttributesToGet": ["meta.x"]})).await["Item"],
            json!({})
        );
        assert_eq!(
            get(json!({"AttributesToGet": ["meta"]})).await["Item"],
            json!({"meta": meta})
        );
        let projected = |extra: Value| {
            let mut request = json!({
                "TableName": table, "ConsistentRead": true,
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "rec1"}}
            });
            for (name, value) in extra.as_object().unwrap() {
                request[name] = value.clone();
            }
            request
        };
        let found = query(
            &store,
            projected(json!({"ProjectionExpression": "Safety.Warning"})),
        )
        .await
        .unwrap();
        assert_eq!(
            (found["Count"].clone(), items(&found)),
            (json!(1), vec![json!({})])
        );
        let found = query(
            &store,
            projected(json!({
                "ProjectionExpression": "#sw",
                "ExpressionAttributeNames": {"#sw": "Safety.Warning"}
            })),
        )
        .await
        .unwrap();
        assert_eq!(
            (found["Count"].clone(), items(&found)),
            (
                json!(1),
                vec![json!({"Safety.Warning": {"S": "flat-value"}})]
            )
        );
    }
);

// AWS behavioural: check_gsi_scan_all_projection_and_filtered_limit_paging.py
store_test!(
    given_all_projection_gsi_when_scanned_with_filter_and_limit_then_matching_rows_cover_all_pages,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
            &[("gpk", "S")],
            vec![gsi(
                "gsi-all",
                &[("gpk", "HASH")],
                json!({"ProjectionType": "ALL"}),
            )],
        )
        .await;
        let rows: Vec<Value> = (0..7)
            .map(|index| {
                json!({
                    "pk": {"S": format!("p{index}")}, "sk": {"S": "fixed"},
                    "gpk": {"S": format!("g{index}")},
                    "category": {"S": if index % 2 == 0 { "even" } else { "odd" }},
                    "body": {"S": format!("body{index}")}
                })
            })
            .collect();
        for row in &rows {
            put(&store, &table, row.clone()).await;
        }
        let everything = scan(&store, json!({"TableName": table, "IndexName": "gsi-all"}))
            .await
            .unwrap();
        let mut found = items(&everything);
        found.sort_by_key(|item| item["pk"]["S"].as_str().unwrap().to_owned());
        assert_eq!(found, rows);
        let walked = pages(
            &store,
            "Scan",
            json!({
                "TableName": table, "IndexName": "gsi-all", "Limit": 2,
                "FilterExpression": "category = :c",
                "ExpressionAttributeValues": {":c": {"S": "even"}}
            }),
        )
        .await;
        let matched = all_items(&walked);
        let mut keys = strs(&matched, "pk", "S");
        keys.sort();
        assert_eq!(keys, ["p0", "p2", "p4", "p6"]);
        assert!(
            matched
                .iter()
                .all(|item| item["category"] == json!({"S": "even"}))
        );
    }
);

// AWS behavioural:
// check_gsi_scan_filter_expression_parenthesized_or_under_and.py
store_test!(
    given_gsi_scan_when_filter_groups_or_under_and_then_grouping_changes_the_selection,
    |store| {
        let table = make_table(
            &store,
            &[("id", "S", "HASH")],
            &[("category", "S")],
            vec![gsi(
                "category-index",
                &[("category", "HASH")],
                json!({"ProjectionType": "ALL"}),
            )],
        )
        .await;
        for (id, category, status, flagged) in [
            ("item-001", "books", "new", true),
            ("item-002", "books", "used", true),
            ("item-003", "toys", "new", true),
            ("item-004", "toys", "used", false),
            ("item-005", "games", "new", false),
            ("item-006", "games", "used", false),
        ] {
            put(
                &store,
                &table,
                json!({
                    "id": {"S": id}, "category": {"S": category},
                    "status": {"S": status}, "flagged": {"BOOL": flagged}
                }),
            )
            .await;
        }
        let run = |filter: &str| {
            let request = json!({
                "TableName": table, "IndexName": "category-index", "FilterExpression": filter,
                "ExpressionAttributeNames": {"#s": "status", "#f": "flagged"},
                "ExpressionAttributeValues": {
                    ":s1": {"S": "new"}, ":s2": {"S": "used"}, ":yes": {"BOOL": true}
                }
            });
            pages(&store, "Scan", request)
        };
        let grouped = run("(#s = :s1 OR #s = :s2) AND #f = :yes").await;
        let mut ids = strs(&all_items(&grouped), "id", "S");
        ids.sort();
        assert_eq!(ids, ["item-001", "item-002", "item-003"]);
        let count: u64 = grouped
            .iter()
            .map(|page| page["Count"].as_u64().unwrap())
            .sum();
        let scanned: u64 = grouped
            .iter()
            .map(|page| page["ScannedCount"].as_u64().unwrap())
            .sum();
        assert_eq!((count, scanned), (3, 6));
        let loose = run("#s = :s1 OR #s = :s2 AND #f = :yes").await;
        let mut ids = strs(&all_items(&loose), "id", "S");
        ids.sort();
        assert_eq!(ids, ["item-001", "item-002", "item-003", "item-005"]);
    }
);

// AWS behavioural: check_gsi_scan_pagination_over_tied_index_sort_key.py
store_test!(
    given_gsi_partition_tied_on_the_index_sort_key_when_scanned_with_limits_then_every_walk_matches_the_unpaged_scan,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
            &[("gk", "S"), ("gs", "S")],
            vec![gsi(
                "gsi1",
                &[("gk", "HASH"), ("gs", "RANGE")],
                json!({"ProjectionType": "ALL"}),
            )],
        )
        .await;
        let ranks = ["x", "X", "_x"];
        let mut count = 0;
        for pk in ["p0", "p1", "p2", "p3"] {
            for sk in ["s0", "s1", "s2"] {
                put(
                    &store,
                    &table,
                    json!({
                        "pk": {"S": pk}, "sk": {"S": sk}, "gk": {"S": "G"},
                        "gs": {"S": ranks[count % 3]}
                    }),
                )
                .await;
                count += 1;
            }
        }
        put(
            &store,
            &table,
            json!({"pk": {"S": "p9"}, "sk": {"S": "s0"}, "gk": {"S": "H"}, "gs": {"S": "x"}}),
        )
        .await;
        let walk = |limit: Option<u64>| {
            let mut request = json!({"TableName": table, "IndexName": "gsi1"});
            if let Some(limit) = limit {
                request["Limit"] = json!(limit);
            }
            pages(&store, "Scan", request)
        };
        let unpaged_pages = walk(None).await;
        let unpaged = all_items(&unpaged_pages);
        let keys = |found: &[Value]| -> Vec<(String, String)> {
            found
                .iter()
                .map(|item| {
                    (
                        item["pk"]["S"].as_str().unwrap().to_owned(),
                        item["sk"]["S"].as_str().unwrap().to_owned(),
                    )
                })
                .collect()
        };
        let unpaged_keys = keys(&unpaged);
        assert_eq!(unpaged_keys.len(), 13);
        let mut distinct = unpaged_keys.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), 13);
        let again = walk(None).await;
        assert_eq!(keys(&all_items(&again)), unpaged_keys);
        let group_ranks: Vec<&str> = unpaged
            .iter()
            .filter(|item| item["gk"]["S"] == "G")
            .map(|item| item["gs"]["S"].as_str().unwrap())
            .collect();
        let expected_ranks: Vec<&str> = ["X", "_x", "x"]
            .iter()
            .flat_map(|rank| std::iter::repeat_n(*rank, 4))
            .collect();
        assert_eq!(group_ranks, expected_ranks);
        for limit in [1u64, 2, 3, 5, 7, 10] {
            let walked = walk(Some(limit)).await;
            assert_eq!(keys(&all_items(&walked)), unpaged_keys, "Limit={limit}");
            assert!(
                walked.iter().all(|page| items(page).len() as u64 <= limit),
                "Limit={limit}"
            );
            let continuations: Vec<&Value> = walked
                .iter()
                .filter_map(|page| page.get("LastEvaluatedKey"))
                .collect();
            assert!(!continuations.is_empty(), "Limit={limit}");
            for key in continuations {
                assert_eq!(names(key), ["gk", "gs", "pk", "sk"], "Limit={limit}");
            }
        }
    }
);

// AWS behavioural:
// check_keys_only_gsi_query_and_scan_page_by_projected_entry_size_not_base_item_size.
// py
store_test!(
    given_keys_only_gsi_when_base_items_exceed_one_megabyte_then_index_reads_return_one_page,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "S", "RANGE")],
            &[("gsi1pk", "S")],
            vec![gsi(
                "idx-keys-only",
                &[("gsi1pk", "HASH")],
                json!({"ProjectionType": "KEYS_ONLY"}),
            )],
        )
        .await;
        let payload = "x".repeat(48_000);
        let expected: Vec<String> = (0..23).map(|index| format!("{index:03}")).collect();
        for sk in &expected {
            put(
                &store,
                &table,
                json!({
                    "pk": {"S": "base"}, "sk": {"S": sk}, "gsi1pk": {"S": "G#1"},
                    "payload": {"S": payload}
                }),
            )
            .await;
        }
        let base = query(
            &store,
            json!({
                "TableName": table, "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "base"}}, "ConsistentRead": true
            }),
        )
        .await
        .unwrap();
        assert!(base.get("LastEvaluatedKey").is_some());
        assert!(base["Count"].as_u64().unwrap() < 23);

        let indexed = query(
            &store,
            json!({
                "TableName": table, "IndexName": "idx-keys-only",
                "KeyConditionExpression": "gsi1pk = :g",
                "ExpressionAttributeValues": {":g": {"S": "G#1"}}
            }),
        )
        .await
        .unwrap();
        assert_eq!(indexed["Count"], 23);
        assert!(indexed.get("LastEvaluatedKey").is_none());
        let mut found = strs(&items(&indexed), "sk", "S");
        found.sort();
        assert_eq!(found, expected);
        assert!(
            items(&indexed)
                .iter()
                .all(|item| names(item) == ["gsi1pk", "pk", "sk"])
        );

        let scanned = scan(
            &store,
            json!({"TableName": table, "IndexName": "idx-keys-only"}),
        )
        .await
        .unwrap();
        assert_eq!(scanned["Count"], 23);
        assert_eq!(scanned["ScannedCount"], scanned["Count"]);
        assert!(scanned.get("LastEvaluatedKey").is_none());
        let mut found = strs(&items(&scanned), "sk", "S");
        found.sort();
        assert_eq!(found, expected);
    }
);

// AWS behavioural: check_numeric_sort_key_orders_numerically.py
store_test!(
    given_numeric_sort_keys_when_queried_then_order_ranges_and_get_item_are_numeric,
    |store| {
        let table = hash_range(&store, "N").await;
        for sk in ["100", "-3", "1000", "9.5", "0", "10", "-20", "2"] {
            put(&store, &table, json!({"pk": {"S": "n"}, "sk": {"N": sk}})).await;
        }
        let run = |condition: &str, values: Value, forward: bool| {
            let mut all = json!({":p": {"S": "n"}});
            for (name, value) in values.as_object().unwrap() {
                all[name] = value.clone();
            }
            query(
                &store,
                json!({
                    "TableName": table, "KeyConditionExpression": condition,
                    "ExpressionAttributeValues": all, "ScanIndexForward": forward,
                    "ConsistentRead": true
                }),
            )
        };
        let numeric = ["-20", "-3", "0", "2", "9.5", "10", "100", "1000"];
        let found = run("pk = :p", json!({}), true).await.unwrap();
        assert_eq!(strs(&items(&found), "sk", "N"), numeric);
        let found = run("pk = :p", json!({}), false).await.unwrap();
        let mut reversed = numeric;
        reversed.reverse();
        assert_eq!(strs(&items(&found), "sk", "N"), reversed);
        let found = run(
            "pk = :p AND sk BETWEEN :lo AND :hi",
            json!({":lo": {"N": "0"}, ":hi": {"N": "100"}}),
            true,
        )
        .await
        .unwrap();
        assert_eq!(
            strs(&items(&found), "sk", "N"),
            ["0", "2", "9.5", "10", "100"]
        );
        let found = run("pk = :p AND sk > :v", json!({":v": {"N": "9"}}), true)
            .await
            .unwrap();
        assert_eq!(
            strs(&items(&found), "sk", "N"),
            ["9.5", "10", "100", "1000"]
        );
        let found = run("pk = :p AND sk < :v", json!({":v": {"N": "0"}}), true)
            .await
            .unwrap();
        assert_eq!(strs(&items(&found), "sk", "N"), ["-20", "-3"]);
        let got = store
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": "n"}, "sk": {"N": "9.5"}},
                    "ConsistentRead": true
                }))
                .unwrap(),
            )
            .await
            .expect("get item");
        assert_eq!(
            serde_json::to_value(got).unwrap()["Item"]["sk"],
            json!({"N": "9.5"})
        );
    }
);

// AWS behavioural: check_query_filter_expression.py
store_test!(
    given_five_items_when_query_filters_then_count_is_post_filter_and_scanned_count_is_pre_filter,
    |store| {
        let table = hash_range(&store, "S").await;
        for index in 0..5 {
            put(
                &store,
                &table,
                json!({
                    "pk": {"S": "f"}, "sk": {"S": format!("{index:03}")},
                    "status": {"S": if index % 2 == 0 { "active" } else { "inactive" }}
                }),
            )
            .await;
        }
        let found = query(
            &store,
            json!({
                "TableName": table, "KeyConditionExpression": "pk = :pk",
                "FilterExpression": "#st = :s", "ExpressionAttributeNames": {"#st": "status"},
                "ExpressionAttributeValues": {":pk": {"S": "f"}, ":s": {"S": "active"}},
                "ConsistentRead": true
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            (found["Count"].clone(), found["ScannedCount"].clone()),
            (json!(3), json!(5))
        );
    }
);

// AWS behavioural:
// check_query_filter_expression_never_and_partly_matching_costs_nothing.py
// (capacity assertions dropped).
store_test!(
    given_never_and_partly_matching_filters_when_queried_then_only_count_drops,
    |store| {
        let table = hash_range(&store, "S").await;
        let rows = [("o1", "pending"), ("o2", "complete"), ("o3", "pending")];
        for (sk, status) in rows {
            put(
                &store,
                &table,
                json!({"pk": {"S": "u1"}, "sk": {"S": sk}, "status": {"S": status}}),
            )
            .await;
        }
        let run = |extra: Value| {
            let mut request = json!({
                "TableName": table, "KeyConditionExpression": "pk = :pk",
                "ConsistentRead": true
            });
            let mut values = json!({":pk": {"S": "u1"}});
            if let Some(filter) = extra.get("filter") {
                request["FilterExpression"] = filter.clone();
                request["ExpressionAttributeNames"] = json!({"#s": "status"});
                values[":v"] = extra["value"].clone();
            }
            request["ExpressionAttributeValues"] = values;
            query(&store, request)
        };
        let unfiltered = run(json!({})).await.unwrap();
        assert_eq!(unfiltered["Count"], 3);
        assert_eq!(unfiltered["ScannedCount"], unfiltered["Count"]);
        let never = run(json!({"filter": "#s = :v", "value": {"S": "archived"}}))
            .await
            .unwrap();
        assert_eq!(
            (never["Count"].clone(), never["ScannedCount"].clone()),
            (json!(0), json!(3))
        );
        let partly = run(json!({"filter": "#s = :v", "value": {"S": "pending"}}))
            .await
            .unwrap();
        assert_eq!(
            (partly["Count"].clone(), partly["ScannedCount"].clone()),
            (json!(2), json!(3))
        );
        assert_eq!(strs(&items(&partly), "sk", "S"), ["o1", "o3"]);
    }
);

// AWS behavioural:
// check_query_filter_expression_undefined_value_rejected_over_a_1mb_partition.
// py
store_test!(
    given_partition_over_one_megabyte_when_filter_uses_an_undefined_value_then_validation_error_precedes_paging,
    |store| {
        let table = hash_range(&store, "S").await;
        let payload = "x".repeat(50 * 1024);
        for index in 0..40 {
            put(
                &store,
                &table,
                json!({
                    "pk": {"S": "big"}, "sk": {"S": format!("{index:03}")},
                    "payload": {"S": payload}
                }),
            )
            .await;
        }
        let request = |filter: &str| {
            json!({
                "TableName": table, "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "big"}}, "ConsistentRead": true,
                "FilterExpression": filter
            })
        };
        let control = query(&store, request("attribute_exists(no_such_attr)")).await.unwrap();
        assert_eq!(control["Count"], 0);
        assert!(items(&control).is_empty());
        assert!(control.get("LastEvaluatedKey").is_some());
        expect_failure(
            "undefined filter value",
            query(&store, request("tag = :missing_tag")).await,
            "ValidationException",
            "Invalid FilterExpression: An expression attribute value used in expression is not defined; attribute value: :missing_tag",
        );
    }
);

// AWS behavioural: check_query_filter_legacy_comparison_operators_beyond_eq.py
// (expression twins only; legacy QueryFilter and KeyConditions are rejected by
// design, writing/dynamodb_parity.md, Query row; capacity assertions dropped).
store_test!(
    given_mixed_item_types_when_filter_expressions_compare_then_items_count_and_scanned_count_match,
    |store| {
        let table = hash_range(&store, "S").await;
        let rows = [
            ("s0", "catalog", 1, 8, "gold", true, Some("a")),
            ("s1", "banana", 2, 9, "silver", false, Some("b")),
            ("s2", "category", 1, 10, "bronze", true, None),
            ("s3", "dog", 3, 11, "gold", false, None),
            ("s4", "cathedral", 1, 100, "platinum", true, Some("c")),
        ];
        for (sk, label, code, points, plan, enabled, memo) in rows {
            let mut item = json!({
                "pk": {"S": "p1"}, "sk": {"S": sk}, "label": {"S": label},
                "code": {"N": code.to_string()}, "points": {"N": points.to_string()},
                "plan": {"S": plan}, "enabled": {"BOOL": enabled}
            });
            if let Some(memo) = memo {
                item["memo"] = json!({"S": memo});
            }
            put(&store, &table, item).await;
        }
        let cases = [
            (
                "CONTAINS",
                "contains(#f, :v)",
                "label",
                json!({":v": {"S": "cat"}}),
                vec!["s0", "s2", "s4"],
            ),
            (
                "NE",
                "#f <> :v",
                "code",
                json!({":v": {"N": "1"}}),
                vec!["s1", "s3"],
            ),
            (
                "GT",
                "#f > :v",
                "points",
                json!({":v": {"N": "9"}}),
                vec!["s2", "s3", "s4"],
            ),
            (
                "BETWEEN",
                "#f BETWEEN :lo AND :hi",
                "points",
                json!({":lo": {"N": "9"}, ":hi": {"N": "11"}}),
                vec!["s1", "s2", "s3"],
            ),
            (
                "IN",
                "#f IN (:a, :b)",
                "plan",
                json!({":a": {"S": "gold"}, ":b": {"S": "silver"}}),
                vec!["s0", "s1", "s3"],
            ),
            (
                "EQ-BOOL",
                "#f = :v",
                "enabled",
                json!({":v": {"BOOL": true}}),
                vec!["s0", "s2", "s4"],
            ),
            (
                "NULL",
                "attribute_not_exists(#f)",
                "memo",
                json!({}),
                vec!["s2", "s3"],
            ),
            (
                "NOT_NULL",
                "attribute_exists(#f)",
                "memo",
                json!({}),
                vec!["s0", "s1", "s4"],
            ),
        ];
        for (label, filter, attribute, extra_values, expected) in cases {
            let mut values = json!({":pk": {"S": "p1"}});
            for (name, value) in extra_values.as_object().unwrap() {
                values[name] = value.clone();
            }
            let found = query(
                &store,
                json!({
                    "TableName": table, "KeyConditionExpression": "pk = :pk",
                    "FilterExpression": filter, "ExpressionAttributeNames": {"#f": attribute},
                    "ExpressionAttributeValues": values, "ConsistentRead": true
                }),
            )
            .await
            .unwrap();
            let mut keys = strs(&items(&found), "sk", "S");
            keys.sort();
            assert_eq!(keys, expected, "{label}");
            assert_eq!(found["Count"], expected.len(), "{label}");
            assert_eq!(found["ScannedCount"], 5, "{label}");
        }
    }
);

// AWS behavioural:
// check_query_key_condition_operand_type_vs_schema_type_rejected.py
store_test!(
    given_sort_key_condition_when_operand_type_is_wrong_then_operator_or_schema_error_is_reported,
    |store| {
        let operand = |kind: &str| match kind {
            "S" => json!({"S": "x"}),
            "N" => json!({"N": "1"}),
            _ => json!({"BOOL": true}),
        };
        let schema_mismatch = "One or more parameter values were invalid: Condition parameter \
                               type does not match schema type";
        let operators = [
            ("begins_with", "pk = :p AND begins_with(sk, :v)"),
            ("<", "pk = :p AND sk < :v"),
            ("BETWEEN", "pk = :p AND sk BETWEEN :v AND :v"),
            ("=", "pk = :p AND sk = :v"),
        ];
        for (sort_type, wrong_types) in [("S", ["N", "BOOL"]), ("N", ["S", "BOOL"])] {
            let table = hash_range(&store, sort_type).await;
            let control = query(
                &store,
                json!({
                    "TableName": table, "KeyConditionExpression": "pk = :p AND sk = :s",
                    "ExpressionAttributeValues": {":p": {"S": "p"}, ":s": operand(sort_type)}
                }),
            )
            .await
            .unwrap();
            assert_eq!(control["Count"], 0);
            for (name, condition) in operators {
                for wrong in wrong_types {
                    let operator_illegal =
                        (name == "begins_with" && wrong == "N") || (name != "=" && wrong == "BOOL");
                    let expected = if operator_illegal {
                        format!(
                            "Invalid KeyConditionExpression: Incorrect operand type for operator \
                             or function; operator or function: {name}, operand type: {wrong}"
                        )
                    } else {
                        schema_mismatch.to_owned()
                    };
                    expect_failure(
                        &format!("{sort_type} sort key, {name}, {wrong}"),
                        query(
                            &store,
                            json!({
                                "TableName": table, "KeyConditionExpression": condition,
                                "ExpressionAttributeValues": {":p": {"S": "p"}, ":v": operand(wrong)}
                            }),
                        )
                        .await,
                        "ValidationException",
                        &expected,
                    );
                }
            }
            let rest = scan(&store, json!({"TableName": table, "ConsistentRead": true}))
                .await
                .unwrap();
            assert_eq!(rest["Count"], 0);
        }
    }
);

const UNDEFINED_VALUE: &str = "Invalid KeyConditionExpression: An expression attribute value used \
                               in expression is not defined; attribute value: :undef";
const BAD_OPERAND: &str = "Invalid KeyConditionExpression: Incorrect operand type for operator or \
                           function; operator or function: begins_with, operand type: N";

// AWS behavioural:
// check_query_key_condition_undefined_value_vs_bad_operand_precedence.py
store_test!(
    given_key_condition_with_undefined_value_and_bad_operand_when_queried_then_first_written_error_wins,
    |store| {
        let table = hash_range(&store, "S").await;
        put(
            &store,
            &table,
            json!({"pk": {"S": "x"}, "sk": {"S": "y"}, "extra": {"S": "z"}}),
        )
        .await;
        let run = |name: &str, condition: &str, values: Value| {
            query(
                &store,
                json!({
                    "TableName": name, "KeyConditionExpression": condition,
                    "ExpressionAttributeValues": values
                }),
            )
        };
        let control = run(
            &table,
            "pk = :pk AND sk = :sk",
            json!({":pk": {"S": "x"}, ":sk": {"S": "y"}}),
        )
        .await
        .unwrap();
        assert_eq!(control["Count"], 1);
        expect_failure(
            "undefined first",
            run(&table, "pk = :undef AND begins_with(sk, :bad)", json!({":bad": {"N": "1"}})).await,
            "ValidationException",
            UNDEFINED_VALUE,
        );
        expect_failure(
            "operand first",
            run(&table, "begins_with(sk, :bad) AND pk = :undef", json!({":bad": {"N": "1"}})).await,
            "ValidationException",
            BAD_OPERAND,
        );
        expect_failure(
            "operand vs third clause",
            run(
                &table,
                "pk = :pk AND begins_with(sk, :bad) AND extra = :extra",
                json!({":pk": {"S": "x"}, ":bad": {"N": "1"}, ":extra": {"S": "z"}}),
            )
            .await,
            "ValidationException",
            BAD_OPERAND,
        );
        expect_failure(
            "well-typed third clause",
            run(
                &table,
                "pk = :pk AND sk = :sk AND extra = :extra",
                json!({":pk": {"S": "x"}, ":sk": {"S": "y"}, ":extra": {"S": "z"}}),
            )
            .await,
            "ValidationException",
            "The number of query conditions (3) exceeds the number of key attributes defined in the schema (2)",
        );
        let absent = unique_table_name().to_string();
        expect_failure(
            "operand vs missing table",
            run(
                &absent,
                "pk = :pk AND begins_with(sk, :bad)",
                json!({":pk": {"S": "x"}, ":bad": {"N": "1"}}),
            )
            .await,
            "ValidationException",
            BAD_OPERAND,
        );
        expect_failure(
            "well-formed call against a missing table",
            run(
                &absent,
                "pk = :pk AND sk = :sk",
                json!({":pk": {"S": "x"}, ":sk": {"S": "y"}}),
            )
            .await,
            "ResourceNotFoundException",
            "Requested resource not found",
        );
        let control = run(
            &table,
            "pk = :pk AND sk = :sk",
            json!({":pk": {"S": "x"}, ":sk": {"S": "y"}}),
        )
        .await
        .unwrap();
        assert_eq!(control["Count"], 1);
    }
);

// AWS behavioural:
// check_query_undefined_expression_value_precedes_resource_not_found.py
store_test!(
    given_absent_table_when_key_condition_has_an_undefined_value_then_validation_error_precedes_not_found,
    |store| {
        let table = hash_range(&store, "S").await;
        put(&store, &table, json!({"pk": {"S": "x"}, "sk": {"S": "y"}})).await;
        let control = query(
            &store,
            json!({
                "TableName": table, "KeyConditionExpression": "pk = :pk AND sk = :sk",
                "ExpressionAttributeValues": {":pk": {"S": "x"}, ":sk": {"S": "y"}}
            }),
        )
        .await
        .unwrap();
        assert_eq!(control["Count"], 1);
        let absent = unique_table_name().to_string();
        let run = |condition: &str, values: Value| {
            query(
                &store,
                json!({
                    "TableName": absent, "KeyConditionExpression": condition,
                    "ExpressionAttributeValues": values
                }),
            )
        };
        expect_failure(
            "well-formed expression",
            run(
                "pk = :pk AND sk = :sk",
                json!({":pk": {"S": "x"}, ":sk": {"S": "y"}}),
            )
            .await,
            "ResourceNotFoundException",
            "Requested resource not found",
        );
        expect_failure(
            "wrong-typed operand and a third clause",
            run(
                "pk = :pk AND sk = :sk AND extra = :extra",
                json!({":pk": {"L": [{"S": "x"}]}, ":sk": {"S": "y"}, ":extra": {"S": "z"}}),
            )
            .await,
            "ResourceNotFoundException",
            "Requested resource not found",
        );
        expect_failure(
            "undefined placeholder",
            run(
                "pk = :pk AND sk = :sk AND extra = :extra",
                json!({":pk": {"L": [{"S": "x"}]}, ":sk": {"S": "y"}}),
            )
            .await,
            "ValidationException",
            "Invalid KeyConditionExpression: An expression attribute value used in expression is not defined; attribute value: :extra",
        );
    }
);

// AWS behavioural:
// check_query_limit_at_partition_count_keeps_lastevaluatedkey_scan_limit_above_count_drops_it.
// py
store_test!(
    given_limit_equal_to_partition_size_when_queried_then_key_is_kept_and_scan_limit_above_count_drops_it,
    |store| {
        let table = hash_range(&store, "S").await;
        for sk in ["001", "002", "003"] {
            put(&store, &table, json!({"pk": {"S": "p1"}, "sk": {"S": sk}})).await;
        }
        let request = json!({
            "TableName": table, "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "p1"}}, "Limit": 3,
            "ConsistentRead": true
        });
        let first = query(&store, request.clone()).await.unwrap();
        assert_eq!(first["Count"], 3);
        assert_eq!(strs(&items(&first), "sk", "S"), ["001", "002", "003"]);
        assert_eq!(
            first["LastEvaluatedKey"],
            json!({"pk": {"S": "p1"}, "sk": {"S": "003"}})
        );
        let mut resumed = request;
        resumed["ExclusiveStartKey"] = first["LastEvaluatedKey"].clone();
        let second = query(&store, resumed).await.unwrap();
        assert_eq!(second["Count"], 0);
        assert!(items(&second).is_empty());
        assert!(second.get("LastEvaluatedKey").is_none());
        let scanned = scan(
            &store,
            json!({"TableName": table, "Limit": 5, "ConsistentRead": true}),
        )
        .await
        .unwrap();
        assert_eq!((scanned["Count"].clone(), scanned["ScannedCount"].clone()), (json!(3), json!(3)));
        assert!(scanned.get("LastEvaluatedKey").is_none());
    }
);

async fn big_partition(
    store: &TestStore,
    count: usize,
    payload_bytes: usize,
) -> (String, Vec<String>) {
    let table = hash_range(store, "S").await;
    let payload = "x".repeat(payload_bytes);
    let sort_keys: Vec<String> = (0..count).map(|index| format!("{index:03}")).collect();
    for sk in &sort_keys {
        put(
            store,
            &table,
            json!({"pk": {"S": "big"}, "sk": {"S": sk}, "payload": {"S": payload}}),
        )
        .await;
    }
    (table, sort_keys)
}

fn big_query(table: &str, forward: bool) -> Value {
    json!({
        "TableName": table, "KeyConditionExpression": "pk = :p",
        "ExpressionAttributeValues": {":p": {"S": "big"}}, "ConsistentRead": true,
        "ScanIndexForward": forward
    })
}

// AWS behavioural: check_query_natural_1mb_page_truncation.py
store_test!(
    given_two_megabyte_partition_when_queried_without_limit_then_pages_are_maximal_prefixes_within_one_megabyte,
    |store| {
        let (table, expected) = big_partition(&store, 40, 50 * 1024).await;
        let walked = pages(&store, "Query", big_query(&table, true)).await;
        assert!(walked.len() >= 2);
        let everything = all_items(&walked);
        let mut consumed = 0;
        for (index, page) in walked.iter().enumerate() {
            let found = items(page);
            consumed += found.len();
            assert_eq!(page["Count"], found.len());
            assert_eq!(page["ScannedCount"], page["Count"]);
            if let Some(key) = page.get("LastEvaluatedKey") {
                assert_maximal_prefix(&found, &everything[consumed], index + 1);
                assert_eq!(key, &key_of(found.last().unwrap()));
            }
        }
        assert!(walked[0]["Count"].as_u64().unwrap() < 40);
        assert_eq!(strs(&all_items(&walked), "sk", "S"), expected);
    }
);

// AWS behavioural: check_query_pages_at_1mb_before_a_never_matching_filter.py
store_test!(
    given_two_megabyte_partition_when_a_never_matching_filter_runs_then_pages_break_at_the_same_keys,
    |store| {
        let (table, _) = big_partition(&store, 40, 50 * 1024).await;
        let unfiltered = pages(&store, "Query", big_query(&table, true)).await;
        assert_eq!(unfiltered.len(), 2);
        let mut filtered_request = big_query(&table, true);
        filtered_request["FilterExpression"] = json!("attribute_exists(no_such_attr)");
        let filtered = pages(&store, "Query", filtered_request).await;
        assert_eq!(filtered.len(), unfiltered.len());
        for (plain, hidden) in unfiltered.iter().zip(&filtered) {
            assert_eq!(hidden["Count"], 0);
            assert!(items(hidden).is_empty());
            assert_eq!(hidden["ScannedCount"], plain["Count"]);
            assert_eq!(hidden.get("LastEvaluatedKey"), plain.get("LastEvaluatedKey"));
        }
        let scanned: u64 = filtered.iter().map(|page| page["ScannedCount"].as_u64().unwrap()).sum();
        assert_eq!(scanned, 40);
    }
);

// AWS behavioural:
// check_query_pagination_covers_every_item_exactly_once_in_key_order.py
store_test!(
    given_two_hundred_numeric_sort_keys_when_paged_with_limit_seven_then_every_key_appears_once_in_order,
    |store| {
        let table = hash_range(&store, "N").await;
        for index in 0..200 {
            put(
                &store,
                &table,
                json!({"pk": {"S": "P"}, "sk": {"N": index.to_string()}}),
            )
            .await;
        }
        let request = |forward: bool| {
            json!({
                "TableName": table, "Limit": 7, "ConsistentRead": true,
                "ScanIndexForward": forward, "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "P"}}
            })
        };
        let forward = pages(&store, "Query", request(true)).await;
        let backward = pages(&store, "Query", request(false)).await;
        let expected: Vec<String> = (0..200).map(|index| index.to_string()).collect();
        assert_eq!(strs(&all_items(&forward), "sk", "N"), expected);
        let mut reversed = expected;
        reversed.reverse();
        assert_eq!(strs(&all_items(&backward), "sk", "N"), reversed);
        assert!(forward.len() >= 29, "got {} pages", forward.len());
    }
);

// AWS behavioural: check_query_reverse_pages_at_1mb_in_descending_key_order.py
store_test!(
    given_two_megabyte_partition_when_queried_in_both_directions_then_pages_break_within_one_item_of_one_megabyte,
    |store| {
        let (table, ascending) = big_partition(&store, 45, 50 * 1024).await;
        for forward in [false, true] {
            let walked = pages(&store, "Query", big_query(&table, forward)).await;
            assert!(walked.len() >= 2, "forward={forward}");
            let largest = item_bytes(&items(&walked[0])[0]);
            for (index, page) in walked.iter().enumerate() {
                let found = items(page);
                assert!(page_bytes(&found) <= ONE_MB + largest, "forward={forward} page {index}");
                if let Some(key) = page.get("LastEvaluatedKey") {
                    assert_eq!(key, &key_of(found.last().unwrap()), "forward={forward}");
                }
            }
            let mut expected = ascending.clone();
            if !forward {
                expected.reverse();
            }
            assert_eq!(strs(&all_items(&walked), "sk", "S"), expected, "forward={forward}");
        }
    }
);

// AWS behavioural: check_query_scan_exclusive_start_key_empty_value_rejected.py
store_test!(
    given_table_and_gsi_when_start_key_has_an_empty_key_value_then_query_and_scan_name_the_attribute,
    |store| {
        let table = make_table(
            &store,
            &[("pk", "S", "HASH"), ("sk", "B", "RANGE")],
            &[("gpk", "B"), ("gsk", "S")],
            vec![gsi(
                "gsi1",
                &[("gpk", "HASH"), ("gsk", "RANGE")],
                json!({"ProjectionType": "ALL"}),
            )],
        )
        .await;
        let item1 = json!({"pk": {"S": "x"}, "sk": {"B": "MQ=="}, "gpk": {"B": "Zw=="}, "gsk": {"S": "h"}});
        let item2 = json!({"pk": {"S": "x"}, "sk": {"B": "Mg=="}, "gpk": {"B": "Zw=="}, "gsk": {"S": "i"}});
        put(&store, &table, item1.clone()).await;
        put(&store, &table, item2.clone()).await;
        let table_query = json!({
            "TableName": table, "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "x"}}
        });
        let gsi_query = json!({
            "TableName": table, "IndexName": "gsi1", "KeyConditionExpression": "gpk = :g",
            "ExpressionAttributeValues": {":g": {"B": "Zw=="}}
        });
        let with = |base: &Value, extra: Vec<(&str, Value)>| {
            let mut request = base.clone();
            for (name, value) in extra {
                request[name] = value;
            }
            request
        };
        let table_scan = json!({"TableName": table});
        let gsi_scan = json!({"TableName": table, "IndexName": "gsi1"});
        let table_esk = query(&store, with(&table_query, vec![("Limit", json!(1)), ("ConsistentRead", json!(true))]))
            .await.unwrap()["LastEvaluatedKey"].clone();
        let scan_esk = scan(&store, with(&table_scan, vec![("Limit", json!(1)), ("ConsistentRead", json!(true))]))
            .await.unwrap()["LastEvaluatedKey"].clone();
        let gsi_query_esk = query(&store, with(&gsi_query, vec![("Limit", json!(1))]))
            .await.unwrap()["LastEvaluatedKey"].clone();
        let gsi_scan_esk = scan(&store, with(&gsi_scan, vec![("Limit", json!(1))]))
            .await.unwrap()["LastEvaluatedKey"].clone();
        assert!(!items(&query(&store, with(&table_query, vec![("ExclusiveStartKey", table_esk.clone())])).await.unwrap()).is_empty());
        assert!(!items(&query(&store, with(&gsi_query, vec![("ExclusiveStartKey", gsi_query_esk.clone())])).await.unwrap()).is_empty());
        scan(&store, with(&table_scan, vec![("ExclusiveStartKey", scan_esk.clone())])).await.unwrap();
        scan(&store, with(&gsi_scan, vec![("ExclusiveStartKey", gsi_scan_esk.clone())])).await.unwrap();

        let replaced = |key: &Value, attribute: &str, value: Value| {
            let mut bad = key.clone();
            bad[attribute] = value;
            bad
        };
        let query_cases = [
            ("table hash", false, replaced(&table_esk, "pk", json!({"S": ""})), "pk", "string"),
            ("table range", false, replaced(&table_esk, "sk", json!({"B": ""})), "sk", "binary"),
            ("GSI hash", true, replaced(&gsi_query_esk, "gpk", json!({"B": ""})), "gpk", "binary"),
            ("GSI range", true, replaced(&gsi_query_esk, "gsk", json!({"S": ""})), "gsk", "string"),
        ];
        for (label, on_index, bad, attribute, kind) in query_cases {
            let base = if on_index { &gsi_query } else { &table_query };
            expect_failure(
                &format!("Query {label}"),
                query(&store, with(base, vec![("ExclusiveStartKey", bad)])).await,
                "ValidationException",
                &format!(
                    "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty {kind} value. Key: {attribute}"
                ),
            );
        }
        let scan_cases = [
            ("table hash", false, replaced(&scan_esk, "pk", json!({"S": ""})), "pk", "string"),
            ("table range", false, replaced(&scan_esk, "sk", json!({"B": ""})), "sk", "binary"),
            ("GSI hash", true, replaced(&gsi_scan_esk, "gpk", json!({"B": ""})), "gpk", "binary"),
            ("GSI range", true, replaced(&gsi_scan_esk, "gsk", json!({"S": ""})), "gsk", "string"),
        ];
        for (label, on_index, bad, attribute, kind) in scan_cases {
            let base = if on_index { &gsi_scan } else { &table_scan };
            expect_failure(
                &format!("Scan {label}"),
                scan(&store, with(base, vec![("ExclusiveStartKey", bad)])).await,
                "ValidationException",
                &format!(
                    "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty {kind} value. IndexName: gsi1, IndexKey: {attribute}"
                ),
            );
        }
        let rest = scan(&store, json!({"TableName": table, "ConsistentRead": true}))
            .await
            .unwrap();
        assert_eq!(items(&rest), [item1, item2]);
    }
);

// AWS behavioural:
// check_query_value_first_sort_key_comparison_and_size_of_absent_attribute.py
store_test!(
    given_numeric_sort_keys_when_value_leads_the_comparison_then_selection_matches_and_size_of_absent_matches_nothing,
    |store| {
        let table = hash_range(&store, "N").await;
        for index in 1..=10 {
            put(
                &store,
                &table,
                json!({
                    "pk": {"S": "u"}, "sk": {"N": index.to_string()},
                    "tag": {"S": if index == 4 { String::new() } else { format!("t{index}") }}
                }),
            )
            .await;
        }
        let run = |condition: &str, values: Value, filter: Option<&str>| {
            let mut all = json!({":p": {"S": "u"}});
            for (name, value) in values.as_object().unwrap() {
                all[name] = value.clone();
            }
            let mut request = json!({
                "TableName": table, "KeyConditionExpression": condition,
                "ConsistentRead": true, "ExpressionAttributeValues": all
            });
            if let Some(filter) = filter {
                request["FilterExpression"] = json!(filter);
            }
            query(&store, request)
        };
        let found = run("pk = :p AND :lo <= sk", json!({":lo": {"N": "3"}}), None).await.unwrap();
        let expected: Vec<String> = (3..=10).map(|index| index.to_string()).collect();
        assert_eq!(strs(&items(&found), "sk", "N"), expected);
        let key_first = run("pk = :p AND sk >= :lo", json!({":lo": {"N": "3"}}), None).await.unwrap();
        assert_eq!(items(&found), items(&key_first));
        let found = run("pk = :p AND :hi > sk", json!({":hi": {"N": "4"}}), None).await.unwrap();
        assert_eq!(strs(&items(&found), "sk", "N"), ["1", "2", "3"]);
        let zero = json!({":zero": {"N": "0"}});
        let absent = run("pk = :p", zero.clone(), Some("size(nonexistent_attr) = :zero")).await.unwrap();
        assert_eq!((absent["Count"].clone(), absent["ScannedCount"].clone()), (json!(0), json!(10)));
        let empty = run("pk = :p", zero, Some("size(tag) = :zero")).await.unwrap();
        assert_eq!(strs(&items(&empty), "sk", "N"), ["4"]);
    }
);

// AWS behavioural:
// check_scan_and_query_filter_set_equality_compares_members_not_order_or_subset.
// py
store_test!(
    given_set_attributes_when_filter_compares_equality_then_members_decide_not_order_or_subset,
    |store| {
        let table = hash_range(&store, "S").await;
        let rows = [
            ("exact_ss", json!({"ssattr": {"SS": ["b", "a"]}})),
            ("subset_ss", json!({"ssattr": {"SS": ["a"]}})),
            ("superset_ss", json!({"ssattr": {"SS": ["a", "b", "c"]}})),
            ("exact_ns", json!({"nsattr": {"NS": ["2", "1"]}})),
            ("subset_ns", json!({"nsattr": {"NS": ["1"]}})),
            ("superset_ns", json!({"nsattr": {"NS": ["1", "2", "3"]}})),
            ("exact_bs", json!({"bsattr": {"BS": ["eQ==", "eA=="]}})),
            ("subset_bs", json!({"bsattr": {"BS": ["eA=="]}})),
            (
                "superset_bs",
                json!({"bsattr": {"BS": ["eA==", "eQ==", "eg=="]}}),
            ),
            ("missing_all", json!({})),
        ];
        let all: Vec<&str> = {
            let mut names: Vec<&str> = rows.iter().map(|(sk, _)| *sk).collect();
            names.sort();
            names
        };
        for (sk, attributes) in &rows {
            let mut item = json!({"pk": {"S": "p1"}, "sk": {"S": sk}});
            for (name, value) in attributes.as_object().unwrap() {
                item[name] = value.clone();
            }
            put(&store, &table, item).await;
        }
        let sorted_sks = |found: &Value| {
            let mut sks = strs(&items(found), "sk", "S");
            sks.sort();
            sks
        };
        for (attribute, value, exact) in [
            ("ssattr", json!({"SS": ["a", "b"]}), "exact_ss"),
            ("nsattr", json!({"NS": ["1", "2"]}), "exact_ns"),
            ("bsattr", json!({"BS": ["eA==", "eQ=="]}), "exact_bs"),
        ] {
            let complement: Vec<&str> = all.iter().copied().filter(|sk| *sk != exact).collect();
            for (operator, expected) in [("=", vec![exact]), ("<>", complement)] {
                let filter = format!("{attribute} {operator} :v");
                let scanned = scan(
                    &store,
                    json!({
                        "TableName": table, "FilterExpression": filter,
                        "ExpressionAttributeValues": {":v": value}, "ConsistentRead": true
                    }),
                )
                .await
                .unwrap();
                assert_eq!(sorted_sks(&scanned), expected, "Scan {filter}");
                if attribute == "ssattr" {
                    let queried = query(
                        &store,
                        json!({
                            "TableName": table, "KeyConditionExpression": "pk = :pk",
                            "FilterExpression": filter,
                            "ExpressionAttributeValues": {":v": value, ":pk": {"S": "p1"}},
                            "ConsistentRead": true
                        }),
                    )
                    .await
                    .unwrap();
                    assert_eq!(sorted_sks(&queried), expected, "Query {filter}");
                }
            }
        }
    }
);

// AWS behavioural:
// check_scan_exclusive_start_key_naming_a_deleted_item_resumes_without_error.py
store_test!(
    given_start_key_of_a_deleted_item_when_scan_resumes_then_exactly_the_unseen_items_return,
    |store| {
        let table = hash_only(&store).await;
        let all: std::collections::BTreeSet<String> =
            (0..16).map(|index| format!("item-{index:03}")).collect();
        for pk in &all {
            put(
                &store,
                &table,
                json!({"pk": {"S": pk}, "category": {"S": "keep"}}),
            )
            .await;
        }
        let first = scan(
            &store,
            json!({"TableName": table, "Limit": 5, "ConsistentRead": true}),
        )
        .await
        .unwrap();
        assert_eq!(items(&first).len(), 5);
        let key = first["LastEvaluatedKey"].clone();
        let seen: std::collections::BTreeSet<String> =
            strs(&items(&first), "pk", "S").into_iter().collect();
        let deleted = key["pk"]["S"].as_str().unwrap().to_owned();
        assert!(seen.contains(&deleted));
        store
            .operations
            .delete_item(
                DeleteItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"S": deleted}}
                }))
                .unwrap(),
            )
            .await
            .expect("delete item");
        let resumed = pages(
            &store,
            "Scan",
            json!({
                "TableName": table, "ConsistentRead": true, "ExclusiveStartKey": key,
                "FilterExpression": "category = :want",
                "ExpressionAttributeValues": {":want": {"S": "keep"}}
            }),
        )
        .await;
        let found: std::collections::BTreeSet<String> =
            strs(&all_items(&resumed), "pk", "S").into_iter().collect();
        let expected: std::collections::BTreeSet<String> = all.difference(&seen).cloned().collect();
        assert_eq!(found, expected);
        let rest = pages(
            &store,
            "Scan",
            json!({"TableName": table, "ConsistentRead": true}),
        )
        .await;
        let remaining: std::collections::BTreeSet<String> =
            strs(&all_items(&rest), "pk", "S").into_iter().collect();
        let mut without = all.clone();
        without.remove(&deleted);
        assert_eq!(remaining, without);
    }
);

// AWS behavioural:
// check_scan_pages_at_1mb_with_and_without_never_matching_filter.py
store_test!(
    given_mixed_size_items_over_one_megabyte_when_scanned_then_pages_break_at_one_megabyte_with_and_without_a_filter,
    |store| {
        let table = hash_only(&store).await;
        let mut written = Vec::new();
        for index in 0..52 {
            let payload = if index % 2 == 0 { "x".repeat(50 * 1024) } else { "t".to_owned() };
            let item = json!({"pk": {"S": format!("i{index:03}")}, "payload": {"S": payload}});
            put(&store, &table, item.clone()).await;
            written.push(item);
        }
        let largest = written.iter().map(item_bytes).max().unwrap();
        let walked = pages(&store, "Scan", json!({"TableName": table, "ConsistentRead": true})).await;
        assert!(walked.len() >= 2);
        assert!(walked[0].get("LastEvaluatedKey").is_some());
        for page in &walked {
            let found = items(page);
            assert!(page_bytes(&found) <= ONE_MB + largest);
            assert_eq!(page["ScannedCount"], page["Count"]);
            assert_eq!(page["Count"], found.len());
        }
        let mut keys = strs(&all_items(&walked), "pk", "S");
        keys.sort();
        let expected: Vec<String> = (0..52).map(|index| format!("i{index:03}")).collect();
        assert_eq!(keys, expected);

        let filtered = pages(
            &store,
            "Scan",
            json!({
                "TableName": table, "ConsistentRead": true,
                "FilterExpression": "attribute_exists(no_such_attr)"
            }),
        )
        .await;
        assert!(filtered.len() >= 2);
        assert!(filtered[0].get("LastEvaluatedKey").is_some());
        assert!(filtered.iter().all(|page| page["Count"] == 0 && items(page).is_empty()));
        let scanned: u64 = filtered.iter().map(|page| page["ScannedCount"].as_u64().unwrap()).sum();
        assert_eq!(scanned, 52);
    }
);

// AWS behavioural:
// check_string_sort_key_ascii_byte_order_in_ranges_prefixes_and_paging.py
store_test!(
    given_string_sort_keys_when_queried_then_utf8_byte_order_governs_ranges_prefixes_and_paging,
    |store| {
        let table = hash_range(&store, "S").await;
        let byte_order = [
            "A", "B", "Z", "_", "a", "a b", "a-b", "aZ", "a_z", "ab", "b", "\u{e9}", "\u{4e2d}",
        ];
        for sk in byte_order.iter().rev() {
            put(&store, &table, json!({"pk": {"S": "p"}, "sk": {"S": sk}})).await;
        }
        let request = |condition: &str, values: Value, forward: bool, limit: Option<u64>| {
            let mut all = json!({":p": {"S": "p"}});
            for (name, value) in values.as_object().unwrap() {
                all[name] = value.clone();
            }
            let mut request = json!({
                "TableName": table, "KeyConditionExpression": condition,
                "ExpressionAttributeValues": all, "ConsistentRead": true,
                "ScanIndexForward": forward
            });
            if let Some(limit) = limit {
                request["Limit"] = json!(limit);
            }
            request
        };
        let sks = |walked: Vec<Value>| strs(&all_items(&walked), "sk", "S");
        let mut reversed = byte_order;
        reversed.reverse();
        assert_eq!(
            sks(pages(&store, "Query", request("pk = :p", json!({}), true, None)).await),
            byte_order
        );
        assert_eq!(
            sks(pages(&store, "Query", request("pk = :p", json!({}), false, None)).await),
            reversed
        );
        assert_eq!(
            sks(pages(
                &store,
                "Query",
                request(
                    "pk = :p AND sk BETWEEN :lo AND :hi",
                    json!({":lo": {"S": "_"}, ":hi": {"S": "b"}}),
                    true,
                    None
                )
            )
            .await),
            ["_", "a", "a b", "a-b", "aZ", "a_z", "ab", "b"]
        );
        assert_eq!(
            sks(pages(
                &store,
                "Query",
                request(
                    "pk = :p AND begins_with(sk, :pre)",
                    json!({":pre": {"S": "a"}}),
                    true,
                    None
                )
            )
            .await),
            ["a", "a b", "a-b", "aZ", "a_z", "ab"]
        );
        assert_eq!(
            sks(pages(
                &store,
                "Query",
                request("pk = :p", json!({}), true, Some(1))
            )
            .await),
            byte_order
        );
        assert_eq!(
            sks(pages(
                &store,
                "Query",
                request("pk = :p", json!({}), false, Some(1))
            )
            .await),
            reversed
        );
    }
);
