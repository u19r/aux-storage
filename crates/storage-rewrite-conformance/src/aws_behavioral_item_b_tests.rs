//! AWS emulator behavioural suite ports.
//!
//! Expected values are AWS's. Each test names its source script.

use std::panic::{AssertUnwindSafe, resume_unwind};

use futures::FutureExt as _;
use serde_json::{Value, json};
use storage_types::{
    BatchGetItemRequest, BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest,
    DescribeTableRequest, GetItemRequest, PutItemRequest, QueryRequest, ScanRequest, StorageError,
    TransactWriteItemsRequest, UpdateItemRequest,
};

use crate::test_support::{TestStore, normalize_attribute_sets, unique_table_name};

const CONDITION_FAILED: (&str, &str) = (
    "ConditionalCheckFailedException",
    "The conditional request failed",
);
const ITEM_TOO_BIG: (&str, &str) = (
    "ValidationException",
    "Item size has exceeded the maximum allowed size",
);
/// Bound for waiting on an asynchronously maintained index.
const INDEX_POLL_SECONDS: u64 = 10;

/// Run `$body` with a fresh store, finishing the store even when it panics.
macro_rules! scenario {
    (| $store:ident | $body:block) => {{
        let $store = TestStore::new().await;
        let outcome = AssertUnwindSafe(async { $body }).catch_unwind().await;
        $store.finish().await;
        if let Err(panic) = outcome {
            resume_unwind(panic);
        }
    }};
}

/// One operation's HTTP-level answer.
struct Reply {
    status: u16,
    body: Value,
}

impl Reply {
    fn ok(&self) -> &Value {
        assert_eq!(self.status, 200, "expected success, got {}", self.body);
        &self.body
    }

    fn error(&self) -> (String, String) {
        assert_ne!(self.status, 200, "expected an error, got {}", self.body);
        let kind = self.body["__type"].as_str().unwrap_or_default();
        let code = kind.rsplit('#').next().unwrap_or_default().to_owned();
        let message = self.body["message"]
            .as_str()
            .or_else(|| self.body["Message"].as_str())
            .unwrap_or_default()
            .to_owned();
        (code, message)
    }

    #[track_caller]
    fn assert_error(&self, expected: (impl AsRef<str>, impl AsRef<str>)) {
        assert_eq!(
            self.error(),
            (
                expected.0.as_ref().to_owned(),
                expected.1.as_ref().to_owned()
            ),
            "{}",
            self.body
        );
    }

    #[track_caller]
    fn assert_validation(&self, message: &str) {
        self.assert_error(("ValidationException", message));
    }
}

fn reply<T>(result: Result<T, StorageError>, to_value: impl FnOnce(T) -> Value) -> Reply {
    match result {
        Ok(response) => Reply {
            status: 200,
            body: to_value(response),
        },
        Err(error) => {
            let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
                http_error::HttpApiError::from(error).into();
            Reply {
                status: status.as_u16(),
                body: serde_json::to_value(body).expect("serialize error"),
            }
        }
    }
}

fn rejected_request(message: String) -> Reply {
    Reply {
        status: 400,
        body: json!({
            "__type": "com.amazon.coral.validate#ValidationException",
            "message": message,
        }),
    }
}

/// Parse and execute one DynamoDB request the way the public route does.
async fn call(store: &TestStore, operation: &str, request: Value) -> Reply {
    macro_rules! run {
        ($request:ty, $method:ident) => {
            match <$request>::try_from(request) {
                Ok(request) => reply(store.operations.$method(request).await, |response| {
                    serde_json::to_value(response).expect("serialize response")
                }),
                Err(message) => rejected_request(message),
            }
        };
    }
    match operation {
        "CreateTable" => run!(CreateTableRequest, create_table),
        "DescribeTable" => run!(DescribeTableRequest, describe_table),
        "PutItem" => run!(PutItemRequest, put_item),
        "GetItem" => run!(GetItemRequest, get_item),
        "UpdateItem" => run!(UpdateItemRequest, update_item),
        "DeleteItem" => run!(DeleteItemRequest, delete_item),
        "Query" => run!(QueryRequest, query),
        "Scan" => run!(ScanRequest, scan),
        "BatchGetItem" => run!(BatchGetItemRequest, batch_get_item),
        "BatchWriteItem" => run!(BatchWriteItemRequest, batch_write_item),
        "TransactWriteItems" => run!(TransactWriteItemsRequest, transact_write_items),
        other => panic!("unsupported operation: {other}"),
    }
}

async fn create_table(store: &TestStore, request: Value) -> String {
    let table = request["TableName"]
        .as_str()
        .expect("table name")
        .to_owned();
    call(store, "CreateTable", request).await.ok();
    table
}

/// A pay-per-request table with a hash key `pk` of `key_type` and an optional
/// range key.
fn table_request(table: &str, hash_type: &str, range: Option<(&str, &str)>) -> Value {
    let mut attributes = vec![json!({"AttributeName": "pk", "AttributeType": hash_type})];
    let mut schema = vec![json!({"AttributeName": "pk", "KeyType": "HASH"})];
    if let Some((name, kind)) = range {
        attributes.push(json!({"AttributeName": name, "AttributeType": kind}));
        schema.push(json!({"AttributeName": name, "KeyType": "RANGE"}));
    }
    json!({
        "TableName": table,
        "AttributeDefinitions": attributes,
        "KeySchema": schema,
        "BillingMode": "PAY_PER_REQUEST",
    })
}

async fn hash_table(store: &TestStore) -> String {
    let table = unique_table_name().to_string();
    create_table(store, table_request(&table, "S", None)).await
}

async fn put(store: &TestStore, table: &str, item: Value) {
    call(store, "PutItem", json!({"TableName": table, "Item": item}))
        .await
        .ok();
}

async fn get(store: &TestStore, table: &str, key: &Value) -> Value {
    let reply = call(
        store,
        "GetItem",
        json!({"TableName": table, "Key": key, "ConsistentRead": true}),
    )
    .await;
    reply.ok()["Item"].clone()
}

#[track_caller]
fn assert_same(actual: &Value, expected: &Value, context: &str) {
    let mut actual = actual.clone();
    let mut expected = expected.clone();
    normalize_attribute_sets(&mut actual);
    normalize_attribute_sets(&mut expected);
    assert_eq!(actual, expected, "{context}");
}

async fn assert_item(store: &TestStore, table: &str, key: &Value, expected: &Value, context: &str) {
    assert_same(&get(store, table, key).await, expected, context);
}

/// Keys of the item `pk` attribute only.
fn pk(value: &str) -> Value {
    json!({"pk": {"S": value}})
}

fn with(base: &Value, extra: Value) -> Value {
    let mut merged = base.as_object().expect("object").clone();
    merged.extend(extra.as_object().expect("object").clone());
    Value::Object(merged)
}

/// AWS behavioural: check_put_item_binary_set_zero_length_member_round_trips.py
#[tokio::test]
async fn given_binary_sets_with_zero_length_member_when_put_then_get_returns_members_and_scan_counts_two()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let solo = with(&pk("solo"), json!({"s": {"BS": [""]}}));
        put(&store, &table, solo.clone()).await;
        assert_item(&store, &table, &pk("solo"), &solo, "BS holding only b''").await;

        let mixed = with(&pk("mixed"), json!({"s": {"BS": ["", "AQ=="]}}));
        put(&store, &table, mixed.clone()).await;
        assert_item(
            &store,
            &table,
            &pk("mixed"),
            &mixed,
            "BS mixing b'' and b'\\x01'",
        )
        .await;

        let scanned = call(
            &store,
            "Scan",
            json!({"TableName": table, "ConsistentRead": true, "Select": "COUNT"}),
        )
        .await;
        assert_eq!(scanned.ok()["Count"], 2);
    });
}

/// AWS behavioural:
/// check_put_item_condition_nested_document_paths_and_functions_rejected.py
#[tokio::test]
async fn given_false_nested_path_and_function_conditions_when_put_then_each_is_refused_and_item_unchanged()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("ddb-put-cond-neg");
        let original = with(
            &key,
            json!({
                "m": {"M": {"k": {"S": "inner"}, "n": {"L": [{"N": "10"}, {"S": "ten"}]}}},
                "tag": {"S": "alpha-needle-beta"},
            }),
        );
        put(&store, &table, original.clone()).await;
        let refused = [
            (
                "attribute_exists(#m.#z)",
                json!({"#m": "m", "#z": "z"}),
                json!({}),
            ),
            (
                "attribute_not_exists(#m.#k)",
                json!({"#m": "m", "#k": "k"}),
                json!({}),
            ),
            (
                "#m.#k = :inner AND #m.#n[0] = :wrong",
                json!({"#m": "m", "#k": "k", "#n": "n"}),
                json!({":inner": {"S": "inner"}, ":wrong": {"N": "11"}}),
            ),
            (
                "#m.#k = :a OR #m.#k = :b",
                json!({"#m": "m", "#k": "k"}),
                json!({":a": {"S": "a"}, ":b": {"S": "b"}}),
            ),
            (
                "NOT (#m.#k = :inner)",
                json!({"#m": "m", "#k": "k"}),
                json!({":inner": {"S": "inner"}}),
            ),
            (
                "contains(#tag, :missing)",
                json!({"#tag": "tag"}),
                json!({":missing": {"S": "missing"}}),
            ),
            (
                "#m.#k = :num",
                json!({"#m": "m", "#k": "k"}),
                json!({":num": {"N": "5"}}),
            ),
        ];
        for (condition, names, values) in refused {
            let mut request = json!({
                "TableName": table,
                "Item": with(&key, json!({"overwritten": {"BOOL": true}})),
                "ConditionExpression": condition,
                "ExpressionAttributeNames": names,
            });
            if values.as_object().is_some_and(|values| !values.is_empty()) {
                request["ExpressionAttributeValues"] = values;
            }
            call(&store, "PutItem", request)
                .await
                .assert_error(CONDITION_FAILED);
            assert_item(&store, &table, &key, &original, condition).await;
        }
        let replacement = with(&key, json!({"replaced": {"BOOL": true}}));
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": replacement,
                "ConditionExpression": "attribute_exists(#m.#k)",
                "ExpressionAttributeNames": {"#m": "m", "#k": "k"},
            }),
        )
        .await
        .ok();
        assert_item(&store, &table, &key, &replacement, "holding control").await;
    });
}

/// AWS behavioural:
/// check_put_item_condition_nested_document_paths_and_functions_that_hold.py
#[tokio::test]
async fn given_holding_nested_path_and_function_conditions_when_put_then_each_write_applies() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("ddb-put-cond");
        let base = json!({
            "m": {"M": {"k": {"S": "inner"}, "n": {"L": [{"N": "10"}, {"S": "ten"}]}}},
            "status": {"S": "active"},
            "tag": {"S": "alpha-needle-beta"},
        });
        put(
            &store,
            &table,
            with(&with(&key, base.clone()), json!({"seq": {"N": "0"}})),
        )
        .await;
        let holding = [
            (
                "attribute_exists(#m.#k)",
                json!({"#m": "m", "#k": "k"}),
                json!(null),
            ),
            (
                "attribute_not_exists(#m.#z)",
                json!({"#m": "m", "#z": "z"}),
                json!(null),
            ),
            (
                "NOT (#m.#k = :other)",
                json!({"#m": "m", "#k": "k"}),
                json!({":other": {"S": "nope"}}),
            ),
            (
                "#m.#k = :inner AND #m.#n[0] = :ten",
                json!({"#m": "m", "#k": "k", "#n": "n"}),
                json!({":inner": {"S": "inner"}, ":ten": {"N": "10"}}),
            ),
            (
                "#m.#k = :other OR #m.#k = :inner",
                json!({"#m": "m", "#k": "k"}),
                json!({":other": {"S": "other"}, ":inner": {"S": "inner"}}),
            ),
            (
                "contains(#tag, :needle)",
                json!({"#tag": "tag"}),
                json!({":needle": {"S": "needle"}}),
            ),
            (
                "m.k = :inner",
                json!(null),
                json!({":inner": {"S": "inner"}}),
            ),
            (
                "#st = :active",
                json!({"#st": "status"}),
                json!({":active": {"S": "active"}}),
            ),
        ];
        for (index, (condition, names, values)) in holding.into_iter().enumerate() {
            let item = with(
                &with(&key, base.clone()),
                json!({"seq": {"N": (index + 1).to_string()}}),
            );
            let mut request = json!({
                "TableName": table, "Item": item, "ConditionExpression": condition,
            });
            if !names.is_null() {
                request["ExpressionAttributeNames"] = names;
            }
            if !values.is_null() {
                request["ExpressionAttributeValues"] = values;
            }
            call(&store, "PutItem", request).await.ok();
            assert_item(&store, &table, &key, &item, condition).await;
        }
        let before = get(&store, &table, &key).await;
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": with(&with(&key, base), json!({"seq": {"N": "9"}})),
                "ConditionExpression": "#m.#k = :other",
                "ExpressionAttributeNames": {"#m": "m", "#k": "k"},
                "ExpressionAttributeValues": {":other": {"S": "other"}},
            }),
        )
        .await
        .assert_error(CONDITION_FAILED);
        assert_item(&store, &table, &key, &before, "failing nested condition").await;
    });
}

/// AWS behavioural:
/// check_put_item_fractional_and_negative_hash_key_omits_consumed_capacity.py
/// (capacity TOTAL assertions dropped: capacity is intentionally unsupported)
#[tokio::test]
async fn given_fractional_and_negative_number_hash_keys_when_put_then_get_round_trips_without_capacity()
 {
    scenario!(|store| {
        let table = unique_table_name().to_string();
        create_table(&store, table_request(&table, "N", None)).await;
        for (number, label) in [("3.14", "frac"), ("-100", "neg")] {
            let item = json!({"pk": {"N": number}, "label": {"S": label}});
            let put = call(
                &store,
                "PutItem",
                json!({"TableName": table, "Item": item, "ReturnConsumedCapacity": "NONE"}),
            )
            .await;
            assert!(put.ok().get("ConsumedCapacity").is_none(), "{}", put.body);
            let got = call(
                &store,
                "GetItem",
                json!({
                    "TableName": table,
                    "Key": {"pk": {"N": number}},
                    "ConsistentRead": true,
                    "ReturnConsumedCapacity": "NONE",
                }),
            )
            .await;
            assert_eq!(got.ok()["Item"], item);
            assert!(got.ok().get("ConsumedCapacity").is_none(), "{}", got.body);
        }
    });
}

/// AWS behavioural:
/// check_put_item_nested_map_set_and_number_list_contents_count_toward_item_size_limit.
/// py
#[tokio::test]
async fn given_oversized_nested_map_string_set_and_number_list_when_put_then_each_is_refused_and_small_forms_round_trip()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let nested = |entries: usize| {
            let map: serde_json::Map<String, Value> = (0..entries)
                .map(|index| {
                    (
                        format!("k{index}"),
                        json!({"M": {"v": {"S": "x".repeat(2000)}}}),
                    )
                })
                .collect();
            json!({"M": map})
        };
        let string_set = |members: usize| {
            let members: Vec<String> = (0..members)
                .map(|index| format!("{index:04}{}", "s".repeat(4196)))
                .collect();
            json!({"SS": members})
        };
        let numbers = |count: usize| json!({"L": vec![json!({"N": "9".repeat(38)}); count]});
        let accepted = [
            ("nested-ok", nested(10)),
            ("ss-ok", string_set(10)),
            ("nlist-ok", numbers(15_000)),
        ];
        for (name, data) in accepted {
            let item = with(&pk(name), json!({"data": data}));
            put(&store, &table, item.clone()).await;
            assert_item(&store, &table, &pk(name), &item, name).await;
        }
        let refused = [
            ("nested-over", nested(250)),
            ("ss-over", string_set(100)),
            ("nlist-over", numbers(21_000)),
        ];
        for (name, data) in refused {
            let item = with(&pk(name), json!({"data": data}));
            call(&store, "PutItem", json!({"TableName": table, "Item": item}))
                .await
                .assert_error(ITEM_TOO_BIG);
            assert!(
                get(&store, &table, &pk(name)).await.is_null(),
                "{name} was stored"
            );
        }
    });
}

/// AWS behavioural:
/// check_put_item_oversized_overwrite_rejected_ignoring_existing_image.py
#[tokio::test]
async fn given_existing_small_item_when_oversized_overwrite_then_refused_and_item_unchanged() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let small = with(&pk("k"), json!({"blob": {"S": "e".repeat(900)}}));
        put(&store, &table, small.clone()).await;
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": with(&pk("k"), json!({"blob": {"S": "o".repeat(410 * 1024)}})),
            }),
        )
        .await
        .assert_error(ITEM_TOO_BIG);
        assert_item(&store, &table, &pk("k"), &small, "rejected overwrite").await;
        let other = with(&pk("k2"), json!({"blob": {"S": "e".repeat(900)}}));
        put(&store, &table, other.clone()).await;
        assert_item(
            &store,
            &table,
            &pk("k2"),
            &other,
            "ordinary write afterwards",
        )
        .await;
    });
}

/// AWS behavioural: check_putitem_condition_comparison_on_missing_attribute.py
#[tokio::test]
async fn given_missing_attribute_when_put_condition_compares_it_then_equals_fails_and_not_equals_applies()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("eq-target");
        put(&store, &table, key.clone()).await;
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": with(&key, json!({"seen": {"BOOL": true}})),
                "ConditionExpression": "seen = :v",
                "ExpressionAttributeValues": {":v": {"BOOL": true}},
            }),
        )
        .await
        .assert_error(CONDITION_FAILED);
        assert_item(&store, &table, &key, &key, "refused '='").await;

        let landed = with(&key, json!({"seen": {"BOOL": true}}));
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": landed,
                "ConditionExpression": "seen <> :v",
                "ExpressionAttributeValues": {":v": {"BOOL": false}},
            }),
        )
        .await
        .ok();
        assert_item(&store, &table, &key, &landed, "'<>' on missing").await;

        let or_key = pk("or-target");
        let or_item = with(&or_key, json!({"status": {"S": "new"}}));
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": or_item,
                "ConditionExpression": "#s <> :v OR #u < :t",
                "ExpressionAttributeNames": {"#s": "status", "#u": "updated_at"},
                "ExpressionAttributeValues": {":v": {"S": "retired"}, ":t": {"N": "100"}},
            }),
        )
        .await
        .ok();
        assert_item(&store, &table, &or_key, &or_item, "OR over two missing").await;
    });
}

/// AWS behavioural: check_putitem_condition_expression_and_or_not_precedence.py
#[tokio::test]
async fn given_condition_expressions_when_put_then_and_binds_tighter_than_or_and_not_tighter_than_and()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("precedence");
        let seed = with(
            &key,
            json!({"a": {"N": "5"}, "b": {"N": "10"}, "marker": {"S": "seed"}}),
        );
        put(&store, &table, seed).await;
        let write = |marker: &'static str, condition: &'static str, values: Value| {
            let request = json!({
                "TableName": table,
                "Item": with(&key, json!({
                    "a": {"N": "5"}, "b": {"N": "10"}, "marker": {"S": marker},
                })),
                "ConditionExpression": condition,
                "ExpressionAttributeValues": values,
            });
            call(&store, "PutItem", request)
        };
        let marker = async || get(&store, &table, &key).await["marker"]["S"].clone();
        let and_or = json!({":five": {"N": "5"}, ":ten": {"N": "10"}, ":twenty": {"N": "20"}});
        write(
            "op1",
            "a = :five OR a = :ten AND b = :twenty",
            and_or.clone(),
        )
        .await
        .ok();
        assert_eq!(marker().await, "op1");
        write("op2", "(a = :five OR a = :ten) AND b = :twenty", and_or)
            .await
            .assert_error(CONDITION_FAILED);
        assert_eq!(marker().await, "op1");
        let five = json!({":five": {"N": "5"}});
        write("op3", "NOT a = :five AND b = :five", five.clone())
            .await
            .assert_error(CONDITION_FAILED);
        assert_eq!(marker().await, "op1");
        write("op4", "NOT (a = :five AND b = :five)", five)
            .await
            .ok();
        assert_eq!(marker().await, "op4");
    });
}

/// AWS behavioural: check_putitem_not_contains_condition_gates_the_write.py
#[tokio::test]
async fn given_string_set_when_put_condition_is_not_contains_then_member_blocks_and_other_type_allows()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("target");
        let initial = with(&key, json!({"roles": {"SS": ["5"]}}));
        put(&store, &table, initial.clone()).await;
        let replacement = with(&initial, json!({"written": {"BOOL": true}}));
        let attempt = |operand: Value| {
            call(
                &store,
                "PutItem",
                json!({
                    "TableName": table,
                    "Item": replacement,
                    "ConditionExpression": "NOT contains(#r, :v)",
                    "ExpressionAttributeNames": {"#r": "roles"},
                    "ExpressionAttributeValues": {":v": operand},
                }),
            )
        };
        attempt(json!({"S": "5"}))
            .await
            .assert_error(CONDITION_FAILED);
        assert_item(&store, &table, &key, &initial, "failing NOT contains").await;
        attempt(json!({"N": "5"})).await.ok();
        assert_item(&store, &table, &key, &replacement, "holding NOT contains").await;
    });
}

fn operand_type_mismatch(operator: &str) -> String {
    format!(
        "Invalid KeyConditionExpression: Incorrect operand type for operator or function; \
         operator or function: {operator}, operand type: BOOL"
    )
}

/// AWS behavioural:
/// check_query_value_first_key_condition_operand_message_states_operator_as_written.
/// py
#[tokio::test]
async fn given_bool_operand_when_query_key_condition_then_message_names_operator_as_written() {
    scenario!(|store| {
        let table = unique_table_name().to_string();
        create_table(&store, table_request(&table, "S", Some(("sk", "S")))).await;
        for sk in ["a", "b", "c"] {
            put(&store, &table, json!({"pk": {"S": "p"}, "sk": {"S": sk}})).await;
        }
        let pairs = [
            ("pk = :p AND :v >= sk", ">=", "pk = :p AND sk <= :v", "<="),
            ("pk = :p AND :v <= sk", "<=", "pk = :p AND sk >= :v", ">="),
            ("pk = :p AND :v > sk", ">", "pk = :p AND sk < :v", "<"),
            ("pk = :p AND :v < sk", "<", "pk = :p AND sk > :v", ">"),
        ];
        for (value_first, value_first_operator, key_first, key_first_operator) in pairs {
            for (expression, operator) in [
                (value_first, value_first_operator),
                (key_first, key_first_operator),
            ] {
                call(
                    &store,
                    "Query",
                    json!({
                        "TableName": table,
                        "KeyConditionExpression": expression,
                        "ExpressionAttributeValues": {":p": {"S": "p"}, ":v": {"BOOL": true}},
                    }),
                )
                .await
                .assert_validation(&operand_type_mismatch(operator));
            }
        }
        let control = call(
            &store,
            "Query",
            json!({
                "TableName": table,
                "KeyConditionExpression": "pk = :p AND :lo <= sk",
                "ConsistentRead": true,
                "ExpressionAttributeValues": {":p": {"S": "p"}, ":lo": {"S": "b"}},
            }),
        )
        .await;
        let sort_keys: Vec<&str> = control.ok()["Items"]
            .as_array()
            .expect("items")
            .iter()
            .map(|item| item["sk"]["S"].as_str().expect("sk"))
            .collect();
        assert_eq!(sort_keys, ["b", "c"]);
    });
}

fn mixing_message(legacy: &str, expression: &str) -> String {
    format!(
        "Can not use both expression and non-expression parameters in the same request: \
         Non-expression parameters: {{{legacy}}} Expression parameters: {{{expression}}}"
    )
}

fn legacy_equals(value: &str) -> Value {
    json!({"ComparisonOperator": "EQ", "AttributeValueList": [{"S": value}]})
}

/// AWS behavioural:
/// check_scan_query_legacy_and_expression_mixing_names_every_parameter_rejected.
/// py
///
/// The script's closing legacy-`KeyConditions` Query successes are dropped:
/// `KeyConditions` is unsupported.
#[tokio::test]
async fn given_legacy_and_expression_parameters_when_scan_or_query_then_message_lists_every_parameter()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let item = json!({"pk": {"S": "k1"}, "a": {"S": "x"}, "b": {"S": "y"}});
        put(&store, &table, item.clone()).await;
        let v = json!({":v": {"S": "x"}});
        let p = json!({":p": {"S": "k1"}});
        let both = with(&p, v.clone());
        let cases = [
            (
                "Scan",
                "ConditionalOperator",
                "ProjectionExpression",
                json!({"ConditionalOperator": "AND", "ProjectionExpression": "a"}),
            ),
            (
                "Query",
                "ConditionalOperator",
                "KeyConditionExpression",
                json!({
                    "ConditionalOperator": "AND",
                    "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": p,
                }),
            ),
            (
                "Scan",
                "AttributesToGet, ScanFilter, ConditionalOperator",
                "ProjectionExpression, FilterExpression",
                json!({
                    "AttributesToGet": ["a"],
                    "ScanFilter": {"a": legacy_equals("x")},
                    "ConditionalOperator": "AND",
                    "FilterExpression": "a = :v",
                    "ProjectionExpression": "b",
                    "ExpressionAttributeValues": v,
                }),
            ),
            (
                "Query",
                "QueryFilter, ConditionalOperator, KeyConditions",
                "ProjectionExpression, FilterExpression, KeyConditionExpression",
                json!({
                    "KeyConditions": {"pk": legacy_equals("k1")},
                    "QueryFilter": {"a": legacy_equals("x")},
                    "ConditionalOperator": "AND",
                    "KeyConditionExpression": "pk = :p",
                    "FilterExpression": "a = :v",
                    "ProjectionExpression": "b",
                    "ExpressionAttributeValues": both,
                }),
            ),
            (
                "Scan",
                "ConditionalOperator",
                "FilterExpression",
                json!({
                    "ConditionalOperator": "AND",
                    "FilterExpression": "a = :v",
                    "ExpressionAttributeValues": {
                        ":v": {"S": "x"}, ":unused": {"S": "y"},
                    },
                }),
            ),
            (
                "Scan",
                "ScanFilter",
                "ProjectionExpression",
                json!({
                    "ScanFilter": {"a": legacy_equals("x")},
                    "ProjectionExpression": "b",
                }),
            ),
            (
                "Query",
                "AttributesToGet",
                "KeyConditionExpression",
                json!({
                    "AttributesToGet": ["a"],
                    "KeyConditionExpression": "pk = :p",
                    "ExpressionAttributeValues": p,
                }),
            ),
            (
                "Query",
                "QueryFilter",
                "FilterExpression, KeyConditionExpression",
                json!({
                    "QueryFilter": {"a": legacy_equals("x")},
                    "KeyConditionExpression": "pk = :p",
                    "FilterExpression": "a = :v",
                    "ExpressionAttributeValues": both,
                }),
            ),
            (
                "Query",
                "QueryFilter",
                "FilterExpression",
                json!({
                    "QueryFilter": {"a": legacy_equals("x")},
                    "FilterExpression": "a = :v",
                    "ExpressionAttributeValues": v,
                }),
            ),
        ];
        for (operation, legacy, expression, parameters) in cases {
            call(
                &store,
                operation,
                with(&json!({"TableName": table}), parameters),
            )
            .await
            .assert_validation(&mixing_message(legacy, expression));
        }
    });
}

const SELECT_WITH_PROJECTION: [(&str, &str); 3] = [
    (
        "ALL_ATTRIBUTES",
        "1 validation error detected: Cannot specify the ProjectionExpression when choosing to \
         get ALL_ATTRIBUTES",
    ),
    (
        "COUNT",
        "1 validation error detected: Cannot specify the ProjectionExpression when choosing to \
         get only the Count",
    ),
    (
        "ALL_PROJECTED_ATTRIBUTES",
        "1 validation error detected: Cannot specify the ProjectionExpression when choosing to \
         get ALL_PROJECTED_ATTRIBUTES",
    ),
];

/// Poll an index read until it returns `count` items, for at most
/// [`INDEX_POLL_SECONDS`].
async fn index_read(store: &TestStore, operation: &str, request: Value, count: u64) -> Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(INDEX_POLL_SECONDS);
    loop {
        let reply = call(store, operation, request.clone()).await;
        let body = reply.ok().clone();
        if body["Count"] == count || std::time::Instant::now() >= deadline {
            return body;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// AWS behavioural:
/// check_select_all_projected_attributes_requires_an_index_rejected.py
#[tokio::test]
async fn given_select_values_when_query_or_scan_then_invalid_combinations_are_refused_and_legal_ones_succeed()
 {
    scenario!(|store| {
        let table = unique_table_name().to_string();
        let mut create = table_request(&table, "S", Some(("sk", "S")));
        create["AttributeDefinitions"]
            .as_array_mut()
            .expect("attributes")
            .push(json!({"AttributeName": "gsi_pk", "AttributeType": "S"}));
        create["GlobalSecondaryIndexes"] = json!([{
            "IndexName": "keys-only-index",
            "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
        }]);
        create_table(&store, create).await;
        let item = json!({
            "pk": {"S": "P"}, "sk": {"S": "S"}, "gsi_pk": {"S": "G"}, "body": {"S": "payload"},
        });
        put(&store, &table, item.clone()).await;
        let base = json!({
            "TableName": table,
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "P"}},
        });
        let on_index = json!({
            "TableName": table,
            "IndexName": "keys-only-index",
            "KeyConditionExpression": "gsi_pk = :g",
            "ExpressionAttributeValues": {":g": {"S": "G"}},
        });
        let query = |request: &Value, extra: Value| call(&store, "Query", with(request, extra));

        query(&base, json!({"Select": "ALL_PROJECTED_ATTRIBUTES"}))
            .await
            .assert_validation(
                "1 validation error detected: ALL_PROJECTED_ATTRIBUTES can be used only when \
                 Querying using an IndexName",
            );
        let projected = index_read(
            &store,
            "Query",
            with(&on_index, json!({"Select": "ALL_PROJECTED_ATTRIBUTES"})),
            1,
        )
        .await;
        assert_eq!(projected["Count"], 1, "{projected}");

        query(&base, json!({"Select": "SPECIFIC_ATTRIBUTES"}))
            .await
            .assert_validation(
                "1 validation error detected: Must specify the AttributesToGet or \
                 ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES",
            );
        let specific = query(
            &base,
            json!({"Select": "SPECIFIC_ATTRIBUTES", "ProjectionExpression": "body"}),
        )
        .await;
        assert_eq!(specific.ok()["Items"], json!([{"body": {"S": "payload"}}]));

        query(&on_index, json!({"Select": "ALL_ATTRIBUTES"}))
            .await
            .assert_validation(
                "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not \
                 supported for global secondary index keys-only-index because its projection type \
                 is not ALL",
            );
        let all = query(
            &base,
            json!({"Select": "ALL_ATTRIBUTES", "ConsistentRead": true}),
        )
        .await;
        assert_eq!(all.ok()["Items"], json!([item]));

        for (select, message) in SELECT_WITH_PROJECTION {
            let request = if select == "ALL_PROJECTED_ATTRIBUTES" {
                &on_index
            } else {
                &base
            };
            query(
                request,
                json!({"Select": select, "ProjectionExpression": "body"}),
            )
            .await
            .assert_validation(message);
            let scan_request = if select == "ALL_PROJECTED_ATTRIBUTES" {
                json!({"TableName": table, "IndexName": "keys-only-index"})
            } else {
                json!({"TableName": table})
            };
            call(
                &store,
                "Scan",
                with(
                    &scan_request,
                    json!({"Select": select, "ProjectionExpression": "body"}),
                ),
            )
            .await
            .assert_validation(message);
        }
        let counted = query(&base, json!({"Select": "COUNT", "ConsistentRead": true})).await;
        assert_eq!(counted.ok()["Count"], 1);
        let scan_all = call(
            &store,
            "Scan",
            json!({"TableName": table, "Select": "ALL_ATTRIBUTES", "ConsistentRead": true}),
        )
        .await;
        assert_eq!(scan_all.ok()["Items"], json!([item]));
        let scan_count = call(
            &store,
            "Scan",
            json!({"TableName": table, "Select": "COUNT", "ConsistentRead": true}),
        )
        .await;
        assert_eq!(scan_count.ok()["Count"], 1);
        let scan_index = index_read(
            &store,
            "Scan",
            json!({
                "TableName": table, "IndexName": "keys-only-index",
                "Select": "ALL_PROJECTED_ATTRIBUTES",
            }),
            1,
        )
        .await;
        assert_eq!(
            scan_index["Items"],
            json!([{"sk": {"S": "S"}, "pk": {"S": "P"}, "gsi_pk": {"S": "G"}}])
        );
    });
}

/// AWS behavioural:
/// check_stale_condition_update_rejected_leaves_counter_and_audit_trail_intact.
/// py
#[tokio::test]
async fn given_stale_expected_value_when_concurrent_updates_then_all_refused_and_current_value_succeeds()
 {
    scenario!(|store| {
        const SETUP_STEPS: u64 = 16;
        const STALE_RACERS: u64 = 12;
        let table = hash_table(&store).await;
        let key = pk("COUNTER#1");
        put(&store, &table, with(&key, json!({"n": {"N": "0"}}))).await;
        let update = |worker: u64, expected: u64, next: u64| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table,
                    "Key": key,
                    "UpdateExpression":
                        "SET #n = :next, #a = list_append(if_not_exists(#a, :empty), :entry)",
                    "ConditionExpression": "#n = :expected",
                    "ExpressionAttributeNames": {"#n": "n", "#a": "audit"},
                    "ExpressionAttributeValues": {
                        ":next": {"N": next.to_string()},
                        ":expected": {"N": expected.to_string()},
                        ":empty": {"L": []},
                        ":entry": {"L": [{"M": {
                            "worker": {"N": worker.to_string()},
                            "before": {"N": expected.to_string()},
                            "after": {"N": next.to_string()},
                        }}]},
                    },
                }),
            )
        };
        for step in 0..SETUP_STEPS {
            update(step, step, step + 1).await.ok();
        }
        let before = get(&store, &table, &key).await;
        assert_eq!(before["n"], json!({"N": SETUP_STEPS.to_string()}));
        assert_eq!(before["audit"]["L"].as_array().expect("audit").len(), 16);

        let replies =
            futures::future::join_all((0..STALE_RACERS).map(|worker| update(900 + worker, 0, 1)))
                .await;
        assert_eq!(replies.len(), 12);
        for reply in &replies {
            reply.assert_error(CONDITION_FAILED);
        }
        assert_item(&store, &table, &key, &before, "after stale racers").await;

        update(999, SETUP_STEPS, SETUP_STEPS + 1).await.ok();
        let after = get(&store, &table, &key).await;
        assert_eq!(after["n"], json!({"N": "17"}));
        let audit = after["audit"]["L"].as_array().expect("audit");
        assert_eq!(audit.len(), 17);
        assert_eq!(audit[16]["M"]["worker"], json!({"N": "999"}));
    });
}

/// AWS behavioural:
/// check_string_revision_token_conditional_writes_refuse_stale_and_unissued_tokens.
/// py
#[tokio::test]
async fn given_revision_token_when_conditional_writes_run_then_stale_and_unissued_tokens_are_refused()
 {
    scenario!(|store| {
        let table = unique_table_name().to_string();
        let mut create = table_request(&table, "S", Some(("doc", "N")));
        create["AttributeDefinitions"][0]["AttributeName"] = json!("tenant");
        create["KeySchema"][0]["AttributeName"] = json!("tenant");
        create_table(&store, create).await;
        let key = json!({"tenant": {"S": "acme"}, "doc": {"N": "907"}});
        let first = with(
            &key,
            json!({
                "rev": {"S": "rev1"}, "status": {"S": "open"}, "body": {"S": "draft"},
            }),
        );
        let put_if_absent = |item: Value| {
            call(
                &store,
                "PutItem",
                json!({
                    "TableName": table, "Item": item,
                    "ConditionExpression": "attribute_not_exists(tenant)",
                }),
            )
        };
        put_if_absent(first.clone()).await.ok();
        put_if_absent(with(
            &key,
            json!({"rev": {"S": "other"}, "body": {"S": "clobber"}}),
        ))
        .await
        .assert_error(CONDITION_FAILED);
        assert_item(&store, &table, &key, &first, "refused put-if-absent").await;

        let rotate = |expected: &str, new: &str, body: &str, status: &str| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table,
                    "Key": key,
                    "UpdateExpression": "SET rev = :new, body = :body",
                    "ConditionExpression": "rev = :expected AND #st = :status",
                    "ExpressionAttributeNames": {"#st": "status"},
                    "ExpressionAttributeValues": {
                        ":new": {"S": new}, ":body": {"S": body},
                        ":expected": {"S": expected}, ":status": {"S": status},
                    },
                    "ReturnValues": "ALL_NEW",
                }),
            )
        };
        let second = with(
            &first,
            json!({"rev": {"S": "rev2"}, "body": {"S": "reviewed"}}),
        );
        let rotated = rotate("rev1", "rev2", "reviewed", "open").await;
        assert_same(&rotated.ok()["Attributes"], &second, "first rotation");
        for (expected, status) in [("rev1", "open"), ("rev3", "open"), ("rev2", "closed")] {
            rotate(expected, "rev3", "lost", status)
                .await
                .assert_error(CONDITION_FAILED);
            assert_item(&store, &table, &key, &second, expected).await;
        }
        let third = with(
            &second,
            json!({"rev": {"S": "rev3"}, "body": {"S": "final"}}),
        );
        let rotated = rotate("rev2", "rev3", "final", "open").await;
        assert_same(&rotated.ok()["Attributes"], &third, "second rotation");
        let sibling = json!({"tenant": {"S": "acme"}, "doc": {"N": "908"}});
        assert!(get(&store, &table, &sibling).await.is_null());
    });
}

/// AWS behavioural:
/// check_transact_write_items_invalid_table_name_names_member_path.py
#[tokio::test]
async fn given_invalid_table_name_in_transact_action_when_written_then_message_names_member_path() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let bad_action = |kind: &str| match kind {
            "Put" => json!({"Put": {"TableName": "bad name", "Item": pk("victim")}}),
            "Update" => json!({"Update": {
                "TableName": "bad name", "Key": pk("victim"),
                "UpdateExpression": "SET v = :v",
                "ExpressionAttributeValues": {":v": {"N": "1"}},
            }}),
            "Delete" => json!({"Delete": {"TableName": "bad name", "Key": pk("victim")}}),
            _ => json!({"ConditionCheck": {
                "TableName": "bad name", "Key": pk("victim"),
                "ConditionExpression": "attribute_not_exists(pk)",
            }}),
        };
        let sibling = json!({"Put": {"TableName": table, "Item": pk("sibling")}});
        call(
            &store,
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": table, "Item": pk("control-a")}},
                {"Put": {"TableName": table, "Item": pk("control-b")}},
            ]}),
        )
        .await
        .ok();
        for (kind, path) in [
            ("Put", "put"),
            ("Update", "update"),
            ("Delete", "delete"),
            ("ConditionCheck", "conditionCheck"),
        ] {
            for position in [1, 2] {
                let items = if position == 1 {
                    [bad_action(kind), sibling.clone()]
                } else {
                    [sibling.clone(), bad_action(kind)]
                };
                call(
                    &store,
                    "TransactWriteItems",
                    json!({"TransactItems": items}),
                )
                .await
                .assert_validation(&format!(
                    "1 validation error detected: Value 'bad name' at \
                     'transactItems.{position}.member.{path}.tableName' failed to satisfy \
                     constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
                ));
            }
        }
        assert!(get(&store, &table, &pk("sibling")).await.is_null());
        assert!(!get(&store, &table, &pk("control-a")).await.is_null());
    });
}

/// AWS behavioural:
/// check_transact_write_items_overlength_token_masks_other_violations_rejected.
/// py
#[tokio::test]
async fn given_overlength_token_when_transact_write_then_only_token_violation_is_reported() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let put_control = json!({"Put": {"TableName": table, "Item": pk("should-not-write-1")}});
        let request = |extra: Value| {
            call(
                &store,
                "TransactWriteItems",
                with(
                    &json!({"TransactItems": [{}, put_control], "ReturnConsumedCapacity": "BOGUS"}),
                    extra,
                ),
            )
        };
        request(json!({"ClientRequestToken": "x".repeat(37)}))
            .await
            .assert_validation(
                "1 validation error detected: Value at 'ClientRequestToken' failed to satisfy \
                 constraint: Member must have length less than or equal to 36",
            );
        request(json!({})).await.assert_validation(
            "2 validation errors detected: Value '' at 'transactItems' failed to satisfy \
             constraint: TransactWriteRequest should contain Delete or Put or Update or \
             ConditionCheck; Value 'BOGUS' at 'returnConsumedCapacity' failed to satisfy \
             constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
        );
        assert!(
            get(&store, &table, &pk("should-not-write-1"))
                .await
                .is_null()
        );
    });
}

const UNBALANCED: &str = "(a < :ab AND b > :bb";
const BALANCED: &str = "(a < :ab AND (b > :bb))";

/// AWS behavioural:
/// check_unbalanced_parenthesized_condition_expression_rejected.py
#[tokio::test]
async fn given_unbalanced_condition_parenthesis_when_put_delete_update_then_syntax_error_and_item_unchanged()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let syntax_error = "1 validation error detected: Invalid ConditionExpression: Syntax \
                            error; token: \"<EOF>\", near: \":bb\"";
        let base = json!({"a": {"N": "5"}, "b": {"N": "10"}});
        let values = json!({":ab": {"N": "10"}, ":bb": {"N": "5"}});
        let with_update_value = with(&values, json!({":v": {"S": "x"}}));

        let put_target = with(&pk("put-target"), base.clone());
        put(&store, &table, put_target.clone()).await;
        let written = with(&put_target, json!({"written": {"BOOL": true}}));
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table, "Item": written, "ConditionExpression": BALANCED,
                "ExpressionAttributeValues": values,
            }),
        )
        .await
        .ok();
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": with(&put_target, json!({"written": {"BOOL": false}})),
                "ConditionExpression": UNBALANCED, "ExpressionAttributeValues": values,
            }),
        )
        .await
        .assert_validation(syntax_error);
        assert_item(
            &store,
            &table,
            &pk("put-target"),
            &written,
            "unbalanced PutItem",
        )
        .await;

        let delete_target = with(&pk("delete-target"), base.clone());
        put(&store, &table, delete_target.clone()).await;
        call(
            &store,
            "DeleteItem",
            json!({
                "TableName": table, "Key": pk("delete-target"),
                "ConditionExpression": UNBALANCED, "ExpressionAttributeValues": values,
            }),
        )
        .await
        .assert_validation(syntax_error);
        assert_item(
            &store,
            &table,
            &pk("delete-target"),
            &delete_target,
            "unbalanced DeleteItem",
        )
        .await;
        call(
            &store,
            "DeleteItem",
            json!({
                "TableName": table, "Key": pk("delete-target"),
                "ConditionExpression": BALANCED, "ExpressionAttributeValues": values,
            }),
        )
        .await
        .ok();
        assert!(get(&store, &table, &pk("delete-target")).await.is_null());

        let update_target = with(&pk("update-target"), base);
        put(&store, &table, update_target.clone()).await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": pk("update-target"), "UpdateExpression": "SET c = :v",
                "ConditionExpression": UNBALANCED, "ExpressionAttributeValues": with_update_value,
            }),
        )
        .await
        .assert_validation(syntax_error);
        assert_item(
            &store,
            &table,
            &pk("update-target"),
            &update_target,
            "unbalanced UpdateItem",
        )
        .await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": pk("update-target"), "UpdateExpression": "SET c = :v",
                "ConditionExpression": BALANCED, "ExpressionAttributeValues": with_update_value,
            }),
        )
        .await
        .ok();
        let applied = with(&update_target, json!({"c": {"S": "x"}}));
        assert_item(
            &store,
            &table,
            &pk("update-target"),
            &applied,
            "balanced UpdateItem",
        )
        .await;
    });
}

/// AWS behavioural: check_update_delete_last_set_element_removes_attribute.py
/// (set DELETE/ADD semantics are already in `update_item_cases.jsonl`; only the
/// empty-set PutItem refusals are new)
#[tokio::test]
async fn given_empty_string_or_number_set_when_put_then_validation_error_and_no_item() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        for (key, attribute, kind, name) in [
            ("empty-ss", "tags", "SS", "string"),
            ("empty-ns", "nums", "NS", "number"),
        ] {
            call(
                &store,
                "PutItem",
                json!({
                    "TableName": table,
                    "Item": with(&pk(key), json!({attribute: {kind: []}})),
                }),
            )
            .await
            .assert_validation(&format!(
                "1 validation error detected: One or more parameter values were invalid: An \
                 {name} set  may not be empty"
            ));
            assert!(
                get(&store, &table, &pk(key)).await.is_null(),
                "{key} was created"
            );
        }
        let control = with(&pk("empty-ss"), json!({"tags": {"SS": ["ok"]}}));
        put(&store, &table, control.clone()).await;
        assert_item(
            &store,
            &table,
            &pk("empty-ss"),
            &control,
            "non-empty set control",
        )
        .await;
    });
}

/// AWS behavioural: check_update_expression_malformed_syntax_rejected.py
#[tokio::test]
async fn given_malformed_update_expression_when_update_then_validation_error_and_item_unchanged() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("item-1");
        let original = with(&key, json!({"foo": {"S": "original"}}));
        put(&store, &table, original.clone()).await;
        let update = |expression: &str, values: Value| {
            let mut request =
                json!({"TableName": table, "Key": key, "UpdateExpression": expression});
            if !values.is_null() {
                request["ExpressionAttributeValues"] = values;
            }
            call(&store, "UpdateItem", request)
        };
        update("SET foo =", json!(null)).await.assert_validation(
            "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \
             \"<EOF>\", near: \"=\"",
        );
        assert_item(&store, &table, &key, &original, "dangling operator").await;
        update("SET foo = :v", json!({":v": {"S": "updated"}}))
            .await
            .ok();

        let numeric = with(&key, json!({"n": {"N": "5"}}));
        put(&store, &table, numeric).await;
        update(
            "SET n = (:a + :b)",
            json!({":a": {"N": "1"}, ":b": {"N": "2"}}),
        )
        .await
        .ok();
        let summed = with(&key, json!({"n": {"N": "3"}}));
        assert_item(&store, &table, &key, &summed, "parenthesised arithmetic").await;
        update("SET n = ((:v))", json!({":v": {"N": "99"}}))
            .await
            .assert_validation(
                "1 validation error detected: Invalid UpdateExpression: The expression has \
                 redundant parentheses;",
            );
        assert_item(&store, &table, &key, &summed, "redundant parentheses").await;
    });
}

/// AWS behavioural:
/// check_update_item_add_delete_empty_string_set_member_and_zero_length_binary_set_member.
/// py
#[tokio::test]
async fn given_empty_string_and_zero_length_binary_set_members_when_add_delete_then_set_semantics_hold()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("sets");
        put(&store, &table, key.clone()).await;
        let step = |expression: &str, kind: &str, members: Value| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": expression,
                    "ExpressionAttributeValues": {":e": {kind: members}},
                    "ReturnValues": "ALL_NEW",
                }),
            )
        };
        let steps = [
            (
                "ADD tags :e",
                "tags",
                "SS",
                json!([""]),
                Some(json!({"SS": [""]})),
            ),
            (
                "ADD tags :e",
                "tags",
                "SS",
                json!([""]),
                Some(json!({"SS": [""]})),
            ),
            (
                "ADD tags :e",
                "tags",
                "SS",
                json!(["a"]),
                Some(json!({"SS": ["", "a"]})),
            ),
            (
                "DELETE tags :e",
                "tags",
                "SS",
                json!([""]),
                Some(json!({"SS": ["a"]})),
            ),
            (
                "ADD tags :e",
                "tags",
                "SS",
                json!([""]),
                Some(json!({"SS": ["", "a"]})),
            ),
            (
                "DELETE tags :e",
                "tags",
                "SS",
                json!(["a"]),
                Some(json!({"SS": [""]})),
            ),
            ("DELETE tags :e", "tags", "SS", json!([""]), None),
            (
                "ADD bins :e",
                "bins",
                "BS",
                json!([""]),
                Some(json!({"BS": [""]})),
            ),
            (
                "ADD bins :e",
                "bins",
                "BS",
                json!(["eA=="]),
                Some(json!({"BS": ["", "eA=="]})),
            ),
            (
                "ADD bins :e",
                "bins",
                "BS",
                json!([""]),
                Some(json!({"BS": ["", "eA=="]})),
            ),
        ];
        for (expression, attribute, kind, members, expected) in steps {
            let reply = step(expression, kind, members).await;
            let attributes = reply.ok()["Attributes"].clone();
            match expected {
                Some(expected) => {
                    assert_same(&attributes[attribute], &expected, expression);
                }
                None => assert!(attributes.get(attribute).is_none(), "{attributes}"),
            }
        }
    });
}

/// AWS behavioural: check_update_item_add_type_mismatch.py
#[tokio::test]
async fn given_string_attribute_when_add_number_then_validation_error_and_item_unchanged() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("type-mismatch-1");
        put(
            &store,
            &table,
            with(
                &key,
                json!({
                    "tag": {"S": "not-a-number"}, "counter": {"N": "10"},
                }),
            ),
        )
        .await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "ADD #c :delta",
                "ExpressionAttributeNames": {"#c": "counter"},
                "ExpressionAttributeValues": {":delta": {"N": "5"}},
            }),
        )
        .await
        .ok();
        let expected = with(
            &key,
            json!({"tag": {"S": "not-a-number"}, "counter": {"N": "15"}}),
        );
        assert_item(&store, &table, &key, &expected, "control ADD").await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "ADD tag :delta",
                "ExpressionAttributeValues": {":delta": {"N": "5"}},
            }),
        )
        .await
        .assert_validation("An operand in the update expression has an incorrect data type");
        assert_item(&store, &table, &key, &expected, "rejected ADD").await;
    });
}

const GSI_EMPTY_KEY: &str = "One or more parameter values are not valid. The update expression \
                             attempted to update a secondary index key to a value that is not \
                             supported. The AttributeValue for a key attribute cannot contain an \
                             empty string value.";
/// AWS behavioural:
/// check_update_item_and_transact_update_empty_gsi_and_lsi_keys_rejected.py
#[tokio::test]
async fn given_empty_index_key_values_when_update_or_transact_update_then_validation_error_not_cancellation()
 {
    scenario!(|store| {
        let table = unique_table_name().to_string();
        let mut create = table_request(&table, "S", Some(("sk", "S")));
        create["AttributeDefinitions"]
            .as_array_mut()
            .expect("attributes")
            .extend([json!({"AttributeName": "gpk", "AttributeType": "S"})]);
        create["GlobalSecondaryIndexes"] = json!([{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"},
        }]);
        create_table(&store, create).await;
        let key = json!({"pk": {"S": "b"}, "sk": {"S": "1"}});
        put(&store, &table, key.clone()).await;
        for (attribute, empty, message) in [("gpk", json!({"S": ""}), GSI_EMPTY_KEY)] {
            let expression = format!("SET {attribute} = :v");
            let update = call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": expression,
                    "ExpressionAttributeValues": {":v": empty},
                }),
            )
            .await;
            update.assert_validation(message);
            let transact = call(
                &store,
                "TransactWriteItems",
                json!({"TransactItems": [{"Update": {
                    "TableName": table, "Key": key, "UpdateExpression": expression,
                    "ExpressionAttributeValues": {":v": empty},
                }}]}),
            )
            .await;
            transact.assert_validation(message);
            assert!(
                transact.body.get("CancellationReasons").is_none(),
                "{}",
                transact.body
            );
        }
        assert_item(&store, &table, &key, &key, "refused index key updates").await;
    });
}

/// AWS behavioural:
/// check_update_item_chained_arithmetic_operators_on_list_index_rejected.py
#[tokio::test]
async fn given_chained_arithmetic_on_list_index_when_update_then_syntax_error_and_list_unchanged() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("doc");
        let numbers = |values: [&str; 3]| values.map(|value| json!({"N": value}));
        let original = with(&key, json!({"a": {"L": numbers(["10", "20", "30"])}}));
        put(&store, &table, original.clone()).await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key,
                "UpdateExpression": "SET a[0] = (a[0] + :x) - :y",
                "ExpressionAttributeValues": {":x": {"N": "5"}, ":y": {"N": "3"}},
            }),
        )
        .await
        .assert_validation(
            "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \"-\", \
             near: \") - :y\"",
        );
        assert_item(
            &store,
            &table,
            &key,
            &original,
            "refused chained arithmetic",
        )
        .await;
        let applied = call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET a[0] = (a[0] + :x)",
                "ExpressionAttributeValues": {":x": {"N": "5"}}, "ReturnValues": "UPDATED_NEW",
            }),
        )
        .await;
        assert_eq!(
            applied.ok()["Attributes"],
            json!({"a": {"L": [{"N": "15"}]}})
        );
        let expected = with(&key, json!({"a": {"L": numbers(["15", "20", "30"])}}));
        assert_item(
            &store,
            &table,
            &key,
            &expected,
            "single parenthesised operator",
        )
        .await;
    });
}

/// AWS behavioural:
/// check_update_item_expression_substitution_total_size_over_2mb_rejected.py
#[tokio::test]
async fn given_expression_values_over_two_megabytes_when_update_then_refused_and_item_unchanged() {
    scenario!(|store| {
        const KB: usize = 1024;
        let table = hash_table(&store).await;
        let key = pk("row");
        put(
            &store,
            &table,
            with(&key, json!({"marker": {"S": "initial"}})),
        )
        .await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key,
                "UpdateExpression": "SET marker = :m DELETE tags :vu",
                "ConditionExpression": "attribute_exists(pk) OR pk = :vc",
                "ExpressionAttributeValues": {
                    ":m": {"S": "written"},
                    ":vc": {"S": "c".repeat(950 * KB)},
                    ":vu": {"SS": ["u".repeat(950 * KB)]},
                },
            }),
        )
        .await
        .ok();
        let written = with(&key, json!({"marker": {"S": "written"}}));
        assert_item(&store, &table, &key, &written, "values under 2 MB").await;

        let mut values = serde_json::Map::new();
        for index in 0..6u8 {
            let filler = char::from(b'a' + index).to_string().repeat(490 * KB);
            values.insert(format!(":o{index}"), json!({"S": filler}));
        }
        values.insert(":m2".to_owned(), json!({"S": "written2"}));
        call(&store, "UpdateItem", json!({
            "TableName": table, "Key": key,
            "UpdateExpression": "SET marker = :m2",
            "ConditionExpression": "attribute_exists(pk) OR :o0 = :o1 OR :o2 = :o3 OR :o4 = :o5",
            "ExpressionAttributeValues": values,
        })).await.assert_validation(
            "1 validation error detected: ExpressionAttributeValues exceeds max size",
        );
        assert_item(&store, &table, &key, &written, "values over 2 MB").await;
    });
}

/// AWS behavioural:
/// check_update_item_if_not_exists_document_path_fallback_and_existing_subtract.
/// py
#[tokio::test]
async fn given_if_not_exists_when_update_then_fallback_path_copies_and_existing_value_subtracts() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let update = |key: &Value, expression: &str, values: Value| {
            let mut request = json!({
                "TableName": table, "Key": key, "UpdateExpression": expression,
                "ReturnValues": "ALL_NEW",
            });
            if !values.is_null() {
                request["ExpressionAttributeValues"] = values;
            }
            call(&store, "UpdateItem", request)
        };
        let copy = pk("row-copy-attr");
        put(
            &store,
            &table,
            with(&copy, json!({"a": {"S": "source-value"}})),
        )
        .await;
        let copied = update(&copy, "SET b = if_not_exists(b, a)", json!(null)).await;
        let expected = with(
            &copy,
            json!({"a": {"S": "source-value"}, "b": {"S": "source-value"}}),
        );
        assert_eq!(copied.ok()["Attributes"], expected);
        update(
            &copy,
            "SET a = :changed",
            json!({":changed": {"S": "changed-value"}}),
        )
        .await
        .ok();
        let kept = update(&copy, "SET b = if_not_exists(b, a)", json!(null)).await;
        let expected = with(
            &copy,
            json!({"a": {"S": "changed-value"}, "b": {"S": "source-value"}}),
        );
        assert_eq!(kept.ok()["Attributes"], expected);
        assert_item(&store, &table, &copy, &expected, "stored copy").await;

        let from_key = pk("row-copy-key");
        put(&store, &table, from_key.clone()).await;
        let copied = update(&from_key, "SET b = if_not_exists(b, pk)", json!(null)).await;
        assert_eq!(
            copied.ok()["Attributes"],
            with(&from_key, json!({"b": {"S": "row-copy-key"}}))
        );

        let amounts = json!({":d": {"N": "100"}, ":amt": {"N": "3"}});
        let existing = pk("row-subtract-existing");
        put(&store, &table, with(&existing, json!({"v": {"N": "10"}}))).await;
        let subtracted = update(
            &existing,
            "SET v = if_not_exists(v, :d) - :amt",
            amounts.clone(),
        )
        .await;
        assert_eq!(
            subtracted.ok()["Attributes"],
            with(&existing, json!({"v": {"N": "7"}}))
        );
        let absent = pk("row-subtract-default");
        put(&store, &table, absent.clone()).await;
        let defaulted = update(&absent, "SET v = if_not_exists(v, :d) - :amt", amounts).await;
        assert_eq!(
            defaulted.ok()["Attributes"],
            with(&absent, json!({"v": {"N": "97"}}))
        );
    });
}

/// AWS behavioural:
/// check_update_item_if_not_exists_second_operand_missing_attribute_rejected.py
#[tokio::test]
async fn given_both_if_not_exists_operands_absent_when_update_then_validation_error_and_item_unchanged()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("row-both-missing");
        let original = with(&key, json!({"label": {"S": "orig"}}));
        put(&store, &table, original.clone()).await;
        let copy = || {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key,
                    "UpdateExpression": "SET b = if_not_exists(b, a)", "ReturnValues": "ALL_NEW",
                }),
            )
        };
        copy().await.assert_validation(
            "The provided expression refers to an attribute that does not exist in the item",
        );
        assert_item(&store, &table, &key, &original, "refused if_not_exists").await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET a = :v",
                "ExpressionAttributeValues": {":v": {"S": "fallback-value"}},
            }),
        )
        .await
        .ok();
        let copied = copy().await;
        let expected = with(
            &original,
            json!({
                "a": {"S": "fallback-value"}, "b": {"S": "fallback-value"},
            }),
        );
        assert_eq!(copied.ok()["Attributes"], expected);
    });
}

/// AWS behavioural:
/// check_update_item_if_not_exists_size_checked_before_condition.py
/// (the repeated us-east-1 pass is not ported: regions are one provider
/// concern)
#[tokio::test]
async fn given_oversized_if_not_exists_operand_when_update_then_size_error_wins_over_the_condition()
{
    scenario!(|store| {
        let table = hash_table(&store).await;
        let update = |key: &Value, value: Value, condition: Option<&str>| {
            let mut request = json!({
                "TableName": table, "Key": key,
                "UpdateExpression": "SET bigattr = if_not_exists(bigattr, :v)",
                "ExpressionAttributeValues": {":v": value},
            });
            if let Some(condition) = condition {
                request["ConditionExpression"] = json!(condition);
                request["ExpressionAttributeValues"][":true"] = json!({"BOOL": true});
            }
            call(&store, "UpdateItem", request)
        };
        let size_error = (
            "ValidationException",
            "Item size to update has exceeded the maximum allowed size",
        );
        let flag = |name: &str, value: bool| with(&pk(name), json!({"flag": {"BOOL": value}}));

        put(&store, &table, flag("ctrl-pass", true)).await;
        update(&pk("ctrl-pass"), json!({"S": "ok"}), Some("flag = :true"))
            .await
            .ok();
        let passed = get(&store, &table, &pk("ctrl-pass")).await;
        assert_eq!(passed["bigattr"], json!({"S": "ok"}));

        put(&store, &table, flag("ctrl-fail", false)).await;
        update(&pk("ctrl-fail"), json!({"S": "ok"}), Some("flag = :true"))
            .await
            .assert_error(CONDITION_FAILED);
        assert_item(
            &store,
            &table,
            &pk("ctrl-fail"),
            &flag("ctrl-fail", false),
            "failed control",
        )
        .await;

        let oversized = json!({"S": "x".repeat(409_700)});
        let cases = [
            (
                "cond-pass",
                Some(flag("cond-pass", true)),
                Some("flag = :true"),
            ),
            (
                "cond-fail",
                Some(flag("cond-fail", false)),
                Some("flag = :true"),
            ),
            ("cond-absent", Some(pk("cond-absent")), None),
        ];
        for (name, seed, condition) in cases {
            let seed = seed.expect("seed");
            put(&store, &table, seed.clone()).await;
            update(&pk(name), oversized.clone(), condition)
                .await
                .assert_error(size_error);
            assert_item(&store, &table, &pk(name), &seed, name).await;
        }
    });
}

/// AWS behavioural: check_update_item_key_attribute_immutable.py
/// (AWS's unquoted attribute name; aux's quoted text is the gap this test
/// exposes)
#[tokio::test]
async fn given_key_attribute_when_update_sets_it_then_validation_error_and_item_unchanged() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("immutable-key");
        put(&store, &table, with(&key, json!({"v": {"S": "1"}}))).await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET v = :v2",
                "ExpressionAttributeValues": {":v2": {"S": "2"}},
            }),
        )
        .await
        .ok();
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET pk = :newpk",
                "ExpressionAttributeValues": {":newpk": {"S": "different-key"}},
            }),
        )
        .await
        .assert_validation(
            "One or more parameter values were invalid: Cannot update attribute pk. This \
             attribute is part of the key",
        );
        let expected = with(&key, json!({"v": {"S": "2"}}));
        assert_item(&store, &table, &key, &expected, "refused key update").await;
    });
}

/// AWS behavioural: check_update_item_key_schema_mismatch.py
#[tokio::test]
async fn given_composite_key_table_when_update_key_mismatches_schema_then_validation_error_and_item_unchanged()
 {
    scenario!(|store| {
        const MISMATCH: &str = "The provided key element does not match the schema";
        let table = unique_table_name().to_string();
        create_table(&store, table_request(&table, "S", Some(("sk", "S")))).await;
        let full_key = json!({"pk": {"S": "schema-mismatch-1"}, "sk": {"S": "sort-1"}});
        put(&store, &table, with(&full_key, json!({"v": {"S": "orig"}}))).await;
        let update = |key: Value, value: &str| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": "SET v = :v",
                    "ExpressionAttributeValues": {":v": {"S": value}},
                }),
            )
        };
        update(full_key.clone(), "live").await.ok();
        let live = with(&full_key, json!({"v": {"S": "live"}}));
        update(pk("schema-mismatch-1"), "hacked")
            .await
            .assert_validation(MISMATCH);
        update(
            json!({"pk": {"S": "schema-mismatch-1"}, "not_a_key_attr": {"S": "z"}}),
            "hacked2",
        )
        .await
        .assert_validation(MISMATCH);
        assert_item(&store, &table, &full_key, &live, "refused UpdateItem keys").await;
        call(
            &store,
            "BatchWriteItem",
            json!({"RequestItems": {table.clone(): [{"PutRequest": {
                "Item": {"pk": {"S": "schema-mismatch-1"}, "sk": {"N": "1"}, "v": {"S": "hacked3"}},
            }}]}}),
        )
        .await
        .assert_validation(MISMATCH);
        assert_item(
            &store,
            &table,
            &full_key,
            &live,
            "refused BatchWriteItem key type",
        )
        .await;
    });
}

/// AWS behavioural:
/// check_update_item_malformed_or_overflowing_number_rejected.py
/// (legacy AttributeUpdates cases dropped: rejected by design in
/// dynamodb_parity.md)
#[tokio::test]
async fn given_malformed_or_overflowing_numbers_when_update_then_validation_error_and_item_unchanged()
 {
    scenario!(|store| {
        const NOT_NUMERIC: &str =
            "1 validation error detected: The parameter cannot be converted to a numeric value: ";
        const OVERFLOW: &str = "Number overflow. Attempting to store a number with magnitude \
                                larger than supported range";
        let max_n = format!("{}E+88", "9".repeat(38));
        let table = hash_table(&store).await;
        let key = pk("row1");
        let item = with(&key, json!({"n": {"N": "5"}}));
        put(&store, &table, item.clone()).await;
        let set_bad = |value: Value| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": "SET bad = :v",
                    "ExpressionAttributeValues": {":v": value},
                }),
            )
        };
        set_bad(json!({"N": "12e"}))
            .await
            .assert_validation(&format!("{NOT_NUMERIC}12e"));
        set_bad(json!({"NS": ["1", "abc"]}))
            .await
            .assert_validation(&format!("{NOT_NUMERIC}abc"));
        set_bad(json!({"N": format!("1{}", "0".repeat(200))}))
            .await
            .assert_validation(&format!("1 validation error detected: {OVERFLOW}"));
        assert_item(&store, &table, &key, &item, "refused malformed numbers").await;

        for (name, number, expression) in [
            ("big", max_n.clone(), "SET v = v + :m"),
            ("big-add", max_n.clone(), "ADD v :m"),
            ("neg", format!("-{max_n}"), "SET v = v - :m"),
        ] {
            let row = pk(name);
            put(&store, &table, with(&row, json!({"v": {"N": number}}))).await;
            let stored = get(&store, &table, &row).await;
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": row, "UpdateExpression": expression,
                    "ExpressionAttributeValues": {":m": {"N": max_n}},
                }),
            )
            .await
            .assert_validation(OVERFLOW);
            assert_item(&store, &table, &row, &stored, expression).await;
        }
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET n = n + :one ADD c :one",
                "ExpressionAttributeValues": {":one": {"N": "1"}},
            }),
        )
        .await
        .ok();
        let expected = with(&key, json!({"n": {"N": "6"}, "c": {"N": "1"}}));
        assert_item(&store, &table, &key, &expected, "in-range arithmetic").await;
    });
}

/// AWS behavioural:
/// check_update_item_nested_arithmetic_mixed_clauses_and_if_not_exists_parent.
/// py
#[tokio::test]
async fn given_nested_paths_when_update_mixes_clauses_then_updated_new_returns_only_touched_leaves()
{
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("doc");
        let number = |value: &str| json!({"N": value});
        put(
            &store,
            &table,
            with(
                &key,
                json!({
                    "a": {"L": [number("10"), number("20"), number("30")]},
                    "info": {"M": {"visits": number("100")}},
                }),
            ),
        )
        .await;
        let update = |expression: &str, values: Value, names: Value| {
            let mut request = json!({
                "TableName": table, "Key": key, "UpdateExpression": expression,
                "ExpressionAttributeValues": values, "ReturnValues": "UPDATED_NEW",
            });
            if !names.is_null() {
                request["ExpressionAttributeNames"] = names;
            }
            call(&store, "UpdateItem", request)
        };
        let returned = |reply: Reply| reply.ok()["Attributes"].clone();

        let reply = update(
            "SET a[0] = (a[0] + :x)",
            json!({":x": number("5")}),
            json!(null),
        )
        .await;
        assert_eq!(returned(reply), json!({"a": {"L": [number("15")]}}));
        let reply = update(
            "SET a[1] = a[1] + :inc ADD info.visits :one",
            json!({":inc": number("5"), ":one": number("1")}),
            json!(null),
        )
        .await;
        assert_eq!(
            returned(reply),
            json!({"a": {"L": [number("25")]}, "info": {"M": {"visits": number("101")}}})
        );
        let reply = update(
            "SET #i.#r = :r, #i.#p = :p",
            json!({":r": number("4.5"), ":p": {"S": "A quiet thriller."}}),
            json!({"#i": "info", "#r": "rating", "#p": "plot"}),
        )
        .await;
        assert_eq!(
            returned(reply),
            json!({"info": {"M": {"rating": number("4.5"), "plot": {"S": "A quiet thriller."}}}})
        );
        let reply = update(
            "SET meta = if_not_exists(meta, :empty)",
            json!({":empty": {"M": {}}}),
            json!(null),
        )
        .await;
        assert_eq!(returned(reply), json!({"meta": {"M": {}}}));
        let reply = update(
            "SET meta.flag = :v",
            json!({":v": {"BOOL": true}}),
            json!(null),
        )
        .await;
        assert_eq!(
            returned(reply),
            json!({"meta": {"M": {"flag": {"BOOL": true}}}})
        );
        let expected = with(
            &key,
            json!({
                "a": {"L": [number("15"), number("25"), number("30")]},
                "info": {"M": {
                    "visits": number("101"), "rating": number("4.5"),
                    "plot": {"S": "A quiet thriller."},
                }},
                "meta": {"M": {"flag": {"BOOL": true}}},
            }),
        );
        assert_item(&store, &table, &key, &expected, "final item").await;
    });
}

/// AWS behavioural:
/// check_update_item_number_set_condition_exact_at_full_precision.py
#[tokio::test]
async fn given_number_set_when_update_condition_differs_beyond_float_precision_then_condition_fails()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("item");
        let stored = "100000000000000001";
        let off_by_one = "100000000000000002";
        put(
            &store,
            &table,
            with(&key, json!({"ns": {"NS": [stored]}, "v": {"N": "0"}})),
        )
        .await;
        let update = |value: &str, set: &str, members: &str| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": "SET v = :nv",
                    "ConditionExpression": "ns = :match",
                    "ExpressionAttributeValues": {":nv": {"N": value}, ":match": {set: [members]}},
                }),
            )
        };
        update("1", "NS", stored).await.ok();
        let after = with(&key, json!({"ns": {"NS": [stored]}, "v": {"N": "1"}}));
        assert_item(&store, &table, &key, &after, "exact match").await;
        update("2", "NS", off_by_one)
            .await
            .assert_error(CONDITION_FAILED);
        assert_item(&store, &table, &key, &after, "off by one digit").await;
    });
}

/// AWS behavioural: check_update_item_numeric_set_precision_preserved.py
#[tokio::test]
async fn given_number_set_when_add_38_digit_member_then_both_members_keep_full_precision() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("item");
        put(&store, &table, with(&key, json!({"nums": {"NS": ["1.1"]}}))).await;
        let big = "123456789012345678901234567890123456.78";
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "ADD nums :n",
                "ExpressionAttributeValues": {":n": {"NS": [big]}},
            }),
        )
        .await
        .ok();
        // Compare as sets of decimal strings: AWS asserts value equality, not
        // member order.
        let expected = with(&key, json!({"nums": {"NS": ["1.1", big]}}));
        assert_item(&store, &table, &key, &expected, "added 38-digit member").await;
    });
}

/// AWS behavioural:
/// check_update_item_remove_missing_map_key_noop_and_set_empty_list_index.py
#[tokio::test]
async fn given_map_and_empty_list_when_remove_missing_key_and_set_index_then_noop_and_single_element()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("has-map");
        let seeded = with(&key, json!({"m": {"M": {"present": {"S": "x"}}}}));
        put(&store, &table, seeded.clone()).await;
        let remove = |name: &str| {
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": "REMOVE m.#k",
                    "ExpressionAttributeNames": {"#k": name}, "ReturnValues": "ALL_NEW",
                }),
            )
        };
        assert_eq!(remove("missing").await.ok()["Attributes"], seeded);
        assert_item(&store, &table, &key, &seeded, "no-op REMOVE").await;
        let emptied = with(&key, json!({"m": {"M": {}}}));
        assert_eq!(remove("present").await.ok()["Attributes"], emptied);

        let list_key = pk("empty-list");
        put(&store, &table, with(&list_key, json!({"l": {"L": []}}))).await;
        let set = call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": list_key, "UpdateExpression": "SET l[0] = :v",
                "ExpressionAttributeValues": {":v": {"S": "only"}}, "ReturnValues": "ALL_NEW",
            }),
        )
        .await;
        let expected = with(&list_key, json!({"l": {"L": [{"S": "only"}]}}));
        assert_eq!(set.ok()["Attributes"], expected);
        assert_item(&store, &table, &list_key, &expected, "SET on empty list").await;
    });
}

/// AWS behavioural:
/// check_update_item_return_values_omit_attributes_when_no_value_qualifies.py
/// (legacy AttributeUpdates DELETE step dropped: rejected by design in
/// dynamodb_parity.md)
#[tokio::test]
async fn given_updates_with_no_qualifying_value_when_return_values_then_attributes_are_omitted() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let key = pk("rv");
        put(
            &store,
            &table,
            with(
                &key,
                json!({
                    "gone": {"S": "present"},
                    "m": {"M": {"k": {"S": "v"}}},
                    "keep": {"S": "old"},
                }),
            ),
        )
        .await;
        let update = |expression: &str, values: Value, return_values: &str| {
            let mut request = json!({
                "TableName": table, "Key": key, "UpdateExpression": expression,
                "ReturnValues": return_values,
            });
            if !values.is_null() {
                request["ExpressionAttributeValues"] = values;
            }
            call(&store, "UpdateItem", request)
        };
        let removed = update("REMOVE gone", json!(null), "UPDATED_NEW").await;
        assert!(removed.ok().get("Attributes").is_none(), "{}", removed.body);
        let fresh = update("SET fresh = :f", json!({":f": {"S": "new"}}), "UPDATED_OLD").await;
        assert!(fresh.ok().get("Attributes").is_none(), "{}", fresh.body);
        let kept = update("SET keep = :k", json!({":k": {"S": "new"}}), "UPDATED_NEW").await;
        assert_eq!(kept.ok()["Attributes"], json!({"keep": {"S": "new"}}));
        let expected = with(
            &key,
            json!({
                "m": {"M": {"k": {"S": "v"}}}, "fresh": {"S": "new"}, "keep": {"S": "new"},
            }),
        );
        assert_item(&store, &table, &key, &expected, "after updates").await;
    });
}

const UPDATE_TOO_BIG: (&str, &str) = (
    "ValidationException",
    "Item size to update has exceeded the maximum allowed size",
);

/// AWS behavioural:
/// check_update_item_set_list_append_add_exceeding_400kb_rejected.py
#[tokio::test]
async fn given_growth_past_400kb_when_set_list_append_or_add_then_refused_and_item_unchanged() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let oversized = "x".repeat(401 * 1024);
        let cases = [
            (
                "via-set",
                json!({}),
                "SET bigattr = :v",
                json!({"S": oversized}),
            ),
            (
                "via-list-append",
                json!({"biglist": {"L": []}}),
                "SET biglist = list_append(biglist, :v)",
                json!({"L": [{"S": oversized}]}),
            ),
            (
                "via-add",
                json!({"bigset": {"SS": ["seed"]}}),
                "ADD bigset :v",
                json!({"SS": [oversized]}),
            ),
        ];
        for (name, extra, expression, value) in cases {
            let key = pk(name);
            let seed = with(&key, extra);
            put(&store, &table, seed.clone()).await;
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": expression,
                    "ExpressionAttributeValues": {":v": value},
                }),
            )
            .await
            .assert_error(UPDATE_TOO_BIG);
            assert_item(&store, &table, &key, &seed, name).await;
        }
        let control = pk("control");
        put(&store, &table, control.clone()).await;
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": control, "UpdateExpression": "SET note = :v",
                "ExpressionAttributeValues": {":v": {"S": "ok"}},
            }),
        )
        .await
        .ok();
        let expected = with(&control, json!({"note": {"S": "ok"}}));
        assert_item(&store, &table, &control, &expected, "small SET control").await;
    });
}

/// AWS behavioural:
/// check_update_item_set_list_index_on_missing_item_rejected.py
#[tokio::test]
async fn given_missing_item_when_set_list_index_then_invalid_path_error_and_no_item() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let values = json!({":v": {"S": "x"}});
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": pk("no-item"), "UpdateExpression": "SET b[1] = :v",
                "ExpressionAttributeValues": values,
            }),
        )
        .await
        .assert_validation(
            "The document path provided in the update expression is invalid for update",
        );
        assert!(get(&store, &table, &pk("no-item")).await.is_null());
        let key = pk("has-item");
        put(
            &store,
            &table,
            with(&key, json!({"b": {"L": [{"S": "v0"}]}})),
        )
        .await;
        let applied = call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET b[1] = :v",
                "ExpressionAttributeValues": values, "ReturnValues": "ALL_NEW",
            }),
        )
        .await;
        let expected = with(&key, json!({"b": {"L": [{"S": "v0"}, {"S": "x"}]}}));
        assert_eq!(applied.ok()["Attributes"], expected);
    });
}

/// AWS behavioural: check_vector_item_size_gate_counts_stored_number_bytes.py
/// (vector index and capacity assertions dropped: unsupported; only the
/// item-size gate remains)
#[tokio::test]
async fn given_numbers_with_many_digits_when_put_then_item_size_counts_stored_number_bytes() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let pad = "x".repeat(370 * 1024);
        let accepted = with(
            &pk("ok"),
            json!({
                "pad": {"S": pad}, "v": {"L": vec![json!({"N": "1"}); 4096]},
            }),
        );
        put(&store, &table, accepted.clone()).await;
        assert_item(&store, &table, &pk("ok"), &accepted, "single-digit vector").await;
        let refused = with(
            &pk("bad"),
            json!({
                "pad": {"S": pad}, "v": {"L": vec![json!({"N": "9".repeat(38)}); 4096]},
            }),
        );
        call(
            &store,
            "PutItem",
            json!({"TableName": table, "Item": refused}),
        )
        .await
        .assert_error(ITEM_TOO_BIG);
        assert!(get(&store, &table, &pk("bad")).await.is_null());
    });
}

const ABSENT_TABLE: (&str, &str) = ("ResourceNotFoundException", "Requested resource not found");

/// AWS behavioural:
/// check_request_validation_precedes_missing_table_lookup_rejected.py
#[tokio::test]
async fn given_missing_table_when_request_is_malformed_then_validation_wins_and_well_formed_is_not_found()
 {
    scenario!(|store| {
        let table = unique_table_name().to_string();
        let key = pk("k");
        let value = json!({":v": {"S": "x"}});
        let cases: Vec<(&str, Value, Option<&str>)> = vec![
            (
                "GetItem",
                json!({"Key": key, "ProjectionExpression": ""}),
                Some("Invalid ProjectionExpression: The expression can not be empty;"),
            ),
            (
                "GetItem",
                json!({"Key": key, "ProjectionExpression": "a..b"}),
                Some("Invalid ProjectionExpression: Syntax error; token: \".\", near: \"..b\""),
            ),
            (
                "GetItem",
                json!({"Key": key, "ProjectionExpression": "a, a.b"}),
                Some(
                    "Invalid ProjectionExpression: Two document paths overlap with each other; \
                     must remove or rewrite one of these paths; path one: [a], path two: [a, b]",
                ),
            ),
            (
                "GetItem",
                json!({"Key": key, "ProjectionExpression": "a, b"}),
                None,
            ),
            (
                "UpdateItem",
                json!({"Key": key, "UpdateExpression": ""}),
                Some(
                    "1 validation error detected: Invalid UpdateExpression: The expression can \
                     not be empty;",
                ),
            ),
            (
                "UpdateItem",
                json!({"Key": key, "UpdateExpression": "SET"}),
                Some(
                    "1 validation error detected: Invalid UpdateExpression: Syntax error; token: \
                     \"<EOF>\", near: \"SET\"",
                ),
            ),
            (
                "UpdateItem",
                json!({"Key": key, "UpdateExpression": "SET status = :v", "ExpressionAttributeValues": value}),
                Some(
                    "1 validation error detected: Invalid UpdateExpression: Attribute name is a \
                     reserved keyword; reserved keyword: status",
                ),
            ),
            (
                "UpdateItem",
                json!({"Key": key, "UpdateExpression": "SET #x = :v", "ExpressionAttributeValues": value}),
                Some(
                    "1 validation error detected: Invalid UpdateExpression: An expression \
                     attribute name used in the document path is not defined; attribute name: #x",
                ),
            ),
            (
                "UpdateItem",
                json!({
                    "Key": key, "UpdateExpression": "SET #s = :v",
                    "ExpressionAttributeNames": {"#s": "status"}, "ExpressionAttributeValues": value,
                }),
                None,
            ),
            (
                "Scan",
                json!({"FilterExpression": "a +"}),
                Some("Invalid FilterExpression: Syntax error; token: \"+\", near: \"a +\""),
            ),
            (
                "Scan",
                json!({"FilterExpression": "attribute_exists(a)"}),
                None,
            ),
        ];
        for (operation, request, validation) in cases {
            let reply = call(
                &store,
                operation,
                with(&json!({"TableName": table}), request),
            )
            .await;
            match validation {
                Some(message) => reply.assert_validation(message),
                None => reply.assert_error(ABSENT_TABLE),
            }
        }
        call(
            &store,
            "BatchGetItem",
            json!({"RequestItems": {table.clone(): {"Keys": [key, key]}}}),
        )
        .await
        .assert_validation("Provided list of item keys contains duplicates");
        call(
            &store,
            "BatchGetItem",
            json!({"RequestItems": {table.clone(): {
                "Keys": [key, pk("k2")],
            }}}),
        )
        .await
        .assert_error(ABSENT_TABLE);
        call(&store, "DescribeTable", json!({"TableName": table }))
            .await
            .assert_error((
                "ResourceNotFoundException",
                &format!("Requested resource not found: Table: {table} not found"),
            ));
    });
}

/// AWS behavioural:
/// check_response_requestid_format_and_content_length_match_body_on_error.py
/// and check_x_amz_crc32_matches_body_on_success_and_error.py
/// (only the body, code and message halves are ported: the `x-amzn-RequestId`,
/// `Content-Length` and `x-amz-crc32` headers belong to the HTTP route tests,
/// not to this store-level harness)
#[tokio::test]
async fn given_refused_requests_when_executed_then_error_codes_messages_and_unicode_items_are_exact()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let item = with(
            &pk("k1"),
            json!({
                "note": {"S": "Gr\u{fc}\u{df}e \u{2014} \u{6771}\u{4eac} \u{1f600}"}, "n": {"N": "42"},
            }),
        );
        put(&store, &table, item.clone()).await;
        assert_item(&store, &table, &pk("k1"), &item, "unicode item").await;
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table, "Item": with(&pk("k1"), json!({"note": {"S": "clobber"}})),
                "ConditionExpression": "attribute_not_exists(pk)",
            }),
        )
        .await
        .assert_error(CONDITION_FAILED);
        assert_item(&store, &table, &pk("k1"), &item, "refused conditional put").await;
        let missing = unique_table_name().to_string();
        call(&store, "DescribeTable", json!({"TableName": missing}))
            .await
            .assert_error((
                "ResourceNotFoundException",
                &format!("Requested resource not found: Table: {missing} not found"),
            ));
        call(
            &store,
            "GetItem",
            json!({"TableName": table, "Key": {"pk": {"N": "1"}}}),
        )
        .await
        .assert_validation("The provided key element does not match the schema");
    });
}

/// AWS behavioural:
/// check_sdk_retry_replays_the_wire_and_only_the_token_stops_the_duplicate.py
/// (the SDK retry-mode matrix and injected 503s are not ported: they exercise
/// client transport; only the server-side token dedupe, the no-write
/// ValidationException and the deleted-table describe are)
#[tokio::test]
async fn given_same_client_request_token_when_transact_update_repeats_then_counter_lands_once() {
    scenario!(|store| {
        let table = hash_table(&store).await;
        let increment = |key: &str| {
            json!({"Update": {
                "TableName": table, "Key": pk(key), "UpdateExpression": "ADD n :one",
                "ExpressionAttributeValues": {":one": {"N": "1"}},
            }})
        };
        let token = "0b1c9a52-5a5b-4a39-9f0c-3a6f0f0e0a11";
        for _ in 0..4 {
            call(
                &store,
                "TransactWriteItems",
                json!({
                    "ClientRequestToken": token, "TransactItems": [increment("token")],
                }),
            )
            .await
            .ok();
        }
        assert_eq!(
            get(&store, &table, &pk("token")).await["n"],
            json!({"N": "1"})
        );

        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": pk("rejected"), "UpdateExpression": "ADD n :one",
                "ExpressionAttributeValues": {":one": {"S": "not-a-number"}},
            }),
        )
        .await
        .assert_validation(
            "1 validation error detected: Invalid UpdateExpression: Incorrect operand type for \
             operator or function; operator: ADD, operand type: STRING, typeSet: \
             ALLOWED_FOR_ADD_OPERAND",
        );
        assert!(get(&store, &table, &pk("rejected")).await.is_null());
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": pk("rejected"), "UpdateExpression": "ADD n :one",
                "ExpressionAttributeValues": {":one": {"N": "1"}},
            }),
        )
        .await
        .ok();
        assert_eq!(
            get(&store, &table, &pk("rejected")).await["n"],
            json!({"N": "1"})
        );
    });
}

/// AWS behavioural: check_validation_messages_are_customer_intelligible.py and
/// check_validation_messages_name_the_concept_without_implementation_leakage.py
#[tokio::test]
async fn given_malformed_requests_when_executed_then_validation_messages_are_exact_and_leak_nothing()
 {
    scenario!(|store| {
        let table = hash_table(&store).await;
        put(&store, &table, with(&pk("ctl"), json!({"n": {"N": "1"}}))).await;
        let mut nested = json!({"S": "leaf"});
        for _ in 0..32 {
            nested = json!({"M": {"n": nested}});
        }
        let cases: Vec<(&str, Value, &str, Option<&str>)> = vec![
            (
                "GetItem",
                json!({"TableName": "ab", "Key": pk("k")}),
                "1 validation error detected: Value 'ab' at 'tableName' failed to satisfy \
                 constraint: Member must have length greater than or equal to 3",
                None,
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": with(&pk("n"), json!({"n": {"N": "9".repeat(39)}}))}),
                "1 validation error detected: Attempting to store more than 38 significant digits \
                 in a Number",
                Some("n"),
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": with(&pk("d"), json!({"s": {"SS": ["a", "a"]}}))}),
                "1 validation error detected: One or more parameter values were invalid: Input \
                 collection [\"a\", \"a\"] contains duplicates.",
                Some("d"),
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": with(&pk("e"), json!({"bad": {}}))}),
                "Supplied AttributeValue is empty, must contain exactly one of the supported \
                 datatypes",
                Some("e"),
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": pk("r"), "ReturnValues": "BOGUS"}),
                "1 validation error detected: Failed to satisfy constraint: Member must satisfy \
                 enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]",
                Some("r"),
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": pk("c"), "ConditionExpression": "attribute_exists("}),
                "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \
                 \"<EOF>\", near: \"(\"",
                Some("c"),
            ),
            (
                "BatchGetItem",
                json!({
                    "RequestItems": {table.clone(): {"Keys": [pk("g")]}},
                    "ReturnConsumedCapacity": "BOGUS",
                }),
                "1 validation error detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
                None,
            ),
            (
                "BatchWriteItem",
                json!({"RequestItems": {table.clone(): [{"PutRequest": {"Item": with(
                    &pk("m"), json!({"nested": {"M": {"": {"S": "v"}}}}),
                )}}]}}),
                "Empty attribute name",
                Some("m"),
            ),
            (
                "GetItem",
                json!({"TableName": table, "Key": pk("ctl"), "ReturnConsumedCapacity": "NOPE"}),
                "1 validation error detected: Value 'NOPE' at 'returnConsumedCapacity' failed to \
                 satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]",
                None,
            ),
            (
                "GetItem",
                json!({"TableName": table, "Key": {"pk": {"N": "1"}}}),
                "The provided key element does not match the schema",
                None,
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": {"pk": {"S": ""}}}),
                "One or more parameter values are not valid. The AttributeValue for a key \
                 attribute cannot contain an empty string value. Key: pk",
                None,
            ),
            (
                "GetItem",
                json!({"TableName": table, "Key": pk("ctl"), "ProjectionExpression": "1+"}),
                "Invalid ProjectionExpression: Syntax error; token: \"1\", near: \"1+\"",
                None,
            ),
            (
                "PutItem",
                json!({"TableName": table, "Item": with(&pk("msg-nest"), json!({"d": nested}))}),
                "1 validation error detected: Nesting Levels have exceeded supported limits: \
                 Attributes in the item have nested levels beyond supported limit",
                Some("msg-nest"),
            ),
        ];
        let leakage = [
            "::",
            "panic",
            "unwrap",
            "serde",
            "traceback",
            "offset",
            "deserializ",
        ];
        for (operation, request, message, written) in cases {
            let reply = call(&store, operation, request).await;
            reply.assert_validation(message);
            let lowered = message.to_lowercase();
            assert!(
                !leakage.iter().any(|marker| lowered.contains(marker)),
                "{message}"
            );
            if let Some(key) = written {
                assert!(
                    get(&store, &table, &pk(key)).await.is_null(),
                    "{key} was stored"
                );
            }
        }
        let scanned = call(
            &store,
            "Scan",
            json!({"TableName": table, "ConsistentRead": true, "Select": "COUNT"}),
        )
        .await;
        assert_eq!(scanned.ok()["Count"], 1, "only the control item may exist");
    });
}

/// AWS behavioural:
/// check_zero_at_exponent_boundary_rejected_one_step_past_range.py and
/// check_zero_with_out_of_range_exponent_rejected.py
#[tokio::test]
async fn given_zero_with_exponent_when_put_then_in_range_canonicalises_and_out_of_range_is_refused()
{
    scenario!(|store| {
        const OVERFLOW: &str = "1 validation error detected: Number overflow. Attempting to store \
                                a number with magnitude larger than supported range";
        const UNDERFLOW: &str = "1 validation error detected: Number underflow. Attempting to \
                                 store a number with magnitude smaller than supported range";
        let table = hash_table(&store).await;
        for raw in ["0E125", "0E-130", "0E5"] {
            let key = pk(&format!("ok-{raw}"));
            put(&store, &table, with(&key, json!({"v": {"N": raw}}))).await;
            assert_eq!(
                get(&store, &table, &key).await["v"],
                json!({"N": "0"}),
                "{raw}"
            );
        }
        let refused = [
            ("over", "0E126", OVERFLOW.to_owned()),
            ("under", "0E-131", UNDERFLOW.to_owned()),
            ("over-300", "0E+300", OVERFLOW.to_owned()),
            ("under-300", "0E-300", UNDERFLOW.to_owned()),
            ("exp-at-max", "1E9223372036854775807", OVERFLOW.to_owned()),
            (
                "exp-past-max",
                "1E9223372036854775808",
                "1 validation error detected: The parameter cannot be converted to a numeric \
                 value: 1E9223372036854775808"
                    .to_owned(),
            ),
        ];
        for (name, raw, message) in refused {
            call(
                &store,
                "PutItem",
                json!({
                    "TableName": table, "Item": with(&pk(name), json!({"v": {"N": raw}})),
                }),
            )
            .await
            .assert_validation(&message);
            assert!(
                get(&store, &table, &pk(name)).await.is_null(),
                "{name} was stored"
            );
        }
        let control = with(&pk("ctrl"), json!({"v": {"N": "42"}}));
        put(&store, &table, control.clone()).await;
        assert_item(&store, &table, &pk("ctrl"), &control, "control").await;
    });
}
