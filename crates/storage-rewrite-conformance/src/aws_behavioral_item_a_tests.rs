//! AWS emulator behavioural suite ports.
//!
//! Expected values are AWS's, copied from the suite scripts named in each
//! test's doc line. Requests go through the same request conversion and error
//! mapping as the HTTP route.

use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use storage_provider::DynamoOperations;

use crate::test_support::{TestStore, unique_table_name};

type Reply = (u16, Value);

const ITEM_TOO_LARGE: &str = "Item size has exceeded the maximum allowed size";
const ITEM_LIMIT_BYTES: usize = 409_600;

/// Runs one DynamoDB operation the way the route does: a request that fails
/// conversion is a 400 validation error, an operation error maps to its body.
async fn call_ops(
    operations: &Arc<dyn DynamoOperations>,
    operation: &str,
    request: Value,
) -> Reply {
    macro_rules! run {
        ($request:ty, $method:ident) => {
            match <$request>::try_from(request) {
                Ok(request) => match operations.$method(request).await {
                    Ok(response) => (200, serde_json::to_value(response).unwrap()),
                    Err(error) => {
                        let (status, axum::Json(body)): (
                            _,
                            axum::Json<http_error::ErrorResponse>,
                        ) = http_error::HttpApiError::from(error).into();
                        (status.as_u16(), serde_json::to_value(body).unwrap())
                    }
                },
                Err(message) => (
                    400,
                    json!({
                        "__type": "com.amazon.coral.validate#ValidationException",
                        "message": message
                    }),
                ),
            }
        };
    }
    match operation {
        "CreateTable" => run!(storage_types::CreateTableRequest, create_table),
        "UpdateTable" => run!(storage_types::UpdateTableRequest, update_table),
        "DescribeTable" => run!(storage_types::DescribeTableRequest, describe_table),
        "DeleteTable" => run!(storage_types::DeleteTableRequest, delete_table),
        "PutItem" => run!(storage_types::PutItemRequest, put_item),
        "GetItem" => run!(storage_types::GetItemRequest, get_item),
        "UpdateItem" => run!(storage_types::UpdateItemRequest, update_item),
        "DeleteItem" => run!(storage_types::DeleteItemRequest, delete_item),
        "Query" => run!(storage_types::QueryRequest, query),
        "Scan" => run!(storage_types::ScanRequest, scan),
        "BatchGetItem" => run!(storage_types::BatchGetItemRequest, batch_get_item),
        "BatchWriteItem" => run!(storage_types::BatchWriteItemRequest, batch_write_item),
        "TransactWriteItems" => run!(
            storage_types::TransactWriteItemsRequest,
            transact_write_items
        ),
        "TransactGetItems" => run!(storage_types::TransactGetItemsRequest, transact_get_items),
        other => panic!("unsupported operation: {other}"),
    }
}

async fn call(store: &TestStore, operation: &str, request: Value) -> Reply {
    call_ops(&store.operations, operation, request).await
}

fn assert_ok(reply: Reply) -> Value {
    assert_eq!(reply.0, 200, "unexpected failure: {}", reply.1);
    reply.1
}

/// Asserts a DynamoDB JSON error: HTTP 400, `__type` ending in `#code`, and the
/// exact message.
#[track_caller]
fn assert_error(reply: &Reply, code: &str, message: &str) {
    assert_eq!(reply.0, 400, "{}", reply.1);
    let error_type = reply.1["__type"].as_str().unwrap();
    assert!(
        error_type.ends_with(&format!("#{code}")),
        "expected {code}, got {}",
        reply.1
    );
    assert_eq!(reply.1["message"].as_str().unwrap_or_default(), message);
}

#[track_caller]
fn assert_validation(reply: &Reply, message: &str) {
    assert_error(reply, "ValidationException", message);
}

#[track_caller]
fn assert_condition_failed(reply: &Reply) {
    assert_error(
        reply,
        "ConditionalCheckFailedException",
        "The conditional request failed",
    );
}

/// A new on-demand table with a string hash key `pk`; returns its name.
async fn pk_table(store: &TestStore) -> String {
    store.create_standard_table().await.as_ref().to_owned()
}

/// A new on-demand table with string hash key `pk` and string range key `sk`.
async fn pk_sk_table(store: &TestStore) -> String {
    let table = unique_table_name().as_ref().to_owned();
    assert_ok(
        call(
            store,
            "CreateTable",
            json!({
                "TableName": table,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await,
    );
    table
}

async fn put(store: &TestStore, table: &str, item: Value) -> Reply {
    call(store, "PutItem", json!({"TableName": table, "Item": item})).await
}

/// The stored item at `key`, or `None`.
async fn get(store: &TestStore, table: &str, key: Value) -> Option<Value> {
    let body = assert_ok(
        call(
            store,
            "GetItem",
            json!({"TableName": table, "Key": key, "ConsistentRead": true}),
        )
        .await,
    );
    body.get("Item").cloned()
}

async fn scan_count(store: &TestStore, table: &str) -> u64 {
    let body = assert_ok(
        call(
            store,
            "Scan",
            json!({"TableName": table, "ConsistentRead": true}),
        )
        .await,
    );
    body["Count"].as_u64().unwrap()
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Pseudo-random alphanumeric text of exactly `length` bytes.
fn random_text(seed: u64, length: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut state = seed;
    (0..length)
        .map(|_| ALPHABET[(splitmix64(&mut state) % 62) as usize] as char)
        .collect()
}

/// Pseudo-random bytes of exactly `length` bytes.
fn random_bytes(seed: u64, length: usize) -> Vec<u8> {
    let mut state = seed;
    (0..length)
        .map(|_| (splitmix64(&mut state) >> 24) as u8)
        .collect()
}

fn binary(bytes: &[u8]) -> Value {
    json!({"B": STANDARD.encode(bytes)})
}

/// AWS behavioural: check_absence_condition_holds_with_no_previous_item.py
#[tokio::test]
async fn given_absent_or_deleted_key_when_presence_condition_runs_then_delete_and_put_fail() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let delete = |key: &str, condition: &str| {
        call(
            &store,
            "DeleteItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": key}},
                "ConditionExpression": condition
            }),
        )
    };

    assert_condition_failed(&delete("never-written", "attribute_exists(pk)").await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "never-written"}})).await,
        None
    );
    let body = assert_ok(delete("never-written", "attribute_not_exists(pk)").await);
    assert!(body.get("Attributes").is_none(), "{body}");
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "never-written"}})).await,
        None
    );

    let key = json!({"pk": {"S": "most-recently-deleted"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "most-recently-deleted"}, "marker": {"S": "seed"}}),
        )
        .await,
    );
    assert_ok(
        call(
            &store,
            "DeleteItem",
            json!({"TableName": table, "Key": key}),
        )
        .await,
    );
    assert_eq!(get(&store, &table, key.clone()).await, None);
    let rejected = call(
        &store,
        "PutItem",
        json!({
            "TableName": table,
            "Item": {"pk": {"S": "most-recently-deleted"}, "marker": {"S": "should-not-land"}},
            "ConditionExpression": "attribute_exists(pk)"
        }),
    )
    .await;
    assert_condition_failed(&rejected);
    assert_eq!(get(&store, &table, key.clone()).await, None);
    assert_ok(
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": {"pk": {"S": "most-recently-deleted"}, "marker": {"S": "restored"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }),
        )
        .await,
    );
    assert_eq!(
        get(&store, &table, key).await,
        Some(json!({"pk": {"S": "most-recently-deleted"}, "marker": {"S": "restored"}}))
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_32_way_conditional_counter_no_lost_update_with_audit_trail.py
#[tokio::test]
async fn given_32_racing_conditional_increments_when_failed_checks_retry_then_no_update_is_lost() {
    const RACERS: u64 = 32;
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "COUNTER#1"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "COUNTER#1"}, "n": {"N": "0"}}),
        )
        .await,
    );

    let mut racers = Vec::new();
    for worker in 0..RACERS {
        let operations = Arc::clone(&store.operations);
        let (table, key) = (table.clone(), key.clone());
        racers.push(tokio::spawn(async move {
            for _ in 0..200 {
                let read = assert_ok(
                    call_ops(
                        &operations,
                        "GetItem",
                        json!({"TableName": table, "Key": key, "ConsistentRead": true}),
                    )
                    .await,
                );
                let current: u64 = read["Item"]["n"]["N"].as_str().unwrap().parse().unwrap();
                let reply = call_ops(
                    &operations,
                    "UpdateItem",
                    json!({
                        "TableName": table, "Key": key,
                        "UpdateExpression": "SET #n = :next, #a = list_append(if_not_exists(#a, :empty), :entry)",
                        "ConditionExpression": "#n = :expected",
                        "ExpressionAttributeNames": {"#n": "n", "#a": "audit"},
                        "ExpressionAttributeValues": {
                            ":next": {"N": (current + 1).to_string()},
                            ":expected": {"N": current.to_string()},
                            ":empty": {"L": []},
                            ":entry": {"L": [{"M": {
                                "worker": {"N": worker.to_string()},
                                "before": {"N": current.to_string()},
                                "after": {"N": (current + 1).to_string()}
                            }}]}
                        }
                    }),
                )
                .await;
                if reply.0 == 200 {
                    return;
                }
                assert_condition_failed(&reply);
            }
            panic!("worker {worker} exhausted its attempts");
        }));
    }
    for racer in racers {
        racer.await.unwrap();
    }

    let item = get(&store, &table, key).await.unwrap();
    assert_eq!(item["n"], json!({"N": "32"}));
    let mut entries: Vec<(u64, u64, u64)> = item["audit"]["L"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            let field = |name: &str| entry["M"][name]["N"].as_str().unwrap().parse().unwrap();
            (field("worker"), field("before"), field("after"))
        })
        .collect();
    assert_eq!(entries.len(), 32);
    let mut workers: Vec<u64> = entries.iter().map(|entry| entry.0).collect();
    workers.sort_unstable();
    assert_eq!(workers, (0..RACERS).collect::<Vec<_>>());
    entries.sort_by_key(|entry| entry.1);
    for (step, (_, before, after)) in entries.iter().enumerate() {
        assert_eq!((*before, *after), (step as u64, step as u64 + 1));
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_atomic_counter_and_set_add_survive_concurrent_updates.py
#[tokio::test]
async fn given_ten_concurrent_add_updates_when_all_run_then_counter_and_set_lose_nothing() {
    const WORKERS: usize = 10;
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "COUNTER#1"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "COUNTER#1"}, "counter": {"N": "0"}}),
        )
        .await,
    );

    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let operations = Arc::clone(&store.operations);
        let (table, key) = (table.clone(), key.clone());
        workers.push(tokio::spawn(async move {
            assert_ok(
                call_ops(
                    &operations,
                    "UpdateItem",
                    json!({
                        "TableName": table, "Key": key,
                        "UpdateExpression": "ADD #c :one, #m :tok",
                        "ExpressionAttributeNames": {"#c": "counter", "#m": "members"},
                        "ExpressionAttributeValues": {
                            ":one": {"N": "1"}, ":tok": {"SS": [format!("worker-{worker}")]}
                        }
                    }),
                )
                .await,
            );
        }));
    }
    for worker in workers {
        worker.await.unwrap();
    }

    let item = get(&store, &table, key).await.unwrap();
    assert_eq!(item["counter"], json!({"N": "10"}));
    let mut members: Vec<String> = item["members"]["SS"]
        .as_array()
        .unwrap()
        .iter()
        .map(|member| member.as_str().unwrap().to_owned())
        .collect();
    members.sort();
    let mut expected: Vec<String> = (0..WORKERS)
        .map(|worker| format!("worker-{worker}"))
        .collect();
    expected.sort();
    assert_eq!(members, expected);
    store.finish().await;
}

/// AWS behavioural: check_attribute_names_are_case_sensitive.py
#[tokio::test]
async fn given_attribute_names_differing_in_case_when_projected_or_updated_then_each_resolves_exactly()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let item = json!({
        "pk": {"S": "case"}, "body": {"S": "lower"}, "Body": {"S": "title"}, "BODY": {"S": "upper"}
    });
    assert_ok(put(&store, &table, item.clone()).await);
    let key = json!({"pk": {"S": "case"}});
    assert_eq!(get(&store, &table, key.clone()).await, Some(item.clone()));

    for (name, expected) in [("body", "lower"), ("Body", "title"), ("BODY", "upper")] {
        let body = assert_ok(
            call(
                &store,
                "GetItem",
                json!({
                    "TableName": table, "Key": key, "ProjectionExpression": "#n",
                    "ExpressionAttributeNames": {"#n": name}, "ConsistentRead": true
                }),
            )
            .await,
        );
        assert_eq!(body["Item"], json!({name: {"S": expected}}));
    }
    assert_ok(
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET #n = :v",
                "ExpressionAttributeNames": {"#n": "Body"},
                "ExpressionAttributeValues": {":v": {"S": "title-updated"}}
            }),
        )
        .await,
    );
    let mut expected = item;
    expected["Body"] = json!({"S": "title-updated"});
    assert_eq!(get(&store, &table, key).await, Some(expected));

    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "guarded"}, "Marker": {"S": "present"}}),
        )
        .await,
    );
    let guarded = json!({"pk": {"S": "guarded"}});
    let update = |condition: &str| {
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": guarded, "UpdateExpression": "SET touched = :t",
                "ExpressionAttributeValues": {":t": {"BOOL": true}},
                "ConditionExpression": condition
            }),
        )
    };
    assert_condition_failed(&update("attribute_exists(marker)").await);
    assert!(
        get(&store, &table, guarded.clone())
            .await
            .unwrap()
            .get("touched")
            .is_none()
    );
    assert_ok(update("attribute_exists(Marker)").await);
    assert_eq!(
        get(&store, &table, guarded).await.unwrap()["touched"],
        json!({"BOOL": true})
    );
    store.finish().await;
}

/// AWS behavioural: check_attribute_updates_add_delete_on_absent_item.py
///
/// Legacy `AttributeUpdates` is rejected by design (dynamodb_parity.md,
/// UpdateItem row), so the ADD leg runs as an `UpdateExpression`; the DELETE
/// legs are legacy-only semantics and are not ported.
#[tokio::test]
async fn given_absent_item_when_add_expression_updates_counter_then_item_starts_from_zero() {
    let store = TestStore::new().await;
    let table = pk_sk_table(&store).await;
    let key = json!({"pk": {"S": "absent-add"}, "sk": {"S": "s1"}});
    let body = assert_ok(
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "ADD #c :seven",
                "ExpressionAttributeNames": {"#c": "counter"},
                "ExpressionAttributeValues": {":seven": {"N": "7"}},
                "ReturnValues": "UPDATED_NEW"
            }),
        )
        .await,
    );
    assert_eq!(body["Attributes"], json!({"counter": {"N": "7"}}));
    assert_eq!(
        get(&store, &table, key).await,
        Some(json!({"pk": {"S": "absent-add"}, "sk": {"S": "s1"}, "counter": {"N": "7"}}))
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_attribute_updates_add_delete_put_return_values_on_existing_item.py
///
/// Legacy `AttributeUpdates` is rejected by design (dynamodb_parity.md,
/// UpdateItem row), so each legacy action runs as its `UpdateExpression`
/// equivalent with the script's return values and final item.
#[tokio::test]
async fn given_existing_item_when_update_expressions_add_delete_and_set_then_return_values_match() {
    let store = TestStore::new().await;
    let table = pk_sk_table(&store).await;
    let key = json!({"pk": {"S": "item1"}, "sk": {"S": "s1"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({
                "pk": {"S": "item1"}, "sk": {"S": "s1"}, "cnt": {"N": "10"},
                "tags": {"SS": ["a", "b"]}, "hist": {"L": [{"S": "x"}]},
                "profile": {"M": {"city": {"S": "nyc"}, "note": {"S": "old"}}}
            }),
        )
        .await,
    );
    let update = |expression: &str, values: Value, return_values: &str| {
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": expression,
                "ExpressionAttributeValues": values, "ReturnValues": return_values
            }),
        )
    };
    let attributes = |reply: Reply| assert_ok(reply)["Attributes"].clone();
    let sorted_set = |value: &Value, name: &str| {
        let mut members: Vec<String> = value[name]["SS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|member| member.as_str().unwrap().to_owned())
            .collect();
        members.sort();
        members
    };

    assert_eq!(
        attributes(update("ADD cnt :v", json!({":v": {"N": "5"}}), "UPDATED_NEW").await),
        json!({"cnt": {"N": "15"}})
    );
    assert_eq!(
        attributes(update("ADD cnt :v", json!({":v": {"N": "3"}}), "UPDATED_OLD").await),
        json!({"cnt": {"N": "15"}})
    );
    let tags = attributes(
        update(
            "ADD tags :v",
            json!({":v": {"SS": ["b", "c"]}}),
            "UPDATED_NEW",
        )
        .await,
    );
    assert_eq!(sorted_set(&tags, "tags"), ["a", "b", "c"]);
    assert_eq!(
        attributes(
            update(
                "SET hist = list_append(hist, :v)",
                json!({":v": {"L": [{"S": "z"}]}}),
                "UPDATED_NEW"
            )
            .await
        ),
        json!({"hist": {"L": [{"S": "x"}, {"S": "z"}]}})
    );
    let old_tags = attributes(
        update(
            "DELETE tags :v",
            json!({":v": {"SS": ["a"]}}),
            "UPDATED_OLD",
        )
        .await,
    );
    assert_eq!(sorted_set(&old_tags, "tags"), ["a", "b", "c"]);
    assert_eq!(
        attributes(
            update(
                "SET profile = :v",
                json!({":v": {"M": {"city": {"S": "sf"}}}}),
                "UPDATED_NEW"
            )
            .await
        ),
        json!({"profile": {"M": {"city": {"S": "sf"}}}})
    );
    let removed = assert_ok(
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "REMOVE hist",
                "ReturnValues": "UPDATED_OLD"
            }),
        )
        .await,
    );
    assert_eq!(
        removed["Attributes"],
        json!({"hist": {"L": [{"S": "x"}, {"S": "z"}]}})
    );

    let item = get(&store, &table, key).await.unwrap();
    assert_eq!(item["cnt"], json!({"N": "18"}));
    assert_eq!(sorted_set(&item, "tags"), ["b", "c"]);
    assert_eq!(item["profile"], json!({"M": {"city": {"S": "sf"}}}));
    assert!(item.get("hist").is_none(), "{item}");
    store.finish().await;
}

/// Seeds `count` items `k00..` carrying 380 KiB of pseudo-random text each.
async fn seed_large_items(store: &TestStore, table: &str, count: usize) {
    for index in 0..count {
        assert_ok(
            put(
                store,
                table,
                json!({
                    "pk": {"S": format!("k{index:02}")},
                    "pad": {"S": random_text(index as u64 + 1, 380 * 1024)}
                }),
            )
            .await,
        );
    }
}

/// AWS behavioural:
/// check_batch_get_item_oversized_echoes_consistent_read_in_unprocessed_keys.py
#[tokio::test]
async fn given_batch_get_over_response_cap_when_keys_defer_then_unprocessed_keys_echo_consistent_read()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    seed_large_items(&store, &table, 44).await;
    let keys: Vec<Value> = (0..44)
        .map(|index| json!({"pk": {"S": format!("k{index:02}")}}))
        .collect();

    for consistent_read in [true, false] {
        let body = assert_ok(
            call(
                &store,
                "BatchGetItem",
                json!({"RequestItems": {&table: {"Keys": keys, "ConsistentRead": consistent_read}}}),
            )
            .await,
        );
        let unprocessed = &body["UnprocessedKeys"][&table];
        assert!(
            unprocessed.is_object(),
            "no unprocessed keys: {}",
            body["UnprocessedKeys"]
        );
        assert_eq!(unprocessed["ConsistentRead"], json!(consistent_read));
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_batch_get_item_unprocessed_keys_are_in_storage_and_drain_to_the_exact_set.
/// py
#[tokio::test]
async fn given_partial_batch_get_when_unprocessed_keys_are_followed_then_exact_key_set_drains() {
    const ITEMS: usize = 60;
    const PAYLOAD_BYTES: usize = 380 * 1024;
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    seed_large_items(&store, &table, ITEMS).await;
    let all_keys: Vec<String> = (0..ITEMS).map(|index| format!("k{index:02}")).collect();
    let mut request = json!({&table: {
        "Keys": all_keys.iter().map(|key| json!({"pk": {"S": key}})).collect::<Vec<_>>(),
        "ConsistentRead": true
    }});

    let names = |items: &Value| -> Vec<String> {
        items
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|item| item["pk"]["S"].as_str().unwrap().to_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    let mut seen: Vec<String> = Vec::new();
    let mut rounds = 0;
    let mut first = true;
    loop {
        let body = assert_ok(call(&store, "BatchGetItem", json!({"RequestItems": request})).await);
        let returned = names(&body["Responses"][&table]);
        for item in body["Responses"][&table].as_array().unwrap() {
            assert_eq!(item["pad"]["S"].as_str().unwrap().len(), PAYLOAD_BYTES);
        }
        let unprocessed = names(&body["UnprocessedKeys"][&table]["Keys"]);
        if first {
            assert!(
                !returned.is_empty() && !unprocessed.is_empty(),
                "response was not partial"
            );
            assert!(
                returned.iter().all(|key| !unprocessed.contains(key)),
                "key both returned and unprocessed"
            );
            let probe = get(&store, &table, json!({"pk": {"S": unprocessed[0]}}))
                .await
                .unwrap();
            assert_eq!(probe["pk"]["S"], json!(unprocessed[0]));
            first = false;
        }
        seen.extend(returned);
        if unprocessed.is_empty() {
            break;
        }
        rounds += 1;
        assert!(
            rounds <= 30,
            "drain did not converge; {} of {ITEMS} seen",
            seen.len()
        );
        request = body["UnprocessedKeys"].clone();
    }
    let mut sorted = seen.clone();
    sorted.sort();
    assert_eq!(sorted, all_keys);
    assert_eq!(seen.len(), ITEMS);
    store.finish().await;
}

/// AWS behavioural: check_batch_write_item_gsi_key_type_mismatch_rejected.py
///
/// The script's LSI leg is dropped: LSIs are validated, not created
/// (dynamodb_parity.md, CreateTable row).
#[tokio::test]
async fn given_gsi_key_of_wrong_type_when_batch_write_puts_then_validation_names_the_index() {
    let store = TestStore::new().await;
    let table = unique_table_name().as_ref().to_owned();
    assert_ok(
        call(
            &store,
            "CreateTable",
            json!({
                "TableName": table,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "gpk", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await,
    );

    let reply = call(
        &store,
        "BatchWriteItem",
        json!({"RequestItems": {&table: [{"PutRequest": {"Item": {
            "pk": {"S": "b"}, "sk": {"S": "1"}, "gpk": {"N": "9"}
        }}}]}}),
    )
    .await;
    assert_validation(
        &reply,
        "One or more parameter values were invalid: Type mismatch for Index Key gpk Expected: S \
         Actual: N IndexName: gsi1",
    );
    assert_eq!(scan_count(&store, &table).await, 0);
    store.finish().await;
}

/// Writes `make(prefix-name, filler)` for the largest accepted filler length
/// and one more: the first is stored whole, the second is refused for size and
/// not persisted. The two key values have equal length, so the boundary is
/// shared.
async fn assert_size_gate(
    store: &TestStore,
    table: &str,
    name: &str,
    max_filler: usize,
    make: impl Fn(&str, usize) -> Value,
) {
    let accepted = format!("acc-{name}");
    let rejected = format!("rej-{name}");
    assert_ok(put(store, table, make(&accepted, max_filler)).await);
    assert_eq!(
        get(store, table, json!({"pk": {"S": accepted}})).await,
        Some(make(&accepted, max_filler))
    );
    assert_validation(
        &put(store, table, make(&rejected, max_filler + 1)).await,
        ITEM_TOO_LARGE,
    );
    assert_eq!(
        get(store, table, json!({"pk": {"S": rejected}})).await,
        None
    );
}

/// AWS behavioural:
/// check_bool_and_null_attribute_byte_cost_at_item_size_gate.py
///
/// The script measures the cost by binary search; here the one-byte cost of a
/// BOOL or NULL value is asserted at the exact boundary: the key (`pk` plus an
/// 8-byte value), the one-byte attribute name and value, and the filler.
#[tokio::test]
async fn given_bool_or_null_attribute_when_item_is_at_size_limit_then_value_costs_one_byte() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let overhead = 2 + "acc-bool".len() + 1 + 1 + "f".len();
    for (name, attribute, value) in [
        ("bool", "b", json!({"BOOL": true})),
        ("fals", "b", json!({"BOOL": false})),
        ("null", "z", json!({"NULL": true})),
    ] {
        assert_size_gate(
            &store,
            &table,
            name,
            ITEM_LIMIT_BYTES - overhead,
            |pk, filler| {
                json!({
                    "pk": {"S": pk}, attribute: value.clone(),
                    "f": {"S": random_text(filler as u64, filler)}
                })
            },
        )
        .await;
    }
    store.finish().await;
}

/// AWS behavioural: check_concurrent_disjoint_nested_map_sets_all_land.py
#[tokio::test]
async fn given_fifty_concurrent_sets_of_distinct_map_keys_when_all_run_then_every_key_lands() {
    const WRITERS: usize = 50;
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "doc"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "doc"}, "d": {"M": {}}, "keep": {"S": "same"}}),
        )
        .await,
    );

    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let operations = Arc::clone(&store.operations);
        let (table, key) = (table.clone(), key.clone());
        writers.push(tokio::spawn(async move {
            assert_ok(
                call_ops(
                    &operations,
                    "UpdateItem",
                    json!({
                        "TableName": table, "Key": key, "UpdateExpression": "SET #d.#t = :v",
                        "ExpressionAttributeNames": {"#d": "d", "#t": format!("k{writer:02}")},
                        "ExpressionAttributeValues": {":v": {"S": format!("v{writer:02}")}}
                    }),
                )
                .await,
            );
        }));
    }
    for writer in writers {
        writer.await.unwrap();
    }

    let item = get(&store, &table, key).await.unwrap();
    let expected: serde_json::Map<String, Value> = (0..WRITERS)
        .map(|writer| {
            (
                format!("k{writer:02}"),
                json!({"S": format!("v{writer:02}")}),
            )
        })
        .collect();
    assert_eq!(item["d"]["M"], Value::Object(expected));
    assert_eq!(item["keep"], json!({"S": "same"}));
    store.finish().await;
}

/// AWS behavioural:
/// check_concurrent_unconditional_list_append_loses_no_element.py
#[tokio::test]
async fn given_24_concurrent_list_appends_when_all_run_then_each_post_image_is_a_prefix_of_the_final_list()
 {
    const WORKERS: usize = 24;
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "race"}});
    assert_ok(put(&store, &table, json!({"pk": {"S": "race"}, "l": {"L": []}})).await);

    let mut workers = Vec::new();
    for worker in 0..WORKERS {
        let operations = Arc::clone(&store.operations);
        let (table, key) = (table.clone(), key.clone());
        workers.push(tokio::spawn(async move {
            let token = format!("w{worker:02}");
            let body = assert_ok(
                call_ops(
                    &operations,
                    "UpdateItem",
                    json!({
                        "TableName": table, "Key": key,
                        "UpdateExpression": "SET l = list_append(l, :me)",
                        "ExpressionAttributeValues": {":me": {"L": [{"S": token}]}},
                        "ReturnValues": "UPDATED_NEW"
                    }),
                )
                .await,
            );
            (token, strings(&body["Attributes"]["l"]["L"]))
        }));
    }
    let mut results = Vec::new();
    for worker in workers {
        results.push(worker.await.unwrap());
    }

    let item = get(&store, &table, key).await.unwrap();
    let final_list = strings(&item["l"]["L"]);
    let mut tokens: Vec<String> = (0..WORKERS).map(|worker| format!("w{worker:02}")).collect();
    let mut sorted_final = final_list.clone();
    sorted_final.sort();
    tokens.sort();
    assert_eq!(sorted_final, tokens);
    let mut lengths: Vec<usize> = results.iter().map(|(_, post)| post.len()).collect();
    lengths.sort_unstable();
    assert_eq!(lengths, (1..=WORKERS).collect::<Vec<_>>());
    for (token, post) in &results {
        assert_eq!(post.last(), Some(token));
        assert_eq!(&final_list[..post.len()], post.as_slice());
    }
    store.finish().await;
}

/// The `S` members of a DynamoDB list value.
fn strings(list: &Value) -> Vec<String> {
    list.as_array()
        .unwrap()
        .iter()
        .map(|element| element["S"].as_str().unwrap().to_owned())
        .collect()
}

/// Retries `probe` until `done` holds, up to ten seconds; the failure message
/// names the awaited state.
async fn poll_until<F, Fut>(description: &str, mut probe: F) -> Value
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<Value>>,
{
    const POLL_BOUND: std::time::Duration = std::time::Duration::from_secs(10);
    let started = std::time::Instant::now();
    loop {
        if let Some(value) = probe().await {
            return value;
        }
        assert!(
            started.elapsed() < POLL_BOUND,
            "not reached within 10 s: {description}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// AWS behavioural:
/// check_ddb_eventual_and_strong_read_are_two_different_reads.py
///
/// Capacity units, the padding canary and the 4 KB billing arithmetic are not
/// ported (capacity is intentionally unsupported). The GSI refusal text is also
/// in query_cases.jsonl; this test adds that the refused read changes nothing.
#[tokio::test]
async fn given_sequential_writes_when_read_strong_and_eventual_then_strong_is_never_stale_nor_eventual_ahead()
 {
    let store = TestStore::new().await;
    let table = unique_table_name().as_ref().to_owned();
    assert_ok(
        call(
            &store,
            "CreateTable",
            json!({
                "TableName": table,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "gk", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gk-index",
                    "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await,
    );
    let key = json!({"pk": {"S": "small"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "small"}, "v": {"N": "0"}}),
        )
        .await,
    );

    for written in 1..=50_u64 {
        assert_ok(
            put(
                &store,
                &table,
                json!({"pk": {"S": "small"}, "v": {"N": written.to_string()}}),
            )
            .await,
        );
        let read = |consistent: bool| {
            call(
                &store,
                "GetItem",
                json!({"TableName": table, "Key": key, "ConsistentRead": consistent}),
            )
        };
        let strong = assert_ok(read(true).await);
        assert_eq!(
            strong["Item"]["v"],
            json!({"N": written.to_string()}),
            "stale strong read"
        );
        let eventual = assert_ok(read(false).await);
        let seen: u64 = eventual["Item"]["v"]["N"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            seen <= written,
            "eventual read returned {seen} after write {written}"
        );
    }
    for consistent in [true, false] {
        let miss = assert_ok(
            call(
                &store,
                "GetItem",
                json!({
                    "TableName": table, "Key": {"pk": {"S": "never-written-key"}},
                    "ConsistentRead": consistent
                }),
            )
            .await,
        );
        assert!(miss.get("Item").is_none(), "{miss}");
    }
    let base_query = |consistent: bool| {
        call(
            &store,
            "Query",
            json!({
                "TableName": table, "KeyConditionExpression": "pk = :p",
                "ExpressionAttributeValues": {":p": {"S": "small"}}, "ConsistentRead": consistent
            }),
        )
    };
    assert_eq!(assert_ok(base_query(true).await)["Count"], json!(1));
    assert_eq!(assert_ok(base_query(false).await)["Count"], json!(1));

    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "indexed"}, "gk": {"S": "g1"}}),
        )
        .await,
    );
    let gsi_query = |extra: Value| {
        let mut request = json!({
            "TableName": table, "IndexName": "gk-index",
            "KeyConditionExpression": "gk = :g",
            "ExpressionAttributeValues": {":g": {"S": "g1"}}
        });
        request
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        call(&store, "Query", request)
    };
    let before = poll_until("the indexed item appears in the GSI", || async {
        let body = assert_ok(gsi_query(json!({})).await);
        (body["Count"] == json!(1)).then_some(body)
    })
    .await;
    assert_validation(
        &gsi_query(json!({"ConsistentRead": true})).await,
        "Consistent reads are not supported on global secondary indexes",
    );
    let after = assert_ok(gsi_query(json!({})).await);
    assert_eq!(after["Items"], before["Items"]);
    assert_eq!(
        after["Items"],
        json!([{"pk": {"S": "indexed"}, "gk": {"S": "g1"}}])
    );
    store.finish().await;
}

fn nested_maps(levels: usize, leaf: &Value) -> Value {
    (0..levels).fold(leaf.clone(), |value, _| json!({"M": {"n": value}}))
}

fn nested_lists(levels: usize, leaf: &Value) -> Value {
    (0..levels).fold(leaf.clone(), |value, _| json!({"L": [value]}))
}

const NESTING_LIMIT_MESSAGE: &str = "1 validation error detected: Nesting Levels have exceeded \
                                     supported limits: Attributes in the item have nested levels \
                                     beyond supported limit";

/// AWS behavioural:
/// check_document_nesting_depth_32_levels_accepted_33_rejected.py
///
/// aux emits 'attribute nesting depth cannot exceed 32' today; the AWS text is
/// the expected value.
#[tokio::test]
async fn given_documents_nested_32_and_33_levels_when_written_then_32_stored_and_33_refused() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let leaf = json!({"S": "leaf"});

    for (name, build) in [
        ("m", nested_maps as fn(usize, &Value) -> Value),
        ("l", nested_lists as fn(usize, &Value) -> Value),
    ] {
        let accepted = format!("{name}32");
        assert_ok(
            put(
                &store,
                &table,
                json!({"pk": {"S": accepted}, "d": build(31, &leaf)}),
            )
            .await,
        );
        let stored = get(&store, &table, json!({"pk": {"S": accepted}}))
            .await
            .unwrap();
        assert_eq!(stored["d"], build(31, &leaf));
        let refused = format!("{name}33");
        assert_validation(
            &put(
                &store,
                &table,
                json!({"pk": {"S": refused}, "d": build(32, &leaf)}),
            )
            .await,
            NESTING_LIMIT_MESSAGE,
        );
        assert_eq!(
            get(&store, &table, json!({"pk": {"S": refused}})).await,
            None
        );
    }

    let deep = nested_maps(32, &leaf);
    assert_validation(
        &call(
            &store,
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "cond33"}, "d": leaf},
                "ConditionExpression": "attribute_not_exists(pk) OR :v = :v",
                "ExpressionAttributeValues": {":v": deep}
            }),
        )
        .await,
        NESTING_LIMIT_MESSAGE,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "cond33"}})).await,
        None
    );
    assert_validation(
        &call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": "m32"}}, "UpdateExpression": "SET e = :v",
                "ExpressionAttributeValues": {":v": deep}
            }),
        )
        .await,
        NESTING_LIMIT_MESSAGE,
    );
    let untouched = get(&store, &table, json!({"pk": {"S": "m32"}}))
        .await
        .unwrap();
    assert!(untouched.get("e").is_none(), "{untouched}");
    assert_eq!(untouched["d"], nested_maps(31, &leaf));
    store.finish().await;
}

/// AWS behavioural:
/// check_duplicate_set_members_in_condition_key_and_transact_rejected.py
#[tokio::test]
async fn given_duplicate_set_members_outside_items_when_requested_then_each_call_is_rejected_and_nothing_lands()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "item1"}, "other": {"S": "x"}}),
        )
        .await,
    );
    let invalid = "One or more parameter values were invalid: ";
    let counted = format!("1 validation error detected: {invalid}");
    let cases = [
        (
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "item1"}, "other": {"S": "y"}},
                "ConditionExpression": "other = :v",
                "ExpressionAttributeValues": {":v": {"SS": ["a", "a"]}}
            }),
            format!("{counted}Input collection [\"a\", \"a\"] contains duplicates."),
        ),
        (
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": "item1"}},
                "UpdateExpression": "SET other = :newval",
                "ConditionExpression": "contains(:v, other)",
                "ExpressionAttributeValues": {":v": {"NS": ["1", "1"]}, ":newval": {"S": "z"}}
            }),
            format!("{counted}Input collection contains duplicates."),
        ),
        (
            "GetItem",
            json!({"TableName": table, "Key": {"pk": {"SS": ["a", "a"]}}}),
            format!("{invalid}Input collection [\"a\", \"a\"] contains duplicates."),
        ),
        (
            "GetItem",
            json!({"TableName": table, "Key": {"pk": {"BS": ["AQ==", "AQ=="]}}}),
            format!("{invalid}Input collection [\"AQ==\",\"AQ==\"]of type BS contains duplicates."),
        ),
        (
            "DeleteItem",
            json!({"TableName": table, "Key": {"pk": {"SS": ["b", "b"]}}}),
            format!("{counted}Input collection [\"b\", \"b\"] contains duplicates."),
        ),
        (
            "BatchGetItem",
            json!({"RequestItems": {&table: {"Keys": [
                {"pk": {"S": "item1"}}, {"pk": {"NS": ["2", "2"]}}
            ]}}}),
            format!("{invalid}Input collection contains duplicates."),
        ),
        (
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": table, "Item": {"pk": {"S": "legit-put"}, "x": {"S": "1"}}}},
                {"Put": {"TableName": table, "Item": {"pk": {"S": "bad-put"}, "s": {"SS": ["d", "d"]}}}}
            ]}),
            format!("{invalid}Input collection [\"d\", \"d\"] contains duplicates."),
        ),
        (
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": table, "Item": {"pk": {"S": "legit-check"}, "x": {"S": "1"}}}},
                {"ConditionCheck": {
                    "TableName": table, "Key": {"pk": {"S": "item1"}},
                    "ConditionExpression": "contains(:v, other)",
                    "ExpressionAttributeValues": {":v": {"BS": ["Ag==", "Ag=="]}}
                }}
            ]}),
            format!("{invalid}Input collection [\"Ag==\",\"Ag==\"]of type BS contains duplicates."),
        ),
        (
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName": table, "Key": {"pk": {"S": "item1"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"SS": ["f", "f"]}}}}
            ]}),
            format!("{invalid}Input collection [\"f\", \"f\"] contains duplicates."),
        ),
    ];
    for (operation, request, message) in cases {
        assert_validation(&call(&store, operation, request).await, &message);
    }
    let scanned = assert_ok(
        call(
            &store,
            "Scan",
            json!({"TableName": table, "ConsistentRead": true}),
        )
        .await,
    );
    assert_eq!(
        scanned["Items"],
        json!([{"pk": {"S": "item1"}, "other": {"S": "x"}}])
    );
    store.finish().await;
}

/// A new on-demand table with the given `(name, type)` hash key and optional
/// range key.
async fn keyed_table(store: &TestStore, hash: (&str, &str), range: Option<(&str, &str)>) -> String {
    let table = unique_table_name().as_ref().to_owned();
    let mut schema = vec![json!({"AttributeName": hash.0, "KeyType": "HASH"})];
    let mut definitions = vec![json!({"AttributeName": hash.0, "AttributeType": hash.1})];
    if let Some((name, kind)) = range {
        schema.push(json!({"AttributeName": name, "KeyType": "RANGE"}));
        definitions.push(json!({"AttributeName": name, "AttributeType": kind}));
    }
    assert_ok(
        call(
            store,
            "CreateTable",
            json!({
                "TableName": table, "KeySchema": schema, "AttributeDefinitions": definitions,
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await,
    );
    table
}

/// AWS behavioural: check_e2e_optimistic_locking.py
#[tokio::test]
async fn given_versioned_item_when_updates_race_on_version_then_stale_and_deleted_updates_fail() {
    let store = TestStore::new().await;
    let table = keyed_table(&store, ("id", "S"), None).await;
    let key = json!({"id": {"S": "doc-1"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"id": {"S": "doc-1"}, "version": {"N": "1"}, "data": {"S": "initial"}}),
        )
        .await,
    );
    let bump = |expected: &str, data: &str| {
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key,
                "UpdateExpression": "SET version = version + :inc, #d = :data",
                "ConditionExpression": "version = :v",
                "ExpressionAttributeNames": {"#d": "data"},
                "ExpressionAttributeValues": {
                    ":inc": {"N": "1"}, ":v": {"N": expected}, ":data": {"S": data}
                }
            }),
        )
    };

    assert_ok(bump("1", "updated-v2").await);
    let item = get(&store, &table, key.clone()).await.unwrap();
    assert_eq!(
        (&item["version"], &item["data"]),
        (&json!({"N": "2"}), &json!({"S": "updated-v2"}))
    );
    assert_condition_failed(&bump("1", "stale-update").await);
    let item = get(&store, &table, key.clone()).await.unwrap();
    assert_eq!(
        (&item["version"], &item["data"]),
        (&json!({"N": "2"}), &json!({"S": "updated-v2"}))
    );
    assert_ok(bump("2", "updated-v3").await);
    let item = get(&store, &table, key.clone()).await.unwrap();
    assert_eq!(
        (&item["version"], &item["data"]),
        (&json!({"N": "3"}), &json!({"S": "updated-v3"}))
    );

    assert_ok(
        call(
            &store,
            "DeleteItem",
            json!({"TableName": table, "Key": key}),
        )
        .await,
    );
    assert_eq!(get(&store, &table, key.clone()).await, None);
    let query = assert_ok(
        call(
            &store,
            "Query",
            json!({
                "TableName": table, "KeyConditionExpression": "id = :id",
                "ExpressionAttributeValues": {":id": {"S": "doc-1"}}, "ConsistentRead": true
            }),
        )
        .await,
    );
    assert_eq!(query["Count"], json!(0));
    let resurrect = call(
        &store,
        "UpdateItem",
        json!({
            "TableName": table, "Key": key, "UpdateExpression": "SET #d = :data",
            "ConditionExpression": "attribute_exists(id)",
            "ExpressionAttributeNames": {"#d": "data"},
            "ExpressionAttributeValues": {":data": {"S": "resurrected"}}
        }),
    )
    .await;
    assert_condition_failed(&resurrect);
    assert_eq!(get(&store, &table, key).await, None);
    store.finish().await;
}

/// AWS behavioural: check_e2e_shopping_cart_workflow.py
#[tokio::test]
async fn given_cart_rows_when_batch_written_updated_and_deleted_then_query_counts_follow() {
    let store = TestStore::new().await;
    let table = keyed_table(&store, ("user_id", "S"), Some(("item_id", "S"))).await;
    let requests: Vec<Value> = (0..5)
        .map(|index| {
            json!({"PutRequest": {"Item": {
                "user_id": {"S": "user-1"}, "item_id": {"S": format!("item-{index:03}")},
                "quantity": {"N": "1"}, "name": {"S": format!("Product {index}")}
            }}})
        })
        .collect();
    assert_ok(
        call(
            &store,
            "BatchWriteItem",
            json!({"RequestItems": {&table: requests}}),
        )
        .await,
    );
    let count = || async {
        assert_ok(
            call(
                &store,
                "Query",
                json!({
                    "TableName": table, "KeyConditionExpression": "user_id = :uid",
                    "ExpressionAttributeValues": {":uid": {"S": "user-1"}}, "ConsistentRead": true
                }),
            )
            .await,
        )["Count"]
            .clone()
    };
    assert_eq!(count().await, json!(5));

    let key = |item: &str| json!({"user_id": {"S": "user-1"}, "item_id": {"S": item}});
    assert_ok(
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key("item-002"), "UpdateExpression": "SET quantity = :q",
                "ExpressionAttributeValues": {":q": {"N": "3"}}
            }),
        )
        .await,
    );
    assert_ok(
        call(
            &store,
            "DeleteItem",
            json!({"TableName": table, "Key": key("item-004")}),
        )
        .await,
    );
    assert_eq!(count().await, json!(4));
    assert_eq!(
        get(&store, &table, key("item-002")).await.unwrap()["quantity"],
        json!({"N": "3"})
    );
    assert_eq!(get(&store, &table, key("item-004")).await, None);
    let refused = call(
        &store,
        "UpdateItem",
        json!({
            "TableName": table, "Key": key("item-004"), "UpdateExpression": "SET quantity = :q",
            "ConditionExpression": "attribute_exists(item_id)",
            "ExpressionAttributeValues": {":q": {"N": "9"}}
        }),
    )
    .await;
    assert_condition_failed(&refused);
    store.finish().await;
}

/// AWS behavioural:
/// check_empty_and_duplicate_set_members_rejected_for_ss_ns_and_bs.py
#[tokio::test]
async fn given_empty_or_duplicate_set_members_when_put_then_rejected_and_legal_sets_round_trip() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    for (key, value) in [
        ("ok-ss", json!({"SS": ["a", "b"]})),
        ("ok-ns", json!({"NS": ["1", "2"]})),
        ("ok-bs", json!({"BS": ["YQ==", "Yg=="]})),
    ] {
        assert_ok(put(&store, &table, json!({"pk": {"S": key}, "s": value})).await);
        let stored = get(&store, &table, json!({"pk": {"S": key}}))
            .await
            .unwrap();
        let set = stored["s"].as_object().unwrap().values().next().unwrap();
        assert_eq!(set.as_array().unwrap().len(), 2);
    }

    let invalid = "1 validation error detected: One or more parameter values were invalid: ";
    let rejections = [
        (
            "ss-empty",
            json!({"SS": []}),
            format!("{invalid}An string set  may not be empty"),
        ),
        (
            "ss-dup",
            json!({"SS": ["a", "a"]}),
            format!("{invalid}Input collection [\"a\", \"a\"] contains duplicates."),
        ),
        (
            "ns-empty",
            json!({"NS": []}),
            format!("{invalid}An number set  may not be empty"),
        ),
        (
            "ns-dup",
            json!({"NS": ["1", "1"]}),
            format!("{invalid}Input collection contains duplicates."),
        ),
        (
            "ns-dup-canonical",
            json!({"NS": ["1", "1.0"]}),
            format!("{invalid}Input collection contains duplicates."),
        ),
        (
            "bs-empty",
            json!({"BS": []}),
            format!("{invalid}Binary sets should not be empty"),
        ),
        (
            "bs-dup",
            json!({"BS": ["YQ==", "YQ=="]}),
            format!("{invalid}Input collection [\"YQ==\",\"YQ==\"]of type BS contains duplicates."),
        ),
    ];
    for (key, set, message) in &rejections {
        assert_validation(
            &put(&store, &table, json!({"pk": {"S": key}, "s": set})).await,
            message,
        );
        assert_eq!(
            get(&store, &table, json!({"pk": {"S": key}})).await,
            None,
            "{key} stored"
        );
    }
    let nested = put(
        &store,
        &table,
        json!({"pk": {"S": "ss-dup-in-m"}, "m": {"M": {"s": {"SS": ["a", "a"]}}}}),
    )
    .await;
    assert_validation(
        &nested,
        &format!("{invalid}Input collection [\"a\", \"a\"] contains duplicates."),
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "ss-dup-in-m"}})).await,
        None
    );

    let condition = call(
        &store,
        "PutItem",
        json!({
            "TableName": table, "Item": {"pk": {"S": "ok-ss"}, "s": {"SS": ["c", "d"]}},
            "ConditionExpression": "contains(:dup, :needle)",
            "ExpressionAttributeValues": {":dup": {"SS": ["z", "z"]}, ":needle": {"S": "z"}}
        }),
    )
    .await;
    assert_validation(
        &condition,
        &format!("{invalid}Input collection [\"z\", \"z\"] contains duplicates."),
    );
    let unchanged = get(&store, &table, json!({"pk": {"S": "ok-ss"}}))
        .await
        .unwrap();
    assert_eq!(unchanged["s"]["SS"].as_array().unwrap().len(), 2);
    let key_reply = call(
        &store,
        "GetItem",
        json!({"TableName": table, "Key": {"pk": {"SS": ["a", "a"]}}}),
    )
    .await;
    assert_validation(
        &key_reply,
        "One or more parameter values were invalid: Input collection [\"a\", \"a\"] contains \
         duplicates.",
    );

    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "ss-empty-member"}, "s": {"SS": ["", "a"]}}),
        )
        .await,
    );
    let stored = get(&store, &table, json!({"pk": {"S": "ss-empty-member"}}))
        .await
        .unwrap();
    let mut members = strings_of(&stored["s"]["SS"]);
    members.sort();
    assert_eq!(members, ["", "a"]);
    assert_eq!(scan_count(&store, &table).await, 4);
    store.finish().await;
}

/// The string elements of a JSON array.
fn strings_of(array: &Value) -> Vec<String> {
    array
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_owned())
        .collect()
}

/// AWS behavioural: check_empty_string_and_binary_base_table_keys_rejected.py
#[tokio::test]
async fn given_empty_string_or_binary_partition_key_when_any_write_or_read_runs_then_validation_names_the_key()
 {
    let store = TestStore::new().await;
    let string_table = keyed_table(&store, ("pk", "S"), None).await;
    let binary_table = keyed_table(&store, ("pk", "B"), None).await;
    assert_ok(
        put(
            &store,
            &string_table,
            json!({"pk": {"S": "ok"}, "v": {"S": "s"}}),
        )
        .await,
    );
    assert_ok(
        put(
            &store,
            &binary_table,
            json!({"pk": {"B": "b2s="}, "v": {"S": "b"}}),
        )
        .await,
    );

    let prefix = "One or more parameter values are not valid. The AttributeValue for a key \
                  attribute cannot contain an empty";
    for (table, empty, good, message) in [
        (
            &string_table,
            json!({"S": ""}),
            json!({"S": "batch-good"}),
            format!("{prefix} string value. Key: pk"),
        ),
        (
            &binary_table,
            json!({"B": ""}),
            json!({"B": "YmF0Y2gtZ29vZA=="}),
            format!("{prefix} binary value. Key: pk"),
        ),
    ] {
        let key = json!({"pk": empty});
        let replies = [
            put(&store, table, json!({"pk": empty, "v": {"S": "x"}})).await,
            call(&store, "GetItem", json!({"TableName": table, "Key": key})).await,
            call(
                &store,
                "DeleteItem",
                json!({"TableName": table, "Key": key}),
            )
            .await,
            call(
                &store,
                "UpdateItem",
                json!({
                    "TableName": table, "Key": key, "UpdateExpression": "SET v = :v",
                    "ExpressionAttributeValues": {":v": {"S": "x"}}
                }),
            )
            .await,
            call(
                &store,
                "BatchWriteItem",
                json!({"RequestItems": {table: [
                    {"PutRequest": {"Item": {"pk": good}}},
                    {"PutRequest": {"Item": {"pk": empty, "v": {"S": "x"}}}}
                ]}}),
            )
            .await,
            call(
                &store,
                "BatchWriteItem",
                json!({"RequestItems": {table: [
                    {"PutRequest": {"Item": {"pk": good}}},
                    {"DeleteRequest": {"Key": {"pk": empty}}}
                ]}}),
            )
            .await,
        ];
        for reply in &replies {
            assert_validation(reply, &message);
        }
        assert_eq!(get(&store, table, json!({"pk": good})).await, None);
        assert_eq!(scan_count(&store, table).await, 1);
    }
    store.finish().await;
}

/// Runs each `(operation, request, message)` and asserts a ValidationException
/// with exactly that message.
async fn assert_validations(store: &TestStore, cases: Vec<(&str, Value, String)>) {
    for (operation, request, message) in cases {
        let reply = call(store, operation, request.clone()).await;
        assert_eq!(reply.0, 400, "{operation} {request}: {}", reply.1);
        assert_validation(&reply, &message);
    }
}

/// AWS behavioural: check_empty_string_expression_rejected_across_operations.py
#[tokio::test]
async fn given_empty_string_expression_when_any_operation_runs_then_validation_rejects_and_item_is_unchanged()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "item-1"}});
    let original = json!({"pk": {"S": "item-1"}, "color": {"S": "red"}});
    assert_ok(put(&store, &table, original.clone()).await);

    let counted = "1 validation error detected: Invalid";
    assert_validations(
        &store,
        vec![
            (
                "PutItem",
                json!({
                    "TableName": table, "Item": {"pk": {"S": "item-1"}, "color": {"S": "blue"}},
                    "ConditionExpression": ""
                }),
                format!("{counted} ConditionExpression: The expression can not be empty;"),
            ),
            (
                "DeleteItem",
                json!({"TableName": table, "Key": key, "ConditionExpression": ""}),
                format!("{counted} ConditionExpression: The expression can not be empty;"),
            ),
            (
                "UpdateItem",
                json!({"TableName": table, "Key": key, "UpdateExpression": ""}),
                format!("{counted} UpdateExpression: The expression can not be empty;"),
            ),
            (
                "Query",
                json!({"TableName": table, "KeyConditionExpression": ""}),
                "Invalid KeyConditionExpression: The expression can not be empty;".to_owned(),
            ),
            (
                "Scan",
                json!({"TableName": table, "FilterExpression": ""}),
                "Invalid FilterExpression: The expression can not be empty;".to_owned(),
            ),
            (
                "GetItem",
                json!({"TableName": table, "Key": key, "ProjectionExpression": ""}),
                "Invalid ProjectionExpression: The expression can not be empty;".to_owned(),
            ),
        ],
    )
    .await;
    assert_eq!(get(&store, &table, key).await, Some(original));
    store.finish().await;
}

/// AWS behavioural:
/// check_expression_attribute_map_key_syntax_and_length_rejected.py
#[tokio::test]
async fn given_malformed_expression_attribute_map_keys_when_get_or_scan_runs_then_validation_names_the_key()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "k1"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "k1"}, "foo": {"S": "a"}, "bar": {"S": "b"}}),
        )
        .await,
    );
    let get_with = |projection: &str, names: Value| {
        (
            "GetItem",
            json!({
                "TableName": table, "Key": key, "ProjectionExpression": projection,
                "ExpressionAttributeNames": names, "ConsistentRead": true
            }),
        )
    };
    let scan_with = |filter: &str, values: Value| {
        (
            "Scan",
            json!({
                "TableName": table, "FilterExpression": filter,
                "ExpressionAttributeValues": values
            }),
        )
    };
    let long_name = format!("#{}", "a".repeat(255));
    let long_bad_name = format!("#{}-x", "a".repeat(255));
    let long_value = format!(":{}", "a".repeat(255));
    let syntax = |kind: &str, key: &str| {
        format!("Expression{kind} contains invalid key: Syntax error; key: \"{key}\"")
    };
    let too_long = |size: usize| {
        format!(
            "ExpressionAttributeNames contains invalid key: The expression attribute map contains \
             a key that is too long; size of key: {size}"
        )
    };
    let cases = vec![
        (
            get_with("#", json!({"#": "foo"})),
            syntax("AttributeNames", "#"),
        ),
        (
            get_with("#a", json!({"#a-b": "foo"})),
            syntax("AttributeNames", "#a-b"),
        ),
        (
            get_with("#a", json!({long_name.clone(): "foo"})),
            too_long(256),
        ),
        (
            get_with("#a", json!({long_bad_name.clone(): "foo"})),
            too_long(258),
        ),
        (
            get_with("#a", json!({"#a": ""})),
            "ExpressionAttributeNames contains invalid value: Empty attribute name for key #a"
                .to_owned(),
        ),
        (
            scan_with("foo = :", json!({":": {"S": "x"}})),
            syntax("AttributeValues", ":"),
        ),
        (
            scan_with("foo = :v", json!({":a-b": {"S": "x"}})),
            syntax("AttributeValues", ":a-b"),
        ),
        (
            scan_with("foo = :v", json!({long_value.clone(): {"S": "x"}})),
            "ExpressionAttributeValues contains invalid key: The expression attribute map \
             contains a key that is too long; size of key: 256"
                .to_owned(),
        ),
    ];
    assert_validations(
        &store,
        cases
            .into_iter()
            .map(|((operation, request), message)| (operation, request, message))
            .collect(),
    )
    .await;

    let projected = assert_ok(
        call(
            &store,
            "GetItem",
            json!({
                "TableName": table, "Key": key, "ProjectionExpression": "#a_1",
                "ExpressionAttributeNames": {"#a_1": "foo"}, "ConsistentRead": true
            }),
        )
        .await,
    );
    assert_eq!(projected["Item"], json!({"foo": {"S": "a"}}));
    let scanned = assert_ok(
        call(
            &store,
            "Scan",
            json!({
                "TableName": table, "FilterExpression": "foo = :v_1",
                "ExpressionAttributeValues": {":v_1": {"S": "a"}}, "ConsistentRead": true
            }),
        )
        .await,
    );
    assert_eq!(scanned["Count"], json!(1));
    store.finish().await;
}

/// AWS behavioural:
/// check_expression_attribute_placeholders_without_an_expression_rejected.py
#[tokio::test]
async fn given_placeholder_maps_without_matching_expressions_when_requested_then_validation_rejects_each()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "item-1"}});
    let original = json!({"pk": {"S": "item-1"}, "color": {"S": "red"}});
    assert_ok(put(&store, &table, original.clone()).await);

    let counted = "1 validation error detected: ";
    let no_values = "ExpressionAttributeValues can only be specified when using expressions";
    let no_names = "ExpressionAttributeNames can only be specified when using expressions";
    let empty_values = "ExpressionAttributeValues must not be empty";
    let empty_names = "ExpressionAttributeNames must not be empty";
    let cases = vec![
        (
            "PutItem",
            json!({"TableName": table, "Item": {"pk": {"S": "new-eav"}}, "ExpressionAttributeValues": {":v": {"S": "x"}}}),
            format!("{counted}{no_values}"),
        ),
        (
            "PutItem",
            json!({"TableName": table, "Item": {"pk": {"S": "new-ean"}}, "ExpressionAttributeNames": {"#n": "n"}}),
            format!("{counted}{no_names}"),
        ),
        (
            "DeleteItem",
            json!({"TableName": table, "Key": key, "ExpressionAttributeValues": {":v": {"S": "x"}}}),
            format!("{counted}{no_values}"),
        ),
        (
            "DeleteItem",
            json!({"TableName": table, "Key": key, "ExpressionAttributeNames": {"#n": "n"}}),
            format!("{counted}{no_names}"),
        ),
        (
            "GetItem",
            json!({"TableName": table, "Key": key, "ExpressionAttributeNames": {"#n": "n"}}),
            no_names.to_owned(),
        ),
        (
            "Scan",
            json!({"TableName": table, "ExpressionAttributeValues": {":v": {"S": "x"}}}),
            no_values.to_owned(),
        ),
        (
            "Scan",
            json!({"TableName": table, "ExpressionAttributeNames": {"#n": "n"}}),
            no_names.to_owned(),
        ),
        (
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "new-emptyeav"}},
                "ConditionExpression": "attribute_not_exists(pk)", "ExpressionAttributeValues": {}
            }),
            format!("{counted}{empty_values}"),
        ),
        (
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "new-emptyean"}},
                "ConditionExpression": "attribute_not_exists(pk)", "ExpressionAttributeNames": {}
            }),
            format!("{counted}{empty_names}"),
        ),
        (
            "DeleteItem",
            json!({
                "TableName": table, "Key": key,
                "ConditionExpression": "attribute_exists(pk)", "ExpressionAttributeValues": {}
            }),
            format!("{counted}{empty_values}"),
        ),
        (
            "GetItem",
            json!({
                "TableName": table, "Key": key,
                "ProjectionExpression": "pk", "ExpressionAttributeNames": {}
            }),
            empty_names.to_owned(),
        ),
        (
            "Scan",
            json!({
                "TableName": table,
                "FilterExpression": "attribute_exists(pk)", "ExpressionAttributeValues": {}
            }),
            empty_values.to_owned(),
        ),
        (
            "Scan",
            json!({
                "TableName": table,
                "FilterExpression": "attribute_exists(pk)", "ExpressionAttributeNames": {}
            }),
            empty_names.to_owned(),
        ),
    ];
    assert_validations(&store, cases).await;

    let scanned = assert_ok(
        call(
            &store,
            "Scan",
            json!({"TableName": table, "ConsistentRead": true}),
        )
        .await,
    );
    assert_eq!(scanned["Items"], json!([original]));
    assert_ok(
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "new-ok"}},
                "ConditionExpression": "attribute_not_exists(#p)",
                "ExpressionAttributeNames": {"#p": "pk"}
            }),
        )
        .await,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "new-ok"}})).await,
        Some(json!({"pk": {"S": "new-ok"}}))
    );
    store.finish().await;
}

/// AWS behavioural: check_gsi_query_attributes_to_get_validation.py
///
/// The script's legacy `KeyConditions` Query cannot be translated (legacy
/// KeyConditions is rejected, and `AttributesToGet` beside
/// `KeyConditionExpression` is a validation error), so the same projection
/// checks run on a Scan of the index.
#[tokio::test]
async fn given_include_gsi_when_query_attributes_to_get_is_invalid_then_each_form_is_rejected() {
    let store = TestStore::new().await;
    let table = unique_table_name().as_ref().to_owned();
    assert_ok(
        call(
            &store,
            "CreateTable",
            json!({
                "TableName": table,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "gsipk", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gsi1",
                    "KeySchema": [{"AttributeName": "gsipk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["kept"]}
                }],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await,
    );
    assert_ok(
        put(
            &store,
            &table,
            json!({
                "pk": {"S": "ITEM#1"}, "sk": {"S": "S1"}, "gsipk": {"S": "BUCKET#A"},
                "kept": {"S": "kept-value"}, "dropped": {"S": "excluded-value"}
            }),
        )
        .await,
    );
    let query = |attributes: Value| {
        call(
            &store,
            "Scan",
            json!({
                "TableName": table, "IndexName": "gsi1",
                "AttributesToGet": attributes
            }),
        )
    };

    let control = poll_until("the projected value appears in gsi1", || async {
        let body = assert_ok(query(json!(["kept"])).await);
        (body["Count"] == json!(1)).then_some(body)
    })
    .await;
    assert_eq!(control["Items"], json!([{"kept": {"S": "kept-value"}}]));
    assert_validation(
        &query(json!([])).await,
        "1 validation error detected: Value at 'AttributesToGet' failed to satisfy constraint: \
         Member must have length greater than or equal to 1",
    );
    assert_validation(
        &query(json!(["kept", "kept"])).await,
        "One or more parameter values were invalid: Duplicate value in attribute name: kept",
    );
    assert_validation(
        &query(json!(["dropped"])).await,
        "One or more parameter values were invalid: Global secondary index gsi1 does not project \
         [dropped]",
    );
    assert_eq!(scan_count(&store, &table).await, 1);
    store.finish().await;
}

/// An item whose `pk` and the named string attributes sum to exactly
/// `total_bytes`, counting every name and value; pseudo-random payloads.
fn item_of_size(pk: &str, names: &[String], total_bytes: usize) -> Value {
    let fixed = "pk".len() + pk.len() + names.iter().map(String::len).sum::<usize>();
    let payload = total_bytes - fixed;
    let (base, extra) = (payload / names.len(), payload % names.len());
    let mut item = serde_json::Map::new();
    item.insert("pk".to_owned(), json!({"S": pk}));
    for (index, name) in names.iter().enumerate() {
        let length = base + if index == 0 { extra } else { 0 };
        item.insert(
            name.clone(),
            json!({"S": random_text(index as u64 + 7, length)}),
        );
    }
    Value::Object(item)
}

/// A unique attribute name of exactly 27 characters.
fn long_attribute_name(index: usize) -> String {
    let prefix = format!("payload-attr-{index:02}-");
    format!("{prefix}{}", "n".repeat(27 - prefix.len()))
}

/// Asserts `PutItem` and `BatchWriteItem` accept `at_limit` whole and refuse
/// `over_limit` for size, storing nothing for the latter.
async fn assert_put_and_batch_size_gate(
    store: &TestStore,
    table: &str,
    at_limit: [Value; 2],
    over_limit: [Value; 2],
) {
    let [put_at, batch_at] = at_limit;
    let [put_over, batch_over] = over_limit;
    assert_ok(put(store, table, put_at.clone()).await);
    // A number is stored in canonical spelling, which has the same cost.
    let stored = |item: &Value| {
        let mut stored = item.clone();
        if let Some(literal) = item["n"]["N"].as_str() {
            stored["n"]["N"] = json!(storage_types::canonical_dynamodb_number(literal).unwrap());
        }
        stored
    };
    assert_eq!(
        get(store, table, json!({"pk": put_at["pk"]})).await,
        Some(stored(&put_at))
    );
    let body = assert_ok(
        call(
            store,
            "BatchWriteItem",
            json!({"RequestItems": {table: [{"PutRequest": {"Item": batch_at.clone()}}]}}),
        )
        .await,
    );
    assert_eq!(body["UnprocessedItems"], json!({}));
    assert_eq!(
        get(store, table, json!({"pk": batch_at["pk"]})).await,
        Some(stored(&batch_at))
    );
    assert_validation(&put(store, table, put_over.clone()).await, ITEM_TOO_LARGE);
    assert_eq!(get(store, table, json!({"pk": put_over["pk"]})).await, None);
    let reply = call(
        store,
        "BatchWriteItem",
        json!({"RequestItems": {table: [{"PutRequest": {"Item": batch_over.clone()}}]}}),
    )
    .await;
    assert_validation(&reply, ITEM_TOO_LARGE);
    assert_eq!(
        get(store, table, json!({"pk": batch_over["pk"]})).await,
        None
    );
}

/// AWS behavioural:
/// check_item_size_boundary_across_multiple_long_attribute_names.py
#[tokio::test]
async fn given_long_attribute_names_when_item_is_at_and_over_limit_then_names_count_toward_size() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let names_2: Vec<String> = (0..2).map(long_attribute_name).collect();
    let names_10: Vec<String> = (0..10).map(long_attribute_name).collect();
    // Same names, each path at the limit and one byte over; the script runs
    // PutItem with 2 attributes and BatchWriteItem with 10, this covers both.
    assert_put_and_batch_size_gate(
        &store,
        &table,
        [
            item_of_size("put-2attr-at-limit", &names_2, ITEM_LIMIT_BYTES),
            item_of_size("batch-10attr-at-limit", &names_10, ITEM_LIMIT_BYTES),
        ],
        [
            item_of_size("put-2attr-over-limit", &names_2, ITEM_LIMIT_BYTES + 1),
            item_of_size("batch-10attr-over-limit", &names_10, ITEM_LIMIT_BYTES + 1),
        ],
    )
    .await;
    store.finish().await;
}

/// AWS behavioural: check_item_size_limit_counts_list_element_overhead.py
///
/// The CapacityUnits == 381.0 assertion is dropped (capacity is zero-valued by
/// design).
#[tokio::test]
async fn given_long_lists_of_short_strings_when_put_then_element_overhead_counts_toward_size() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let list = |elements: usize| json!({"L": vec![json!({"S": "aa"}); elements]});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "under"}, "data": list(130_000)}),
        )
        .await,
    );
    let stored = get(&store, &table, json!({"pk": {"S": "under"}}))
        .await
        .unwrap();
    assert_eq!(stored["data"]["L"].as_array().unwrap().len(), 130_000);
    assert_validation(
        &put(
            &store,
            &table,
            json!({"pk": {"S": "over"}, "data": list(150_000)}),
        )
        .await,
        ITEM_TOO_LARGE,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "over"}})).await,
        None
    );
    store.finish().await;
}

/// AWS behavioural: check_item_size_limit_enforced_at_400kb.py and
/// check_item_size_limit_is_aggregate_not_per_attribute_rejected.py (one copy
/// of the shared aggregate case).
#[tokio::test]
async fn given_items_around_400kb_when_put_or_grown_by_update_then_aggregate_size_is_enforced() {
    const KB: usize = 1024;
    let store = TestStore::new().await;
    let table = pk_table(&store).await;

    let under = random_text(1, 380 * KB);
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "under"}, "blob": {"S": under}}),
        )
        .await,
    );
    let stored = get(&store, &table, json!({"pk": {"S": "under"}}))
        .await
        .unwrap();
    assert_eq!(stored["blob"]["S"].as_str().unwrap(), under);

    assert_validation(
        &put(
            &store,
            &table,
            json!({"pk": {"S": "over"}, "blob": {"S": random_text(2, 410 * KB)}}),
        )
        .await,
        ITEM_TOO_LARGE,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "over"}})).await,
        None
    );

    let spread = |pk: &str, attributes: u64| {
        let mut item = serde_json::Map::new();
        item.insert("pk".to_owned(), json!({"S": pk}));
        for index in 0..attributes {
            item.insert(
                format!("a{index}"),
                json!({"S": random_text(index + 10, 40 * KB)}),
            );
        }
        Value::Object(item)
    };
    assert_validation(
        &put(&store, &table, spread("spread", 11)).await,
        ITEM_TOO_LARGE,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "spread"}})).await,
        None
    );
    assert_ok(put(&store, &table, spread("few", 9)).await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "few"}})).await,
        Some(spread("few", 9))
    );

    let grown = random_text(3, 390 * KB);
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "grow"}, "blob": {"S": grown}}),
        )
        .await,
    );
    let update = call(
        &store,
        "UpdateItem",
        json!({
            "TableName": table, "Key": {"pk": {"S": "grow"}}, "UpdateExpression": "SET extra = :v",
            "ExpressionAttributeValues": {":v": {"S": random_text(4, 30 * KB)}}
        }),
    )
    .await;
    assert_validation(
        &update,
        "Item size to update has exceeded the maximum allowed size",
    );
    let untouched = get(&store, &table, json!({"pk": {"S": "grow"}}))
        .await
        .unwrap();
    assert!(untouched.get("extra").is_none());
    assert_eq!(untouched["blob"]["S"].as_str().unwrap(), grown);
    store.finish().await;
}

/// AWS behavioural:
/// check_malformed_n_in_expression_value_rejected_before_evaluation.py
///
/// The legacy `Expected` leg is dropped (rejected by design, dynamodb_parity.md
/// PutItem row).
#[tokio::test]
async fn given_malformed_number_in_expression_value_when_condition_would_fail_then_validation_wins()
{
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "item"}});
    let original = json!({"pk": {"S": "item"}, "v": {"N": "5"}, "note": {"S": "orig"}});
    assert_ok(put(&store, &table, original.clone()).await);

    let put_with_condition = |value: &str| {
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table,
                "Item": {"pk": {"S": "item"}, "v": {"N": "99"}, "note": {"S": "changed"}},
                "ConditionExpression": "v = :cond", "ExpressionAttributeValues": {":cond": {"N": value}}
            }),
        )
    };
    assert_condition_failed(&put_with_condition("7").await);
    assert_eq!(
        get(&store, &table, key.clone()).await,
        Some(original.clone())
    );
    for malformed in ["abc", "", "1e99999"] {
        let message = if malformed == "1e99999" {
            "1 validation error detected: Number overflow. Attempting to store a number with \
             magnitude larger than supported range"
                .to_owned()
        } else {
            format!(
                "1 validation error detected: The parameter cannot be converted to a numeric \
                 value: {malformed}"
            )
        };
        assert_validation(&put_with_condition(malformed).await, &message);
        assert_eq!(
            get(&store, &table, key.clone()).await,
            Some(original.clone())
        );
        let update = call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET note = :val",
                "ConditionExpression": "v = :cond",
                "ExpressionAttributeValues": {":val": {"N": malformed}, ":cond": {"N": "7"}}
            }),
        )
        .await;
        assert_validation(&update, &message);
        assert_eq!(
            get(&store, &table, key.clone()).await,
            Some(original.clone())
        );
    }
    assert_ok(
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "SET note = :val",
                "ExpressionAttributeValues": {":val": {"S": "final"}}
            }),
        )
        .await,
    );
    assert_eq!(
        get(&store, &table, key).await.unwrap()["note"],
        json!({"S": "final"})
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_multiple_simultaneous_constraint_violations_rejected.py
#[tokio::test]
async fn given_several_constraint_violations_when_requested_then_one_validation_names_each_in_fixed_order()
 {
    let store = TestStore::new().await;
    assert_validation(
        &call(
            &store,
            "UpdateItem",
            json!({"TableName": "", "ReturnConsumedCapacity": "BOGUS", "ReturnValues": "BOGUS2"}),
        )
        .await,
        "4 validation errors detected: Value 'BOGUS' at 'returnConsumedCapacity' failed to \
         satisfy constraint: Member must satisfy enum value set: [INDEXES, TOTAL, NONE]; Value '' \
         at 'tableName' failed to satisfy constraint: Member must have length greater than or \
         equal to 1; Value 'BOGUS2' at 'returnValues' failed to satisfy constraint: Member must \
         satisfy enum value set: [ALL_NEW, UPDATED_OLD, ALL_OLD, NONE, UPDATED_NEW]; Value null \
         at 'key' failed to satisfy constraint: Member must not be null",
    );

    let broken = unique_table_name().as_ref().to_owned();
    let create = call(
        &store,
        "CreateTable",
        json!({
            "TableName": broken,
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "ab",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "cd",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    assert_validation(
        &create,
        "2 validation errors detected: Value 'ab' at 'globalSecondaryIndexes.1.member.indexName' \
         failed to satisfy constraint: Member must have length greater than or equal to 3; Value \
         'cd' at 'localSecondaryIndexes.1.member.indexName' failed to satisfy constraint: Member \
         must have length greater than or equal to 3",
    );
    let described = call(&store, "DescribeTable", json!({"TableName": broken})).await;
    assert!(
        described.1["__type"]
            .as_str()
            .unwrap()
            .ends_with("#ResourceNotFoundException"),
        "{}",
        described.1
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_nesting_depth_error_precedence_across_malformed_requests.py
///
/// aux emits 'attribute nesting depth cannot exceed 32' today; the AWS text is
/// the expected value, and the precedence order is AWS's.
#[tokio::test]
async fn given_over_deep_document_with_second_defect_when_requested_then_nesting_error_masks_only_syntax_and_unused_value()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let deep = nested_maps(39, &json!({"S": "leaf"}));
    let shallow = json!({"S": "leaf"});
    assert_ok(put(&store, &table, json!({"pk": {"S": "ctrl"}, "doc": shallow})).await);

    let return_values = "1 validation error detected: Failed to satisfy constraint: Member must \
                         satisfy enum value set: [ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATE_NEW, NONE]";
    let empty_name = "1 validation error detected: ExpressionAttributeNames contains invalid \
                      value: Empty attribute name for key #x";
    let syntax = "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \
                  \"<EOF>\", near: \"(\"";
    let unused = "1 validation error detected: Value provided in ExpressionAttributeValues unused \
                  in expressions: keys: {:unused}";
    let mut cases = Vec::new();
    for (key, doc) in [("a-combo", &deep), ("a-solo", &shallow)] {
        cases.push((
            "PutItem",
            json!({"TableName": table, "Item": {"pk": {"S": key}, "doc": doc}, "ReturnValues": "BOGUS_VALUE"}),
            return_values.to_owned(),
        ));
    }
    for (key, doc) in [("b-combo", &deep), ("b-solo", &shallow)] {
        cases.push((
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": key}}, "UpdateExpression": "SET #x = :v",
                "ExpressionAttributeNames": {"#x": ""}, "ExpressionAttributeValues": {":v": doc}
            }),
            empty_name.to_owned(),
        ));
    }
    for (key, doc, message) in [
        ("c-combo", &deep, NESTING_LIMIT_MESSAGE),
        ("c-solo", &shallow, syntax),
    ] {
        cases.push((
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": key}}, "UpdateExpression": "SET doc = :v",
                "ConditionExpression": "attribute_exists(", "ExpressionAttributeValues": {":v": doc}
            }),
            message.to_owned(),
        ));
    }
    for (key, doc, message) in [
        ("d-combo", &deep, NESTING_LIMIT_MESSAGE),
        ("d-solo", &shallow, unused),
    ] {
        cases.push((
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": key}}, "UpdateExpression": "SET doc = :v",
                "ExpressionAttributeValues": {":v": doc, ":unused": {"S": "x"}}
            }),
            message.to_owned(),
        ));
    }
    assert_validations(&store, cases).await;
    assert_eq!(
        scan_count(&store, &table).await,
        1,
        "a rejected write stored an item"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_nesting_depth_limit_applies_to_attribute_updates_and_condition_only_values.
/// py
///
/// Legacy `AttributeUpdates` PUT is rejected by design (dynamodb_parity.md,
/// UpdateItem row), so that leg runs as `SET deep = :d`. aux emits a different
/// depth text today; the AWS text is the expected value.
#[tokio::test]
async fn given_values_32_and_31_levels_deep_when_only_set_or_read_by_conditions_then_32_refused_and_31_accepted()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "target"}});
    let seeded = json!({"pk": {"S": "target"}, "v": {"S": "orig"}});
    assert_ok(put(&store, &table, seeded.clone()).await);
    let leaf = json!({"S": "leaf"});
    let (deep, shallow) = (nested_maps(32, &leaf), nested_maps(31, &leaf));

    // Each request is built from a value; the same four shapes run with both.
    let requests = |target: &Value, value: &Value| {
        vec![
            (
                "UpdateItem",
                json!({
                    "TableName": table, "Key": target, "UpdateExpression": "SET deep = :d",
                    "ExpressionAttributeValues": {":d": value}
                }),
            ),
            (
                "UpdateItem",
                json!({
                    "TableName": table, "Key": target,
                    "UpdateExpression": "SET deep2 = if_not_exists(deep2, :d)",
                    "ExpressionAttributeValues": {":d": value}
                }),
            ),
            (
                "UpdateItem",
                json!({
                    "TableName": table, "Key": target, "UpdateExpression": "SET v = :s",
                    "ConditionExpression": "attribute_exists(pk) OR :d = :d",
                    "ExpressionAttributeValues": {":s": {"S": "changed"}, ":d": value}
                }),
            ),
            (
                "DeleteItem",
                json!({
                    "TableName": table, "Key": target,
                    "ConditionExpression": "attribute_exists(pk) OR :d = :d",
                    "ExpressionAttributeValues": {":d": value}
                }),
            ),
        ]
    };
    for (operation, request) in requests(&key, &deep) {
        assert_validation(
            &call(&store, operation, request).await,
            NESTING_LIMIT_MESSAGE,
        );
    }
    assert_eq!(get(&store, &table, key.clone()).await, Some(seeded));

    let control_key = json!({"pk": {"S": "at-limit"}});
    assert_ok(
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "at-limit"}, "v": {"S": "ok"}},
                "ConditionExpression": "attribute_not_exists(pk) OR :d = :d",
                "ExpressionAttributeValues": {":d": shallow}
            }),
        )
        .await,
    );
    assert_eq!(
        get(&store, &table, control_key.clone()).await,
        Some(json!({"pk": {"S": "at-limit"}, "v": {"S": "ok"}}))
    );
    for (operation, request) in requests(&control_key, &shallow) {
        assert_ok(call(&store, operation, request).await);
    }
    assert_eq!(
        get(&store, &table, control_key).await,
        None,
        "the final DeleteItem removes the item"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_nesting_depth_limit_covers_transact_condition_check_and_legacy_expected_values.
/// py
///
/// The legacy `Expected` leg is dropped (rejected by design, dynamodb_parity.md
/// PutItem row). aux emits a different depth text today; the AWS text (without
/// the validation-count prefix for a ConditionCheck) is the expected value.
#[tokio::test]
async fn given_over_deep_condition_check_value_when_transacted_then_validation_and_no_write() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;

    let set_leaf = json!({"NS": ["1", "2"]});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "ns"}, "doc": nested_maps(31, &set_leaf)}),
        )
        .await,
    );
    let stored = get(&store, &table, json!({"pk": {"S": "ns"}}))
        .await
        .unwrap();
    let mut leaf = &stored["doc"];
    for _ in 0..31 {
        leaf = &leaf["M"]["n"];
    }
    let mut members = strings_of(&leaf["NS"]);
    members.sort();
    assert_eq!(members, ["1", "2"]);

    let seeded = json!({"pk": {"S": "target"}, "v": {"S": "orig"}});
    assert_ok(put(&store, &table, seeded.clone()).await);
    let reply = call(
        &store,
        "TransactWriteItems",
        json!({"TransactItems": [
            {"ConditionCheck": {
                "TableName": table, "Key": {"pk": {"S": "target"}},
                "ConditionExpression": "attribute_exists(pk) OR :d = :d",
                "ExpressionAttributeValues": {":d": nested_maps(32, &json!({"S": "leaf"}))}
            }},
            {"Put": {"TableName": table, "Item": {"pk": {"S": "marker"}}}}
        ]}),
    )
    .await;
    assert_validation(
        &reply,
        "Nesting Levels have exceeded supported limits: Attributes in the item have nested levels \
         beyond supported limit",
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "marker"}})).await,
        None
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "target"}})).await,
        Some(seeded)
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_null_false_batchget_mixed_projection_and_query_keyconditions_with_expressions.
/// py
///
/// Section 3 (legacy `KeyConditions`) is dropped: aux rejects it by design.
#[tokio::test]
async fn given_null_false_and_mixed_batch_get_parameters_when_requested_then_validation_rejects() {
    let store = TestStore::new().await;
    let first = pk_table(&store).await;
    let second = pk_table(&store).await;

    assert_validation(
        &put(
            &store,
            &first,
            json!({"pk": {"S": "n1"}, "flag": {"NULL": false}}),
        )
        .await,
        "1 validation error detected: One or more parameter values were invalid: Null attribute \
         value types must have the value of true",
    );
    assert_eq!(get(&store, &first, json!({"pk": {"S": "n1"}})).await, None);
    assert_ok(
        put(
            &store,
            &first,
            json!({"pk": {"S": "n1"}, "flag": {"NULL": true}}),
        )
        .await,
    );
    assert_eq!(
        get(&store, &first, json!({"pk": {"S": "n1"}})).await,
        Some(json!({"pk": {"S": "n1"}, "flag": {"NULL": true}}))
    );

    assert_ok(
        put(
            &store,
            &first,
            json!({"pk": {"S": "k1"}, "a": {"S": "A"}, "b": {"S": "B"}}),
        )
        .await,
    );
    assert_ok(
        put(
            &store,
            &second,
            json!({"pk": {"S": "k1"}, "c": {"S": "C"}, "d": {"S": "D"}}),
        )
        .await,
    );
    let key = json!([{"pk": {"S": "k1"}}]);
    let mixed = call(
        &store,
        "BatchGetItem",
        json!({"RequestItems": {
            &first: {"Keys": key, "ProjectionExpression": "a"},
            &second: {"Keys": key, "AttributesToGet": ["c"]}
        }}),
    )
    .await;
    assert_validation(
        &mixed,
        "Can not use both expression and non-expression parameters in the same request: \
         Non-expression parameters: {AttributesToGet} Expression parameters: \
         {ProjectionExpression}",
    );
    let both = assert_ok(
        call(
            &store,
            "BatchGetItem",
            json!({"RequestItems": {
                &first: {"Keys": key, "ProjectionExpression": "a"},
                &second: {"Keys": key, "ProjectionExpression": "c"}
            }}),
        )
        .await,
    );
    assert_eq!(both["Responses"][&first], json!([{"a": {"S": "A"}}]));
    assert_eq!(both["Responses"][&second], json!([{"c": {"S": "C"}}]));
    store.finish().await;
}

/// The byte cost of one N literal as an attribute value: the size of an item
/// holding only attribute `n`, less the one-byte name.
fn number_byte_cost(literal: &str) -> u64 {
    let value = storage_types::AttributeValue::N(literal.to_owned());
    storage_types::LogicalItemSize::sum_attribute_bytes([("n", &value)]).unwrap() - 1
}

/// AWS behavioural:
/// check_number_byte_cost_exact_at_400kb_boundary_put_item_and_batch_write_item.
/// py and check_number_byte_cost_matches_documented_approximation.py
///
/// The scripts find each cost by searching the 409600-byte gate; here the cost
/// is asserted on the size function directly, and the gate itself is exercised
/// in the next test.
#[test]
fn given_number_literals_when_sized_then_byte_cost_matches_aws_and_documented_approximation() {
    for (literal, cost) in [
        ("0", 1),
        ("1", 2),
        ("123", 3),
        ("1234", 3),
        ("123456", 4),
        ("1.5", 3),
        ("3.14159", 5),
        ("0.15", 2),
        ("-42", 3),
        ("1E125", 2),
        ("1E-100", 2),
        ("12345678901234567890123456789012345678", 20),
    ] {
        assert_eq!(number_byte_cost(literal), cost, "{literal}");
    }
    for literal in [
        "0",
        "1.5",
        "100.5",
        "3.14159",
        "-42",
        "12345678901234567890",
    ] {
        let digits = literal.trim_start_matches('-').replace('.', "");
        let significant = digits.trim_start_matches('0').len() as u64;
        let approximation = significant.div_ceil(2) + 1;
        assert!(
            number_byte_cost(literal).abs_diff(approximation) <= 1,
            "{literal}: cost {} vs approximation {approximation}",
            number_byte_cost(literal)
        );
    }
}

/// AWS behavioural:
/// check_number_byte_cost_exact_at_400kb_boundary_put_item_and_batch_write_item.
/// py
#[tokio::test]
async fn given_item_sized_by_number_byte_cost_when_put_or_batch_written_at_and_over_limit_then_gate_is_exact()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let item = |pk: &str, literal: &str, cost: usize, total: usize| {
        let filler = total - ("pk".len() + pk.len() + "n".len() + cost + "f".len());
        json!({
            "pk": {"S": pk}, "n": {"N": literal},
            "f": {"S": random_text(filler as u64, filler)}
        })
    };
    for (index, (literal, cost)) in [
        ("123456", 4),
        ("1.5", 3),
        ("-42", 3),
        ("1E125", 2),
        ("12345678901234567890123456789012345678", 20),
    ]
    .into_iter()
    .enumerate()
    {
        // Equal-length keys per pair keep the byte arithmetic of the script.
        assert_put_and_batch_size_gate(
            &store,
            &table,
            [
                item(&format!("put-at-{index}"), literal, cost, ITEM_LIMIT_BYTES),
                item(
                    &format!("batch-at-{index}"),
                    literal,
                    cost,
                    ITEM_LIMIT_BYTES,
                ),
            ],
            [
                item(
                    &format!("put-over-{index}"),
                    literal,
                    cost,
                    ITEM_LIMIT_BYTES + 1,
                ),
                item(
                    &format!("batch-over-{index}"),
                    literal,
                    cost,
                    ITEM_LIMIT_BYTES + 1,
                ),
            ],
        )
        .await;
    }
    store.finish().await;
}

const OVERFLOW_MESSAGE: &str = "1 validation error detected: Number overflow. Attempting to store \
                                a number with magnitude larger than supported range";
const UNDERFLOW_MESSAGE: &str = "1 validation error detected: Number underflow. Attempting to \
                                 store a number with magnitude smaller than supported range";

/// Puts `{pk, v: {N: literal}}`.
async fn put_number(store: &TestStore, table: &str, pk: &str, literal: &str) -> Reply {
    put(store, table, json!({"pk": {"S": pk}, "v": {"N": literal}})).await
}

/// AWS behavioural: check_number_canonicalization_boundaries.py
#[tokio::test]
async fn given_number_spellings_when_put_then_canonical_strings_are_stored_and_malformed_or_out_of_range_refused()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let max_magnitude = "9.9999999999999999999999999999999999999E+125";
    let thirty_eight_digits = "1.2345678901234567890123456789012345678";

    assert_ok(
        put(
            &store,
            &table,
            json!({
                "pk": {"S": "canon"}, "padded": {"N": "0100.50"}, "neg_padded": {"N": "-0.500"},
                "zero": {"N": "0"}, "exp": {"N": "1e3"}, "precise": {"N": thirty_eight_digits}
            }),
        )
        .await,
    );
    let item = get(&store, &table, json!({"pk": {"S": "canon"}}))
        .await
        .unwrap();
    assert_eq!(item["padded"], json!({"N": "100.5"}));
    assert_eq!(item["neg_padded"], json!({"N": "-0.5"}));
    assert_eq!(item["zero"], json!({"N": "0"}));
    assert_eq!(item["exp"], json!({"N": "1000"}));
    assert_eq!(item["precise"], json!({"N": thirty_eight_digits}));

    for (literal, expected) in [
        ("+7", "7"),
        ("+1.5", "1.5"),
        ("+0", "0"),
        ("-0", "0"),
        ("+0.0", "0"),
        ("+1e2", "100"),
        ("1e+2", "100"),
        ("1.5E+3", "1500"),
        ("+.5", "0.5"),
        (".5", "0.5"),
        ("5.", "5"),
        ("1.e4", "10000"),
        ("00042", "42"),
        ("+1e-2", "0.01"),
        ("1.23E10", "12300000000"),
    ] {
        let pk = format!("n-canon-{literal}");
        assert_ok(put_number(&store, &table, &pk, literal).await);
        let stored = get(&store, &table, json!({"pk": {"S": pk}})).await.unwrap();
        assert_eq!(stored["v"], json!({"N": expected}), "{literal}");
    }

    for literal in [
        "e2", "+e2", "1e", "1e+", "++7", "+-7", "-+7", "1+2", "+1+2", "1.2.3", "+1.2.3", "1.2e3.4",
        ".", "+", "-", "0x5", "NaN", "Infinity", "1_000", " 7", "7 ", "1 5", "1e1.5",
    ] {
        assert_validation(
            &put_number(&store, &table, "n-malformed", literal).await,
            &format!(
                "1 validation error detected: The parameter cannot be converted to a numeric \
                 value: {literal}"
            ),
        );
    }
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "n-malformed"}})).await,
        None
    );

    assert_ok(put_number(&store, &table, "cmp", "01.10").await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "cmp"}}))
            .await
            .unwrap()["v"],
        json!({"N": "1.1"})
    );
    assert_ok(
        call(
            &store,
            "PutItem",
            json!({
                "TableName": table, "Item": {"pk": {"S": "cmp"}, "v": {"N": "2"}},
                "ConditionExpression": "v = :old", "ExpressionAttributeValues": {":old": {"N": "1.1"}}
            }),
        )
        .await,
    );

    assert_ok(put_number(&store, &table, "n-max", max_magnitude).await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "n-max"}}))
            .await
            .unwrap()["v"],
        json!({"N": format!("{}{}", "9".repeat(38), "0".repeat(88))})
    );
    for (pk, literal, message) in [
        ("n-oob", "1E+126", OVERFLOW_MESSAGE),
        ("n-oob-neg", "-1E+126", OVERFLOW_MESSAGE),
        ("n-zero-over", "0E+300", OVERFLOW_MESSAGE),
        ("n-zero-under", "0E-300", UNDERFLOW_MESSAGE),
    ] {
        assert_validation(&put_number(&store, &table, pk, literal).await, message);
        assert_eq!(
            get(&store, &table, json!({"pk": {"S": pk}})).await,
            None,
            "{pk}"
        );
    }
    assert_ok(put_number(&store, &table, "n-ok", "42").await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "n-ok"}}))
            .await
            .unwrap()["v"],
        json!({"N": "42"})
    );
    store.finish().await;
}

/// AWS behavioural: check_number_magnitude_floor_put_update_batchwrite.py
#[tokio::test]
async fn given_numbers_at_magnitude_floor_and_ceiling_when_written_then_accepted_and_one_below_floor_refused()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let floor_stored = format!("0.{}1", "0".repeat(129));
    let ceiling = "9.9999999999999999999999999999999999999E+125";
    let ceiling_stored = format!("{}{}", "9".repeat(38), "0".repeat(88));
    let stored = |pk: &'static str| {
        let (store, table) = (&store, &table);
        async move { get(store, table, json!({"pk": {"S": pk}})).await }
    };

    for (pk, literal, expected) in [
        ("put-floor", "1E-130".to_owned(), floor_stored.clone()),
        (
            "put-floor-neg",
            "-1E-130".to_owned(),
            format!("-{floor_stored}"),
        ),
        (
            "put-ceil-neg",
            format!("-{ceiling}"),
            format!("-{ceiling_stored}"),
        ),
    ] {
        assert_ok(put_number(&store, &table, pk, &literal).await);
        assert_eq!(
            stored(pk).await.unwrap()["v"],
            json!({"N": expected}),
            "{pk}"
        );
    }
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "put-ns"}, "v": {"NS": [ceiling, format!("-{ceiling}"), "1E-130", "0"]}}),
        )
        .await,
    );
    let mut members = strings_of(&stored("put-ns").await.unwrap()["v"]["NS"]);
    members.sort();
    let mut expected = vec![
        ceiling_stored.clone(),
        format!("-{ceiling_stored}"),
        floor_stored.clone(),
        "0".to_owned(),
    ];
    expected.sort();
    assert_eq!(members, expected);
    for (pk, literal) in [("put-under", "1E-131"), ("put-under-neg", "-1E-131")] {
        assert_validation(
            &put_number(&store, &table, pk, literal).await,
            UNDERFLOW_MESSAGE,
        );
        assert_eq!(get(&store, &table, json!({"pk": {"S": pk}})).await, None);
    }

    let set_value = |literal: &str| {
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"S": "upd"}}, "UpdateExpression": "SET v = :v",
                "ExpressionAttributeValues": {":v": {"N": literal}}
            }),
        )
    };
    assert_ok(set_value("1E-130").await);
    assert_eq!(
        stored("upd").await.unwrap()["v"],
        json!({"N": floor_stored})
    );
    assert_validation(&set_value("1E-131").await, UNDERFLOW_MESSAGE);
    assert_eq!(
        stored("upd").await.unwrap()["v"],
        json!({"N": floor_stored})
    );

    let batch = |items: Vec<Value>| {
        let requests: Vec<Value> = items
            .into_iter()
            .map(|item| json!({"PutRequest": {"Item": item}}))
            .collect();
        call(
            &store,
            "BatchWriteItem",
            json!({"RequestItems": {&table: requests}}),
        )
    };
    let body = assert_ok(
        batch(vec![
            json!({"pk": {"S": "batch-floor"}, "v": {"N": "1E-130"}}),
        ])
        .await,
    );
    assert_eq!(body["UnprocessedItems"], json!({}));
    assert_eq!(
        stored("batch-floor").await.unwrap()["v"],
        json!({"N": floor_stored})
    );
    // The batch error carries the bare message, without the validation-count
    // envelope.
    let refused = batch(vec![
        json!({"pk": {"S": "batch-good"}, "v": {"N": "42"}}),
        json!({"pk": {"S": "batch-under"}, "v": {"N": "1E-131"}}),
    ])
    .await;
    assert_validation(
        &refused,
        "Number underflow. Attempting to store a number with magnitude smaller than supported \
         range",
    );
    assert_eq!(stored("batch-good").await, None);
    assert_eq!(stored("batch-under").await, None);
    store.finish().await;
}

/// AWS behavioural:
/// check_number_precision_overflow_in_exponent_form_rejected.py
#[tokio::test]
async fn given_exponent_form_numbers_when_digits_exceed_38_then_precision_is_refused_after_folding()
{
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let ok_input = format!("9.{}E124", "9".repeat(37));
    let over_input = format!("9.{}E124", "9".repeat(38));
    let message = "1 validation error detected: Attempting to store more than 38 significant \
                   digits in a Number";

    assert_ok(put_number(&store, &table, "ok", &ok_input).await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "ok"}}))
            .await
            .unwrap()["v"],
        json!({"N": format!("{}{}", "9".repeat(38), "0".repeat(87))})
    );
    assert_validation(
        &put_number(&store, &table, "over", &over_input).await,
        message,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "over"}})).await,
        None
    );
    assert_validation(
        &put(
            &store,
            &table,
            json!({"pk": {"S": "over-ns"}, "ns": {"NS": [ok_input, over_input]}}),
        )
        .await,
        message,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "over-ns"}})).await,
        None
    );
    assert_ok(put_number(&store, &table, "control", "42").await);
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "control"}}))
            .await
            .unwrap()["v"],
        json!({"N": "42"})
    );
    store.finish().await;
}

/// AWS behavioural: check_number_set_add_type_mismatch_rejected.py
#[tokio::test]
async fn given_string_set_when_number_set_is_added_then_type_error_and_same_type_add_succeeds() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let key = json!({"pk": {"S": "item"}});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "item"}, "tags": {"SS": ["a", "b"]}}),
        )
        .await,
    );
    let add = |value: Value| {
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": key, "UpdateExpression": "ADD tags :n",
                "ExpressionAttributeValues": {":n": value}
            }),
        )
    };
    assert_validation(
        &add(json!({"NS": ["1"]})).await,
        "An operand in the update expression has an incorrect data type",
    );
    let sorted = |item: Value| {
        let mut members = strings_of(&item["tags"]["SS"]);
        members.sort();
        members
    };
    assert_eq!(
        sorted(get(&store, &table, key.clone()).await.unwrap()),
        ["a", "b"]
    );
    assert_ok(add(json!({"SS": ["c"]})).await);
    assert_eq!(
        sorted(get(&store, &table, key).await.unwrap()),
        ["a", "b", "c"]
    );
    store.finish().await;
}

/// AWS behavioural: check_numeric_key_is_canonicalized_on_every_path.py
#[tokio::test]
async fn given_number_key_spelled_non_canonically_when_any_path_runs_then_it_reaches_the_canonical_item()
 {
    let store = TestStore::new().await;
    let table = keyed_table(&store, ("pk", "N"), None).await;
    let lookup = |spelling: &str| {
        let (store, table, key) = (&store, &table, json!({"pk": {"N": spelling}}));
        async move { get(store, table, key).await }
    };

    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"N": "42"}, "via": {"S": "put"}}),
        )
        .await,
    );
    for spelling in ["42", "00042", "42.0", "4.2e1"] {
        assert_eq!(
            lookup(spelling).await,
            Some(json!({"pk": {"N": "42"}, "via": {"S": "put"}})),
            "{spelling}"
        );
    }
    assert_ok(
        call(
            &store,
            "BatchWriteItem",
            json!({"RequestItems": {&table: [
                {"PutRequest": {"Item": {"pk": {"N": "00043"}, "via": {"S": "batch"}}}}
            ]}}),
        )
        .await,
    );
    assert_eq!(
        lookup("43").await,
        Some(json!({"pk": {"N": "43"}, "via": {"S": "batch"}}))
    );
    assert_ok(
        call(
            &store,
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": table, "Item": {"pk": {"N": "0044.00"}, "via": {"S": "transact"}}}}
            ]}),
        )
        .await,
    );
    assert_eq!(
        lookup("44").await,
        Some(json!({"pk": {"N": "44"}, "via": {"S": "transact"}}))
    );
    assert_ok(
        call(
            &store,
            "UpdateItem",
            json!({
                "TableName": table, "Key": {"pk": {"N": "004.2e1"}}, "UpdateExpression": "SET via = :v",
                "ExpressionAttributeValues": {":v": {"S": "updated"}}
            }),
        )
        .await,
    );
    assert_eq!(
        lookup("42").await,
        Some(json!({"pk": {"N": "42"}, "via": {"S": "updated"}}))
    );
    let removed = assert_ok(
        call(
            &store,
            "DeleteItem",
            json!({"TableName": table, "Key": {"pk": {"N": "0042.0"}}, "ReturnValues": "ALL_OLD"}),
        )
        .await,
    );
    assert_eq!(
        removed["Attributes"],
        json!({"pk": {"N": "42"}, "via": {"S": "updated"}})
    );
    assert_eq!(lookup("42").await, None);
    assert!(lookup("43").await.is_some() && lookup("44").await.is_some());
    store.finish().await;
}

/// AWS behavioural:
/// check_oversized_item_to_a_missing_table_not_found_on_put_rejected_on_batch_write.
/// py
///
/// The ordering (PutItem looks the table up before sizing, BatchWriteItem sizes
/// first) is AWS's; it is asserted as written.
#[tokio::test]
async fn given_oversized_item_when_table_is_missing_then_put_is_not_found_and_batch_write_is_size_refused()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let missing = format!("{table}-absent");
    let big = json!({"pk": {"S": "big"}, "blob": {"S": random_text(5, 400 * 1024)}});
    let batch = |name: &str| {
        call(
            &store,
            "BatchWriteItem",
            json!({"RequestItems": {name: [{"PutRequest": {"Item": big.clone()}}]}}),
        )
    };

    assert_error(
        &call(
            &store,
            "PutItem",
            json!({"TableName": missing, "Item": big}),
        )
        .await,
        "ResourceNotFoundException",
        "Requested resource not found",
    );
    assert_validation(&batch(&missing).await, ITEM_TOO_LARGE);
    assert_validation(&put(&store, &table, big.clone()).await, ITEM_TOO_LARGE);
    assert_validation(&batch(&table).await, ITEM_TOO_LARGE);
    assert_eq!(get(&store, &table, json!({"pk": {"S": "big"}})).await, None);
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "small"}, "blob": {"S": "x"}}),
        )
        .await,
    );
    assert_eq!(
        get(&store, &table, json!({"pk": {"S": "small"}})).await,
        Some(json!({"pk": {"S": "small"}, "blob": {"S": "x"}}))
    );
    assert_error(
        &call(&store, "DescribeTable", json!({"TableName": missing})).await,
        "ResourceNotFoundException",
        &format!("Requested resource not found: Table: {missing} not found"),
    );
    store.finish().await;
}

/// GetItem with `projection` and the names it uses; returns the projected item.
async fn project(
    store: &TestStore,
    table: &str,
    key: Value,
    projection: &str,
    names: Value,
) -> Value {
    let body = assert_ok(
        call(
            store,
            "GetItem",
            json!({
                "TableName": table, "Key": key, "ConsistentRead": true,
                "ProjectionExpression": projection, "ExpressionAttributeNames": names
            }),
        )
        .await,
    );
    body["Item"].clone()
}

/// AWS behavioural: check_projection_list_indices_compact_in_index_order.py
#[tokio::test]
async fn given_list_attribute_when_projecting_indices_then_dense_list_in_index_order() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let slots = json!({"L": [{"S": "zero"}, {"S": "one"}, {"S": "two"}, {"S": "three"}]});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "list-idx"}, "slots": slots}),
        )
        .await,
    );
    let key = json!({"pk": {"S": "list-idx"}});
    let names = json!({"#s": "slots"});
    for (projection, expected) in [
        ("#s[2]", json!({"L": [{"S": "two"}]})),
        ("#s[3], #s[1]", json!({"L": [{"S": "one"}, {"S": "three"}]})),
        (
            "#s[0], #s[3]",
            json!({"L": [{"S": "zero"}, {"S": "three"}]}),
        ),
        ("#s[0], #s[1], #s[2], #s[3]", slots.clone()),
        ("#s", slots.clone()),
    ] {
        let item = project(&store, &table, key.clone(), projection, names.clone()).await;
        assert_eq!(item, json!({"slots": expected}), "{projection}");
    }
    assert_eq!(get(&store, &table, key).await.unwrap()["slots"], slots);
    store.finish().await;
}

/// AWS behavioural:
/// check_projection_shared_list_index_merges_into_one_element.py
#[tokio::test]
async fn given_list_of_maps_when_projection_paths_share_an_index_then_they_merge_into_one_element()
{
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let rows = json!({"L": [
        {"M": {"a": {"S": "a0"}, "b": {"S": "b0"}, "c": {"S": "c0"}}},
        {"M": {"a": {"S": "a1"}, "b": {"S": "b1"}}},
        {"M": {"a": {"S": "a2"}, "b": {"S": "b2"}}}
    ]});
    let outer = json!({"L": [{"M": {"inner": {"L": [
        {"M": {"p": {"S": "p00"}, "q": {"S": "q00"}}},
        {"M": {"p": {"S": "p01"}, "q": {"S": "q01"}}}
    ]}}}]});
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "shared-idx"}, "rows": rows, "outer": outer}),
        )
        .await,
    );
    let key = json!({"pk": {"S": "shared-idx"}});
    let names = json!({"#r": "rows", "#o": "outer", "#i": "inner", "#a": "a", "#b": "b", "#p": "p", "#q": "q"});
    let used = |projection: &str| {
        let used: serde_json::Map<String, Value> = names
            .as_object()
            .unwrap()
            .iter()
            .filter(|(name, _)| {
                projection.match_indices(name.as_str()).any(|(at, _)| {
                    !projection
                        .get(at + name.len()..)
                        .unwrap_or_default()
                        .starts_with(|next: char| next.is_ascii_alphanumeric() || next == '_')
                })
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        Value::Object(used)
    };
    let merged = json!({"rows": {"L": [{"M": {"a": {"S": "a0"}, "b": {"S": "b0"}}}]}});
    let cases = [
        ("#r[0].#a, #r[0].#b", merged.clone()),
        ("#r[0].#b, #r[0].#a", merged),
        (
            "#r[0].#a, #r[2].#b",
            json!({"rows": {"L": [{"M": {"a": {"S": "a0"}}}, {"M": {"b": {"S": "b2"}}}]}}),
        ),
        (
            "#r[0].#a, #r[0].#b, #r[2].#a",
            json!({"rows": {"L": [
                {"M": {"a": {"S": "a0"}, "b": {"S": "b0"}}},
                {"M": {"a": {"S": "a2"}}}
            ]}}),
        ),
        (
            "#o[0].#i[0].#p, #o[0].#i[0].#q",
            json!({"outer": {"L": [{"M": {"inner": {"L": [{"M": {"p": {"S": "p00"}, "q": {"S": "q00"}}}]}}}]}}),
        ),
        (
            "#o[0].#i[0].#p, #o[0].#i[1].#q",
            json!({"outer": {"L": [{"M": {"inner": {"L": [
                {"M": {"p": {"S": "p00"}}},
                {"M": {"q": {"S": "q01"}}}
            ]}}}]}}),
        ),
        (
            "#r[0]",
            json!({"rows": {"L": [{"M": {"a": {"S": "a0"}, "b": {"S": "b0"}, "c": {"S": "c0"}}}]}}),
        ),
    ];
    for (projection, expected) in cases {
        let item = project(&store, &table, key.clone(), projection, used(projection)).await;
        assert_eq!(item, expected, "{projection}");
    }
    assert_eq!(
        get(&store, &table, key).await.unwrap()["rows"]["L"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_projection_shared_list_index_and_nested_map_merge_across_read_operations.
/// py
#[tokio::test]
async fn given_nested_list_and_map_when_projection_shares_an_index_then_query_scan_and_batch_get_merge()
 {
    let store = TestStore::new().await;
    let table = pk_sk_table(&store).await;
    let key = json!({"pk": {"S": "row"}, "sk": {"S": "only"}});
    let list = json!({"L": [
        {"M": {
            "a": {"S": "a0"}, "b": {"S": "b0"},
            "m": {"M": {"x": {"S": "mx0"}, "y": {"S": "my0"}}}
        }},
        {"M": {"a": {"S": "a1"}, "b": {"S": "b1"}}}
    ]});
    let item = json!({
        "pk": {"S": "row"}, "sk": {"S": "only"}, "l": list,
        "m": {"M": {"n": {"S": "n0"}, "z": {"S": "z0"}}}
    });
    assert_ok(put(&store, &table, item).await);

    let items_of = |operation: &'static str, projection: &str, names: Value| {
        let (store, table, key) = (&store, &table, &key);
        let projection = projection.to_owned();
        async move {
            let request = match operation {
                "Query" => {
                    let mut names = names;
                    names["#pk"] = json!("pk");
                    json!({
                        "TableName": table, "KeyConditionExpression": "#pk = :pk",
                        "ExpressionAttributeNames": names,
                        "ExpressionAttributeValues": {":pk": {"S": "row"}},
                        "ProjectionExpression": projection, "ConsistentRead": true
                    })
                }
                "Scan" => json!({
                    "TableName": table, "ProjectionExpression": projection,
                    "ExpressionAttributeNames": names, "ConsistentRead": true
                }),
                _ => json!({"RequestItems": {table: {
                    "Keys": [key], "ProjectionExpression": projection,
                    "ExpressionAttributeNames": names, "ConsistentRead": true
                }}}),
            };
            let body = assert_ok(call(store, operation, request).await);
            let items = if operation == "BatchGetItem" {
                body["Responses"][table].clone()
            } else {
                body["Items"].clone()
            };
            let items = items.as_array().unwrap().clone();
            assert_eq!(items.len(), 1, "{operation} {projection}");
            items[0].clone()
        }
    };
    let merged = json!({"l": {"L": [{"M": {"a": {"S": "a0"}, "b": {"S": "b0"}}}]}});
    for operation in ["Query", "Scan", "BatchGetItem"] {
        let got = items_of(
            operation,
            "#l[0].#a, #l[0].#b",
            json!({"#l": "l", "#a": "a", "#b": "b"}),
        )
        .await;
        assert_eq!(got, merged, "{operation}");
    }
    let leaf_and_map = project(
        &store,
        &table,
        key.clone(),
        "#l[0].#a, #l[0].#m.#x",
        json!({"#l": "l", "#a": "a", "#m": "m", "#x": "x"}),
    )
    .await;
    assert_eq!(
        leaf_and_map,
        json!({"l": {"L": [{"M": {"a": {"S": "a0"}, "m": {"M": {"x": {"S": "mx0"}}}}}]}})
    );
    let map_pair = project(
        &store,
        &table,
        key.clone(),
        "#l[0].#m.#x, #l[0].#m.#y",
        json!({"#l": "l", "#m": "m", "#x": "x", "#y": "y"}),
    )
    .await;
    assert_eq!(
        map_pair,
        json!({"l": {"L": [{"M": {"m": {"M": {"x": {"S": "mx0"}, "y": {"S": "my0"}}}}}]}})
    );
    let rebuilt = items_of(
        "Query",
        "#m.#n, #l[1]",
        json!({"#m": "m", "#n": "n", "#l": "l"}),
    )
    .await;
    assert_eq!(
        rebuilt,
        json!({
            "m": {"M": {"n": {"S": "n0"}}},
            "l": {"L": [{"M": {"a": {"S": "a1"}, "b": {"S": "b1"}}}]}
        })
    );
    assert_eq!(
        get(&store, &table, key).await.unwrap()["l"]["L"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    store.finish().await;
}

/// AWS behavioural: check_put_get_empty_vs_absent_attribute_preserved.py
#[tokio::test]
async fn given_empty_string_and_binary_attributes_when_put_then_get_keeps_them_and_omits_the_absent_one()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    assert_ok(
        put(
            &store,
            &table,
            json!({"pk": {"S": "item"}, "emptyStr": {"S": ""}, "emptyBin": {"B": ""}, "present": {"S": "x"}}),
        )
        .await,
    );
    let item = get(&store, &table, json!({"pk": {"S": "item"}}))
        .await
        .unwrap();
    assert_eq!(item["emptyStr"], json!({"S": ""}));
    assert_eq!(item["emptyBin"], json!({"B": ""}));
    assert_eq!(item["present"], json!({"S": "x"}));
    assert!(item.get("absentAttr").is_none(), "{item}");
    store.finish().await;
}

/// AWS behavioural: check_put_item_and_batch_write_item_item_size_boundary.py
#[tokio::test]
async fn given_string_item_when_put_or_batch_written_at_and_over_409600_bytes_then_gate_is_exact() {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let item = |pk: &str, total: usize| {
        let filler = total - ("pk".len() + pk.len() + "data".len());
        json!({"pk": {"S": pk}, "data": {"S": random_text(filler as u64, filler)}})
    };
    assert_put_and_batch_size_gate(
        &store,
        &table,
        [
            item("put-at-limit", ITEM_LIMIT_BYTES),
            item("batch-at-limit", ITEM_LIMIT_BYTES),
        ],
        [
            item("put-over-limit", ITEM_LIMIT_BYTES + 1),
            item("batch-over-limit", ITEM_LIMIT_BYTES + 1),
        ],
    )
    .await;
    store.finish().await;
}

/// AWS behavioural:
/// check_put_item_and_batch_write_item_binary_item_size_boundary_raw_bytes.py
#[tokio::test]
async fn given_binary_item_when_put_or_batch_written_at_and_over_409600_raw_bytes_then_size_counts_decoded_bytes()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let item = |pk: &str, total: usize| {
        let filler = total - ("pk".len() + pk.len() + "data".len());
        json!({"pk": {"S": pk}, "data": binary(&random_bytes(filler as u64, filler))})
    };
    let at_limit = item("put-at-limit", ITEM_LIMIT_BYTES);
    assert!(
        at_limit["data"]["B"].as_str().unwrap().len() > ITEM_LIMIT_BYTES,
        "base64 text must be longer than the raw limit for this test to separate the two"
    );
    assert_put_and_batch_size_gate(
        &store,
        &table,
        [at_limit, item("batch-at-limit", ITEM_LIMIT_BYTES)],
        [
            item("put-over-limit", ITEM_LIMIT_BYTES + 1),
            item("batch-over-limit", ITEM_LIMIT_BYTES + 1),
        ],
    )
    .await;
    store.finish().await;
}

/// A string of exactly `byte_length` UTF-8 bytes, mostly pseudo-random 2-byte
/// characters so that its character count differs from its byte count.
fn multibyte_text(seed: u64, byte_length: usize) -> String {
    let mut state = seed;
    let mut text: String = (0..byte_length / 2)
        .map(|_| char::from_u32(0xA1 + (splitmix64(&mut state) % 94) as u32).unwrap())
        .collect();
    if byte_length % 2 == 1 {
        text.push('x');
    }
    assert_eq!(text.len(), byte_length);
    text
}

/// AWS behavioural:
/// check_put_item_and_batch_write_item_multibyte_string_size_utf8_bytes.py
#[tokio::test]
async fn given_multibyte_string_item_when_put_or_batch_written_at_and_over_409600_utf8_bytes_then_size_counts_bytes()
 {
    let store = TestStore::new().await;
    let table = pk_table(&store).await;
    let item = |pk: &str, total: usize| {
        let filler = total - ("pk".len() + pk.len() + "data".len());
        json!({"pk": {"S": pk}, "data": {"S": multibyte_text(filler as u64, filler)}})
    };
    assert_put_and_batch_size_gate(
        &store,
        &table,
        [
            item("put-at-limit", ITEM_LIMIT_BYTES),
            item("batch-at-limit", ITEM_LIMIT_BYTES),
        ],
        [
            item("put-over-limit", ITEM_LIMIT_BYTES + 1),
            item("batch-over-limit", ITEM_LIMIT_BYTES + 1),
        ],
    )
    .await;
    store.finish().await;
}
