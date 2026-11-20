//! AWS emulator behavioural suite ports.
//!
//! Transactions, idempotency tokens, cancellation reasons and concurrent item
//! writes. Expected values are AWS's, copied from the scripts; where aux
//! differs today the test is expected to fail and the runner reports it.

use std::{collections::BTreeSet, sync::Arc};

use serde_json::{Value, json};
use storage_provider::DynamoOperations;
use storage_types::{
    CreateTableRequest, DeleteItemRequest, GetItemRequest, PutItemRequest, StorageError,
    TransactGetItemsRequest, TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest,
};
use tokio::sync::Barrier;

use crate::test_support::{TestStore, unique_table_name};

type Ops = Arc<dyn DynamoOperations>;
/// HTTP status and JSON body of a rejected call.
type Wire = (u16, Value);

const COERAL_VALIDATION: &str = "com.amazon.coral.validate#ValidationException";
const CCF_MESSAGE: &str = "The conditional request failed";
const IDEMPOTENT_MISMATCH: &str = "Specified idempotent token was used with different request \
                                   parameters within the idempotency window";
const NESTING_MESSAGE: &str = "Nesting Levels have exceeded supported limits: Attributes in the \
                               item have nested levels beyond supported limit";
const ITEM_LIMIT_BYTES: usize = 409_600;
const RACERS: usize = 8;

fn wire(error: StorageError) -> Wire {
    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::from(error).into();
    (status.as_u16(), serde_json::to_value(body).unwrap())
}

macro_rules! call {
    ($ops:expr, $method:ident, $request:ty, $json:expr) => {{
        let request = match <$request>::try_from($json) {
            Ok(request) => request,
            Err(message) => {
                return Err((
                    400,
                    json!({"__type": COERAL_VALIDATION, "message": message}),
                ));
            }
        };
        $ops.$method(request)
            .await
            .map(|response| serde_json::to_value(response).unwrap())
            .map_err(wire)
    }};
}

async fn tx_write(ops: &Ops, request: Value) -> Result<Value, Wire> {
    call!(
        ops,
        transact_write_items,
        TransactWriteItemsRequest,
        request
    )
}

async fn tx_get(ops: &Ops, request: Value) -> Result<Value, Wire> {
    call!(ops, transact_get_items, TransactGetItemsRequest, request)
}

async fn put_item(ops: &Ops, request: Value) -> Result<Value, Wire> {
    call!(ops, put_item, PutItemRequest, request)
}

async fn update_item(ops: &Ops, request: Value) -> Result<Value, Wire> {
    call!(ops, update_item, UpdateItemRequest, request)
}

async fn delete_item(ops: &Ops, request: Value) -> Result<Value, Wire> {
    call!(ops, delete_item, DeleteItemRequest, request)
}

async fn update_table(ops: &Ops, request: Value) -> Result<Value, Wire> {
    call!(ops, update_table, UpdateTableRequest, request)
}

async fn seed(ops: &Ops, table: &str, item: Value) {
    put_item(ops, json!({"TableName": table, "Item": item}))
        .await
        .unwrap_or_else(|error| panic!("seed put failed: {error:?}"));
}

/// The stored item, or `Value::Null` when absent.
async fn item(ops: &Ops, table: &str, key: Value) -> Value {
    let request = GetItemRequest::try_from(json!({
        "TableName": table, "Key": key, "ConsistentRead": true
    }))
    .unwrap();
    let response = ops.get_item(request).await.unwrap();
    serde_json::to_value(response).unwrap()["Item"].clone()
}

async fn pk_item(ops: &Ops, table: &str, pk: &str) -> Value {
    item(ops, table, json!({"pk": {"S": pk}})).await
}

fn name_of(table: &impl AsRef<str>) -> String {
    table.as_ref().to_owned()
}

/// Table with a string `pk` hash key.
async fn new_table(store: &TestStore) -> String {
    make_table(store, "pk", None, false).await
}

async fn make_table(store: &TestStore, hash: &str, range: Option<&str>, gsi: bool) -> String {
    let name = name_of(&unique_table_name());
    let mut attributes = vec![json!({"AttributeName": hash, "AttributeType": "S"})];
    let mut schema = vec![json!({"AttributeName": hash, "KeyType": "HASH"})];
    if let Some(range) = range {
        attributes.push(json!({"AttributeName": range, "AttributeType": "S"}));
        schema.push(json!({"AttributeName": range, "KeyType": "RANGE"}));
    }
    let mut request = json!({
        "TableName": name, "AttributeDefinitions": attributes, "KeySchema": schema,
        "BillingMode": "PAY_PER_REQUEST"
    });
    if gsi {
        request["AttributeDefinitions"]
            .as_array_mut()
            .unwrap()
            .push(json!({"AttributeName": "gpk", "AttributeType": "S"}));
        request["GlobalSecondaryIndexes"] = json!([{
            "IndexName": "gsi1",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]);
    }
    store
        .operations
        .create_table(CreateTableRequest::try_from(request).unwrap())
        .await
        .unwrap();
    name
}

fn s(value: &str) -> Value {
    json!({"S": value})
}

fn n(value: impl ToString) -> Value {
    json!({"N": value.to_string()})
}

fn message(error: &Wire) -> &str {
    error.1["message"]
        .as_str()
        .or_else(|| error.1["Message"].as_str())
        .unwrap_or_else(|| panic!("error body has no message: {}", error.1))
}

fn assert_error(error: &Wire, name: &str, expected: &str) {
    assert_eq!(error.0, 400, "{:?}", error.1);
    assert_eq!(
        error.1["__type"],
        format!("com.amazonaws.dynamodb.v20120810#{name}")
    );
    assert_eq!(message(error), expected);
}

fn assert_validation(error: &Wire, expected: &str) {
    assert_eq!(error.0, 400, "{:?}", error.1);
    assert_eq!(error.1["__type"], COERAL_VALIDATION);
    assert_eq!(message(error), expected);
    assert!(
        error.1.get("CancellationReasons").is_none(),
        "a top-level validation error carries no CancellationReasons"
    );
}

fn assert_ccf(error: &Wire) {
    assert_error(error, "ConditionalCheckFailedException", CCF_MESSAGE);
}

fn cancelled_message(codes: &[&str]) -> String {
    format!(
        "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
        codes.join(", ")
    )
}

/// Cancellation with exactly these reason codes; reason messages are checked
/// only where `Some`.
fn assert_cancelled(error: &Wire, reasons: &[(&str, Option<&str>)]) {
    let codes = reasons.iter().map(|(code, _)| *code).collect::<Vec<_>>();
    assert_error(
        error,
        "TransactionCanceledException",
        &cancelled_message(&codes),
    );
    let actual = error.1["CancellationReasons"].as_array().unwrap();
    assert_eq!(actual.len(), reasons.len(), "{:?}", error.1);
    for (actual, (code, text)) in actual.iter().zip(reasons) {
        assert_eq!(actual["Code"], *code, "{:?}", error.1);
        if let Some(text) = text {
            assert_eq!(actual["Message"], *text, "{:?}", error.1);
        }
    }
}

fn reason_codes(error: &Wire) -> Vec<String> {
    error.1["CancellationReasons"]
        .as_array()
        .unwrap_or_else(|| panic!("no CancellationReasons: {:?}", error.1))
        .iter()
        .map(|reason| reason["Code"].as_str().unwrap().to_owned())
        .collect()
}

/// Run `work(index)` for every index with all calls started together.
async fn race<F, Fut>(count: usize, work: F) -> Vec<Result<Value, Wire>>
where
    F: Fn(usize) -> Fut,
    Fut: std::future::Future<Output = Result<Value, Wire>> + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(count));
    let tasks = (0..count)
        .map(|index| {
            let barrier = Arc::clone(&barrier);
            let call = work(index);
            tokio::spawn(async move {
                barrier.wait().await;
                call.await
            })
        })
        .collect::<Vec<_>>();
    let mut outcomes = Vec::with_capacity(count);
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    outcomes
}

fn winners(outcomes: &[Result<Value, Wire>]) -> Vec<usize> {
    outcomes
        .iter()
        .enumerate()
        .filter(|(_, outcome)| outcome.is_ok())
        .map(|(index, _)| index)
        .collect()
}

fn losers(outcomes: &[Result<Value, Wire>]) -> Vec<(usize, &Wire)> {
    outcomes
        .iter()
        .enumerate()
        .filter_map(|(index, outcome)| outcome.as_ref().err().map(|error| (index, error)))
        .collect()
}

/// Pseudo-random text (splitmix64), incompressible enough to exceed native
/// value limits after LZ4.
fn filler(seed: u64, length: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut state = seed;
    let mut out = String::with_capacity(length);
    while out.len() < length {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        for shift in (0..60).step_by(6) {
            if out.len() < length {
                out.push(char::from(ALPHABET[((z >> shift) & 63) as usize]));
            }
        }
    }
    out
}

/// DynamoDB item size of a `pk` + `data` item: attribute names plus values.
fn data_item_size(pk: &str, data_len: usize) -> usize {
    "pk".len() + pk.len() + "data".len() + data_len
}

fn data_item(pk: &str, data_len: usize, seed: u64) -> Value {
    json!({"pk": s(pk), "data": s(&filler(seed, data_len))})
}

fn nested(levels: usize) -> Value {
    let mut value = s("leaf");
    for _ in 0..levels {
        value = json!({"M": {"n": value}});
    }
    value
}

fn put_member(table: &str, item: Value) -> Value {
    json!({"Put": {"TableName": table, "Item": item}})
}

// ---------------------------------------------------------------------------
// Idempotency tokens
// ---------------------------------------------------------------------------

/// AWS behavioural: check_conc_ddb_optimistic_cancelled_token_is_not_burned.py
#[tokio::test]
async fn given_cancelled_first_use_when_token_retries_then_commits_once_and_refreshed_version_mismatches()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(
        ops,
        &table,
        json!({"pk": s("acct"), "version": n(1), "n": n(0)}),
    )
    .await;
    let request = |expected: u32| {
        json!({"ClientRequestToken": "cancelled-token", "TransactItems": [
            {"Update": {
                "TableName": table, "Key": {"pk": s("acct")},
                "UpdateExpression": "SET version = :next ADD n :one",
                "ConditionExpression": "version = :expected",
                "ExpressionAttributeValues": {
                    ":expected": n(expected), ":next": n(expected + 1), ":one": n(1)}
            }},
            put_member(&table, json!({"pk": s("receipt")}))
        ]})
    };
    let state = || async {
        let acct = pk_item(ops, &table, "acct").await;
        let receipt = !pk_item(ops, &table, "receipt").await.is_null();
        (acct["version"].clone(), acct["n"].clone(), receipt)
    };

    let error = tx_write(ops, request(2)).await.unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
            ("None", None),
        ],
    );
    assert_eq!(state().await, (n(1), n(0), false));

    update_item(
        ops,
        json!({
            "TableName": table, "Key": {"pk": s("acct")}, "UpdateExpression": "SET version = :v",
            "ExpressionAttributeValues": {":v": n(2)}
        }),
    )
    .await
    .unwrap();
    tx_write(ops, request(2))
        .await
        .expect("a cancelled use does not burn the token");
    assert_eq!(state().await, (n(3), n(1), true));

    tx_write(ops, request(2))
        .await
        .expect("replay of a committed token");
    assert_eq!(state().await, (n(3), n(1), true), "replay applies nothing");

    let error = tx_write(ops, request(3)).await.unwrap_err();
    assert_error(
        &error,
        "IdempotentParameterMismatchException",
        IDEMPOTENT_MISMATCH,
    );
    assert_eq!(state().await, (n(3), n(1), true));
    store.finish().await;
}

/// AWS behavioural: check_transact_write_idempotency_mismatch.py
/// AWS behavioural:
/// check_transact_write_items_client_token_idempotent_mismatch_rejected.py
///
/// The existing
/// `given_committed_token_when_transaction_changes_then_mismatch_response_and_state_remain`
/// asserts aux's own message; AWS's text is asserted here.
#[tokio::test]
async fn given_committed_token_when_put_changes_then_aws_mismatch_message_and_original_item_remain()
{
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let request = |pk: &str, v: u32| {
        json!({"ClientRequestToken": "idem-token", "TransactItems": [
            put_member(&table, json!({"pk": s(pk), "v": n(v)}))
        ]})
    };
    tx_write(ops, request("idem-m1", 1)).await.unwrap();

    let error = tx_write(ops, request("idem-m2", 1)).await.unwrap_err();
    assert_error(
        &error,
        "IdempotentParameterMismatchException",
        IDEMPOTENT_MISMATCH,
    );
    assert!(pk_item(ops, &table, "idem-m2").await.is_null());
    assert_eq!(
        pk_item(ops, &table, "idem-m1").await,
        json!({"pk": s("idem-m1"), "v": n(1)})
    );

    // Same key, different content under the same token is refused too.
    let error = tx_write(ops, request("idem-m1", 2)).await.unwrap_err();
    assert_error(
        &error,
        "IdempotentParameterMismatchException",
        IDEMPOTENT_MISMATCH,
    );
    assert_eq!(
        pk_item(ops, &table, "idem-m1").await,
        json!({"pk": s("idem-m1"), "v": n(1)})
    );

    tx_write(ops, request("idem-m1", 1))
        .await
        .expect("identical replay is a no-op");
    assert_eq!(
        pk_item(ops, &table, "idem-m1").await,
        json!({"pk": s("idem-m1"), "v": n(1)})
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_conc_ddb_optimistic_token_fingerprint_covers_return_options_rejected.
/// py
///
/// Capacity values are not asserted (capacity is unsupported). Changing
/// `ReturnConsumedCapacity` or `ReturnItemCollectionMetrics` is kept as an
/// explicit gap check on the token fingerprint.
#[tokio::test]
async fn given_committed_token_when_return_options_change_then_mismatch_and_add_applies_once() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("ctr"), "n": n(0)})).await;
    let request = |extra: Value| {
        let mut request = json!({"ClientRequestToken": "fingerprint-token", "TransactItems": [
            {"Update": {
                "TableName": table, "Key": {"pk": s("ctr")}, "UpdateExpression": "ADD n :one",
                "ExpressionAttributeValues": {":one": n(1)}
            }}
        ]});
        for (name, value) in extra.as_object().unwrap() {
            request[name] = value.clone();
        }
        request
    };
    let count = || async { pk_item(ops, &table, "ctr").await["n"].clone() };

    tx_write(ops, request(json!({"ReturnConsumedCapacity": "TOTAL"})))
        .await
        .unwrap();
    assert_eq!(count().await, n(1));
    tx_write(ops, request(json!({"ReturnConsumedCapacity": "TOTAL"})))
        .await
        .expect("exact replay succeeds");
    assert_eq!(count().await, n(1), "exact replay does not re-apply");

    let error = tx_write(ops, request(json!({"ReturnConsumedCapacity": "NONE"})))
        .await
        .unwrap_err();
    assert_error(
        &error,
        "IdempotentParameterMismatchException",
        IDEMPOTENT_MISMATCH,
    );
    assert_eq!(count().await, n(1));

    let error = tx_write(
        ops,
        request(json!({"ReturnConsumedCapacity": "TOTAL", "ReturnItemCollectionMetrics": "SIZE"})),
    )
    .await
    .unwrap_err();
    assert_error(
        &error,
        "IdempotentParameterMismatchException",
        IDEMPOTENT_MISMATCH,
    );
    assert_eq!(count().await, n(1));

    let mut fresh = request(json!({}));
    fresh["ClientRequestToken"] = json!("fingerprint-token-fresh");
    tx_write(ops, fresh).await.unwrap();
    assert_eq!(count().await, n(2), "a fresh token applies the ADD again");
    store.finish().await;
}

// ---------------------------------------------------------------------------
// Concurrent item writes
// ---------------------------------------------------------------------------

/// AWS behavioural: check_conc_ddb_optimistic_delete_race_losers_see_no_item.py
#[tokio::test]
async fn given_eight_version_guarded_deletes_when_racing_then_one_deleter_and_losers_see_no_item() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let seeded = json!({"pk": s("lease"), "version": n(3), "holder": s("seed")});
    seed(ops, &table, seeded.clone()).await;
    let delete = |expected: u32| {
        json!({
            "TableName": table, "Key": {"pk": s("lease")},
            "ConditionExpression": "version = :expected",
            "ExpressionAttributeValues": {":expected": n(expected)},
            "ReturnValues": "ALL_OLD", "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
        })
    };

    let control = delete_item(ops, delete(2)).await.unwrap_err();
    assert_ccf(&control);
    assert_eq!(
        control.1["Item"], seeded,
        "a live item rides on the refusal"
    );
    assert_eq!(pk_item(ops, &table, "lease").await, seeded);

    let outcomes = race(RACERS, |_| {
        let ops = Arc::clone(&store.operations);
        let request = delete(3);
        async move { delete_item(&ops, request).await }
    })
    .await;
    let won = winners(&outcomes);
    assert_eq!(won.len(), 1, "exactly one caller deletes the item");
    assert_eq!(outcomes[won[0]].as_ref().unwrap()["Attributes"], seeded);
    let lost = losers(&outcomes);
    assert_eq!(lost.len(), RACERS - 1);
    for (_, error) in lost {
        assert_ccf(error);
        assert!(
            error.1.get("Item").is_none(),
            "no ALL_OLD image of a deleted item"
        );
    }
    assert!(pk_item(ops, &table, "lease").await.is_null());
    store.finish().await;
}

/// AWS behavioural:
/// check_conc_ddb_optimistic_loser_all_old_is_winner_post_image_rejected.py
#[tokio::test]
async fn given_eight_version_guarded_updates_when_racing_then_loser_item_is_winner_post_image() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(
        ops,
        &table,
        json!({"pk": s("cart"), "version": n(0), "writer": s("seed")}),
    )
    .await;
    let update = |expected: u32, me: &str, all_old: bool, all_new: bool| {
        let mut request = json!({
            "TableName": table, "Key": {"pk": s("cart")},
            "UpdateExpression": "SET version = :next, writer = :me",
            "ConditionExpression": "version = :expected",
            "ExpressionAttributeValues": {
                ":expected": n(expected), ":next": n(expected + 1), ":me": s(me)}
        });
        if all_old {
            request["ReturnValuesOnConditionCheckFailure"] = json!("ALL_OLD");
        }
        if all_new {
            request["ReturnValues"] = json!("ALL_NEW");
        }
        request
    };
    let ids = (0..RACERS).map(|i| format!("w{i}")).collect::<Vec<_>>();
    let outcomes = race(RACERS, |index| {
        let ops = Arc::clone(&store.operations);
        let request = update(0, &ids[index], true, true);
        async move { update_item(&ops, request).await }
    })
    .await;
    let won = winners(&outcomes);
    assert_eq!(won.len(), 1);
    let post_image = outcomes[won[0]].as_ref().unwrap()["Attributes"].clone();
    assert_eq!(
        post_image,
        json!({"pk": s("cart"), "version": n(1), "writer": s(&ids[won[0]])})
    );
    let lost = losers(&outcomes);
    assert_eq!(lost.len(), RACERS - 1);
    for (_, error) in &lost {
        assert_ccf(error);
        assert_eq!(error.1["Item"], post_image);
    }

    let bare = update_item(ops, update(0, "stale-no-param", false, false))
        .await
        .unwrap_err();
    assert_ccf(&bare);
    assert!(bare.1.get("Item").is_none(), "no parameter, no Item");
    assert_eq!(
        pk_item(ops, &table, "cart").await,
        post_image,
        "refusals change nothing"
    );

    // Each loser retries from the image it holds, never reading.
    let mut last = String::new();
    for (index, error) in lost {
        let mut held = error.1["Item"].clone();
        let mut landed = false;
        for _ in 0..2 {
            let version: u32 = held["version"]["N"].as_str().unwrap().parse().unwrap();
            match update_item(ops, update(version, &ids[index], true, false)).await {
                Ok(_) => {
                    landed = true;
                    break;
                }
                Err(error) => {
                    assert_ccf(&error);
                    held = error.1["Item"].clone();
                    assert!(!held.is_null(), "retry refusal carries the current item");
                }
            }
        }
        assert!(
            landed,
            "{} could not land from its ALL_OLD image",
            ids[index]
        );
        last = ids[index].clone();
    }
    let final_item = pk_item(ops, &table, "cart").await;
    assert_eq!(final_item["version"], n(RACERS));
    assert_eq!(final_item["writer"], s(&last));
    store.finish().await;
}

/// AWS behavioural: check_conc_ddb_optimistic_put_race_winner_payload_intact.py
#[tokio::test]
async fn given_version_guarded_puts_when_racing_over_rounds_then_item_holds_only_winner_attributes()
{
    const ROUNDS: usize = 4;
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(
        ops,
        &table,
        json!({
            "pk": s("doc"), "version": n(0), "writer": s("seed"), "only_seed": s("seed")
        }),
    )
    .await;
    for round in 0..ROUNDS {
        let ids = (0..RACERS)
            .map(|i| format!("r{round}w{i}"))
            .collect::<Vec<_>>();
        let outcomes = race(RACERS, |index| {
            let ops = Arc::clone(&store.operations);
            let me = ids[index].clone();
            let request = json!({
                "TableName": table,
                "Item": {"pk": s("doc"), "version": n(round + 1), "writer": s(&me),
                         format!("only_{me}"): s(&me)},
                "ConditionExpression": "version = :expected",
                "ExpressionAttributeValues": {":expected": n(round)}
            });
            async move { put_item(&ops, request).await }
        })
        .await;
        let won = winners(&outcomes);
        assert_eq!(won.len(), 1, "round {round}: one winner");
        for (_, error) in losers(&outcomes) {
            assert_ccf(error);
        }
        let stored = pk_item(ops, &table, "doc").await;
        let attributes = stored
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let expected = ["pk", "version", "writer"]
            .into_iter()
            .map(str::to_owned)
            .chain([format!("only_{}", ids[won[0]])])
            .collect::<BTreeSet<_>>();
        assert_eq!(attributes, expected, "round {round}");
        assert_eq!(stored["writer"], s(&ids[won[0]]));
        assert_eq!(stored["version"], n(round + 1));
    }
    assert_eq!(pk_item(ops, &table, "doc").await["version"], n(ROUNDS));
    store.finish().await;
}

/// AWS behavioural:
/// check_conc_ddb_optimistic_version_race_one_winner_per_round.py
#[tokio::test]
async fn given_version_guarded_updates_when_racing_over_rounds_then_history_equals_winners() {
    const ROUNDS: usize = 5;
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(
        ops,
        &table,
        json!({
            "pk": s("account"), "version": n(0), "writer": s("seed"), "history": {"L": []}
        }),
    )
    .await;
    let mut history = Vec::new();
    for round in 0..ROUNDS {
        let ids = (0..RACERS)
            .map(|i| format!("r{round}-w{i}"))
            .collect::<Vec<_>>();
        let outcomes = race(RACERS, |index| {
            let ops = Arc::clone(&store.operations);
            let request = json!({
                "TableName": table, "Key": {"pk": s("account")},
                "UpdateExpression":
                    "SET version = :next, writer = :me, history = list_append(history, :me_l)",
                "ConditionExpression": "version = :expected",
                "ExpressionAttributeValues": {
                    ":expected": n(round), ":next": n(round + 1), ":me": s(&ids[index]),
                    ":me_l": {"L": [s(&ids[index])]}}
            });
            async move { update_item(&ops, request).await }
        })
        .await;
        let won = winners(&outcomes);
        assert_eq!(won.len(), 1, "round {round}: one winner");
        for (_, error) in losers(&outcomes) {
            assert_ccf(error);
        }
        let stored = pk_item(ops, &table, "account").await;
        assert_eq!(stored["version"], n(round + 1));
        assert_eq!(stored["writer"], s(&ids[won[0]]));
        history.push(ids[won[0]].clone());
    }
    let stored = pk_item(ops, &table, "account").await;
    assert_eq!(stored["version"], n(ROUNDS));
    let recorded = stored["history"]["L"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["S"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(recorded, history);
    store.finish().await;
}

/// AWS behavioural:
/// check_conc_ddb_optimistic_transact_race_cancelled_leaves_no_partial_write.py
#[tokio::test]
async fn given_eight_transactions_on_version_guarded_item_when_racing_then_cancelled_ones_write_nothing()
 {
    const ROUNDS: usize = 3;
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("shared"), "version": n(0)})).await;
    let request = |expected: u64, private: &str| {
        json!({"TransactItems": [
            {"Update": {
                "TableName": table, "Key": {"pk": s("shared")},
                "UpdateExpression": "SET version = :next, last_tx = :me",
                "ConditionExpression": "version = :expected",
                "ExpressionAttributeValues": {
                    ":expected": n(expected), ":next": n(expected + 1), ":me": s(private)}
            }},
            {"Put": {"TableName": table, "Item": {"pk": s(private)},
                     "ConditionExpression": "attribute_not_exists(pk)"}},
            {"ConditionCheck": {"TableName": table,
                                "Key": {"pk": s(&format!("anchor-{private}"))},
                                "ConditionExpression": "attribute_exists(pk)"}}
        ]})
    };
    let version = || async {
        let stored = pk_item(ops, &table, "shared").await;
        stored["version"]["N"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };

    seed(ops, &table, json!({"pk": s("anchor-private-control")})).await;
    tx_write(ops, request(0, "private-control"))
        .await
        .expect("uncontended control commits");
    let mut commits = 1;
    for round in 0..ROUNDS {
        let expected = version().await;
        let privates = (0..RACERS)
            .map(|i| format!("private-r{round}-t{i}"))
            .collect::<Vec<_>>();
        for private in &privates {
            seed(ops, &table, json!({"pk": s(&format!("anchor-{private}"))})).await;
        }
        let outcomes = race(RACERS, |index| {
            let ops = Arc::clone(&store.operations);
            let request = request(expected, &privates[index]);
            async move { tx_write(&ops, request).await }
        })
        .await;
        let won = winners(&outcomes);
        assert!(won.len() <= 1, "round {round}: at most one commit");
        for (index, error) in losers(&outcomes) {
            let codes = reason_codes(error);
            assert_eq!(codes.len(), 3, "positional reasons");
            assert_ne!(codes[0], "None", "the contended slot names its failure");
            assert_eq!(&codes[1..], ["None", "None"]);
            assert_error(
                error,
                "TransactionCanceledException",
                &cancelled_message(&codes.iter().map(String::as_str).collect::<Vec<_>>()),
            );
            assert!(pk_item(ops, &table, &privates[index]).await.is_null());
        }
        commits += won.len() as u64;
        if let Some(&winner) = won.first() {
            assert_eq!(
                pk_item(ops, &table, "shared").await["last_tx"],
                s(&privates[winner])
            );
        }
        assert_eq!(version().await, commits);
    }
    store.finish().await;
}

/// AWS behavioural: check_plain_write_racing_a_transaction_rejected.py
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_plain_updates_and_transactions_on_one_key_when_racing_then_refusals_are_conflicts_and_counter_is_exact()
 {
    const ROUNDS: usize = 5;
    const TRANSACTORS: usize = 3;
    const PLAIN: usize = 3;
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("CONTENDED"), "hits": n(0)})).await;
    let bump = json!({
        "TableName": table, "Key": {"pk": s("CONTENDED")}, "UpdateExpression": "ADD hits :one",
        "ExpressionAttributeValues": {":one": n(1)}
    });
    let mut accepted = 0;
    for _ in 0..ROUNDS {
        let outcomes = race(TRANSACTORS + PLAIN, |index| {
            let ops = Arc::clone(&store.operations);
            let bump = bump.clone();
            async move {
                if index < TRANSACTORS {
                    tx_write(&ops, json!({"TransactItems": [{"Update": bump}]})).await
                } else {
                    update_item(&ops, bump).await
                }
            }
        })
        .await;
        accepted += winners(&outcomes).len();
        for (index, error) in losers(&outcomes) {
            if index < TRANSACTORS {
                assert_error(
                    error,
                    "TransactionCanceledException",
                    &cancelled_message(&["TransactionConflict"]),
                );
            } else {
                assert_eq!(
                    error.1["__type"],
                    "com.amazonaws.dynamodb.v20120810#TransactionConflictException",
                    "{:?}",
                    error.1
                );
            }
        }
    }
    assert_eq!(pk_item(ops, &table, "CONTENDED").await["hits"], n(accepted));
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_conflict_reason_is_transaction_conflict.py
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_overlapping_two_item_transactions_when_racing_then_cancellations_name_transaction_conflict_on_shared_slot()
 {
    const TRANSACTIONS: usize = 12;
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("shared"), "hits": n(0)})).await;
    let outcomes = race(TRANSACTIONS, |index| {
        let ops = Arc::clone(&store.operations);
        let request = json!({"TransactItems": [
            {"Update": {"TableName": table, "Key": {"pk": s("shared")},
                        "UpdateExpression": "ADD hits :one",
                        "ExpressionAttributeValues": {":one": n(1)}}},
            put_member(&table, json!({"pk": s(&format!("own-{index}"))}))
        ]});
        async move { tx_write(&ops, request).await }
    })
    .await;
    // Zero cancellations is allowed; the shared counter equals the commits.
    for (_, error) in losers(&outcomes) {
        assert_cancelled(
            error,
            &[
                (
                    "TransactionConflict",
                    Some("Transaction is ongoing for the item"),
                ),
                ("None", None),
            ],
        );
    }
    assert_eq!(
        pk_item(ops, &table, "shared").await["hits"],
        n(winners(&outcomes).len())
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_skew_on_absent_keys_never_commits_both.py
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_two_transactions_checking_each_others_absent_key_when_racing_then_never_both_commit()
{
    const ROUNDS: usize = 6;
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let mut commits = 0;
    for round in 0..ROUNDS {
        let keys = [format!("x-{round:02}"), format!("y-{round:02}")];
        let outcomes = race(2, |index| {
            let ops = Arc::clone(&store.operations);
            let request = json!({"TransactItems": [
                {"ConditionCheck": {"TableName": table, "Key": {"pk": s(&keys[1 - index])},
                                    "ConditionExpression": "attribute_not_exists(pk)"}},
                put_member(&table, json!({"pk": s(&keys[index])}))
            ]});
            async move { tx_write(&ops, request).await }
        })
        .await;
        assert!(
            !losers(&outcomes).is_empty(),
            "round {round}: both transactions committed"
        );
        commits += winners(&outcomes).len();
        for (index, outcome) in outcomes.iter().enumerate() {
            let stored = pk_item(ops, &table, &keys[index]).await;
            match outcome {
                Ok(_) => assert_eq!(stored, json!({"pk": s(&keys[index])})),
                Err(error) => {
                    let codes = reason_codes(error);
                    assert_eq!(codes.len(), 2);
                    assert!(codes.iter().all(|code| {
                        ["None", "ConditionalCheckFailed", "TransactionConflict"]
                            .contains(&code.as_str())
                    }));
                    assert!(codes.iter().any(|code| code != "None"));
                    assert_error(
                        error,
                        "TransactionCanceledException",
                        &cancelled_message(&codes.iter().map(String::as_str).collect::<Vec<_>>()),
                    );
                    assert!(stored.is_null(), "a cancelled Put is not stored");
                }
            }
        }
    }
    assert!(commits >= ROUNDS, "every round has at least one commit");
    store.finish().await;
}

/// AWS behavioural: check_e2e_order_fulfillment_concurrent_stock_contention.py
#[tokio::test]
async fn given_one_unit_of_stock_when_two_settlements_race_then_one_fulfils_and_loser_is_cancelled()
{
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(
        ops,
        &table,
        json!({"pk": s("STOCK#sku-scarce"), "quantity": n(1)}),
    )
    .await;
    let orders = ["ORDER#A", "ORDER#B"];
    let reservations = ["RESERVATION#A", "RESERVATION#B"];
    for index in 0..2 {
        seed(
            ops,
            &table,
            json!({
                "pk": s(orders[index]), "status": s("RESERVED"), "sku": s("STOCK#sku-scarce")
            }),
        )
        .await;
        seed(ops, &table, json!({
            "pk": s(reservations[index]), "order": s(orders[index]), "qty": n(1), "status": s("HELD")
        }))
        .await;
    }
    let outcomes = race(2, |index| {
        let ops = Arc::clone(&store.operations);
        let request = json!({"TransactItems": [
            {"Update": {
                "TableName": table, "Key": {"pk": s("STOCK#sku-scarce")},
                "UpdateExpression": "SET quantity = quantity - :qty",
                "ConditionExpression": "quantity >= :qty",
                "ExpressionAttributeValues": {":qty": n(1)}}},
            {"Update": {
                "TableName": table, "Key": {"pk": s(orders[index])},
                "UpdateExpression": "SET #s = :new_status",
                "ConditionExpression": "#s = :expected_status",
                "ExpressionAttributeNames": {"#s": "status"},
                "ExpressionAttributeValues": {
                    ":new_status": s("FULFILLED"), ":expected_status": s("RESERVED")}}},
            {"Delete": {"TableName": table, "Key": {"pk": s(reservations[index])},
                        "ConditionExpression": "attribute_exists(pk)"}}
        ]});
        async move { tx_write(&ops, request).await }
    })
    .await;
    let won = winners(&outcomes);
    assert_eq!(won.len(), 1, "exactly one settlement commits");
    let (winner, loser) = (won[0], 1 - won[0]);
    let error = outcomes[loser].as_ref().unwrap_err();
    assert_eq!(
        error.1["__type"],
        "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
    );
    assert!(message(error).starts_with(
        "Transaction cancelled, please refer cancellation reasons for specific reasons ["
    ));
    assert_eq!(
        pk_item(ops, &table, orders[winner]).await["status"],
        s("FULFILLED")
    );
    assert!(pk_item(ops, &table, reservations[winner]).await.is_null());
    assert_eq!(
        pk_item(ops, &table, "STOCK#sku-scarce").await["quantity"],
        n(0)
    );
    assert_eq!(
        pk_item(ops, &table, orders[loser]).await["status"],
        s("RESERVED")
    );
    assert_eq!(
        pk_item(ops, &table, reservations[loser]).await["status"],
        s("HELD")
    );
    store.finish().await;
}

/// AWS behavioural: check_e2e_order_fulfillment_conditional_workflow.py
#[tokio::test]
async fn given_reservation_and_status_change_when_settlement_transaction_runs_then_stock_order_and_reservation_are_consistent()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(
        ops,
        &table,
        json!({"pk": s("STOCK#sku-1"), "quantity": n(5)}),
    )
    .await;
    seed(
        ops,
        &table,
        json!({"pk": s("ORDER#1"), "status": s("PENDING"), "sku": s("STOCK#sku-1")}),
    )
    .await;
    put_item(ops, json!({
        "TableName": table,
        "Item": {"pk": s("RESERVATION#1"), "order": s("ORDER#1"), "qty": n(2), "status": s("HELD")},
        "ConditionExpression": "attribute_not_exists(pk)"
    }))
    .await
    .unwrap();
    update_item(ops, json!({
        "TableName": table, "Key": {"pk": s("ORDER#1")},
        "UpdateExpression": "SET #s = :new_status", "ConditionExpression": "#s = :expected_status",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":new_status": s("RESERVED"), ":expected_status": s("PENDING")}
    }))
    .await
    .unwrap();
    tx_write(ops, json!({"TransactItems": [
        {"Update": {
            "TableName": table, "Key": {"pk": s("STOCK#sku-1")},
            "UpdateExpression": "SET quantity = quantity - :qty",
            "ConditionExpression": "quantity >= :qty", "ExpressionAttributeValues": {":qty": n(2)}}},
        {"Update": {
            "TableName": table, "Key": {"pk": s("ORDER#1")},
            "UpdateExpression": "SET #s = :new_status",
            "ConditionExpression": "#s = :expected_status",
            "ExpressionAttributeNames": {"#s": "status"},
            "ExpressionAttributeValues": {
                ":new_status": s("FULFILLED"), ":expected_status": s("RESERVED")}}},
        {"Delete": {"TableName": table, "Key": {"pk": s("RESERVATION#1")},
                    "ConditionExpression": "attribute_exists(pk)"}}
    ]}))
    .await
    .unwrap();
    assert_eq!(
        pk_item(&store.operations, &table, "STOCK#sku-1").await["quantity"],
        n(3)
    );
    assert_eq!(
        pk_item(&store.operations, &table, "ORDER#1").await["status"],
        s("FULFILLED")
    );
    assert!(
        pk_item(&store.operations, &table, "RESERVATION#1")
            .await
            .is_null()
    );
    store.finish().await;
}

// ---------------------------------------------------------------------------
// TransactGetItems
// ---------------------------------------------------------------------------

/// AWS behavioural:
/// check_transact_get_items_aggregate_size_over_4mb_rejected.py
#[tokio::test]
async fn given_items_over_4mb_in_aggregate_when_transact_get_runs_then_validation_error_and_under_limit_returns_whole()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let items = (0..11u64)
        .map(|i| data_item(&format!("big-{i:02}"), 390 * 1024, 100 + i))
        .collect::<Vec<_>>();
    for item in &items {
        seed(ops, &table, item.clone()).await;
    }
    let gets = |count: usize| {
        json!({"TransactItems": items[..count].iter().map(|item| {
            json!({"Get": {"TableName": table, "Key": {"pk": item["pk"]}}})
        }).collect::<Vec<_>>()})
    };
    let response = tx_get(ops, gets(10))
        .await
        .expect("10 items stay under 4 MB");
    let responses = response["Responses"].as_array().unwrap();
    assert_eq!(responses.len(), 10);
    for (response, item) in responses.iter().zip(&items) {
        assert_eq!(response["Item"], *item);
    }

    let error = tx_get(ops, gets(11)).await.unwrap_err();
    assert_validation(&error, "Payload in response is too large.");
    let last = items.last().unwrap();
    assert_eq!(item(ops, &table, json!({"pk": last["pk"]})).await, *last);
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_get_items_at_the_100_item_cap_across_two_tables.py
#[tokio::test]
async fn given_hundred_gets_alternating_two_tables_when_transact_get_runs_then_responses_follow_request_order()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let tables = [new_table(&store).await, new_table(&store).await];
    let mut members = Vec::new();
    for i in 0..100usize {
        let table = &tables[i % 2];
        let key = json!({"pk": s(&format!("k{i}"))});
        seed(ops, table, json!({"pk": key["pk"], "slot": n(i)})).await;
        members.push(json!({"Get": {"TableName": table, "Key": key}}));
    }
    let response = tx_get(ops, json!({"TransactItems": members}))
        .await
        .unwrap();
    let responses = response["Responses"].as_array().unwrap();
    assert_eq!(responses.len(), 100);
    for (i, entry) in responses.iter().enumerate() {
        assert_eq!(entry["Item"]["slot"], n(i), "slot {i}");
    }
    store.finish().await;
}

/// AWS behavioural: check_transact_get_items_never_observes_a_torn_transfer.py
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_concurrent_transfers_when_transact_get_reads_then_every_read_sums_to_total() {
    const MOVES: usize = 10;
    const READS: usize = 10;
    const AMOUNT: u32 = 10;
    const TOTAL: u32 = 100;
    const ATTEMPTS: usize = 500;
    let store = TestStore::new().await;
    let table = new_table(&store).await;
    for name in ["A", "B"] {
        seed(
            &store.operations,
            &table,
            json!({"pk": s(name), "bal": n(TOTAL / 2)}),
        )
        .await;
    }
    let writer = {
        let (ops, table) = (Arc::clone(&store.operations), table.clone());
        tokio::spawn(async move {
            let mut failures = Vec::new();
            for i in 0..MOVES {
                let (src, dst) = if i % 2 == 0 { ("A", "B") } else { ("B", "A") };
                let mut done = false;
                for _ in 0..ATTEMPTS {
                    let request = json!({"TransactItems": [
                        {"Update": {"TableName": table, "Key": {"pk": s(src)},
                                    "UpdateExpression": "SET bal = bal - :d",
                                    "ConditionExpression": "bal >= :d",
                                    "ExpressionAttributeValues": {":d": n(AMOUNT)}}},
                        {"Update": {"TableName": table, "Key": {"pk": s(dst)},
                                    "UpdateExpression": "SET bal = bal + :d",
                                    "ExpressionAttributeValues": {":d": n(AMOUNT)}}}
                    ]});
                    match tx_write(&ops, request).await {
                        Ok(_) => {
                            done = true;
                            break;
                        }
                        Err(error) => {
                            let cancelled = error.1["__type"]
                                == "com.amazonaws.dynamodb.v20120810#TransactionCanceledException";
                            if !cancelled
                                || reason_codes(&error)
                                    .contains(&"ConditionalCheckFailed".to_owned())
                            {
                                failures.push(format!("{src}->{dst}: {:?}", error.1));
                                done = true;
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    }
                }
                if !done {
                    failures.push(format!("{src}->{dst}: still conflicting"));
                }
            }
            failures
        })
    };
    let reader = {
        let (ops, table) = (Arc::clone(&store.operations), table.clone());
        tokio::spawn(async move {
            let mut pairs = Vec::new();
            for _ in 0..READS {
                for _ in 0..ATTEMPTS {
                    let request = json!({"TransactItems": [
                        {"Get": {"TableName": table, "Key": {"pk": s("A")}}},
                        {"Get": {"TableName": table, "Key": {"pk": s("B")}}}
                    ]});
                    match tx_get(&ops, request).await {
                        Ok(response) => {
                            let balance = |slot: usize| -> u32 {
                                response["Responses"][slot]["Item"]["bal"]["N"]
                                    .as_str()
                                    .unwrap()
                                    .parse()
                                    .unwrap()
                            };
                            pairs.push((balance(0), balance(1)));
                            break;
                        }
                        Err(error) => {
                            assert_eq!(
                                error.1["__type"],
                                "com.amazonaws.dynamodb.v20120810#TransactionCanceledException"
                            );
                            tokio::task::yield_now().await;
                        }
                    }
                }
            }
            pairs
        })
    };
    let failures = writer.await.unwrap();
    let pairs = reader.await.unwrap();
    assert!(failures.is_empty(), "writer failures: {failures:?}");
    assert_eq!(pairs.len(), READS);
    let torn = pairs
        .iter()
        .filter(|(a, b)| a + b != TOTAL)
        .collect::<Vec<_>>();
    assert!(torn.is_empty(), "torn reads: {torn:?}");
    let ops = &store.operations;
    assert_eq!(pk_item(ops, &table, "A").await["bal"], n(TOTAL / 2));
    assert_eq!(pk_item(ops, &table, "B").await["bal"], n(TOTAL / 2));
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_get_items_top_level_vs_cancellation_error_class.py
#[tokio::test]
async fn given_bad_table_projection_or_key_when_transact_get_runs_then_error_class_matches_defect()
{
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = make_table(&store, "pk", Some("sk"), false).await;
    let missing = name_of(&unique_table_name());
    seed(
        ops,
        &table,
        json!({"pk": s("item1"), "sk": s("main"), "color": s("blue")}),
    )
    .await;
    let key = json!({"pk": s("item1"), "sk": s("main")});
    let valid = json!({"Get": {"TableName": table, "Key": key}});

    let control = tx_get(ops, json!({"TransactItems": [valid]}))
        .await
        .unwrap();
    assert_eq!(control["Responses"][0]["Item"]["color"], s("blue"));

    let error = tx_get(
        ops,
        json!({"TransactItems": [
            valid, {"Get": {"TableName": missing, "Key": key}}
        ]}),
    )
    .await
    .unwrap_err();
    assert_error(
        &error,
        "ResourceNotFoundException",
        "Requested resource not found",
    );
    assert!(error.1.get("CancellationReasons").is_none());

    let error = tx_get(
        ops,
        json!({"TransactItems": [
            {"Get": {"TableName": table, "Key": key, "ProjectionExpression": "color AND"}}
        ]}),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        "Invalid ProjectionExpression: Syntax error; token: \"AND\", near: \"color AND\"",
    );

    let error = tx_get(
        ops,
        json!({"TransactItems": [
            valid, {"Get": {"TableName": table, "Key": {"pk": s("item1")}}}
        ]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("None", None),
            (
                "ValidationError",
                Some("The provided key element does not match the schema"),
            ),
        ],
    );

    let projected = tx_get(
        ops,
        json!({"TransactItems": [
            {"Get": {"TableName": table, "Key": key, "ProjectionExpression": "absent"}}
        ]}),
    )
    .await
    .unwrap();
    assert!(projected["Responses"][0].get("Item").is_none());
    store.finish().await;
}

// ---------------------------------------------------------------------------
// TransactWriteItems: cancellation reasons and atomicity
// ---------------------------------------------------------------------------

/// AWS behavioural:
/// check_transact_update_condition_needs_item_cancels_the_passing_sibling.py
#[tokio::test]
async fn given_update_condition_needing_absent_item_when_transaction_runs_then_passing_sibling_is_cancelled_and_standalone_creates_nothing()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("sibling"), "count": n(10)})).await;
    let absent_update = json!({
        "TableName": table, "Key": {"pk": s("absent")}, "UpdateExpression": "SET seen = :one",
        "ConditionExpression": "score > :zero",
        "ExpressionAttributeValues": {":one": n(1), ":zero": n(0)}
    });
    let error = tx_write(ops, json!({"TransactItems": [
        {"Update": absent_update},
        {"Update": {"TableName": table, "Key": {"pk": s("sibling")},
                    "UpdateExpression": "SET #c = #c + :one", "ConditionExpression": "#c = :ten",
                    "ExpressionAttributeNames": {"#c": "count"},
                    "ExpressionAttributeValues": {":one": n(1), ":ten": n(10)}}}
    ]}))
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
            ("None", None),
        ],
    );
    assert_eq!(pk_item(ops, &table, "sibling").await["count"], n(10));

    let error = update_item(ops, absent_update).await.unwrap_err();
    assert_ccf(&error);
    assert!(pk_item(ops, &table, "absent").await.is_null());
    store.finish().await;
}

/// AWS behavioural: check_transact_write_all_or_nothing.py
#[tokio::test]
async fn given_put_first_and_failing_put_condition_when_transaction_runs_then_nothing_is_written() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("tw-existing"), "v": n(1)})).await;
    let error = tx_write(
        ops,
        json!({"TransactItems": [
            put_member(&table, json!({"pk": s("tw-new"), "v": n(1)})),
            {"Put": {"TableName": table, "Item": {"pk": s("tw-existing"), "v": n(2)},
                     "ConditionExpression": "attribute_not_exists(pk)"}}
        ]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("None", None),
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
        ],
    );
    assert!(pk_item(ops, &table, "tw-new").await.is_null());
    assert_eq!(pk_item(ops, &table, "tw-existing").await["v"], n(1));

    tx_write(
        ops,
        json!({"TransactItems": [
            put_member(&table, json!({"pk": s("tw-new"), "v": n(1)})),
            put_member(&table, json!({"pk": s("tw-other"), "v": n(1)}))
        ]}),
    )
    .await
    .unwrap();
    assert!(!pk_item(ops, &table, "tw-new").await.is_null());
    assert!(!pk_item(ops, &table, "tw-other").await.is_null());
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_delete_with_failed_condition_cancels_other_deletes_too.
/// py
#[tokio::test]
async fn given_three_deletes_when_middle_condition_fails_then_none_apply_and_unconditioned_control_removes_all()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let items = ["d1", "d2", "d3"].map(|pk| json!({"pk": s(pk), "v": n(1)}));
    for item in &items {
        seed(ops, &table, item.clone()).await;
    }
    let error = tx_write(
        ops,
        json!({"TransactItems": [
            {"Delete": {"TableName": table, "Key": {"pk": s("d1")}}},
            {"Delete": {"TableName": table, "Key": {"pk": s("d2")},
                        "ConditionExpression": "attribute_not_exists(pk)"}},
            {"Delete": {"TableName": table, "Key": {"pk": s("d3")}}}
        ]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("None", None),
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
            ("None", None),
        ],
    );
    for (pk, item) in ["d1", "d2", "d3"].iter().zip(&items) {
        assert_eq!(pk_item(ops, &table, pk).await, *item);
    }

    tx_write(
        ops,
        json!({"TransactItems": [
            {"Delete": {"TableName": table, "Key": {"pk": s("d1")}}},
            {"Delete": {"TableName": table, "Key": {"pk": s("d3")}}}
        ]}),
    )
    .await
    .unwrap();
    assert!(pk_item(ops, &table, "d1").await.is_null());
    assert!(pk_item(ops, &table, "d3").await.is_null());
    assert_eq!(pk_item(ops, &table, "d2").await, items[1]);
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_update_attribute_not_exists_upserts.py
#[tokio::test]
async fn given_update_with_attribute_not_exists_when_key_absent_then_item_is_created_and_replay_cancels()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let request = json!({"TransactItems": [
        {"Update": {"TableName": table, "Key": {"pk": s("upsert")},
                    "UpdateExpression": "SET a = :a, b = :b",
                    "ConditionExpression": "attribute_not_exists(pk)",
                    "ExpressionAttributeValues": {":a": s("one"), ":b": n(2)}}},
        put_member(&table, json!({"pk": s("sibling"), "v": n(1)}))
    ]});
    tx_write(ops, request.clone()).await.unwrap();
    let created = json!({"pk": s("upsert"), "a": s("one"), "b": n(2)});
    assert_eq!(pk_item(ops, &table, "upsert").await, created);
    assert_eq!(
        pk_item(ops, &table, "sibling").await,
        json!({"pk": s("sibling"), "v": n(1)})
    );

    let error = tx_write(ops, request).await.unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
            ("None", None),
        ],
    );
    assert_eq!(pk_item(ops, &table, "upsert").await, created);
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_stale_version_update_cancels_its_companion_put.py
#[tokio::test]
async fn given_stale_version_guard_when_transaction_runs_then_companion_put_is_cancelled_and_current_version_commits_both()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = make_table(&store, "id", None, false).await;
    let account = json!({"id": s("account-7"), "version": n(41), "balance": n(100)});
    seed(ops, &table, account.clone()).await;
    let entry_key = json!({"id": s("ledger-7-42")});
    let transfer = |expected: u32| {
        json!({"TransactItems": [
            {"Update": {
                "TableName": table, "Key": {"id": s("account-7")},
                "UpdateExpression": "SET version = version + :one, balance = balance - :amt",
                "ConditionExpression": "version = :v",
                "ExpressionAttributeValues": {":one": n(1), ":amt": n(30), ":v": n(expected)}}},
            {"Put": {"TableName": table, "Item": {"id": s("ledger-7-42"), "amount": n(30)},
                     "ConditionExpression": "attribute_not_exists(id)"}}
        ]})
    };
    let entry = || item(ops, &table, entry_key.clone());
    let acct = || item(ops, &table, json!({"id": s("account-7")}));

    for stale in [40, 42] {
        let error = tx_write(ops, transfer(stale)).await.unwrap_err();
        assert_cancelled(
            &error,
            &[
                ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
                ("None", None),
            ],
        );
        assert_eq!(acct().await, account);
        assert!(entry().await.is_null());
    }
    tx_write(ops, transfer(41)).await.unwrap();
    let committed = json!({"id": s("account-7"), "version": n(42), "balance": n(70)});
    let ledger = json!({"id": s("ledger-7-42"), "amount": n(30)});
    assert_eq!(acct().await, committed);
    assert_eq!(entry().await, ledger);

    let error = tx_write(ops, transfer(41)).await.unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
        ],
    );
    assert_eq!(acct().await, committed);
    assert_eq!(entry().await, ledger);
    store.finish().await;
}

/// AWS behavioural: check_transact_write_multi_table_atomicity.py
#[tokio::test]
async fn given_two_tables_when_condition_on_second_fails_then_first_table_put_rolls_back() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let (table_a, table_b) = (new_table(&store).await, new_table(&store).await);
    seed(ops, &table_b, json!({"pk": s("row-b"), "v": s("1")})).await;
    let request = |put: &str| {
        json!({"TransactItems": [
            put_member(&table_a, json!({"pk": s(put)})),
            {"Update": {"TableName": table_b, "Key": {"pk": s("row-b")},
                        "UpdateExpression": "SET v = :two", "ConditionExpression": "v = :one",
                        "ExpressionAttributeValues": {":one": s("1"), ":two": s("2")}}}
        ]})
    };
    tx_write(ops, request("row-a-1")).await.unwrap();
    assert!(!pk_item(ops, &table_a, "row-a-1").await.is_null());
    assert_eq!(pk_item(ops, &table_b, "row-b").await["v"], s("2"));

    let error = tx_write(ops, request("row-a-new")).await.unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("None", None),
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
        ],
    );
    assert!(pk_item(ops, &table_a, "row-a-new").await.is_null());
    assert_eq!(pk_item(ops, &table_b, "row-b").await["v"], s("2"));
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_parenthesized_conditions_apply_and_cancel.py
#[tokio::test]
async fn given_parenthesized_conditions_when_transaction_runs_then_they_apply_and_failures_cancel_all()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let seeds = [
        json!({"pk": s("tw-put"), "status": s("pending"), "qty": n(5)}),
        json!({"pk": s("tw-update"), "status": s("active"), "qty": n(10)}),
        json!({"pk": s("tw-delete"), "status": s("ready"), "qty": n(0)}),
        json!({"pk": s("tw-check"), "status": s("checked"), "qty": n(1)}),
    ];
    for seeded in &seeds {
        seed(ops, &table, seeded.clone()).await;
    }
    let names = json!({"#s": "status"});
    tx_write(ops, json!({"TransactItems": [
        {"Put": {"TableName": table,
                 "Item": {"pk": s("tw-put"), "status": s("done"), "qty": n(99)},
                 "ConditionExpression": "(#s = :s) AND (qty = :q)",
                 "ExpressionAttributeNames": names,
                 "ExpressionAttributeValues": {":s": s("pending"), ":q": n(5)}}},
        {"Update": {"TableName": table, "Key": {"pk": s("tw-update")},
                    "UpdateExpression": "SET qty = :newq",
                    "ConditionExpression": "(#s = :s AND qty = :q)",
                    "ExpressionAttributeNames": names,
                    "ExpressionAttributeValues": {":s": s("active"), ":q": n(10), ":newq": n(20)}}},
        {"Delete": {"TableName": table, "Key": {"pk": s("tw-delete")},
                    "ConditionExpression": "(#s = :s AND (qty = :q))",
                    "ExpressionAttributeNames": names,
                    "ExpressionAttributeValues": {":s": s("ready"), ":q": n(0)}}},
        {"ConditionCheck": {"TableName": table, "Key": {"pk": s("tw-check")},
                            "ConditionExpression": "(#s = :s) AND (qty = :q)",
                            "ExpressionAttributeNames": names,
                            "ExpressionAttributeValues": {":s": s("checked"), ":q": n(1)}}}
    ]}))
    .await
    .unwrap();
    assert_eq!(
        pk_item(ops, &table, "tw-put").await,
        json!({"pk": s("tw-put"), "status": s("done"), "qty": n(99)})
    );
    assert_eq!(
        pk_item(ops, &table, "tw-update").await,
        json!({"pk": s("tw-update"), "status": s("active"), "qty": n(20)})
    );
    assert!(pk_item(ops, &table, "tw-delete").await.is_null());
    assert_eq!(pk_item(ops, &table, "tw-check").await, seeds[3]);

    let error = tx_write(ops, json!({"TransactItems": [
        put_member(&table, json!({"pk": s("tw-neg-put")})),
        {"Update": {"TableName": table, "Key": {"pk": s("tw-update")},
                    "UpdateExpression": "SET qty = :newq",
                    "ConditionExpression": "(#s = :s AND (qty = :q))",
                    "ExpressionAttributeNames": names,
                    "ExpressionAttributeValues": {":s": s("active"), ":q": n(999), ":newq": n(1)}}}
    ]}))
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[
            ("None", None),
            ("ConditionalCheckFailed", Some(CCF_MESSAGE)),
        ],
    );
    assert!(pk_item(ops, &table, "tw-neg-put").await.is_null());
    assert_eq!(pk_item(ops, &table, "tw-update").await["qty"], n(20));
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_condition_non_orderable_literal_rejected.py
#[tokio::test]
async fn given_condition_ordering_against_bool_when_any_action_kind_runs_then_top_level_validation_and_false_condition_cancels()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let base = json!({"pk": s("p1"), "n": n(5)});
    seed(ops, &table, base.clone()).await;
    let bad = json!({":v": {"BOOL": true}});
    let key = json!({"pk": s("p1")});
    let other = put_member(&table, json!({"pk": s("p2"), "note": s("side")}));
    let actions = [
        json!({"Put": {"TableName": table, "Item": {"pk": s("p1"), "n": n(5), "note": s("put")},
                       "ConditionExpression": "n > :v", "ExpressionAttributeValues": bad}}),
        json!({"Delete": {"TableName": table, "Key": key, "ConditionExpression": "n > :v",
                          "ExpressionAttributeValues": bad}}),
        json!({"Update": {"TableName": table, "Key": key, "UpdateExpression": "SET note = :c",
                          "ConditionExpression": "n > :v",
                          "ExpressionAttributeValues": {":v": {"BOOL": true}, ":c": s("update")}}}),
        json!({"ConditionCheck": {"TableName": table, "Key": key, "ConditionExpression": "n > :v",
                                  "ExpressionAttributeValues": bad}}),
    ];
    for action in actions {
        let error = tx_write(ops, json!({"TransactItems": [action, other]}))
            .await
            .unwrap_err();
        assert_validation(
            &error,
            "Invalid ConditionExpression: Incorrect operand type for operator or function; \
             operator or function: >, operand type: BOOL",
        );
    }
    assert_eq!(pk_item(ops, &table, "p1").await, base);
    assert!(pk_item(ops, &table, "p2").await.is_null());

    let error = tx_write(
        ops,
        json!({"TransactItems": [{"ConditionCheck": {
        "TableName": table, "Key": key, "ConditionExpression": "n > :v",
        "ExpressionAttributeValues": {":v": n(9)}}}]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(&error, &[("ConditionalCheckFailed", Some(CCF_MESSAGE))]);

    tx_write(
        ops,
        json!({"TransactItems": [
            {"Update": {"TableName": table, "Key": key, "UpdateExpression": "SET note = :c",
                        "ConditionExpression": "n > :v",
                        "ExpressionAttributeValues": {":v": n(0), ":c": s("update")}}},
            other
        ]}),
    )
    .await
    .unwrap();
    assert_eq!(pk_item(ops, &table, "p1").await["note"], s("update"));
    assert!(!pk_item(ops, &table, "p2").await.is_null());
    store.finish().await;
}

// ---------------------------------------------------------------------------
// TransactWriteItems: validation before cancellation
// ---------------------------------------------------------------------------

/// AWS behavioural:
/// check_transact_write_items_condition_expression_defects_match_putitem_message.
/// py
#[tokio::test]
async fn given_malformed_put_condition_when_transaction_runs_then_message_is_putitem_message_minus_prefix()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    tx_write(
        ops,
        json!({"TransactItems": [{"Put": {
        "TableName": table, "Item": {"pk": s("control")},
        "ConditionExpression": "attribute_not_exists(pk)"}}]}),
    )
    .await
    .unwrap();
    assert!(!pk_item(ops, &table, "control").await.is_null());

    let over_4kb = format!(
        "attribute_exists(pk){}",
        " ".repeat(4097 - "attribute_exists(pk)".len())
    );
    let prefix = "1 validation error detected: ";
    let defects = [
        (
            "empty",
            String::new(),
            "Invalid ConditionExpression: The expression can not be empty;",
        ),
        (
            "malformed",
            "attribute_exists((".to_owned(),
            "Invalid ConditionExpression: Syntax error; token: \"<EOF>\", near: \"(\"",
        ),
        (
            "over-4kb",
            over_4kb,
            "Invalid ConditionExpression: Expression size has exceeded the maximum allowed size;",
        ),
    ];
    for (label, expression, expected) in defects {
        let put_key = format!("single-{label}");
        let error = put_item(
            ops,
            json!({
                "TableName": table, "Item": {"pk": s(&put_key)}, "ConditionExpression": expression
            }),
        )
        .await
        .unwrap_err();
        assert_validation(&error, &format!("{prefix}{expected}"));

        let tx_key = format!("transact-{label}");
        let error = tx_write(
            ops,
            json!({"TransactItems": [{"Put": {
            "TableName": table, "Item": {"pk": s(&tx_key)}, "ConditionExpression": expression}}]}),
        )
        .await
        .unwrap_err();
        assert_validation(&error, expected);
        for key in [put_key, tx_key] {
            assert!(
                pk_item(ops, &table, &key).await.is_null(),
                "{key} was written"
            );
        }
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_first_fatal_defect_reported_depends_on_defect_class.
/// py
///
/// The nesting text is the AWS one; aux's differs today.
#[tokio::test]
async fn given_two_defects_in_one_transaction_when_listed_in_either_order_then_defect_class_decides_which_is_reported()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let missing = name_of(&unique_table_name());
    seed(ops, &table, json!({"pk": s("existing"), "v": n(1)})).await;
    tx_write(
        ops,
        json!({"TransactItems": [put_member(&table, json!({"pk": s("control")}))]}),
    )
    .await
    .unwrap();

    let unused = json!({"Update": {
        "TableName": table, "Key": {"pk": s("existing")}, "UpdateExpression": "SET v = v + :inc",
        "ExpressionAttributeValues": {":inc": n(1), ":unused": n(99)}}});
    let missing_table = put_member(&missing, json!({"pk": s("x")}));
    let unused_message =
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:unused}";
    for members in [[&missing_table, &unused], [&unused, &missing_table]] {
        let error = tx_write(ops, json!({"TransactItems": members}))
            .await
            .unwrap_err();
        assert_validation(&error, unused_message);
    }

    let malformed = json!({"ConditionCheck": {
        "TableName": table, "Key": {"pk": s("cc-key")},
        "ConditionExpression": "attribute_exists(pk AND"}});
    let deep = put_member(&table, json!({"pk": s("deep33"), "blob": nested(33)}));
    let error = tx_write(ops, json!({"TransactItems": [malformed, deep]}))
        .await
        .unwrap_err();
    assert_validation(
        &error,
        "Invalid ConditionExpression: Syntax error; token: \"AND\", near: \"pk AND\"",
    );
    let error = tx_write(ops, json!({"TransactItems": [deep, malformed]}))
        .await
        .unwrap_err();
    assert_validation(&error, NESTING_MESSAGE);
    assert!(pk_item(ops, &table, "deep33").await.is_null());
    assert_eq!(pk_item(ops, &table, "existing").await["v"], n(1));
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_nesting_violation_is_top_level_validation_not_cancellation.
/// py
///
/// aux's depth text differs from AWS's today.
#[tokio::test]
async fn given_33_level_item_or_set_value_when_transaction_runs_then_top_level_validation_and_nothing_lands()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let existing = json!({"pk": s("existing"), "v": n(1)});
    seed(ops, &table, existing.clone()).await;
    let sibling = {
        let table = &table;
        move || put_member(table, json!({"pk": s("new")}))
    };

    let error = tx_write(
        ops,
        json!({"TransactItems": [
            put_member(&table, json!({"pk": s("deep"), "blob": nested(32)})), sibling()
        ]}),
    )
    .await
    .unwrap_err();
    assert_validation(&error, NESTING_MESSAGE);

    let error = tx_write(
        ops,
        json!({"TransactItems": [
            {"Update": {"TableName": table, "Key": {"pk": s("existing")},
                        "UpdateExpression": "SET blob = :deep",
                        "ExpressionAttributeValues": {":deep": nested(32)}}},
            sibling()
        ]}),
    )
    .await
    .unwrap_err();
    assert_validation(&error, NESTING_MESSAGE);
    assert!(pk_item(ops, &table, "deep").await.is_null());
    assert!(pk_item(ops, &table, "new").await.is_null());
    assert_eq!(pk_item(ops, &table, "existing").await, existing);
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_put_condition_value_nesting_depth_rejected.py
///
/// aux's depth text differs from AWS's today.
#[tokio::test]
async fn given_33_level_condition_value_when_put_transaction_runs_then_top_level_validation_and_32_level_commits()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    seed(ops, &table, json!({"pk": s("sibling"), "ctr": n(1)})).await;
    let request = |key: &str, levels: usize| {
        json!({"TransactItems": [
            {"Put": {"TableName": table, "Item": {"pk": s(key)},
                     "ConditionExpression": "attribute_not_exists(pk) OR :v = :v",
                     "ExpressionAttributeValues": {":v": nested(levels)}}},
            {"Update": {"TableName": table, "Key": {"pk": s("sibling")},
                        "UpdateExpression": "ADD ctr :one",
                        "ExpressionAttributeValues": {":one": n(1)}}}
        ]})
    };
    let error = tx_write(ops, request("deep", 32)).await.unwrap_err();
    assert_validation(&error, NESTING_MESSAGE);
    assert!(pk_item(ops, &table, "deep").await.is_null());
    assert_eq!(pk_item(ops, &table, "sibling").await["ctr"], n(1));

    tx_write(ops, request("ok", 31))
        .await
        .expect("a 32-level value is accepted");
    assert!(!pk_item(ops, &table, "ok").await.is_null());
    assert_eq!(pk_item(ops, &table, "sibling").await["ctr"], n(2));
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_empty_index_key_validates_not_cancels.py
///
/// aux lacks the `IndexKey:` text of the Put message today.
#[tokio::test]
async fn given_empty_gsi_key_when_put_or_update_transaction_runs_then_top_level_validation_not_cancellation()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = make_table(&store, "pk", Some("sk"), true).await;
    let key = json!({"pk": s("b"), "sk": s("1")});
    let error = tx_write(
        ops,
        json!({"TransactItems": [put_member(
        &table, json!({"pk": s("b"), "sk": s("1"), "gpk": s("")}))]}),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        "One or more parameter values are not valid. A value specified for a secondary index key \
         is not supported. The AttributeValue for a key attribute cannot contain an empty string \
         value. IndexName: gsi1, IndexKey: gpk",
    );
    assert!(item(ops, &table, key.clone()).await.is_null());

    let error = tx_write(
        ops,
        json!({"TransactItems": [{"Update": {
        "TableName": table, "Key": key, "UpdateExpression": "SET gpk = :v",
        "ExpressionAttributeValues": {":v": s("")}}}]}),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        "One or more parameter values are not valid. The update expression attempted to update a \
         secondary index key to a value that is not supported. The AttributeValue for a key \
         attribute cannot contain an empty string value.",
    );
    assert!(item(ops, &table, key).await.is_null());
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_update_key_and_empty_index_key_rejections.py
///
/// aux quotes the attribute name in the key-update text today.
#[tokio::test]
async fn given_update_setting_key_or_empty_gsi_key_when_transaction_runs_then_key_cancels_and_empty_index_key_validates()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = make_table(&store, "pk", Some("sk"), true).await;
    let key = json!({"pk": s("item"), "sk": s("s1")});
    let original = json!({"pk": s("item"), "sk": s("s1"), "gpk": s("g1"), "val": n(1)});
    seed(ops, &table, original.clone()).await;

    let error = tx_write(
        ops,
        json!({"TransactItems": [{"Update": {
        "TableName": table, "Key": key, "UpdateExpression": "SET pk = :v",
        "ExpressionAttributeValues": {":v": s("other")}}}]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[(
            "ValidationError",
            Some(
                "One or more parameter values were invalid: Cannot update attribute pk. This \
                 attribute is part of the key",
            ),
        )],
    );

    let error = tx_write(
        ops,
        json!({"TransactItems": [{"Update": {
        "TableName": table, "Key": key, "UpdateExpression": "SET gpk = :e",
        "ExpressionAttributeValues": {":e": s("")}}}]}),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        "One or more parameter values are not valid. The update expression attempted to update a \
         secondary index key to a value that is not supported. The AttributeValue for a key \
         attribute cannot contain an empty string value.",
    );
    assert_eq!(item(ops, &table, key).await, original);
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_index_key_type_mismatch_cancels_not_validates.py
///
/// aux's transaction text for the index key type mismatch differs today.
#[tokio::test]
async fn given_wrong_typed_gsi_key_when_put_transaction_runs_then_validation_error_cancels_it() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = make_table(&store, "pk", Some("sk"), true).await;
    let error = tx_write(
        ops,
        json!({"TransactItems": [put_member(
        &table, json!({"pk": s("a"), "sk": s("1"), "gpk": n(5)}))]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(
        &error,
        &[(
            "ValidationError",
            Some(
                "One or more parameter values were invalid: Type mismatch for Index Key gpk \
                 Expected: S Actual: N IndexName: gsi1",
            ),
        )],
    );
    assert!(
        item(ops, &table, json!({"pk": s("a"), "sk": s("1")}))
            .await
            .is_null()
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_wrong_typed_key_cancels_and_empty_put_key_validates.py
#[tokio::test]
async fn given_wrong_typed_key_when_delete_update_or_get_runs_then_validation_error_cancels_and_empty_put_key_validates()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let seeded = json!({"pk": s("seed"), "v": s("orig")});
    seed(ops, &table, seeded.clone()).await;
    let wrong = json!({"pk": n(1)});
    let update_seed = json!({"Update": {
        "TableName": table, "Key": {"pk": s("seed")}, "UpdateExpression": "SET v = :v",
        "ExpressionAttributeValues": {":v": s("changed")}}});
    let put_fresh = put_member(&table, json!({"pk": s("fresh"), "v": s("1")}));
    let key_mismatch = [
        ("None", None),
        (
            "ValidationError",
            Some("The provided key element does not match the schema"),
        ),
    ];

    let error = tx_write(
        ops,
        json!({"TransactItems": [
        update_seed, {"Delete": {"TableName": table, "Key": wrong}}]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(&error, &key_mismatch);
    let error = tx_write(
        ops,
        json!({"TransactItems": [
        put_fresh, {"Update": {"TableName": table, "Key": wrong, "UpdateExpression": "SET v = :v",
                               "ExpressionAttributeValues": {":v": s("x")}}}]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(&error, &key_mismatch);
    let error = tx_get(
        ops,
        json!({"TransactItems": [
        {"Get": {"TableName": table, "Key": {"pk": s("seed")}}},
        {"Get": {"TableName": table, "Key": wrong}}]}),
    )
    .await
    .unwrap_err();
    assert_cancelled(&error, &key_mismatch);

    let error = tx_write(
        ops,
        json!({"TransactItems": [
        update_seed, put_member(&table, json!({"pk": s(""), "v": s("1")}))]}),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        "One or more parameter values are not valid. The AttributeValue for a key attribute \
         cannot contain an empty string value. Key: pk",
    );
    assert_eq!(pk_item(ops, &table, "seed").await, seeded);
    assert!(pk_item(ops, &table, "fresh").await.is_null());

    tx_write(ops, json!({"TransactItems": [update_seed, put_fresh]}))
        .await
        .unwrap();
    assert_eq!(pk_item(ops, &table, "seed").await["v"], s("changed"));
    assert_eq!(pk_item(ops, &table, "fresh").await["v"], s("1"));
    store.finish().await;
}

// ---------------------------------------------------------------------------
// TransactWriteItems: size limits
// ---------------------------------------------------------------------------

/// AWS behavioural: check_transact_write_aggregate_size_over_4mb_rejected.py
///
/// aux's text is 'Aggregate size of transaction write items exceeds 4 MB'.
#[tokio::test]
async fn given_eleven_390kb_puts_when_transaction_runs_then_aggregate_over_4mb_is_validation_and_two_items_commit()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let filler_len = 390 * 1024;
    let items = (0..11u64)
        .map(|i| data_item(&format!("big-{i:02}"), filler_len, 200 + i))
        .collect::<Vec<_>>();
    let aggregate = (0..11)
        .map(|i| data_item_size(&format!("big-{i:02}"), filler_len))
        .sum::<usize>();
    let error = tx_write(
        ops,
        json!({"TransactItems": items.iter()
        .map(|item| put_member(&table, item.clone())).collect::<Vec<_>>()}),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        &format!("Transaction payload size cannot exceed 4MB. Payload Size: {aggregate}"),
    );
    for item in &items {
        assert!(item_of(ops, &table, item).await.is_null());
    }

    tx_write(
        ops,
        json!({"TransactItems": items[..2].iter()
        .map(|item| put_member(&table, item.clone())).collect::<Vec<_>>()}),
    )
    .await
    .unwrap();
    for item in &items[..2] {
        assert_eq!(item_of(ops, &table, item).await, *item);
    }
    store.finish().await;
}

async fn item_of(ops: &Ops, table: &str, item: &Value) -> Value {
    self::item(ops, table, json!({"pk": item["pk"]})).await
}

/// AWS behavioural: check_transact_write_items_near_4mb_aggregate_commits.py
#[tokio::test]
async fn given_ten_350kb_puts_when_transaction_runs_then_all_commit_and_read_back_exactly() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let items = (0..10u64)
        .map(|i| data_item(&format!("near-{i:02}"), 350 * 1024, 300 + i))
        .collect::<Vec<_>>();
    tx_write(
        ops,
        json!({"TransactItems": items.iter()
        .map(|item| put_member(&table, item.clone())).collect::<Vec<_>>()}),
    )
    .await
    .unwrap();
    for item in &items {
        assert_eq!(item_of(ops, &table, item).await, *item);
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_transact_write_items_put_and_update_accept_flat_409600_byte_item.py
///
/// The scripts disagree on the over-limit Update (cancellation here, a
/// top-level error in `..._update_grows_item_over_size_limit.py`); both are
/// ported as written.
#[tokio::test]
async fn given_item_of_exactly_409600_bytes_when_put_or_update_transaction_runs_then_accepted_and_one_byte_more_cancels()
 {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let put_pk = "limit-put";
    let put = data_item(put_pk, ITEM_LIMIT_BYTES - data_item_size(put_pk, 0), 400);
    tx_write(
        ops,
        json!({"TransactItems": [put_member(&table, put.clone())]}),
    )
    .await
    .unwrap();
    assert_eq!(item_of(ops, &table, &put).await, put);

    let update_pk = "limit-update";
    seed(ops, &table, json!({"pk": s(update_pk), "data": s("start")})).await;
    let grow = |length: usize| {
        json!({"TransactItems": [{"Update": {
            "TableName": table, "Key": {"pk": s(update_pk)},
            "UpdateExpression": "SET #d = :v", "ExpressionAttributeNames": {"#d": "data"},
            "ExpressionAttributeValues": {":v": s(&filler(401, length))}}}]})
    };
    let exact = ITEM_LIMIT_BYTES - data_item_size(update_pk, 0);
    tx_write(ops, grow(exact)).await.unwrap();
    assert_eq!(
        pk_item(ops, &table, update_pk).await,
        json!({"pk": s(update_pk), "data": s(&filler(401, exact))})
    );

    let over_pk = "limit-over";
    seed(ops, &table, json!({"pk": s(over_pk), "data": s("start")})).await;
    let error = tx_write(
        ops,
        json!({"TransactItems": [{"Update": {
        "TableName": table, "Key": {"pk": s(over_pk)},
        "UpdateExpression": "SET #d = :v", "ExpressionAttributeNames": {"#d": "data"},
        "ExpressionAttributeValues": {
            ":v": s(&filler(402, ITEM_LIMIT_BYTES + 1 - data_item_size(over_pk, 0)))}}}]}),
    )
    .await
    .unwrap_err();
    assert_error(
        &error,
        "TransactionCanceledException",
        &cancelled_message(&["ValidationError"]),
    );
    assert_eq!(reason_codes(&error), ["ValidationError"]);
    assert_eq!(pk_item(ops, &table, over_pk).await["data"], s("start"));
    store.finish().await;
}

// ---------------------------------------------------------------------------
// UpdateTable
// ---------------------------------------------------------------------------

/// AWS behavioural: check_disabling_a_stream_that_does_not_exist_rejected.py
///
/// aux accepts the disable silently today.
#[tokio::test]
async fn given_table_without_stream_when_update_table_disables_stream_then_validation_exception() {
    let store = TestStore::new().await;
    let ops = &store.operations;
    let table = new_table(&store).await;
    let error = update_table(
        ops,
        json!({
            "TableName": table, "StreamSpecification": {"StreamEnabled": false}
        }),
    )
    .await
    .unwrap_err();
    assert_validation(
        &error,
        &format!("Table has no stream to disable: TableName: {table}"),
    );
    store.finish().await;
}
