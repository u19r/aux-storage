use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue},
};
use serde_json::json;
use storage::DatabaseManager;
use storage_observability_test_support::observation::{Observation, observe};

use crate::{routes::dynamodb::dynamodb_endpoint, types::AppState};

fn breakdown<T>(observation: &Observation<T>) -> BTreeMap<String, u64> {
    observation
        .costs
        .db_call_breakdown
        .iter()
        .map(|entry| (entry.operation.clone(), entry.calls))
        .collect()
}

async fn http(
    app: &Arc<AppState>,
    target: &'static str,
    payload: serde_json::Value,
) -> Observation<()> {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-target", HeaderValue::from_static(target));
    let body = Bytes::from(serde_json::to_vec(&payload).expect("payload"));
    let observed = observe(dynamodb_endpoint(State(Arc::clone(app)), headers, body)).await;
    assert!(observed.result.is_ok(), "{target} failed");
    Observation {
        result: (),
        costs: observed.costs,
        events: observed.events,
        spans: observed.spans,
    }
}

#[tokio::test]
async fn given_create_table_when_called_over_http_and_library_then_same_operation_metrics() {
    let db = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let app = Arc::new(AppState::new(Arc::clone(&db)));
    let request = |name: &str| {
        json!({
            "TableName": name,
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        })
    };
    let over_http = http(
        &app,
        "DynamoDB_20120810.CreateTable",
        request("FrontDoorHttp"),
    )
    .await;
    let library_request: storage_types::CreateTableRequest =
        serde_json::from_value(request("FrontDoorLibrary")).unwrap();
    let library = observe(db.create_table(&library_request)).await;
    library.result.as_ref().expect("library create");
    assert_eq!(breakdown(&over_http), breakdown(&library));
    assert_eq!(over_http.costs.db_calls, library.costs.db_calls);
    assert_eq!(breakdown(&over_http).get("create_table"), Some(&1));
    db.shutdown_background_services().await.unwrap();
}

#[tokio::test]
async fn given_each_dynamodb_arm_when_called_over_http_then_its_operation_metric_is_recorded_once()
{
    let db = Arc::new(DatabaseManager::new_for_test().await.unwrap());
    let app = Arc::new(AppState::new(Arc::clone(&db)));
    let steps: Vec<(&'static str, serde_json::Value, &str)> = vec![
        (
            "CreateTable",
            json!({"TableName":"FrontDoorArms","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST","StreamSpecification":{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}),
            "create_table",
        ),
        (
            "DescribeTable",
            json!({"TableName":"FrontDoorArms"}),
            "describe_table",
        ),
        ("ListTables", json!({}), "list_tables"),
        (
            "PutItem",
            json!({"TableName":"FrontDoorArms","Item":{"pk":{"S":"a"},"v":{"N":"1"}}}),
            "put_item",
        ),
        (
            "GetItem",
            json!({"TableName":"FrontDoorArms","Key":{"pk":{"S":"a"}}}),
            "get_item",
        ),
        (
            "UpdateItem",
            json!({"TableName":"FrontDoorArms","Key":{"pk":{"S":"a"}},"UpdateExpression":"SET v = :v","ExpressionAttributeValues":{":v":{"N":"2"}}}),
            "update_item",
        ),
        (
            "Query",
            json!({"TableName":"FrontDoorArms","KeyConditionExpression":"pk = :p","ExpressionAttributeValues":{":p":{"S":"a"}}}),
            "query",
        ),
        ("Scan", json!({"TableName":"FrontDoorArms"}), "scan_table"),
        (
            "BatchWriteItem",
            json!({"RequestItems":{"FrontDoorArms":[{"PutRequest":{"Item":{"pk":{"S":"b"}}}}]}}),
            "batch_write_item",
        ),
        (
            "BatchGetItem",
            json!({"RequestItems":{"FrontDoorArms":{"Keys":[{"pk":{"S":"a"}},{"pk":{"S":"b"}}]}}}),
            "batch_get_item",
        ),
        (
            "TransactWriteItems",
            json!({"TransactItems":[{"Put":{"TableName":"FrontDoorArms","Item":{"pk":{"S":"c"}}}}]}),
            "transact_write_items",
        ),
        (
            "TransactGetItems",
            json!({"TransactItems":[{"Get":{"TableName":"FrontDoorArms","Key":{"pk":{"S":"c"}}}}]}),
            "transact_get_items",
        ),
        (
            "UpdateTimeToLive",
            json!({"TableName":"FrontDoorArms","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"expires"}}),
            "update_time_to_live",
        ),
        (
            "DescribeTimeToLive",
            json!({"TableName":"FrontDoorArms"}),
            "describe_time_to_live",
        ),
        (
            "ListStreams",
            json!({"TableName":"FrontDoorArms"}),
            "list_streams",
        ),
        (
            "DeleteItem",
            json!({"TableName":"FrontDoorArms","Key":{"pk":{"S":"a"}}}),
            "delete_item",
        ),
        (
            "DeleteTable",
            json!({"TableName":"FrontDoorArms"}),
            "delete_table",
        ),
    ];
    for (operation, payload, label) in steps {
        // Stream operations are routed only under the DynamoDBStreams prefix.
        let service = if operation == "ListStreams" {
            "DynamoDBStreams_20120810"
        } else {
            "DynamoDB_20120810"
        };
        let target: &'static str = Box::leak(format!("{service}.{operation}").into_boxed_str());
        let observation = http(&app, target, payload).await;
        assert_eq!(breakdown(&observation).get(label), Some(&1), "{target}");
    }
    db.shutdown_background_services().await.unwrap();
}
