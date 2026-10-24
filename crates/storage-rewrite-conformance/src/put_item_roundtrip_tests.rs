//! Cucumber PutItem value round trips through the replacement provider
//! boundary.

use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{GetItemRequest, PutItemRequest};

use super::test_support::{TestStore, assert_conformance_body};

// Business and format rules: successful writes preserve every Dynamo value
// shape.
#[tokio::test]
async fn given_mixed_sets_binary_large_and_unicode_values_when_put_item_runs_then_get_item_returns_them()
 {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    let large = "x".repeat(1_000);
    let large_set: Vec<_> = (0..100).map(|i| format!("set-{i:03}")).collect();
    let cases = [
        json!({
            "pk": {"S": "sets"},
            "strings": {"SS": ["production", "critical", "web"]},
            "numbers": {"NS": ["100", "250", "175"]},
            "binary": {"BS": ["YWJjZGVm", "Z2hpams="]},
            "single_binary": {"B": "aGVsbG8gd29ybGQ="}
        }),
        json!({
            "pk": {"S": "mixed"},
            "active": {"BOOL": true},
            "negative": {"N": "-12345.6789"},
            "precise": {"N": "123456789012345678901234567890.12345678"},
            "absent": {"NULL": true},
            "nested": {"M": {"tags": {"L": [{"S": "one"}, {"N": "42"}]}}}
        }),
        json!({
            "pk": {"S": "large"},
            "largeString": {"S": large},
            "largeSet": {"SS": large_set}
        }),
        json!({
            "pk": {"S": "special"},
            "text": {"S": "Special chars: !@#$%^&*()[]{}PIPE;':,./<>?"},
            "unicode": {"S": "Unicode: 你好世界 🌍"}
        }),
    ];

    for item in cases {
        let key = item["pk"].clone();
        let put = PutItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Item": item
        }))
        .unwrap();
        let written = observe(store.operations.put_item(put)).await;
        written.assert_no_warning_or_error();
        assert_eq!(
            serde_json::to_value(written.result.unwrap()).unwrap(),
            json!({})
        );

        let get = GetItemRequest::try_from(json!({
            "TableName": table.as_ref(), "Key": {"pk": key}
        }))
        .unwrap();
        let read = store.operations.get_item(get).await.unwrap();
        assert_conformance_body(
            &serde_json::to_value(read).unwrap(),
            &json!({"Item": item}),
            key["S"].as_str().unwrap(),
        );
    }
    store.finish().await;
}

#[tokio::test]
async fn given_equivalent_numeric_keys_when_replaced_then_lookup_returns_latest_item() {
    use storage_types::{
        AttributeDefinition, BillingMode, CreateTableRequest, KeyAttributeType, KeySchemaElement,
        KeyType, TableName,
    };

    let store = TestStore::new().await;
    let table = TableName::new("dynamo-number-key");
    store
        .operations
        .create_table(CreateTableRequest::new(
            table.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::N,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        ))
        .await
        .unwrap();
    for (key, value) in [
        ("7", "v7"),
        ("42", "v42"),
        ("100", "v100"),
        ("42.0", "v42-replaced"),
    ] {
        store
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table.as_ref(),
                    "Item": {"pk": {"N": key}, "value": {"S": value}},
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let read = store
        .operations
        .get_item(
            GetItemRequest::try_from(json!({
                "TableName": table.as_ref(), "Key": {"pk": {"N": "42.00"}},
                "ConsistentRead": true,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let body = serde_json::to_value(read).unwrap();
    assert_eq!(body["Item"]["value"], json!({"S": "v42-replaced"}));
    store.finish().await;
}
