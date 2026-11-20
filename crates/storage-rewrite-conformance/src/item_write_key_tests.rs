//! Write observations match physical keys independently of wire encoding.

use serde_json::json;
use storage_types::{CreateTableRequest, GetItemRequest, PutItemRequest};

use super::test_support::{TestStore, unique_table_name};

#[tokio::test]
async fn given_unpadded_binary_key_when_item_is_replaced_then_observation_matches_native_key()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = unique_table_name();
        fixture
            .operations
            .create_table(
                CreateTableRequest::try_from(json!({
                    "TableName": table,
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "B"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST"
                }))
                .unwrap(),
            )
            .await?;
        for value in ["before", "after"] {
            fixture
                .operations
                .put_item(
                    PutItemRequest::try_from(json!({
                        "TableName": table, "Item": {"pk": {"B": "YQ"}, "value": {"S": value}}
                    }))
                    .unwrap(),
                )
                .await?;
        }
        let result = fixture
            .operations
            .get_item(
                GetItemRequest::try_from(json!({
                    "TableName": table, "Key": {"pk": {"B": "YQ"}}, "ConsistentRead": true
                }))
                .unwrap(),
            )
            .await?;
        anyhow::ensure!(serde_json::to_value(result)?["Item"]["value"] == json!({"S": "after"}));
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
