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

#[tokio::test]
#[ignore = "paired full-provider batch-write performance report"]
async fn given_live_gsi_items_when_batch_replacements_run_then_report_provider_latency()
-> anyhow::Result<()> {
    use storage_types::BatchWriteItemRequest;

    use super::gsi_workflow_tests::{group_keys, table_request};
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = "batch_write_profile";
        fixture
            .operations
            .create_table(table_request(table))
            .await?;
        let batch = |group| {
            BatchWriteItemRequest::try_from(json!({
                "RequestItems": {table: (0..25).map(|ordinal| json!({"PutRequest": {"Item": {
                    "pk": {"S": format!("key-{ordinal:02}")},
                    "gsi_pk": {"S": group}, "gsi_sk": {"S": format!("key-{ordinal:02}")}
                }}})).collect::<Vec<_>>()}
            }))
            .unwrap()
        };
        let seed = fixture.operations.batch_write_item(batch("before")).await?;
        anyhow::ensure!(
            seed.unprocessed_items
                .as_ref()
                .is_none_or(|items| items.values().all(Vec::is_empty))
        );
        let requests = (0..64)
            .map(|ordinal| batch(if ordinal % 2 == 0 { "before" } else { "after" }))
            .collect::<Vec<_>>();
        let started = std::time::Instant::now();
        for request in requests {
            let response = fixture.operations.batch_write_item(request).await?;
            anyhow::ensure!(
                response
                    .unprocessed_items
                    .as_ref()
                    .is_none_or(|items| items.values().all(Vec::is_empty))
            );
        }
        let elapsed = started.elapsed();
        anyhow::ensure!(group_keys(&fixture, table, "before").await?.is_empty());
        let expected = (0..25)
            .map(|ordinal| format!("key-{ordinal:02}"))
            .collect::<Vec<_>>();
        anyhow::ensure!(group_keys(&fixture, table, "after").await? == expected);
        eprintln!(
            "batch_write_provider source={} batches=64 items_per_batch=25 elapsed_us={} \
             us_per_batch={:.1}",
            env!("CARGO_MANIFEST_DIR"),
            elapsed.as_micros(),
            elapsed.as_secs_f64() * 1_000_000.0 / 64.0
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
