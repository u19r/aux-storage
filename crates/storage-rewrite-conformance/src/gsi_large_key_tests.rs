use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use storage_provider::TableReclaimStep;
use storage_types::{
    AttributeMap, DeleteItemRequest, DeleteTableRequest, PutItemRequest, QueryRequest, TableName,
    WireItem,
};

use super::test_support::TestStore;

#[tokio::test]
async fn given_maximum_binary_keys_and_chunked_images_when_paged_overwritten_and_reclaimed_then_gsi_order_and_new_incarnation_survive()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let table = TableName::new("maximum_binary_projection");
    let outcome =
        async {
            fixture
                .operations
                .create_table(table_request(&table))
                .await?;
            let mut expected = Vec::new();
            for ordinal in [0, 1] {
                let item = item(ordinal, &"\u{0001}".repeat(200_000));
                fixture
                    .operations
                    .put_item(PutItemRequest::new(table.clone(), WireItem::try_from(&item)?))
                    .await.with_context(|| format!("write maximum binary item {ordinal}"))?;
                expected.push(item);
            }
            verify_pages(&fixture, &table, &expected).await.context("verify maximum binary projection pages")?;
            let replacement = item(0, "small replacement");
            fixture
                .operations
                .put_item(PutItemRequest::new(table.clone(), WireItem::try_from(&replacement)?))
                .await?;
            expected[0] = replacement;
            verify_pages(&fixture, &table, &expected).await.context("verify maximum binary projection pages")?;
            fixture.operations.delete_item(DeleteItemRequest::try_from(json!({
            "TableName": table, "Key": {"pk": expected[0].get("pk").unwrap(), "sk": expected[0].get("sk").unwrap()}
        })).map_err(anyhow::Error::msg)?).await?;
            verify_pages(&fixture, &table, &expected[1..]).await?;
            fixture
                .operations
                .delete_table(DeleteTableRequest {
                    table_name: table.clone(),
                })
                .await?;
            fixture
                .operations
                .create_table(table_request(&table))
                .await?;
            verify_pages(&fixture, &table, &[]).await?;
            let fresh = item(0, "fresh incarnation");
            fixture
                .operations
                .put_item(PutItemRequest::new(table.clone(), WireItem::try_from(&fresh)?))
                .await?;
            let mut advanced = 0;
            for _ in 0..1000 {
                match fixture
                    .table_reclaim_repository
                    .reclaim_table_page()
                    .await?
                {
                    TableReclaimStep::Advanced => advanced += 1,
                    TableReclaimStep::Complete => break,
                }
            }
            anyhow::ensure!(advanced > 0, "old wide projections require reclamation");
            anyhow::ensure!(
                fixture
                    .table_reclaim_repository
                    .reclaim_table_page()
                    .await?
                    == TableReclaimStep::Complete
            );
            verify_pages(&fixture, &table, &[fresh]).await
        }
        .await;
    fixture.finish().await;
    outcome
}

fn table_request(table: &TableName) -> storage_types::CreateTableRequest {
    serde_json::from_value(json!({
        "TableName": table, "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "B"},
            {"AttributeName": "sk", "AttributeType": "B"},
            {"AttributeName": "gsi_pk", "AttributeType": "B"},
            {"AttributeName": "gsi_sk", "AttributeType": "B"}
        ],
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "GlobalSecondaryIndexes": [{
            "IndexName": "wide", "Projection": {"ProjectionType": "ALL"},
            "KeySchema": [
                {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                {"AttributeName": "gsi_sk", "KeyType": "RANGE"}
            ]
        }]
    }))
    .unwrap()
}

fn item(ordinal: u8, payload: &str) -> AttributeMap {
    let mut sort = vec![0; storage_types::MAX_SORT_KEY_BYTES];
    *sort.last_mut().unwrap() = ordinal;
    serde_json::from_value(json!({
        "pk": {"B": STANDARD.encode(vec![255; storage_types::MAX_PARTITION_KEY_BYTES])},
        "sk": {"B": STANDARD.encode(sort)},
        "gsi_pk": {"B": STANDARD.encode(vec![128; storage_types::MAX_PARTITION_KEY_BYTES])},
        "gsi_sk": {"B": STANDARD.encode(vec![0; storage_types::MAX_SORT_KEY_BYTES])},
        "payload": {"S": payload}
    }))
    .unwrap()
}

async fn verify_pages(
    fixture: &TestStore,
    table: &TableName,
    expected: &[AttributeMap],
) -> anyhow::Result<()> {
    for forward in [true, false] {
        for limit in [None, Some(1)] {
            let mut start = None;
            let mut actual = Vec::new();
            loop {
                let request = QueryRequest::try_from(json!({
                    "TableName": table, "IndexName": "wide",
                    "KeyConditionExpression": "gsi_pk = :partition",
                    "ExpressionAttributeValues": {":partition": {
                        "B": STANDARD.encode(vec![128; storage_types::MAX_PARTITION_KEY_BYTES])
                    }},
                    "ScanIndexForward": forward, "Limit": limit, "ExclusiveStartKey": start
                }))
                .map_err(anyhow::Error::msg)?;
                let page = fixture.operations.query(request).await.with_context(|| {
                    format!("query maximum binary projection forward={forward} limit={limit:?}")
                })?;
                for value in page.items.unwrap_or_default() {
                    actual.push(value.to_attribute_map()?);
                }
                start = page.last_evaluated_key;
                if start.is_none() {
                    break;
                }
                anyhow::ensure!(
                    actual.len() <= expected.len(),
                    "native cursor failed to advance"
                );
            }
            let expected: Vec<_> = if forward {
                expected.iter().collect()
            } else {
                expected.iter().rev().collect()
            };
            anyhow::ensure!(
                actual.iter().collect::<Vec<_>>() == expected,
                "GSI values or ordering differ"
            );
        }
    }
    Ok(())
}
