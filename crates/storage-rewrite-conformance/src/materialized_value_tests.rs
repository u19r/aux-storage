//! Escaped values exercise native rows, chunking and logical page accounting.

use anyhow::Context as _;
use serde_json::json;
use storage_types::{
    AttributeMap, BatchGetItemRequest, IndexName, ProjectionType, PutItemRequest, QueryRequest,
    TableName,
};

use super::{gsi_workflow_tests::table_request, test_support::TestStore};

#[tokio::test]
async fn given_large_escaped_items_when_reading_primary_and_frozen_gsis_then_logical_pages_are_complete()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("materialized_escaped_values");
        create_projected_table(&fixture, &table).await?;
        let expected = put_escaped_items(&fixture, &table).await?;
        let result = fixture
            .operations
            .batch_get_item(
                BatchGetItemRequest::try_from(json!({
                    "RequestItems": { table.as_ref(): {"Keys": [
                        {"pk": {"S": "a"}}, {"pk": {"S": "b"}}
                    ]}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await
            .context("read expanded primary batch")?;
        let actual = result
            .responses
            .as_ref()
            .and_then(|tables| tables.get(&table))
            .ok_or_else(|| anyhow::anyhow!("missing primary batch response"))?;
        let actual = actual
            .iter()
            .map(|item| item.to_attribute_map())
            .collect::<Result<Vec<_>, _>>()?;
        anyhow::ensure!(actual.len() == 2 && expected.iter().all(|item| actual.contains(item)));
        verify_projected_pages(&fixture, &table, 2).await?;
        for key in ["a", "b"] {
            fixture
                .operations
                .delete_item(storage_types::DeleteItemRequest::new(
                    table.clone(),
                    [(
                        "pk".to_owned(),
                        storage_types::AttributeValue::S(key.into()),
                    )],
                ))
                .await
                .context("delete expanded primary and three projections")?;
        }
        verify_projected_pages(&fixture, &table, 0).await
    }
    .await;
    fixture.finish().await;
    outcome
}

async fn create_projected_table(fixture: &TestStore, table: &TableName) -> anyhow::Result<()> {
    let mut request = table_request(table.as_ref());
    let template = request.global_secondary_indexes.as_ref().unwrap()[0].clone();
    request.global_secondary_indexes = Some(
        [
            ("all_values", ProjectionType::All),
            ("included_values", ProjectionType::Include),
            ("key_values", ProjectionType::KeysOnly),
        ]
        .into_iter()
        .map(|(name, kind)| {
            let mut index = template.clone();
            index.index_name = IndexName::new(name);
            index.projection.non_key_attributes =
                matches!(kind, ProjectionType::Include).then(|| vec!["payload".into()]);
            index.projection.projection_type = Some(kind);
            index
        })
        .collect(),
    );
    fixture.operations.create_table(request).await?;
    Ok(())
}

async fn put_escaped_items(
    fixture: &TestStore,
    table: &TableName,
) -> anyhow::Result<Vec<AttributeMap>> {
    let mut expected = Vec::new();
    for key in ["a", "b"] {
        let item = json!({
            "pk": {"S": key}, "gsi_pk": {"S": "group"}, "gsi_sk": {"S": key},
            "payload": {"S": "\u{0001}".repeat(200_000)}, "unprojected": {"S": "private"}
        });
        expected.push(serde_json::from_value(item.clone())?);
        fixture
            .operations
            .put_item(
                PutItemRequest::try_from(json!({
                    "TableName": table, "Item": item
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await
            .context("write expanded primary and three projections")?;
    }
    Ok(expected)
}

async fn verify_projected_pages(
    fixture: &TestStore,
    table: &TableName,
    expected_count: u32,
) -> anyhow::Result<()> {
    for name in ["all_values", "included_values", "key_values"] {
        let result = fixture
            .operations
            .query(
                QueryRequest::try_from(json!({
                    "TableName": table, "IndexName": name,
                    "KeyConditionExpression": "gsi_pk = :group",
                    "ExpressionAttributeValues": {":group": {"S": "group"}}
                }))
                .map_err(anyhow::Error::msg)?,
            )
            .await
            .with_context(|| format!("read expanded {name} projection"))?;
        anyhow::ensure!(result.count == expected_count && result.scanned_count == expected_count);
        anyhow::ensure!(
            result.last_evaluated_key.is_none(),
            "escaped bytes shrank a logical page"
        );
        for item in result.items.unwrap_or_default() {
            let attributes = item.to_attribute_map()?;
            anyhow::ensure!(attributes.contains_key("payload") == (name != "key_values"));
            if let Some(storage_types::AttributeValue::S(payload)) = attributes.get("payload") {
                anyhow::ensure!(payload.len() == 200_000 && payload.bytes().all(|byte| byte == 1));
            }
            anyhow::ensure!(attributes.contains_key("unprojected") == (name == "all_values"));
            anyhow::ensure!(attributes.contains_key("pk") && attributes.contains_key("gsi_sk"));
        }
    }
    Ok(())
}
