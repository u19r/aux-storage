//! Exact public reads and bounded pages as unrelated data grows.

use anyhow::Context as _;

use super::test_support::TestStore;

/// Functional growth coverage; native work and network waves require separate
/// measurement.
#[tokio::test]
async fn given_unrelated_rows_when_point_read_and_scan_page_then_exact_data_and_continuations_survive_growth()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = storage_types::TableName::new("growth_pages");
        fixture.operations.create_table(super::test_support::hash_table_request(table.clone())).await?;
        for start in (0..200).step_by(25) {
            let writes = (start..start + 25).map(|index| serde_json::json!({"PutRequest": {"Item": {
                "pk": {"S": format!("unrelated-{index:04}")}, "value": {"S": "payload"}
            }}})).collect::<Vec<_>>();
            let request = storage_types::BatchWriteItemRequest::try_from(serde_json::json!({
                "RequestItems": {table.as_ref(): writes}
            })).map_err(anyhow::Error::msg)?;
            let result = fixture.operations.batch_write_item(request).await?;
            anyhow::ensure!(result.unprocessed_items.as_ref().is_none_or(|items| items.values().all(Vec::is_empty)));
        }
        fixture.operations.put_item(storage_types::PutItemRequest::try_from(serde_json::json!({
            "TableName": table, "Item": {"pk": {"S": "target"}, "value": {"S": "wanted"}}
        })).map_err(anyhow::Error::msg)?).await?;
        let found = fixture.operations.get_item(storage_types::GetItemRequest::try_from(serde_json::json!({
            "TableName": table, "Key": {"pk": {"S": "target"}}, "ConsistentRead": true
        })).map_err(anyhow::Error::msg)?).await?.item.context("target missing among unrelated rows")?;
        anyhow::ensure!(found.to_attribute_map()? == serde_json::from_value::<storage_types::AttributeMap>(serde_json::json!({
            "pk": {"S": "target"}, "value": {"S": "wanted"}
        }))?);
        let mut continuation = None;
        let mut seen = std::collections::BTreeSet::new();
        let mut terminal = false;
        for _ in 0..102 {
            let request = storage_types::ScanRequest::try_from(serde_json::json!({
                "TableName": table, "Limit": 2, "ConsistentRead": true, "ExclusiveStartKey": continuation
            })).map_err(anyhow::Error::msg)?;
            let page = fixture.operations.scan(request).await?;
            let items = page.items.unwrap_or_default();
            anyhow::ensure!(items.len() <= 2, "scan exceeded page bound");
            for item in items {
                let key = item.required_string_attribute("pk")?.to_owned();
                anyhow::ensure!(seen.insert(key), "scan repeated an item");
            }
            continuation = page.last_evaluated_key;
            if continuation.is_none() { terminal = true; break; }
        }
        anyhow::ensure!(terminal, "bounded scan never reached its end");
        let mut expected = (0..200).map(|index| format!("unrelated-{index:04}")).collect::<std::collections::BTreeSet<_>>();
        expected.insert("target".to_owned());
        anyhow::ensure!(seen == expected, "scan omitted or fabricated rows under growth");
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}
