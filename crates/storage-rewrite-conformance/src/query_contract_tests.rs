//! Query membership and ordered continuation through the public operation
//! owner.

use std::collections::HashMap;

use storage_types::{
    AttributeValue, ExclusiveStartKey, KeyAttributeType, PutItemRequest, QueryRequest, TableName,
    WireItem,
};

use super::test_support::TestStore;

fn composite_item(pk: &str, sk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S(sk.to_owned())),
        ("value".to_owned(), AttributeValue::S(value.to_owned())),
    ])
}

#[tokio::test]
async fn given_items_when_queried_with_limit_then_pages_cover_all_without_duplicates()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .create_hash_range_table(TableName::new("dynamo-query-pages"), KeyAttributeType::S)
        .await;
    let outcome = async {
        let table = TableName::new("dynamo-query-pages");
        for index in 1..=5 {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&composite_item(
                        "user#1",
                        &format!("item#{index:03}"),
                        "data",
                    ))?,
                ))
                .await?;
        }
        let values =
            || HashMap::from([(":pk".to_string(), AttributeValue::S("user#1".to_string()))]);
        let full_page = fixture
            .operations
            .query(QueryRequest {
                expression_attribute_values: Some(values().into()),
                scan_index_forward: Some(true),
                consistent_read: Some(false),
                ..QueryRequest::new(table.clone(), "pk = :pk".to_string())
            })
            .await?;
        let full = full_page.items.unwrap_or_default();
        let full_lek = full_page.last_evaluated_key.map(ExclusiveStartKey::Key);
        anyhow::ensure!(full.len() == 5, "expected 5 items, got {}", full.len());
        anyhow::ensure!(
            full_lek.is_none(),
            "unlimited query must not return a continuation key"
        );
        let mut seen: Vec<String> = Vec::new();
        let mut start_key: Option<ExclusiveStartKey> = None;
        loop {
            let result = fixture
                .operations
                .query(QueryRequest {
                    expression_attribute_values: Some(values().into()),
                    limit: Some(2),
                    exclusive_start_key: start_key,
                    scan_index_forward: Some(true),
                    consistent_read: Some(false),
                    ..QueryRequest::new(table.clone(), "pk = :pk".to_string())
                })
                .await?;
            let page = result.items.unwrap_or_default();
            let lek = result.last_evaluated_key.map(ExclusiveStartKey::Key);
            anyhow::ensure!(!page.is_empty(), "query page is unexpectedly empty");
            for item in page {
                let attrs = item.to_attribute_map()?;
                let Some(AttributeValue::S(sk)) = attrs.get("sk") else {
                    anyhow::bail!("paged item missing sk: {attrs:?}");
                };
                seen.push(sk.clone());
            }
            match lek {
                Some(next) => start_key = Some(next),
                None => break,
            }
        }
        anyhow::ensure!(
            seen == vec![
                "item#001".to_string(),
                "item#002".to_string(),
                "item#003".to_string(),
                "item#004".to_string(),
                "item#005".to_string(),
            ],
            "paged query coverage mismatch: {seen:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_items_when_queried_backward_then_descending_order() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .create_hash_range_table(TableName::new("dynamo-query-reverse"), KeyAttributeType::S)
        .await;
    let outcome = async {
        let table = TableName::new("dynamo-query-reverse");
        for index in 1..=3 {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&composite_item(
                        "user#1",
                        &format!("item#{index:03}"),
                        "data",
                    ))?,
                ))
                .await?;
        }
        let values = HashMap::from([(":pk".to_string(), AttributeValue::S("user#1".to_string()))]);
        let mut keys = Vec::new();
        let mut start = None;
        loop {
            let result = fixture
                .operations
                .query(QueryRequest {
                    expression_attribute_values: Some(values.clone().into()),
                    limit: Some(1),
                    exclusive_start_key: start,
                    scan_index_forward: Some(false),
                    consistent_read: Some(false),
                    ..QueryRequest::new(table.clone(), "pk = :pk".to_string())
                })
                .await?;
            let items = result.items.unwrap_or_default();
            let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
            for item in items {
                let attrs = item.to_attribute_map()?;
                let Some(AttributeValue::S(sk)) = attrs.get("sk") else {
                    anyhow::bail!("reverse item missing sk: {attrs:?}");
                };
                keys.push(sk.clone());
            }
            let Some(next) = next else {
                break;
            };
            start = Some(next);
        }
        anyhow::ensure!(
            keys == vec![
                "item#003".to_string(),
                "item#002".to_string(),
                "item#001".to_string(),
            ],
            "reverse order mismatch: {keys:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_sort_key_conditions_when_query_resumes_then_pages_keep_expected_members()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .create_hash_range_table(TableName::new("dynamo-query-sorts"), KeyAttributeType::S)
        .await;
    let outcome = async {
        let table = TableName::new("dynamo-query-sorts");
        for index in 1..=5 {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&composite_item("group", &format!("item#{index:03}"), "v"))?,
                ))
                .await?;
        }
        for (expression, bound, expected) in [
            (
                "pk = :pk AND sk >= :lo",
                vec![(":lo", "item#003")],
                vec!["item#003", "item#004", "item#005"],
            ),
            (
                "pk = :pk AND sk BETWEEN :lo AND :hi",
                vec![(":lo", "item#003"), (":hi", "item#004")],
                vec!["item#003", "item#004"],
            ),
            (
                "pk = :pk AND begins_with(sk, :prefix)",
                vec![(":prefix", "item#00")],
                vec!["item#001", "item#002", "item#003", "item#004", "item#005"],
            ),
        ] {
            let mut values =
                HashMap::from([(":pk".to_string(), AttributeValue::S("group".into()))]);
            values.extend(
                bound
                    .into_iter()
                    .map(|(name, value)| (name.to_string(), AttributeValue::S(value.into()))),
            );
            let mut seen = Vec::new();
            let mut start = None;
            loop {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        expression_attribute_values: Some(values.clone().into()),
                        limit: Some(1),
                        exclusive_start_key: start,
                        scan_index_forward: Some(true),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), expression.into())
                    })
                    .await?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                for item in page {
                    let attrs = item.to_attribute_map()?;
                    let Some(AttributeValue::S(sk)) = attrs.get("sk") else {
                        anyhow::bail!("query item missing sort key: {attrs:?}");
                    };
                    seen.push(sk.clone());
                }
                let Some(next) = next else {
                    break;
                };
                start = Some(next);
            }
            anyhow::ensure!(seen == expected, "{expression}: {seen:?}");
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
