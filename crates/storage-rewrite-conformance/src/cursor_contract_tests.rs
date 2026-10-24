//! Typed cursor identity and numeric/binary ordering on the initialized store.

use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableRequest, DeleteItemRequest,
    ExclusiveStartKey, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, PutItemRequest,
    QueryRequest, ScanRequest, TableName, WireItem,
};

use super::test_support::{TestStore, hash_table_request};

fn key_attributes(key: &str) -> KeyAttributes {
    HashMap::from([("pk".to_owned(), AttributeValue::S(key.to_owned()))]).into()
}

fn composite_item(pk: &str, sk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S(sk.to_owned())),
        ("value".to_owned(), AttributeValue::S(value.to_owned())),
    ])
}

#[tokio::test]
async fn given_hash_only_query_at_consumed_key_when_resumed_then_page_is_empty()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-hash-resume")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-hash-resume");
        for (key, value) in [("k0", "v0"), ("k1", "v1"), ("k2", "v2"), ("k3", "v3")] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".to_owned(), AttributeValue::S(key.to_owned())),
                        ("value".to_owned(), AttributeValue::S(value.to_owned())),
                    ]))?,
                ))
                .await?;
        }
        let mut cursors = Vec::new();
        let mut start = None;
        loop {
            let result = fixture
                .operations
                .scan(ScanRequest {
                    limit: Some(1),
                    exclusive_start_key: start,
                    consistent_read: Some(false),
                    ..ScanRequest::new(table.clone())
                })
                .await?;
            let page = result.items.unwrap_or_default();
            let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
            anyhow::ensure!(page.len() == 1, "scan fixture page");
            let key = page[0].to_attribute_map()?.get("pk").cloned();
            cursors.push((key, next.clone()));
            let Some(next) = next else { break };
            start = Some(next);
        }
        anyhow::ensure!(
            cursors.iter().map(|(key, _)| key).collect::<Vec<_>>()
                == vec![
                    &Some(AttributeValue::S("k0".into())),
                    &Some(AttributeValue::S("k1".into())),
                    &Some(AttributeValue::S("k2".into())),
                    &Some(AttributeValue::S("k3".into())),
                ],
            "scan fixture order: {cursors:?}"
        );
        for (forward, expected) in [(true, [true, false, false]), (false, [false, false, true])] {
            for (index, should_return) in expected.into_iter().enumerate() {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        expression_attribute_values: Some(Into::into(HashMap::from([(
                            ":pk".into(),
                            AttributeValue::S("k1".into()),
                        )]))),
                        limit: Some(1),
                        exclusive_start_key: cursors[index].1.clone(),
                        scan_index_forward: Some(forward),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), "pk = :pk".into())
                    })
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("hash resume query {forward} {index}: {error:?}")
                    })?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                anyhow::ensure!(
                    page.len() == usize::from(should_return) && next.is_none(),
                    "hash cursor forward={forward} index={index}: page={page:?} next={next:?}"
                );
            }
        }
        fixture
            .operations
            .delete_item(DeleteItemRequest::new(table.clone(), key_attributes("k3")))
            .await?;
        let empty_page = fixture
            .operations
            .scan(ScanRequest {
                limit: Some(1),
                exclusive_start_key: cursors[2].1.clone(),
                consistent_read: Some(false),
                ..ScanRequest::new(table)
            })
            .await?;
        let empty_scan = empty_page.items.unwrap_or_default();
        let next = empty_page.last_evaluated_key.map(ExclusiveStartKey::Key);
        anyhow::ensure!(
            empty_scan.is_empty() && next.is_none(),
            "consumed Scan cursor"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_partition_only_query_with_other_partition_cursor_when_resumed_then_members_follow_order()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .create_hash_range_table(
            TableName::new("dynamo-partition-cursor"),
            KeyAttributeType::S,
        )
        .await;
    let outcome = async {
        let table = TableName::new("dynamo-partition-cursor");
        for partition in ["a", "b", "c", "d"] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&composite_item(partition, "one", "v"))?,
                ))
                .await?;
        }
        let mut cursors = Vec::new();
        let mut start = None;
        for partition in ["a", "b", "c"] {
            let result = fixture
                .operations
                .scan(ScanRequest {
                    limit: Some(1),
                    exclusive_start_key: start,
                    consistent_read: Some(false),
                    ..ScanRequest::new(table.clone())
                })
                .await?;
            let page = result.items.unwrap_or_default();
            let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
            anyhow::ensure!(
                page.len() == 1
                    && page[0].to_attribute_map()?.get("pk")
                        == Some(&AttributeValue::S(partition.into())),
                "fixture scan {partition}"
            );
            start = next.clone();
            cursors.push(next);
        }
        for (forward, expected) in [(true, [true, false, false]), (false, [false, false, true])] {
            for (index, should_return) in expected.into_iter().enumerate() {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        expression_attribute_values: Some(Into::into(HashMap::from([(
                            ":pk".into(),
                            AttributeValue::S("b".into()),
                        )]))),
                        limit: Some(1),
                        exclusive_start_key: cursors[index].clone(),
                        scan_index_forward: Some(forward),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), "pk = :pk".into())
                    })
                    .await?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                anyhow::ensure!(
                    page.len() == usize::from(should_return) && next.is_none(),
                    "composite cursor forward={forward} index={index}: page={page:?} next={next:?}"
                );
            }
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_numeric_partition_and_binary_sort_keys_when_query_pages_then_cursors_preserve_order()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let table = TableName::new("dynamo-number-binary-pages");
    fixture
        .operations
        .create_table(CreateTableRequest::new(
            table.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "table_pk".into(),
                    attribute_type: KeyAttributeType::N,
                },
                AttributeDefinition {
                    attribute_name: "sk".into(),
                    attribute_type: KeyAttributeType::B,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "table_pk".into(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".into(),
                    key_type: KeyType::Range,
                },
            ],
            BillingMode::PayPerRequest,
        ))
        .await?;
    let outcome = async {
        for (ordinal, binary) in ["AA==", "AAA=", "/w=="].into_iter().enumerate() {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("table_pk".into(), AttributeValue::N("1".into())),
                        ("sk".into(), AttributeValue::B(binary.into())),
                        ("value".into(), AttributeValue::S(format!("v{ordinal}"))),
                    ]))?,
                ))
                .await?;
        }
        for forward in [true, false] {
            let mut seen = Vec::new();
            let mut start = None;
            loop {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        expression_attribute_values: Some(Into::into(HashMap::from([(
                            ":pk".into(),
                            AttributeValue::N("1.0".into()),
                        )]))),
                        limit: Some(1),
                        exclusive_start_key: start,
                        scan_index_forward: Some(forward),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), "table_pk = :pk".into())
                    })
                    .await?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                anyhow::ensure!(page.len() == 1, "binary key page missing");
                let attrs = page[0].to_attribute_map()?;
                let Some(AttributeValue::B(binary)) = attrs.get("sk") else {
                    anyhow::bail!("binary key missing: {attrs:?}");
                };
                seen.push(binary.clone());
                let Some(next) = next else { break };
                start = Some(next);
            }
            let expected = if forward {
                vec!["AA==", "AAA=", "/w=="]
            } else {
                vec!["/w==", "AAA=", "AA=="]
            };
            anyhow::ensure!(seen == expected, "binary page order: {seen:?}");
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_numeric_sort_keys_when_query_pages_then_equivalent_bound_preserves_order()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let table = TableName::new("dynamo-number-sort-pages");
    fixture
        .operations
        .create_table(CreateTableRequest::new(
            table.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".into(),
                    attribute_type: KeyAttributeType::N,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".into(),
                    key_type: KeyType::Range,
                },
            ],
            BillingMode::PayPerRequest,
        ))
        .await?;
    let outcome = async {
        for number in ["-10", "-1.5", "0", "1", "2", "10"] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".into(), AttributeValue::S("hot".into())),
                        ("sk".into(), AttributeValue::N(number.into())),
                    ]))?,
                ))
                .await?;
        }
        for forward in [true, false] {
            let mut seen = Vec::new();
            let mut start = None;
            loop {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        expression_attribute_values: Some(Into::into(HashMap::from([
                            (":pk".into(), AttributeValue::S("hot".into())),
                            (":lo".into(), AttributeValue::N("-10.00".into())),
                        ]))),
                        limit: Some(1),
                        exclusive_start_key: start,
                        scan_index_forward: Some(forward),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), "pk = :pk AND sk >= :lo".into())
                    })
                    .await?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                anyhow::ensure!(
                    page.len() == 1,
                    "numeric page missing forward={forward} seen={seen:?} next={next:?}"
                );
                let attrs = page[0].to_attribute_map()?;
                let Some(AttributeValue::N(number)) = attrs.get("sk") else {
                    anyhow::bail!("numeric key missing: {attrs:?}");
                };
                seen.push(number.clone());
                let Some(next) = next else { break };
                start = Some(next);
            }
            let expected = if forward {
                vec!["-10", "-1.5", "0", "1", "2", "10"]
            } else {
                vec!["10", "2", "1", "0", "-1.5", "-10"]
            };
            anyhow::ensure!(seen == expected, "numeric page order: {seen:?}");
        }
        let mut seen = Vec::new();
        let mut start = None;
        loop {
            let result = fixture
                .operations
                .scan(ScanRequest {
                    limit: Some(1),
                    exclusive_start_key: start,
                    consistent_read: Some(false),
                    ..ScanRequest::new(table.clone())
                })
                .await?;
            let page = result.items.unwrap_or_default();
            let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
            anyhow::ensure!(page.len() == 1, "numeric scan page missing");
            let attrs = page[0].to_attribute_map()?;
            let Some(AttributeValue::N(number)) = attrs.get("sk") else {
                anyhow::bail!("numeric scan key missing: {attrs:?}");
            };
            seen.push(number.clone());
            let Some(next) = next else { break };
            start = Some(next);
        }
        anyhow::ensure!(
            seen == ["-10", "-1.5", "0", "1", "2", "10"],
            "numeric scan order: {seen:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_items_when_scanned_with_limit_then_pages_cover_all() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-scan-pages")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-scan-pages");
        for index in 1..=5 {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".to_owned(), AttributeValue::S(format!("k{index}"))),
                        ("value".to_owned(), AttributeValue::S("v".to_owned())),
                    ]))?,
                ))
                .await?;
        }
        let mut seen: Vec<String> = Vec::new();
        let mut start_key: Option<ExclusiveStartKey> = None;
        loop {
            let result = fixture
                .operations
                .scan(ScanRequest {
                    limit: Some(2),
                    exclusive_start_key: start_key,
                    consistent_read: Some(false),
                    ..ScanRequest::new(table.clone())
                })
                .await?;
            let page = result.items.unwrap_or_default();
            let lek = result.last_evaluated_key.map(ExclusiveStartKey::Key);
            anyhow::ensure!(!page.is_empty(), "scan page is unexpectedly empty");
            for item in page {
                let attrs = item.to_attribute_map()?;
                let Some(AttributeValue::S(pk)) = attrs.get("pk") else {
                    anyhow::bail!("scan item missing pk: {attrs:?}");
                };
                seen.push(pk.clone());
            }
            match lek {
                Some(next) => start_key = Some(next),
                None => break,
            }
        }
        seen.sort();
        anyhow::ensure!(
            seen == vec![
                "k1".to_string(),
                "k2".to_string(),
                "k3".to_string(),
                "k4".to_string(),
                "k5".to_string(),
            ],
            "paged scan coverage mismatch: {seen:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
