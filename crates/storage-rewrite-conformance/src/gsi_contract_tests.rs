//! GSI ordering, foreground visibility, projection, and consistency contracts.

use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, DeleteItemRequest, ExclusiveStartKey, GetItemRequest, IndexName,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, Projection, ProjectionType,
    PutItemRequest, QueryRequest, TableName, UpdateItemRequest, WireItem,
};

use super::test_support::TestStore;

fn gsi_table_request(name: &str) -> CreateTableRequest {
    let mut request = CreateTableRequest::new(
        TableName::new(name),
        vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".into(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".into(),
                attribute_type: KeyAttributeType::S,
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
    );
    request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("TestGSI"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".into(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);
    request
}

fn gsi_keys_only_table_request(name: &str) -> CreateTableRequest {
    let mut request = gsi_table_request(name);
    request
        .global_secondary_indexes
        .as_mut()
        .expect("GSI request")
        .first_mut()
        .expect("GSI definition")
        .projection
        .projection_type = Some(ProjectionType::KeysOnly);
    request
}

fn gsi_item(pk: &str, sk: &str, gsi_pk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S(pk.to_string())),
        ("sk".to_string(), AttributeValue::S(sk.to_string())),
        ("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string())),
        ("value".to_string(), AttributeValue::S(value.to_string())),
    ])
}

#[tokio::test]
async fn given_numeric_gsi_keys_when_query_pages_then_native_order_is_preserved()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let table = TableName::new("dynamo-number-gsi-pages");
    let mut request = gsi_table_request("dynamo-number-gsi-pages");
    request.attribute_definitions.push(AttributeDefinition {
        attribute_name: "gsi_sk".into(),
        attribute_type: KeyAttributeType::N,
    });
    request.global_secondary_indexes.as_mut().expect("GSI")[0]
        .key_schema
        .push(KeySchemaElement {
            attribute_name: "gsi_sk".into(),
            key_type: KeyType::Range,
        });
    fixture.operations.create_table(request).await?;
    let outcome = async {
        for (ordinal, number) in ["-10", "-1.5", "0", "1", "2", "10"].into_iter().enumerate() {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".into(), AttributeValue::S("hot".into())),
                        ("sk".into(), AttributeValue::S(format!("{ordinal:02}"))),
                        ("gsi_pk".into(), AttributeValue::S("group".into())),
                        ("gsi_sk".into(), AttributeValue::N(number.into())),
                    ]))?,
                ))
                .await?;
        }
        for (forward, expected) in [
            (true, vec!["-10", "-1.5", "0", "1", "2", "10"]),
            (false, vec!["10", "2", "1", "0", "-1.5", "-10"]),
        ] {
            let mut seen = Vec::new();
            let mut start = None;
            loop {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        index_name: Some(IndexName::new("TestGSI")),
                        expression_attribute_values: Some(Into::into(HashMap::from([
                            (":pk".into(), AttributeValue::S("group".into())),
                            (":lo".into(), AttributeValue::N("-10.00".into())),
                        ]))),
                        limit: Some(1),
                        exclusive_start_key: start,
                        scan_index_forward: Some(forward),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), "gsi_pk = :pk AND gsi_sk >= :lo".into())
                    })
                    .await?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                anyhow::ensure!(page.len() == 1, "numeric GSI page missing");
                let attrs = page[0].to_attribute_map()?;
                let Some(AttributeValue::N(number)) = attrs.get("gsi_sk") else {
                    anyhow::bail!("numeric GSI key missing: {attrs:?}");
                };
                seen.push(number.clone());
                let Some(next) = next else { break };
                start = Some(next);
            }
            anyhow::ensure!(seen == expected, "numeric GSI order: {seen:?}");
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_gsi_table_when_post_creation_writes_then_gsi_query_finds_them() -> anyhow::Result<()>
{
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(gsi_table_request("dynamo-gsi-query"))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-gsi-query");
        for (pk, sk) in [("u1", "s1"), ("u1", "s2"), ("u2", "s3")] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&gsi_item(pk, sk, "grp", "v"))?,
                ))
                .await?;
        }
        let result = fixture
            .operations
            .query(QueryRequest {
                index_name: Some(IndexName::new("TestGSI")),
                expression_attribute_values: Some(Into::into(HashMap::from([(
                    ":p".to_string(),
                    AttributeValue::S("grp".to_string()),
                )]))),
                limit: Some(100),
                scan_index_forward: Some(true),
                consistent_read: Some(false),
                ..QueryRequest::new(table.clone(), "gsi_pk = :p".to_string())
            })
            .await?;
        let items = result.items.unwrap_or_default();
        anyhow::ensure!(
            items.len() == 3,
            "expected 3 GSI items, got {}",
            items.len()
        );
        for forward in [true, false] {
            let mut seen = Vec::new();
            let mut start = None;
            loop {
                let result = fixture
                    .operations
                    .query(QueryRequest {
                        index_name: Some(IndexName::new("TestGSI")),
                        expression_attribute_values: Some(Into::into(HashMap::from([(
                            ":p".into(),
                            AttributeValue::S("grp".into()),
                        )]))),
                        limit: Some(1),
                        exclusive_start_key: start,
                        scan_index_forward: Some(forward),
                        consistent_read: Some(false),
                        ..QueryRequest::new(table.clone(), "gsi_pk = :p".into())
                    })
                    .await?;
                let page = result.items.unwrap_or_default();
                let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                anyhow::ensure!(!page.is_empty(), "GSI continuation returned no row");
                for item in page {
                    let attrs = item.to_attribute_map()?;
                    let (Some(AttributeValue::S(pk)), Some(AttributeValue::S(sk))) =
                        (attrs.get("pk"), attrs.get("sk"))
                    else {
                        anyhow::bail!("GSI row lacks base keys: {attrs:?}");
                    };
                    seen.push((pk.clone(), sk.clone()));
                }
                let Some(next) = next else {
                    break;
                };
                start = Some(next);
            }
            let expected = if forward {
                vec![("u1", "s1"), ("u1", "s2"), ("u2", "s3")]
            } else {
                vec![("u2", "s3"), ("u1", "s2"), ("u1", "s1")]
            };
            anyhow::ensure!(
                seen == expected
                    .into_iter()
                    .map(|(pk, sk)| (pk.to_string(), sk.to_string()))
                    .collect::<Vec<_>>(),
                "GSI page order mismatch: {seen:?}"
            );
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_gsi_when_consistent_read_then_rejected_while_base_allows() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(gsi_table_request("dynamo-gsi-consistent"))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-gsi-consistent");
        let error = fixture
            .operations
            .query(QueryRequest {
                index_name: Some(IndexName::new("TestGSI")),
                expression_attribute_values: Some(Into::into(HashMap::from([(
                    ":p".to_string(),
                    AttributeValue::S("grp".to_string()),
                )]))),
                scan_index_forward: Some(true),
                consistent_read: Some(true),
                ..QueryRequest::new(table.clone(), "gsi_pk = :p".to_string())
            })
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("consistent GSI read unexpectedly succeeded"))?;
        anyhow::ensure!(
            error.to_string().contains("Consistent"),
            "unexpected GSI consistent-read error: {error:?}"
        );
        let base_page = fixture
            .operations
            .query(QueryRequest {
                expression_attribute_values: Some(Into::into(HashMap::from([(
                    ":pk".to_string(),
                    AttributeValue::S("nobody".to_string()),
                )]))),
                scan_index_forward: Some(true),
                consistent_read: Some(true),
                ..QueryRequest::new(table, "pk = :pk".to_string())
            })
            .await?;
        let base = base_page.items.unwrap_or_default();
        anyhow::ensure!(base.is_empty(), "base consistent read mismatch");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_keys_only_gsi_when_query_projects_unavailable_value_then_rejected()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let mut request = gsi_keys_only_table_request("dynamo-gsi-projection");
    request.global_secondary_indexes.as_mut().unwrap()[0]
        .key_schema
        .push(KeySchemaElement {
            attribute_name: "sk".into(),
            key_type: KeyType::Range,
        });
    fixture.operations.create_table(request).await?;
    let outcome = async {
        let table = TableName::new("dynamo-gsi-projection");
        let payload = "x".repeat(380_000);
        for (pk, sk, value) in [("u1", "s1", "one"), ("u2", "s2", "two")] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&gsi_item(pk, sk, "grp", &format!("{value}{payload}")))?,
                ))
                .await?;
        }
        let query = |projection_expression,
                     expression_attribute_names: Option<HashMap<String, String>>,
                     exclusive_start_key| QueryRequest {
            index_name: Some(IndexName::new("TestGSI")),
            expression_attribute_names: expression_attribute_names.map(Into::into),
            expression_attribute_values: Some(Into::into(HashMap::from([(
                ":p".to_string(),
                AttributeValue::S("grp".to_string()),
            )]))),
            projection_expression,
            limit: Some(1),
            exclusive_start_key,
            scan_index_forward: Some(true),
            consistent_read: Some(false),
            ..QueryRequest::new(table.clone(), "gsi_pk = :p".to_string())
        };
        let error = fixture
            .operations
            .query(query(
                Some("#v".to_string()),
                Some(HashMap::from([("#v".to_string(), "value".to_string())])),
                None,
            ))
            .await
            .expect_err("KEYS_ONLY index cannot project a base-only attribute");
        anyhow::ensure!(
            error.to_string().contains("does not project [value]"),
            "wrong nonprojected attribute error: {error:?}"
        );
        let first_page = fixture
            .operations
            .query(query(None, None, None))
            .await
            .map_err(|error| anyhow::anyhow!("first GSI page: {:?}", error.into_enum()))?;
        let first = first_page.items.unwrap_or_default();
        let continuation = first_page.last_evaluated_key.map(ExclusiveStartKey::Key);
        anyhow::ensure!(first.len() == 1, "expected first key-only item: {first:?}");
        let continuation =
            continuation.ok_or_else(|| anyhow::anyhow!("first key-only page lost continuation"))?;
        let second_page = fixture
            .operations
            .query(query(None, None, Some(continuation)))
            .await?;
        let second = second_page.items.unwrap_or_default();
        let end = second_page.last_evaluated_key.map(ExclusiveStartKey::Key);
        anyhow::ensure!(
            second.len() == 1 && end.is_none(),
            "wrong final key-only page: {second:?}, {end:?}"
        );
        for item in first.into_iter().chain(second) {
            let attributes = item.to_attribute_map()?;
            anyhow::ensure!(
                !attributes.contains_key("value"),
                "KEYS_ONLY result leaked base-only value: {attributes:?}"
            );
        }
        for (pk, sk) in [("u3", "s3"), ("u4", "s4")] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&gsi_item(pk, sk, "grp", &payload))?,
                ))
                .await?;
        }
        let mut complete = query(None, None, None);
        complete.limit = None;
        let complete = fixture
            .operations
            .query(complete)
            .await
            .map_err(|error| anyhow::anyhow!("complete GSI page: {:?}", error.into_enum()))?;
        anyhow::ensure!(
            complete.count == 4
                && complete.scanned_count == 4
                && complete.last_evaluated_key.is_none(),
            "small GSI projections did not fit one logical page"
        );
        anyhow::ensure!(
            complete
                .items
                .unwrap()
                .iter()
                .all(|item| !item.to_attribute_map().unwrap().contains_key("value")),
            "source payload leaked into KEYS_ONLY projection"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_binary_base_and_gsi_keys_when_paged_and_mutated_then_identity_is_shared()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let table = TableName::new("dynamo-binary-identity");
    let mut request = CreateTableRequest::new(
        table.clone(),
        ["pk", "sk", "gsi_pk", "gsi_sk"]
            .into_iter()
            .map(|name| AttributeDefinition {
                attribute_name: name.into(),
                attribute_type: KeyAttributeType::B,
            })
            .collect(),
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
    );
    request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
        index_name: IndexName::new("BinaryGSI"),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".into(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".into(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);
    fixture.operations.create_table(request).await?;
    let outcome = async {
        let values = ["AA==", "AAA=", "AQAC", "fw==", "gA==", "/w==", "/wA="];
        for binary in values {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".into(), AttributeValue::B("AQ==".into())),
                        ("sk".into(), AttributeValue::B(binary.into())),
                        ("gsi_pk".into(), AttributeValue::B("Ag==".into())),
                        ("gsi_sk".into(), AttributeValue::B(binary.into())),
                        ("value".into(), AttributeValue::S("before".into())),
                    ]))?,
                ))
                .await?;
        }
        for index_name in [None, Some(IndexName::new("BinaryGSI"))] {
            for forward in [true, false] {
                let mut seen = Vec::new();
                let mut start = None;
                let mut cursors = std::collections::HashSet::new();
                loop {
                    let result = fixture
                        .operations
                        .query(QueryRequest {
                            index_name: index_name.clone(),
                            expression_attribute_values: Some(Into::into(HashMap::from([(
                                ":pk".into(),
                                AttributeValue::B(
                                    if index_name.is_some() { "Ag==" } else { "AQ==" }.into(),
                                ),
                            )]))),
                            limit: Some(1),
                            exclusive_start_key: start,
                            scan_index_forward: Some(forward),
                            consistent_read: Some(false),
                            ..QueryRequest::new(
                                table.clone(),
                                if index_name.is_some() {
                                    "gsi_pk = :pk".into()
                                } else {
                                    "pk = :pk".into()
                                },
                            )
                        })
                        .await?;
                    let page = result.items.unwrap_or_default();
                    let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                    anyhow::ensure!(page.len() == 1, "binary page missing");
                    let attrs = page[0].to_attribute_map()?;
                    let field = if index_name.is_some() { "gsi_sk" } else { "sk" };
                    let Some(AttributeValue::B(binary)) = attrs.get(field) else {
                        anyhow::bail!("binary key missing: {attrs:?}");
                    };
                    seen.push(binary.clone());
                    anyhow::ensure!(
                        seen.len() <= values.len(),
                        "binary scan exceeded expected count"
                    );
                    let Some(next) = next else { break };
                    anyhow::ensure!(
                        cursors.insert(serde_json::to_string(&next)?),
                        "repeated binary cursor"
                    );
                    start = Some(next);
                }
                let expected = if forward {
                    values.to_vec()
                } else {
                    values.iter().rev().copied().collect::<Vec<_>>()
                };
                anyhow::ensure!(
                    seen == expected,
                    "binary order index={index_name:?} forward={forward}: {seen:?}"
                );
            }
        }
        // A byte-string prefix has no finite immediate predecessor.
        // The reverse strict bound must still include the shorter key.
        for forward in [true, false] {
            let page_result = fixture
                .operations
                .query(QueryRequest {
                    expression_attribute_values: Some(Into::into(HashMap::from([
                        (":pk".into(), AttributeValue::B("AQ==".into())),
                        (":bound".into(), AttributeValue::B("AAA=".into())),
                    ]))),
                    limit: Some(10),
                    scan_index_forward: Some(forward),
                    consistent_read: Some(false),
                    ..QueryRequest::new(table.clone(), "pk = :pk AND sk < :bound".into())
                })
                .await?;
            let page = page_result.items.unwrap_or_default();
            anyhow::ensure!(
                page.len() == 1
                    && page[0].to_attribute_map()?.get("sk")
                        == Some(&AttributeValue::B("AA==".into())),
                "binary strict prefix bound forward={forward}: {page:?}"
            );
        }
        // The base partition beside this one must stay out of both
        // the base and index scans. The second indexed row also
        // proves that equality spans every table-key tie.
        for (pk, sk, gsi_pk, gsi_sk) in [
            ("AA==", "AA==", "AA==", "AA=="),
            ("Aw==", "AAA=", "Ag==", "AAA="),
        ] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".into(), AttributeValue::B(pk.into())),
                        ("sk".into(), AttributeValue::B(sk.into())),
                        ("gsi_pk".into(), AttributeValue::B(gsi_pk.into())),
                        ("gsi_sk".into(), AttributeValue::B(gsi_sk.into())),
                    ]))?,
                ))
                .await?;
        }
        let cases = [
            ("= :bound", "AAA=", None, vec!["AAA="]),
            ("< :bound", "AAA=", None, vec!["AA=="]),
            ("<= :bound", "AAA=", None, vec!["AA==", "AAA="]),
            (
                "> :bound",
                "AAA=",
                None,
                vec!["AQAC", "fw==", "gA==", "/w==", "/wA="],
            ),
            (
                ">= :bound",
                "AAA=",
                None,
                vec!["AAA=", "AQAC", "fw==", "gA==", "/w==", "/wA="],
            ),
            (
                "BETWEEN :bound AND :upper",
                "AAA=",
                Some("gA=="),
                vec!["AAA=", "AQAC", "fw==", "gA=="],
            ),
            (
                "begins_with({field}, :bound)",
                "/w==",
                None,
                vec!["/w==", "/wA="],
            ),
            (
                "begins_with({field}, :bound)",
                "AA==",
                None,
                vec!["AA==", "AAA="],
            ),
        ];
        for index_name in [None, Some(IndexName::new("BinaryGSI"))] {
            let (hash_field, range_field, hash_value) = if index_name.is_some() {
                ("gsi_pk", "gsi_sk", "Ag==")
            } else {
                ("pk", "sk", "AQ==")
            };
            for (operator, bound, upper, members) in &cases {
                let condition = format!(
                    "{hash_field} = :pk AND {}",
                    if operator.contains("{field}") {
                        operator.replace("{field}", range_field)
                    } else {
                        format!("{range_field} {operator}")
                    }
                );
                let mut expected = Vec::new();
                for member in members {
                    expected.push(((*member).to_owned(), "AQ==".to_owned()));
                    if index_name.is_some() && *member == "AAA=" {
                        expected.push(((*member).to_owned(), "Aw==".to_owned()));
                    }
                }
                for forward in [true, false] {
                    let ordered = if forward {
                        expected.clone()
                    } else {
                        expected.iter().rev().cloned().collect()
                    };
                    for limit in [1, 20] {
                        let mut seen = Vec::new();
                        let mut start = None;
                        let mut cursors = std::collections::HashSet::new();
                        loop {
                            let mut bindings = HashMap::from([
                                (":pk".into(), AttributeValue::B(hash_value.into())),
                                (":bound".into(), AttributeValue::B((*bound).into())),
                            ]);
                            if let Some(upper) = upper {
                                bindings
                                    .insert(":upper".into(), AttributeValue::B((*upper).into()));
                            }
                            let result = fixture
                                .operations
                                .query(QueryRequest {
                                    index_name: index_name.clone(),
                                    expression_attribute_values: Some(Into::into(bindings)),
                                    limit: Some(limit),
                                    exclusive_start_key: start,
                                    scan_index_forward: Some(forward),
                                    consistent_read: Some(false),
                                    ..QueryRequest::new(table.clone(), condition.clone())
                                })
                                .await?;
                            let page = result.items.unwrap_or_default();
                            let next = result.last_evaluated_key.map(ExclusiveStartKey::Key);
                            for item in page {
                                let attrs = item.to_attribute_map()?;
                                let Some(AttributeValue::B(value)) = attrs.get(range_field) else {
                                    anyhow::bail!("missing binary range key: {attrs:?}");
                                };
                                let Some(AttributeValue::B(pk)) = attrs.get("pk") else {
                                    anyhow::bail!("missing binary base hash: {attrs:?}");
                                };
                                seen.push((value.clone(), pk.clone()));
                            }
                            anyhow::ensure!(
                                seen.len() <= ordered.len(),
                                "range scan exceeded expected count: {condition} {seen:?}"
                            );
                            let Some(next) = next else { break };
                            anyhow::ensure!(
                                cursors.insert(serde_json::to_string(&next)?),
                                "repeated range cursor: {condition}"
                            );
                            start = Some(next);
                        }
                        anyhow::ensure!(
                            seen == ordered,
                            "binary interval index={index_name:?} condition={condition} \
                             forward={forward} limit={limit}: {seen:?}, expected {ordered:?}"
                        );
                    }
                }
            }
        }
        let exact_page = fixture
            .operations
            .query(QueryRequest {
                index_name: Some(IndexName::new("BinaryGSI")),
                expression_attribute_values: Some(Into::into(HashMap::from([
                    (":pk".into(), AttributeValue::B("Ag==".into())),
                    (":sk".into(), AttributeValue::B("AA==".into())),
                ]))),
                limit: Some(10),
                scan_index_forward: Some(true),
                consistent_read: Some(false),
                ..QueryRequest::new(table.clone(), "gsi_pk = :pk AND gsi_sk = :sk".into())
            })
            .await?;
        let exact = exact_page.items.unwrap_or_default();
        anyhow::ensure!(
            exact.len() == 1
                && exact[0].to_attribute_map()?.get("gsi_sk")
                    == Some(&AttributeValue::B("AA==".into())),
            "binary GSI exact range bound leaked prefix extension: {exact:?}"
        );
        let key: KeyAttributes = HashMap::from([
            ("pk".into(), AttributeValue::B("AQ==".into())),
            ("sk".into(), AttributeValue::B("AQAC".into())),
        ])
        .into();
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key.clone())
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("binary point Get missed item"))?;
        anyhow::ensure!(
            found.to_attribute_map()?.get("value") == Some(&AttributeValue::S("before".into())),
            "wrong binary point item"
        );
        let update = UpdateItemRequest::builder()
            .table_name(table.clone())
            .key(key.clone())
            .update_expression("SET #value = :value")
            .expression_attribute_names(Some(Into::into(HashMap::from([(
                "#value".into(),
                "value".into(),
            )]))))
            .expression_attribute_values(Some(Into::into(HashMap::from([(
                ":value".into(),
                AttributeValue::S("after".into()),
            )]))))
            .build();
        fixture.operations.update_item(update).await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key.clone())
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("binary point Update lost item"))?;
        anyhow::ensure!(
            found.to_attribute_map()?.get("value") == Some(&AttributeValue::S("after".into())),
            "binary point Update missed identity"
        );
        fixture
            .operations
            .delete_item(DeleteItemRequest::new(table.clone(), key.clone()))
            .await?;
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key)
                })
                .await?
                .item
                .is_none(),
            "binary point Delete missed identity"
        );
        let remaining_page = fixture
            .operations
            .query(QueryRequest {
                index_name: Some(IndexName::new("BinaryGSI")),
                expression_attribute_values: Some(Into::into(HashMap::from([(
                    ":pk".into(),
                    AttributeValue::B("Ag==".into()),
                )]))),
                limit: Some(10),
                scan_index_forward: Some(true),
                consistent_read: Some(false),
                ..QueryRequest::new(table, "gsi_pk = :pk".into())
            })
            .await?;
        let remaining = remaining_page.items.unwrap_or_default();
        anyhow::ensure!(remaining.len() == 7, "binary GSI deletion not reflected");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
