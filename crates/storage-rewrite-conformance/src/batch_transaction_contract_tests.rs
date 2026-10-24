//! Batch and transaction atomicity, ordinal, and idempotency contracts.

use std::collections::HashMap;

use storage_types::{
    AttributeValue, DeleteRequest, GetItemRequest, KeyAttributes, KeysAndAttributes,
    PutItemRequest, PutRequest, StorageEnum, TableName, TransactConditionCheckRequest,
    TransactDeleteRequest, TransactPutRequest, TransactUpdateRequest, TransactWriteItem,
    TransactWriteItemsRequest, WireItem, WriteRequest,
};

use super::test_support::{TestStore, hash_table_request};

fn key_attributes(key: &str) -> KeyAttributes {
    HashMap::from([("pk".to_owned(), AttributeValue::S(key.to_owned()))]).into()
}

#[tokio::test]
async fn given_transact_with_failing_check_when_run_then_cancelled_and_nothing_persists()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-tx-atomic")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-tx-atomic");
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("existing").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                ]))?,
            ))
            .await?;
        let request = TransactWriteItemsRequest {
            transact_items: vec![
                TransactWriteItem {
                    put: Some(TransactPutRequest {
                        table_name: table.clone(),
                        item: WireItem::try_from(&HashMap::from([
                            ("pk".to_string(), AttributeValue::S("new".to_string())),
                            ("value".to_string(), AttributeValue::S("v".to_string())),
                        ]))
                        .expect("generated transaction item encodes"),
                        indexers: None,
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    ..TransactWriteItem::default()
                },
                TransactWriteItem {
                    condition_check: Some(TransactConditionCheckRequest {
                        table_name: table.clone(),
                        key: key_attributes("existing"),
                        condition_expression: "attribute_not_exists(pk)".to_string(),
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                    ..TransactWriteItem::default()
                },
            ],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        let error = fixture
            .operations
            .transact_write_items(request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("failing transaction unexpectedly succeeded"))?;
        anyhow::ensure!(
            matches!(
                error.as_ref(),
                StorageEnum::TransactionCanceled { .. }
                    | StorageEnum::ConditionalCheckFailed
                    | StorageEnum::ConditionalCheckFailedWithItem { .. }
            ),
            "expected transaction cancellation, got {error:?}"
        );
        let rolled_back = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("new"))
            })
            .await?
            .item;
        anyhow::ensure!(
            rolled_back.is_none(),
            "cancelled transaction left its successful member behind"
        );
        let survivor = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("existing"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("condition-check target vanished"))?;
        anyhow::ensure!(
            survivor.to_attribute_map()?.get("value") == Some(&AttributeValue::S("v".to_string())),
            "cancelled transaction mutated the check target"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_mixed_transact_verbs_when_check_moves_then_identity_and_rollback_hold()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let check = |table: &TableName| TransactWriteItem {
            condition_check: Some(TransactConditionCheckRequest {
                table_name: table.clone(),
                key: key_attributes("existing"),
                condition_expression: "attribute_not_exists(pk)".to_string(),
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
            }),
            ..TransactWriteItem::default()
        };
        let put = |table: &TableName, key: &str| TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: table.clone(),
                item: WireItem::try_from(&HashMap::from([
                    ("pk".to_string(), AttributeValue::S(key.to_string())),
                    ("value".to_string(), AttributeValue::S("new".to_string())),
                ]))
                .expect("generated transaction item encodes"),
                indexers: None,
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..TransactWriteItem::default()
        };
        let delete = |table: &TableName, key: &str| TransactWriteItem {
            delete: Some(TransactDeleteRequest {
                table_name: table.clone(),
                key: key_attributes(key),
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..TransactWriteItem::default()
        };
        let update = |table: &TableName| TransactWriteItem {
            update: Some(TransactUpdateRequest {
                table_name: table.clone(),
                key: key_attributes("updated"),
                update_expression: "SET #v = :v".to_string(),
                indexers: None,
                condition_expression: None,
                expression_attribute_names: Some(Into::into(HashMap::from([(
                    "#v".to_string(),
                    "value".to_string(),
                )]))),
                expression_attribute_values: Some(Into::into(HashMap::from([(
                    ":v".to_string(),
                    AttributeValue::S("changed".to_string()),
                )]))),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..TransactWriteItem::default()
        };
        let run_cancelled = |request: TransactWriteItemsRequest, failed_ordinal: usize| {
            let storage = &fixture.operations;
            async move {
                let error = storage
                    .transact_write_items(request)
                    .await
                    .err()
                    .ok_or_else(|| anyhow::anyhow!("mixed transaction unexpectedly succeeded"))?;
                let StorageEnum::TransactionCanceled { reasons } = error.as_ref() else {
                    if let StorageEnum::InternalServerError { message } = error.as_ref() {
                        anyhow::bail!(
                            "expected ordered transaction cancellation, got internal error: \
                             {message}"
                        );
                    }
                    anyhow::bail!("expected ordered transaction cancellation, got {error:?}");
                };
                anyhow::ensure!(
                    reasons
                        .get(failed_ordinal)
                        .is_some_and(|reason| reason.contains("ConditionalCheckFailed")),
                    "missing cancellation reason at ordinal {failed_ordinal}: {reasons:?}"
                );
                Ok::<(), anyhow::Error>(())
            }
        };

        let first = TableName::new("dynamo-tx-order-first");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new(first.as_ref())))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                first.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("existing").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                ]))?,
            ))
            .await?;
        run_cancelled(
            TransactWriteItemsRequest {
                transact_items: vec![check(&first), put(&first, "created")],
                ..TransactWriteItemsRequest::default()
            },
            0,
        )
        .await?;
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(first.clone(), key_attributes("created"))
                })
                .await?
                .item
                .is_none(),
            "check-first cancellation left a Put behind"
        );

        let middle = TableName::new("dynamo-tx-order-middle");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new(middle.as_ref())))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                middle.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("existing").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                ]))?,
            ))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                middle.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("deleted").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                ]))?,
            ))
            .await?;
        run_cancelled(
            TransactWriteItemsRequest {
                transact_items: vec![
                    put(&middle, "created"),
                    check(&middle),
                    delete(&middle, "deleted"),
                ],
                ..TransactWriteItemsRequest::default()
            },
            1,
        )
        .await?;
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(middle.clone(), key_attributes("created"))
                })
                .await?
                .item
                .is_none(),
            "check-middle cancellation left a Put behind"
        );
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(middle, key_attributes("deleted"))
                })
                .await?
                .item
                .is_some(),
            "check-middle cancellation removed a later Delete target"
        );

        let last = TableName::new("dynamo-tx-order-last");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new(last.as_ref())))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                last.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("existing").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                ]))?,
            ))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                last.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("updated").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("old").to_owned())),
                ]))?,
            ))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                last.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("deleted").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                ]))?,
            ))
            .await?;
        run_cancelled(
            TransactWriteItemsRequest {
                transact_items: vec![update(&last), delete(&last, "deleted"), check(&last)],
                ..TransactWriteItemsRequest::default()
            },
            2,
        )
        .await?;
        let updated = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(last.clone(), key_attributes("updated"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("check-last update target vanished"))?;
        anyhow::ensure!(
            updated.to_attribute_map()?.get("value") == Some(&AttributeValue::S("old".to_string())),
            "check-last cancellation committed the Update"
        );
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(last, key_attributes("deleted"))
                })
                .await?
                .item
                .is_some(),
            "check-last cancellation committed the Delete"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_mixed_transact_verbs_when_committed_then_identity_and_effects_hold()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("dynamo-tx-order-success");
        fixture
            .operations
            .create_table(hash_table_request(TableName::new(table.as_ref())))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("existing").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("kept").to_owned())),
                ]))?,
            ))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("updated").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("old").to_owned())),
                ]))?,
            ))
            .await?;
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("deleted").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("old").to_owned())),
                ]))?,
            ))
            .await?;

        let request = TransactWriteItemsRequest {
            transact_items: vec![
                TransactWriteItem {
                    put: Some(TransactPutRequest {
                        table_name: table.clone(),
                        item: WireItem::try_from(&HashMap::from([
                            ("pk".to_string(), AttributeValue::S("created".to_string())),
                            ("value".to_string(), AttributeValue::S("new".to_string())),
                        ]))
                        .expect("generated transaction item encodes"),
                        indexers: None,
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    ..TransactWriteItem::default()
                },
                TransactWriteItem {
                    condition_check: Some(TransactConditionCheckRequest {
                        table_name: table.clone(),
                        key: key_attributes("existing"),
                        condition_expression: "attribute_exists(pk)".to_string(),
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                    }),
                    ..TransactWriteItem::default()
                },
                TransactWriteItem {
                    update: Some(TransactUpdateRequest {
                        table_name: table.clone(),
                        key: key_attributes("updated"),
                        update_expression: "SET #v = :v".to_string(),
                        indexers: None,
                        condition_expression: None,
                        expression_attribute_names: Some(Into::into(HashMap::from([(
                            "#v".to_string(),
                            "value".to_string(),
                        )]))),
                        expression_attribute_values: Some(Into::into(HashMap::from([(
                            ":v".to_string(),
                            AttributeValue::S("changed".to_string()),
                        )]))),
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    ..TransactWriteItem::default()
                },
                TransactWriteItem {
                    delete: Some(TransactDeleteRequest {
                        table_name: table.clone(),
                        key: key_attributes("deleted"),
                        condition_expression: None,
                        expression_attribute_names: None,
                        expression_attribute_values: None,
                        return_values_on_condition_check_failure: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    ..TransactWriteItem::default()
                },
            ],
            ..TransactWriteItemsRequest::default()
        };
        fixture.operations.transact_write_items(request).await?;

        let created = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("created"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("committed Put is absent"))?;
        anyhow::ensure!(
            created.to_attribute_map()?.get("value") == Some(&AttributeValue::S("new".to_string())),
            "committed Put has the wrong value"
        );
        let updated = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("updated"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("committed Update target is absent"))?;
        anyhow::ensure!(
            updated.to_attribute_map()?.get("value")
                == Some(&AttributeValue::S("changed".to_string())),
            "committed Update has the wrong value"
        );
        anyhow::ensure!(
            fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key_attributes("deleted"))
                })
                .await?
                .item
                .is_none(),
            "committed Delete target survived"
        );
        let existing = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("existing"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("successful condition target is absent"))?;
        anyhow::ensure!(
            existing.to_attribute_map()?.get("value")
                == Some(&AttributeValue::S("kept".to_string())),
            "condition check target changed"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_transact_with_token_when_replayed_then_applies_exactly_once() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-tx-idempotent")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-tx-idempotent");
        let request = || TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                put: Some(TransactPutRequest {
                    table_name: table.clone(),
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S("k1".to_string())),
                        ("value".to_string(), AttributeValue::S("v1".to_string())),
                    ]))
                    .expect("generated transaction item encodes"),
                    indexers: None,
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..TransactWriteItem::default()
            }],
            client_request_token: Some("dynamo-tx-idempotent-token".to_string()),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        let first = request();
        fixture.operations.transact_write_items(first).await?;
        let second = request();
        fixture.operations.transact_write_items(second).await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("idempotent item is absent"))?;
        anyhow::ensure!(
            found.to_attribute_map()?.get("value") == Some(&AttributeValue::S("v1".to_string())),
            "idempotent replay changed the item"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_same_token_when_request_changes_then_mismatch_without_new_effects()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-tx-mismatch")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-tx-mismatch");
        let put = |pk: &str| TransactWriteItem {
            put: Some(TransactPutRequest {
                table_name: table.clone(),
                item: WireItem::try_from(&HashMap::from([
                    ("pk".to_string(), AttributeValue::S(pk.to_string())),
                    ("value".to_string(), AttributeValue::S("v".to_string())),
                ]))
                .expect("generated transaction item encodes"),
                indexers: None,
                condition_expression: None,
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..TransactWriteItem::default()
        };
        let first = TransactWriteItemsRequest {
            transact_items: vec![put("original")],
            client_request_token: Some("dynamo-tx-mismatch-token".to_string()),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        fixture.operations.transact_write_items(first).await?;
        let changed = TransactWriteItemsRequest {
            transact_items: vec![put("changed")],
            client_request_token: Some("dynamo-tx-mismatch-token".to_string()),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        let error = fixture
            .operations
            .transact_write_items(changed)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("mismatched token replay unexpectedly succeeded"))?;
        anyhow::ensure!(
            matches!(error.as_ref(), StorageEnum::IdempotentParameterMismatch),
            "expected idempotent-parameter mismatch, got {error:?}"
        );
        let absent = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("changed"))
            })
            .await?
            .item;
        anyhow::ensure!(
            absent.is_none(),
            "mismatched token replay applied the new parameters"
        );
        let kept = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("original"))
            })
            .await?
            .item;
        anyhow::ensure!(kept.is_some(), "original token write was lost");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_batch_puts_when_run_then_all_visible() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-batch-ok")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-batch-ok");
        let mut writes = Vec::new();
        for index in 1..=3 {
            writes.push(WriteRequest {
                put_request: Some(PutRequest {
                    item: WireItem::try_from(&HashMap::from([
                        ("pk".to_string(), AttributeValue::S(format!("k{index}"))),
                        ("value".to_string(), AttributeValue::S("v".to_string())),
                    ]))
                    .expect("generated transaction item encodes"),
                    indexers: None,
                    aux_item_stream_ttl_hours: None,
                }),
                delete_request: None,
            });
        }
        let request = storage_types::BatchWriteItemRequest {
            request_items: HashMap::from([(table.clone(), writes)]).into(),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        fixture.operations.batch_write_item(request).await?;
        for index in 1..=3 {
            let found = fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key_attributes(&format!("k{index}")))
                })
                .await?
                .item;
            anyhow::ensure!(found.is_some(), "batch member k{index} is absent");
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_batch_get_when_run_then_all_items_returned() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-batch-get")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-batch-get");
        for (key, value) in [("k1", "v1"), ("k2", "v2")] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".to_owned(), AttributeValue::S((key).to_owned())),
                        ("value".to_owned(), AttributeValue::S((value).to_owned())),
                    ]))?,
                ))
                .await?;
        }
        let response = fixture
            .operations
            .batch_get_item(storage_types::BatchGetItemRequest {
                request_items: HashMap::from([(
                    table.clone(),
                    KeysAndAttributes {
                        keys: [key_attributes("k1"), key_attributes("k2")]
                            .into_iter()
                            .collect(),
                        attributes_to_get: None,
                        projection_expression: None,
                        expression_attribute_names: None,
                        consistent_read: None,
                    },
                )])
                .into(),
                return_consumed_capacity: None,
            })
            .await?;
        let mut returned = response
            .responses
            .as_ref()
            .and_then(|map| map.get(&table))
            .map(|items| {
                items
                    .iter()
                    .map(|item| item.to_attribute_map())
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        returned.sort_by(|left, right| {
            left.get("pk")
                .and_then(|value| value.inner_str().ok())
                .cmp(&right.get("pk").and_then(|value| value.inner_str().ok()))
        });
        anyhow::ensure!(
            returned.len() == 2,
            "expected 2 items, got {}",
            returned.len()
        );
        anyhow::ensure!(
            returned[0].get("value") == Some(&AttributeValue::S("v1".to_string()))
                && returned[1].get("value") == Some(&AttributeValue::S("v2".to_string())),
            "batch get values mismatch: {returned:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_batch_with_missing_table_when_run_then_fails_and_existing_preserved()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-batch-fail")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-batch-fail");
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("seed").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("kept").to_owned())),
                ]))?,
            ))
            .await?;
        let request = storage_types::BatchWriteItemRequest {
            request_items: HashMap::from([
                (
                    table.clone(),
                    vec![WriteRequest {
                        put_request: Some(PutRequest {
                            item: WireItem::try_from(&HashMap::from([
                                ("pk".to_string(), AttributeValue::S("new".to_string())),
                                ("value".to_string(), AttributeValue::S("v".to_string())),
                            ]))
                            .expect("generated batch item encodes"),
                            indexers: None,
                            aux_item_stream_ttl_hours: None,
                        }),
                        delete_request: None,
                    }],
                ),
                (
                    TableName::new("dynamo-batch-fail-absent"),
                    vec![WriteRequest {
                        put_request: Some(PutRequest {
                            item: WireItem::try_from(&HashMap::from([(
                                "pk".to_string(),
                                AttributeValue::S("x".to_string()),
                            )]))
                            .expect("generated batch item encodes"),
                            indexers: None,
                            aux_item_stream_ttl_hours: None,
                        }),
                        delete_request: None,
                    }],
                ),
            ])
            .into(),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        let error = fixture
            .operations
            .batch_write_item(request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("batch with missing table unexpectedly succeeded"))?;
        anyhow::ensure!(
            !error.is_retryable_write(),
            "missing-table batch failure must not be classified retryable"
        );
        let kept = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("seed"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("unaffected seed item was lost"))?;
        anyhow::ensure!(
            kept.to_attribute_map()?.get("value") == Some(&AttributeValue::S("kept".to_string())),
            "unaffected seed item was mutated"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_invalid_transact_when_validated_then_rejected_before_mutation() -> anyhow::Result<()>
{
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-tx-empty")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-tx-empty");
        fixture
            .operations
            .put_item(PutItemRequest::new(
                table.clone(),
                WireItem::try_from(&HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(("seed").to_owned())),
                    ("value".to_owned(), AttributeValue::S(("kept").to_owned())),
                ]))?,
            ))
            .await?;
        let request = TransactWriteItemsRequest {
            transact_items: Vec::new(),
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        let error = fixture
            .operations
            .transact_write_items(request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("empty transaction unexpectedly succeeded"))?;
        anyhow::ensure!(
            matches!(
                error.as_ref(),
                StorageEnum::Validation { .. } | StorageEnum::RawValidation { .. }
            ),
            "expected validation failure, got {error:?}"
        );
        let kept = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("seed"))
            })
            .await?
            .item;
        anyhow::ensure!(kept.is_some(), "validation failure removed the seed item");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_transact_delete_when_committed_then_item_removed() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-tx-delete")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-tx-delete");
        for key in ["gone", "kept"] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".to_owned(), AttributeValue::S((key).to_owned())),
                        ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                    ]))?,
                ))
                .await?;
        }
        let request = TransactWriteItemsRequest {
            transact_items: vec![TransactWriteItem {
                delete: Some(TransactDeleteRequest {
                    table_name: table.clone(),
                    key: key_attributes("gone"),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..TransactWriteItem::default()
            }],
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        fixture.operations.transact_write_items(request).await?;
        let gone = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("gone"))
            })
            .await?
            .item;
        anyhow::ensure!(gone.is_none(), "transacted delete did not remove the item");
        let kept = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("kept"))
            })
            .await?
            .item;
        anyhow::ensure!(kept.is_some(), "transacted delete removed its sibling");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_batch_delete_when_run_then_targets_removed_and_sibling_kept() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-batch-mixed")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-batch-mixed");
        for key in ["doomed", "sibling"] {
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    WireItem::try_from(&HashMap::from([
                        ("pk".to_owned(), AttributeValue::S((key).to_owned())),
                        ("value".to_owned(), AttributeValue::S(("v").to_owned())),
                    ]))?,
                ))
                .await?;
        }
        let request = storage_types::BatchWriteItemRequest {
            request_items: HashMap::from([(
                table.clone(),
                vec![
                    WriteRequest {
                        put_request: None,
                        delete_request: Some(DeleteRequest {
                            key: key_attributes("doomed"),
                            aux_item_stream_ttl_hours: None,
                        }),
                    },
                    WriteRequest {
                        put_request: Some(PutRequest {
                            item: WireItem::try_from(&HashMap::from([
                                ("pk".to_string(), AttributeValue::S("fresh".to_string())),
                                ("value".to_string(), AttributeValue::S("v".to_string())),
                            ]))
                            .expect("generated batch item encodes"),
                            indexers: None,
                            aux_item_stream_ttl_hours: None,
                        }),
                        delete_request: None,
                    },
                ],
            )])
            .into(),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        };
        fixture.operations.batch_write_item(request).await?;
        let fresh = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("fresh"))
            })
            .await?
            .item;
        anyhow::ensure!(fresh.is_some(), "batch put member is absent");
        let gone = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("doomed"))
            })
            .await?
            .item;
        anyhow::ensure!(gone.is_none(), "batch delete member survived");
        let sibling = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("sibling"))
            })
            .await?
            .item;
        anyhow::ensure!(sibling.is_some(), "batch removed its sibling");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
