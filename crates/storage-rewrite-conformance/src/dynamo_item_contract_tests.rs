//! Point-operation preservation and return-image contracts on the initialized
//! store.

use std::collections::HashMap;

use storage_types::{
    AllOld, AttributeValue, DeleteItemRequest, GetItemRequest, KeyAttributeType, KeyAttributes,
    PutItemRequest, ReturnValuesOldNewUpdated, ScanRequest, StorageEnum, TableName,
    UpdateItemRequest, WireItem,
};

use super::test_support::{TestStore, hash_table_request};

fn key_attributes(key: &str) -> KeyAttributes {
    HashMap::from([("pk".to_owned(), AttributeValue::S(key.to_owned()))]).into()
}

fn put_request(
    table: TableName,
    item: HashMap<String, AttributeValue>,
) -> anyhow::Result<PutItemRequest> {
    Ok(PutItemRequest::new(table, WireItem::try_from(&item)?))
}

fn direct_put(table: TableName, key: &str, value: &str) -> anyhow::Result<PutItemRequest> {
    put_request(
        table,
        HashMap::from([
            ("pk".to_owned(), AttributeValue::S(key.to_owned())),
            ("value".to_owned(), AttributeValue::S(value.to_owned())),
        ]),
    )
}

#[tokio::test]
async fn given_missing_item_when_read_then_none() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-missing")))
        .await?;
    let outcome = async {
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(TableName::new("dynamo-missing"), key_attributes("absent"))
            })
            .await?
            .item;
        anyhow::ensure!(found.is_none(), "missing item read returned a value");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_item_when_put_then_read_returns_value() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-roundtrip")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-roundtrip");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "v1")?)
            .await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item;
        let found = found.ok_or_else(|| anyhow::anyhow!("written item is absent on read"))?;
        let attrs = found.to_attribute_map()?.to_hashmap();
        let expected = HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            ("value".to_string(), AttributeValue::S("v1".to_string())),
        ]);
        anyhow::ensure!(attrs == expected, "roundtrip item mismatch: {attrs:?}");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_conditional_put_when_condition_fails_then_item_preserved() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-conditional")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-conditional");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "original")?)
            .await?;
        let mut guarded = direct_put(table.clone(), "k1", "replacement")?;
        guarded.condition_expression = Some("attribute_not_exists(pk)".to_string());
        let error = fixture
            .operations
            .put_item(guarded)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("conditional put unexpectedly succeeded"))?;
        anyhow::ensure!(
            matches!(
                error.as_ref(),
                StorageEnum::ConditionalCheckFailed
                    | StorageEnum::ConditionalCheckFailedWithItem { .. }
            ),
            "expected conditional failure, got {error:?}"
        );
        let preserved = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("original item missing after failed condition"))?;
        let attrs = preserved.to_attribute_map()?.to_hashmap();
        let expected = HashMap::from([
            ("pk".to_string(), AttributeValue::S("k1".to_string())),
            (
                "value".to_string(),
                AttributeValue::S("original".to_string()),
            ),
        ]);
        anyhow::ensure!(
            attrs == expected,
            "failed condition mutated the item: {attrs:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_existing_item_when_deleted_then_read_returns_none() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-delete")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-delete");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "v1")?)
            .await?;
        let deleted = fixture
            .operations
            .delete_item(DeleteItemRequest::new(table.clone(), key_attributes("k1")))
            .await?
            .attributes;
        anyhow::ensure!(deleted.is_none(), "default Delete returned an item");
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("k1"))
            })
            .await?
            .item;
        anyhow::ensure!(found.is_none(), "deleted item still readable");
        // Second delete is a no-op: still succeeds (or at worst reports
        // success) and the item stays absent.
        let deleted = fixture
            .operations
            .delete_item(DeleteItemRequest::new(table.clone(), key_attributes("k1")))
            .await?
            .attributes;
        anyhow::ensure!(
            deleted.is_none(),
            "Delete of an absent item returned an item"
        );
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item;
        anyhow::ensure!(found.is_none(), "item reappeared after second delete");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_delete_with_all_old_when_item_exists_then_deleted_image_is_returned()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new(
            "dynamo-delete-return-old",
        )))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-delete-return-old");
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("k1".to_owned())),
            ("value".to_owned(), AttributeValue::S("before".to_owned())),
        ]);
        fixture
            .operations
            .put_item(put_request(table.clone(), item.clone())?)
            .await?;

        let mut request = DeleteItemRequest::new(table.clone(), key_attributes("k1"));
        request.return_values = Some(AllOld::AllOld);
        let returned = fixture
            .operations
            .delete_item(request)
            .await?
            .attributes
            .ok_or_else(|| anyhow::anyhow!("ALL_OLD Delete omitted its image"))?;
        anyhow::ensure!(
            returned.to_hashmap() == item,
            "wrong deleted image: {returned:?}"
        );

        let mut absent_request = DeleteItemRequest::new(table.clone(), key_attributes("k1"));
        absent_request.return_values = Some(AllOld::AllOld);
        anyhow::ensure!(
            fixture
                .operations
                .delete_item(absent_request)
                .await?
                .attributes
                .is_none(),
            "ALL_OLD Delete returned an image for an absent item"
        );

        let absent = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item;
        anyhow::ensure!(absent.is_none(), "ALL_OLD Delete left the item present");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_existing_item_when_delete_condition_fails_then_old_image_preserved()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-delete-allold")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-delete-allold");
        let mut item = HashMap::from([("pk".to_string(), AttributeValue::S("k1".to_string()))]);
        item.insert(
            "payload".to_string(),
            AttributeValue::M(HashMap::from([(
                "nested".to_string(),
                AttributeValue::L(vec![AttributeValue::N("42".to_string())]),
            )])),
        );
        fixture
            .operations
            .put_item(put_request(table.clone(), item.clone())?)
            .await?;
        let mut request = DeleteItemRequest::new(table.clone(), key_attributes("k1"));
        request.condition_expression = Some("attribute_not_exists(pk)".to_string());
        request.return_values_on_condition_check_failure = Some("ALL_OLD".to_string());
        let error = fixture
            .operations
            .delete_item(request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("failing delete condition unexpectedly passed"))?;
        let StorageEnum::ConditionalCheckFailedWithItem { item: returned } = error.as_ref() else {
            anyhow::bail!("expected conditional failure with ALL_OLD, got {error:?}");
        };
        anyhow::ensure!(returned.to_hashmap() == item, "ALL_OLD image mismatch");
        let preserved = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("item missing after failed delete"))?;
        anyhow::ensure!(
            preserved.to_attribute_map()?.to_hashmap() == item,
            "failed delete mutated the item"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_missing_item_when_put_with_exists_condition_then_rejected_without_effect()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-put-exists-cond")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-put-exists-cond");
        let mut request = PutItemRequest::new(
            table.clone(),
            storage_types::WireItem::try_from(
                &(HashMap::from([
                    ("pk".to_string(), AttributeValue::S("k1".to_string())),
                    ("value".to_string(), AttributeValue::S("v1".to_string())),
                ])),
            )
            .unwrap(),
        );
        request.condition_expression = Some("attribute_exists(pk)".to_string());
        let error = fixture
            .operations
            .put_item(request)
            .await
            .err()
            .ok_or_else(|| {
                anyhow::anyhow!("attribute_exists(pk) unexpectedly passed on missing item")
            })?;
        anyhow::ensure!(
            matches!(
                error.as_ref(),
                StorageEnum::ConditionalCheckFailed
                    | StorageEnum::ConditionalCheckFailedWithItem { .. }
            ),
            "expected conditional failure, got {error:?}"
        );
        anyhow::ensure!(
            !error.is_retryable_write(),
            "conditional failure must not be classified retryable"
        );
        anyhow::ensure!(
            error.is_conditional_write_contention(),
            "conditional failure must be classified as write contention"
        );
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item;
        anyhow::ensure!(found.is_none(), "failed put produced an item");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_item_when_updated_then_new_value_visible() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-update")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-update");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "before")?)
            .await?;
        let request = UpdateItemRequest::builder()
            .table_name(table.clone())
            .key(HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("k1".to_owned()),
            )]))
            .update_expression("SET #value = :value".to_string())
            .expression_attribute_names(Some(Into::into(HashMap::from([(
                "#value".to_owned(),
                "value".to_owned(),
            )]))))
            .expression_attribute_values(Some(Into::into(HashMap::from([(
                ":value".to_owned(),
                AttributeValue::S("after".to_owned()),
            )]))))
            .build();
        fixture.operations.update_item(request).await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("updated item is absent"))?;
        let attrs = found.to_attribute_map()?.to_hashmap();
        anyhow::ensure!(
            attrs.get("value") == Some(&AttributeValue::S("after".to_string())),
            "update did not apply: {attrs:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_item_when_updated_with_each_return_mode_then_response_and_store_match()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-update-returns")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-update-returns");
        let old_fields = HashMap::from([
            ("value".to_owned(), AttributeValue::S("before".to_owned())),
            ("stale".to_owned(), AttributeValue::S("remove".to_owned())),
            ("count".to_owned(), AttributeValue::N("5".to_owned())),
            (
                "tags".to_owned(),
                AttributeValue::SS(vec!["a".to_owned(), "b".to_owned()]),
            ),
        ]);
        let new_fields = HashMap::from([
            ("value".to_owned(), AttributeValue::S("after".to_owned())),
            ("count".to_owned(), AttributeValue::N("6".to_owned())),
            ("tags".to_owned(), AttributeValue::SS(vec!["b".to_owned()])),
        ]);
        for (index, mode) in [
            ReturnValuesOldNewUpdated::None,
            ReturnValuesOldNewUpdated::AllOld,
            ReturnValuesOldNewUpdated::AllNew,
            ReturnValuesOldNewUpdated::UpdatedOld,
            ReturnValuesOldNewUpdated::UpdatedNew,
        ]
        .into_iter()
        .enumerate()
        {
            let key = format!("k{index}");
            let key_field = ("pk".to_owned(), AttributeValue::S(key.clone()));
            let mut old_item = old_fields.clone();
            old_item.insert(key_field.0.clone(), key_field.1.clone());
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    storage_types::WireItem::try_from(&(old_item.clone())).unwrap(),
                ))
                .await?;
            let request = UpdateItemRequest::builder()
                .table_name(table.clone())
                .key(key_attributes(&key))
                .update_expression(
                    "SET #value = :after REMOVE #stale ADD #count :one DELETE #tags :drop"
                        .to_owned(),
                )
                .expression_attribute_names(Some(Into::into(HashMap::from([
                    ("#value".to_owned(), "value".to_owned()),
                    ("#stale".to_owned(), "stale".to_owned()),
                    ("#count".to_owned(), "count".to_owned()),
                    ("#tags".to_owned(), "tags".to_owned()),
                ]))))
                .expression_attribute_values(Some(Into::into(HashMap::from([
                    (":after".to_owned(), AttributeValue::S("after".to_owned())),
                    (":one".to_owned(), AttributeValue::N("1".to_owned())),
                    (":drop".to_owned(), AttributeValue::SS(vec!["a".to_owned()])),
                ]))))
                .return_values(Some(mode.clone()))
                .build();
            let response = fixture.operations.update_item(request).await?;
            let mut stored = new_fields.clone();
            stored.insert(key_field.0.clone(), key_field.1.clone());
            let expected = match mode {
                ReturnValuesOldNewUpdated::None => None,
                ReturnValuesOldNewUpdated::AllOld => Some(old_item),
                ReturnValuesOldNewUpdated::AllNew => Some(stored.clone()),
                ReturnValuesOldNewUpdated::UpdatedOld => Some(old_fields.clone()),
                ReturnValuesOldNewUpdated::UpdatedNew => Some(new_fields.clone()),
            };
            anyhow::ensure!(
                response.attributes.map(HashMap::from) == expected,
                "wrong response for {mode:?}"
            );
            let actual = fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key_attributes(&key))
                })
                .await?
                .item
                .ok_or_else(|| anyhow::anyhow!("updated item absent"))?
                .to_attribute_map()?
                .to_hashmap();
            anyhow::ensure!(actual == stored, "wrong stored item for {mode:?}");
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_nested_map_and_list_when_updated_then_filtered_images_keep_paths()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new(
            "dynamo-update-nested-returns",
        )))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-update-nested-returns");
        for (key, mode, expected_leaf, expected_head) in [
            (
                "old",
                ReturnValuesOldNewUpdated::UpdatedOld,
                "before",
                "first",
            ),
            (
                "new",
                ReturnValuesOldNewUpdated::UpdatedNew,
                "after",
                "changed",
            ),
        ] {
            let item = HashMap::from([
                ("pk".to_owned(), AttributeValue::S(key.to_owned())),
                (
                    "doc".to_owned(),
                    AttributeValue::M(HashMap::from([
                        ("leaf".to_owned(), AttributeValue::S("before".to_owned())),
                        ("keep".to_owned(), AttributeValue::S("untouched".to_owned())),
                    ])),
                ),
                (
                    "entries".to_owned(),
                    AttributeValue::L(vec![
                        AttributeValue::S("first".to_owned()),
                        AttributeValue::S("second".to_owned()),
                    ]),
                ),
            ]);
            fixture
                .operations
                .put_item(PutItemRequest::new(
                    table.clone(),
                    storage_types::WireItem::try_from(&(item)).unwrap(),
                ))
                .await?;
            let request = UpdateItemRequest::builder()
                .table_name(table.clone())
                .key(key_attributes(key))
                .update_expression("SET #doc.#leaf = :after, #entries[0] = :changed".to_owned())
                .expression_attribute_names(Some(Into::into(HashMap::from([
                    ("#doc".to_owned(), "doc".to_owned()),
                    ("#leaf".to_owned(), "leaf".to_owned()),
                    ("#entries".to_owned(), "entries".to_owned()),
                ]))))
                .expression_attribute_values(Some(Into::into(HashMap::from([
                    (":after".to_owned(), AttributeValue::S("after".to_owned())),
                    (
                        ":changed".to_owned(),
                        AttributeValue::S("changed".to_owned()),
                    ),
                ]))))
                .return_values(Some(mode))
                .build();
            let response = fixture.operations.update_item(request).await?;
            let expected = HashMap::from([
                (
                    "doc".to_owned(),
                    AttributeValue::M(HashMap::from([(
                        "leaf".to_owned(),
                        AttributeValue::S(expected_leaf.to_owned()),
                    )])),
                ),
                (
                    "entries".to_owned(),
                    AttributeValue::L(vec![AttributeValue::S(expected_head.to_owned())]),
                ),
            ]);
            anyhow::ensure!(
                response.attributes.map(HashMap::from) == Some(expected),
                "nested Update response differs for {key}"
            );
            let stored = fixture
                .operations
                .get_item(GetItemRequest {
                    consistent_read: Some(true),
                    ..GetItemRequest::new(table.clone(), key_attributes(key))
                })
                .await?
                .item
                .ok_or_else(|| anyhow::anyhow!("nested updated item absent"))?
                .to_attribute_map()?
                .to_hashmap();
            anyhow::ensure!(
                stored.get("doc")
                    == Some(&AttributeValue::M(HashMap::from([
                        ("leaf".to_owned(), AttributeValue::S("after".to_owned())),
                        ("keep".to_owned(), AttributeValue::S("untouched".to_owned())),
                    ]))),
                "nested stored map differs"
            );
            anyhow::ensure!(
                stored.get("entries")
                    == Some(&AttributeValue::L(vec![
                        AttributeValue::S("changed".to_owned()),
                        AttributeValue::S("second".to_owned()),
                    ])),
                "nested stored list differs"
            );
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_update_without_expression_when_existing_or_missing_then_old_item_is_kept_or_created()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new(
            "dynamo-update-no-expression",
        )))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-update-no-expression");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "existing", "keep")?)
            .await?;

        for key in ["existing", "missing"] {
            let request = UpdateItemRequest::builder()
                .table_name(table.clone())
                .key(key_attributes(key))
                .return_values(Some(ReturnValuesOldNewUpdated::AllNew))
                .build();
            let response = fixture.operations.update_item(request).await?;
            let expected = if key == "existing" {
                HashMap::from([
                    ("pk".to_owned(), AttributeValue::S(key.to_owned())),
                    ("value".to_owned(), AttributeValue::S("keep".to_owned())),
                ])
            } else {
                HashMap::from([("pk".to_owned(), AttributeValue::S(key.to_owned()))])
            };
            anyhow::ensure!(
                response.attributes.map(HashMap::from) == Some(expected),
                "empty Update returned the wrong image for {key}"
            );
        }

        let existing = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("existing"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("Update removed the existing item"))?
            .to_attribute_map()?
            .to_hashmap();
        anyhow::ensure!(
            existing.get("value") == Some(&AttributeValue::S("keep".to_owned())),
            "Update removed existing attributes: {existing:?}"
        );

        let created = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("missing"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("Update without an expression did not upsert"))?
            .to_attribute_map()?
            .to_hashmap();
        anyhow::ensure!(
            created == HashMap::from([("pk".to_owned(), AttributeValue::S("missing".to_owned()))]),
            "empty Update created an unexpected item: {created:?}"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_existing_item_when_update_condition_fails_then_value_preserved() -> anyhow::Result<()>
{
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-update-cond")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-update-cond");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "original")?)
            .await?;
        let request = UpdateItemRequest::builder()
            .table_name(table.clone())
            .key(HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("k1".to_owned()),
            )]))
            .update_expression("SET #value = :value".to_string())
            .condition_expression(Some("attribute_not_exists(pk)".to_string()))
            .expression_attribute_names(Some(Into::into(HashMap::from([(
                "#value".to_owned(),
                "value".to_owned(),
            )]))))
            .expression_attribute_values(Some(Into::into(HashMap::from([(
                ":value".to_owned(),
                AttributeValue::S("replacement".to_owned()),
            )]))))
            .return_values_on_condition_check_failure(Some("ALL_OLD".to_owned()))
            .build();
        let error = fixture
            .operations
            .update_item(request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("failing update condition unexpectedly passed"))?;
        let StorageEnum::ConditionalCheckFailedWithItem { item } = error.as_ref() else {
            anyhow::bail!("expected failed Update with old image, got {error:?}");
        };
        anyhow::ensure!(
            item.to_hashmap()
                == HashMap::from([
                    ("pk".to_owned(), AttributeValue::S("k1".to_owned())),
                    ("value".to_owned(), AttributeValue::S("original".to_owned())),
                ]),
            "failed Update returned the wrong old image"
        );
        let preserved = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("k1"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("item missing after failed update"))?;
        anyhow::ensure!(
            preserved.to_attribute_map()?.to_hashmap().get("value")
                == Some(&AttributeValue::S("original".to_string())),
            "failed update mutated the item"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

fn composite_key(pk: &str, sk: &str) -> KeyAttributes {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S(sk.to_owned())),
    ])
    .into()
}

fn composite_item(pk: &str, sk: &str, value: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_owned(), AttributeValue::S(pk.to_owned())),
        ("sk".to_owned(), AttributeValue::S(sk.to_owned())),
        ("value".to_owned(), AttributeValue::S(value.to_owned())),
    ])
}

#[tokio::test]
async fn given_put_without_key_when_validated_then_rejected_and_table_empty() -> anyhow::Result<()>
{
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-key-edge")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-key-edge");
        let request = PutItemRequest::new(
            table.clone(),
            storage_types::WireItem::try_from(
                &(HashMap::from([("value".to_string(), AttributeValue::S("no-key".to_string()))])),
            )
            .unwrap(),
        );
        let error = fixture
            .operations
            .put_item(request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("keyless put unexpectedly succeeded"))?;
        anyhow::ensure!(
            matches!(
                error.as_ref(),
                StorageEnum::Validation { .. }
                    | StorageEnum::RawValidation { .. }
                    | StorageEnum::KeyValidation(_)
            ),
            "expected validation failure, got {error:?}"
        );
        anyhow::ensure!(
            !error.is_retryable_write(),
            "validation failure must not be classified retryable"
        );
        let scan_page = fixture
            .operations
            .scan(ScanRequest {
                consistent_read: Some(false),
                ..ScanRequest::new(table)
            })
            .await?;
        let items = scan_page.items.unwrap_or_default();
        anyhow::ensure!(items.is_empty(), "rejected put left a visible item");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_unrelated_rows_when_point_ops_then_target_exact() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-point-bounded")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-point-bounded");
        for index in 0..50 {
            fixture
                .operations
                .put_item(direct_put(
                    table.clone(),
                    &format!("unrelated-{index:03}"),
                    "noise",
                )?)
                .await?;
        }
        fixture
            .operations
            .put_item(direct_put(table.clone(), "target", "before")?)
            .await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("target"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("target missing among unrelated rows"))?;
        anyhow::ensure!(
            found.to_attribute_map()?.to_hashmap().get("value")
                == Some(&AttributeValue::S("before".to_string())),
            "point read returned a neighbor's value"
        );
        let mut guarded = direct_put(table.clone(), "target", "after")?;
        guarded.condition_expression = Some("attribute_exists(pk)".to_string());
        fixture.operations.put_item(guarded).await?;
        let updated = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("target"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("target missing after conditional put"))?;
        anyhow::ensure!(
            updated.to_attribute_map()?.to_hashmap().get("value")
                == Some(&AttributeValue::S("after".to_string())),
            "conditional point write missed its target"
        );
        let neighbor = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("unrelated-007"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("unrelated row went missing"))?;
        anyhow::ensure!(
            neighbor.to_attribute_map()?.to_hashmap().get("value")
                == Some(&AttributeValue::S("noise".to_string())),
            "point write contaminated a neighbor"
        );
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_composite_key_when_partial_key_read_then_rejected() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .create_hash_range_table(TableName::new("dynamo-partial-key"), KeyAttributeType::S)
        .await;
    let outcome = async {
        let table = TableName::new("dynamo-partial-key");
        fixture
            .operations
            .put_item(put_request(table.clone(), composite_item("p", "s", "v"))?)
            .await?;
        let full = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), composite_key("p", "s"))
            })
            .await?
            .item;
        anyhow::ensure!(full.is_some(), "full-key read missed");
        let partial: KeyAttributes =
            HashMap::from([("pk".to_string(), AttributeValue::S("p".to_string()))]).into();
        let error = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), partial)
            })
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("partial-key read unexpectedly succeeded"))?;
        anyhow::ensure!(
            matches!(
                error.as_ref(),
                StorageEnum::Validation { .. }
                    | StorageEnum::RawValidation { .. }
                    | StorageEnum::KeyValidation(_)
            ),
            "expected key validation failure, got {error:?}"
        );
        let survivor = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, composite_key("p", "s"))
            })
            .await?
            .item;
        anyhow::ensure!(survivor.is_some(), "failed read removed the item");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_same_key_when_put_twice_then_last_wins() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-upsert")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-upsert");
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "first")?)
            .await?;
        fixture
            .operations
            .put_item(direct_put(table.clone(), "k1", "second")?)
            .await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table.clone(), key_attributes("k1"))
            })
            .await?
            .item
            .ok_or_else(|| anyhow::anyhow!("upserted item is absent"))?;
        let attrs = found.to_attribute_map()?.to_hashmap();
        anyhow::ensure!(
            attrs.get("value") == Some(&AttributeValue::S("second".to_string())),
            "last put did not win: {attrs:?}"
        );
        let scan_page = fixture
            .operations
            .scan(ScanRequest {
                consistent_read: Some(false),
                ..ScanRequest::new(table)
            })
            .await?;
        let items = scan_page.items.unwrap_or_default();
        anyhow::ensure!(items.len() == 1, "upsert duplicated the key");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

#[tokio::test]
async fn given_delete_without_condition_on_missing_then_noop_and_absent() -> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    fixture
        .operations
        .create_table(hash_table_request(TableName::new("dynamo-delete-noop")))
        .await?;
    let outcome = async {
        let table = TableName::new("dynamo-delete-noop");
        fixture
            .operations
            .delete_item(DeleteItemRequest::new(
                table.clone(),
                key_attributes("ghost"),
            ))
            .await?;
        let found = fixture
            .operations
            .get_item(GetItemRequest {
                consistent_read: Some(true),
                ..GetItemRequest::new(table, key_attributes("ghost"))
            })
            .await?
            .item;
        anyhow::ensure!(found.is_none(), "no-op delete produced an item");
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
