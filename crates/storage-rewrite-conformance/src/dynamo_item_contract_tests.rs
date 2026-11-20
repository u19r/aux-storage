//! Point-operation preservation and return-image contracts on the initialized
//! store.

use std::collections::HashMap;

use storage_types::{
    AttributeValue, GetItemRequest, KeyAttributes, PutItemRequest, ReturnValuesOldNewUpdated,
    TableName, UpdateItemRequest, WireItem,
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
