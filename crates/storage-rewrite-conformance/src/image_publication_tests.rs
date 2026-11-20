//! Image identity and publication order through the initialized public owner.

use serde_json::json;
use storage_types::{
    AttributeMap, AttributeValue, BatchWriteItemRequest, DeleteItemRequest, PutItemRequest,
    TableName,
};

use super::{
    gsi_workflow_tests::table_request,
    image_retention_tests::{canonical_history_reads, table_records},
    test_support::TestStore,
};

#[tokio::test]
async fn given_mixed_writes_when_extension_history_is_read_then_each_snapshot_is_exact()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("image_publication");
        let mut request = table_request(table.as_ref());
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request).await?;
        let mut expected = Vec::<Option<AttributeMap>>::new();
        for step in 0..5 {
            if step == 3 {
                fixture
                    .operations
                    .delete_item(DeleteItemRequest::new(
                        table.clone(),
                        [("pk".to_owned(), AttributeValue::S("key".to_owned()))],
                    ))
                    .await?;
                expected.push(None);
                continue;
            }
            let payload = (0..128)
                .map(|part| {
                    uuid::Uuid::new_v5(
                        &uuid::Uuid::NAMESPACE_OID,
                        format!("{step}/{part}").as_bytes(),
                    )
                    .to_string()
                })
                .collect::<String>();
            let item = json!({"pk": {"S": "key"}, "gsi_pk": {"S": format!("group-{step}")},
                "gsi_sk": {"S": "key"}, "payload": {"S": payload},
                "number": {"N": format!("{step}")},
                "numbers": {"NS": ["1700000000", "1"]},
                "nested": {"M": {"zero": {"N": "0"}}}});
            expected.push(Some(serde_json::from_value(item.clone())?));
            if step == 1 || step == 4 {
                let response = fixture
                    .operations
                    .batch_write_item(
                        BatchWriteItemRequest::try_from(json!({
                            "RequestItems": {table.as_ref(): [{"PutRequest": {"Item": item}}]}
                        }))
                        .map_err(anyhow::Error::msg)?,
                    )
                    .await?;
                anyhow::ensure!(
                    response
                        .unprocessed_items
                        .as_ref()
                        .is_none_or(|items| items.values().all(Vec::is_empty))
                );
            } else {
                fixture
                    .operations
                    .put_item(
                        PutItemRequest::try_from(json!({
                            "TableName": table, "Item": item
                        }))
                        .map_err(anyhow::Error::msg)?,
                    )
                    .await?;
            }
        }
        fixture.wait_until_changes_are_readable().await;
        let records = table_records(&fixture, &table).await?;
        let arn = fixture
            .operations
            .describe_table(storage_types::DescribeTableRequest {
                table_name: table.clone(),
            })
            .await?
            .table
            .latest_stream_arn
            .unwrap();
        let standard = super::table_history_retention_tests::read_standard(&fixture, &arn).await?;
        anyhow::ensure!(records.len() == 5 && standard.len() == 5);
        for (ordinal, (record, event)) in records.iter().zip(&standard).enumerate() {
            anyhow::ensure!(record.item == expected[ordinal]);
            anyhow::ensure!(event.dynamodb.new_image == expected[ordinal]);
            anyhow::ensure!(
                event.dynamodb.old_image
                    == ordinal
                        .checked_sub(1)
                        .and_then(|index| expected[index].clone())
            );
            anyhow::ensure!(record.system_position.is_none());
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}

fn boundary_image(key: &str, seed: &str, length: usize) -> AttributeMap {
    let mut state = seed.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |state, byte| {
        state
            .wrapping_mul(0x0000_0100_0000_01b3)
            .wrapping_add(u64::from(byte))
    });
    let payload = (0..length)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            char::from(b'a' + ((state >> 32) % 26) as u8)
        })
        .collect::<String>();
    let mut image = AttributeMap::with_capacity(2);
    image.insert("pk".to_owned(), AttributeValue::S(key.to_owned()));
    image.insert("payload".to_owned(), AttributeValue::S(payload));
    image
}

#[tokio::test]
async fn given_declared_indexers_and_different_payload_sizes_when_stream_is_read_then_snapshots_are_exact()
-> anyhow::Result<()> {
    let fixture = TestStore::new().await;
    let outcome = async {
        let table = TableName::new("image_boundary");
        let mut request = super::test_support::hash_table_request(table.clone());
        request.max_indexers = storage_types::MaxIndexers::try_new(1)?;
        request.stream_specification = Some(storage_types::StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(storage_types::StreamViewType::NewAndOldImages),
        });
        fixture.operations.create_table(request).await?;
        let mut expected = Vec::new();
        for total in [32, 1024, 128_000] {
            let key = format!("size-{total}");
            let old = boundary_image(&key, "old", 32);
            let new = boundary_image(&key, "new", total);
            for image in [&old, &new] {
                let mut put =
                    PutItemRequest::new(table.clone(), storage_types::WireItem::try_from(image)?);
                put.indexers = Some(vec!["payload".to_owned()]);
                fixture.operations.put_item(put).await?;
            }
            expected.push((total, old, new));
        }
        fixture.wait_until_changes_are_readable().await;
        let records = table_records(&fixture, &table).await?;
        let reads = canonical_history_reads(&fixture, &table).await?;
        let pages = (fixture.canonical_history_read)(reads).await?;
        let entries = pages
            .iter()
            .flat_map(|page| &page.entries)
            .collect::<Vec<_>>();
        anyhow::ensure!(records.len() == 6 && entries.len() == 6);
        for (total, old, new) in expected {
            anyhow::ensure!(
                records
                    .iter()
                    .any(|record| record.item.as_ref() == Some(&new)),
                "public stream omitted boundary {total}"
            );
            let entry = entries
                .iter()
                .find(|entry| {
                    entry.entry.new_image().is_some_and(|image| {
                        image.item.to_attribute_map().ok().as_ref() == Some(&new)
                    })
                })
                .ok_or_else(|| anyhow::anyhow!("missing canonical image"))?;
            let new_image = entry.entry.new_image().unwrap();
            let old_image = entry.entry.old_image().unwrap();
            anyhow::ensure!(new_image.item.to_attribute_map()? == new);
            anyhow::ensure!(old_image.item.to_attribute_map()? == old);
            anyhow::ensure!(new_image.indexers == ["payload"] && old_image.indexers == ["payload"]);
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
