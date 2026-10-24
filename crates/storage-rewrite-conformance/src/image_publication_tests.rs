//! Image identity and publication order through the initialized public owner.

use serde_json::json;
use storage_types::{
    AttributeMap, AttributeValue, BatchWriteItemRequest, DeleteItemRequest, PutItemRequest,
    TableName, storage_serde,
};
use stream_provider::{StoredStreamPointer, StreamDataType, StreamRowKind};

use super::{
    gsi_workflow_tests::table_request,
    image_retention_tests::{table_records, table_stream},
    test_support::TestStore,
};

#[tokio::test]
async fn given_mixed_writes_when_history_is_read_then_images_and_predecessors_are_exact()
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
                "number": {"N": format!("{step}.00")},
                "numbers": {"NS": ["1700000000", "1.00"]},
                "nested": {"M": {"zero": {"N": "-0.00"}}}});
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
        let records = table_records(&fixture, &table).await?;
        let pointers = fixture
            .stream_operations
            .read_forward(table_stream(&fixture, &table).await?, None, 10)
            .await?;
        anyhow::ensure!(
            records.len() == 5 && pointers.items.len() == 5,
            "one pointer and public record per committed mutation"
        );
        let mut previous = None;
        for (ordinal, (record, row)) in records.iter().zip(&pointers.items).enumerate() {
            anyhow::ensure!(record.new_image == expected[ordinal]);
            anyhow::ensure!(
                record.old_image
                    == ordinal
                        .checked_sub(1)
                        .and_then(|index| expected[index].clone())
            );
            anyhow::ensure!(
                record.system_position.is_none(),
                "table records must not fabricate system positions"
            );
            anyhow::ensure!(record.sequence_number == row.id.to_string());
            let pointer: StoredStreamPointer = storage_serde::from_bytes(&row.data)?;
            anyhow::ensure!(
                matches!(pointer, StoredStreamPointer::Pointer { .. }),
                "large images must exercise referenced history"
            );
            let version = pointer.target_item_stream_version();
            anyhow::ensure!(previous.is_none_or(|previous| version > previous));
            let history = fixture
                .stream_operations
                .read_backward(
                    StreamRowKind::Item(pointer.history_address().physical_stream_name()),
                    Some(storage_types::StreamItemId::from(version).increment()),
                    2,
                )
                .await?;
            anyhow::ensure!(
                history.items.first().map(|image| image.id)
                    == Some(storage_types::StreamItemId::from(version))
            );
            if let Some(previous) = previous {
                anyhow::ensure!(
                    history.items.get(1).map(|image| image.id)
                        == Some(storage_types::StreamItemId::from(previous))
                );
            }
            for (image, expected) in history.items.iter().zip([
                expected[ordinal].as_ref(),
                ordinal
                    .checked_sub(1)
                    .and_then(|index| expected[index].as_ref()),
            ]) {
                let actual: Option<AttributeMap> =
                    if image.data_type == StreamDataType::DeleteMarker {
                        None
                    } else {
                        Some(storage_serde::from_bytes(&image.data)?)
                    };
                anyhow::ensure!(
                    actual.as_ref() == expected,
                    "durable image differs from committed mutation"
                );
            }
            previous = Some(version);
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
async fn given_embedding_boundary_writes_when_stream_is_read_then_images_and_indexers_are_exact()
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
        for total in [
            stream_provider::STREAM_EMBEDDED_MAX_BYTES - 1,
            stream_provider::STREAM_EMBEDDED_MAX_BYTES,
            stream_provider::STREAM_EMBEDDED_MAX_BYTES + 1,
        ] {
            let key = format!("boundary-{total}");
            let old = boundary_image(&key, "old", 32);
            let old_len = storage_serde::to_bytes(&old)?.len();
            let new = (0..=total)
                .map(|length| boundary_image(&key, "new", length))
                .find(|image| {
                    storage_serde::to_bytes(image).is_ok_and(|bytes| bytes.len() + old_len == total)
                })
                .ok_or_else(|| {
                    anyhow::anyhow!("cannot construct exact encoded boundary {total}")
                })?;
            for image in [&old, &new] {
                let mut put =
                    PutItemRequest::new(table.clone(), storage_types::WireItem::try_from(image)?);
                put.indexers = Some(vec!["payload".to_owned()]);
                fixture.operations.put_item(put).await?;
            }
            expected.push((total, old, new));
        }
        let records = table_records(&fixture, &table).await?;
        let pointers = fixture
            .stream_operations
            .read_forward(table_stream(&fixture, &table).await?, None, 100)
            .await?;
        anyhow::ensure!(records.len() == 6 && pointers.items.len() == 6);
        for (total, old, new) in expected {
            let record = records
                .iter()
                .find(|record| record.new_image.as_ref() == Some(&new))
                .ok_or_else(|| anyhow::anyhow!("public stream omitted boundary {total}"))?;
            anyhow::ensure!(record.old_image.as_ref() == Some(&old));
            let row = pointers
                .items
                .iter()
                .find(|row| row.id.to_string() == record.sequence_number)
                .ok_or_else(|| anyhow::anyhow!("public record has no durable pointer"))?;
            let pointer: StoredStreamPointer = storage_serde::from_bytes(&row.data)?;
            anyhow::ensure!(
                matches!(pointer, StoredStreamPointer::Embedded { .. })
                    == (total <= stream_provider::STREAM_EMBEDDED_MAX_BYTES),
                "wrong representation at boundary {total}"
            );
            let declarations = ["payload".to_owned()];
            anyhow::ensure!(pointer.indexers() == declarations);
            anyhow::ensure!(pointer.old_indexers() == Some(declarations.as_slice()));
        }
        Ok(())
    }
    .await;
    fixture.finish().await;
    outcome
}
