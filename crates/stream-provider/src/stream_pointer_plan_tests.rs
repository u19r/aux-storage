use storage_types::{ItemStreamVersion, StreamName, TableName};

use crate::{
    STREAM_EMBEDDED_MAX_BYTES, StoredStreamPointer, StreamDataType, StreamPointerInput,
    StreamPointerPlan,
};

fn input<'a>(
    stream: &'a StreamName,
    table: &'a TableName,
    item: &'a [u8],
    old: Option<&'a [u8]>,
    indexers: &'a [String],
) -> StreamPointerInput<'a> {
    StreamPointerInput {
        history_id: 1u32.into(),
        item_revision: storage_types::ItemRevision::initial(),
        item_stream: stream,
        table_name: table,
        item_stream_version: ItemStreamVersion::new(7),
        item_data: item,
        old_item_data: old,
        data_type: StreamDataType::DeleteMarker,
        indexers,
        old_indexers: None,
    }
}

#[test]
fn given_delete_images_at_budget_when_encoded_then_old_image_and_indexers_survive() {
    let stream = StreamName::new(b"item-stream");
    let table = TableName::new("test_table");
    let item = vec![b'n'; STREAM_EMBEDDED_MAX_BYTES - 3];
    let indexers = vec!["new-index".to_owned()];
    let bytes = StreamPointerPlan::try_new(input(&stream, &table, &item, Some(b"old"), &indexers))
        .expect("encode pointer")
        .encoded;
    let pointer: StoredStreamPointer =
        storage_types::storage_serde::from_bytes(&bytes).expect("decode pointer");

    let StoredStreamPointer::Embedded {
        items, indexers, ..
    } = pointer
    else {
        panic!("images at the budget should embed");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].data, item);
    assert_eq!(items[0].data_type, StreamDataType::DeleteMarker);
    assert_eq!(items[1].data, b"old");
    assert_eq!(items[1].data_type, StreamDataType::DynamoDbJson);
    assert_eq!(indexers, vec!["new-index"]);
}

#[test]
fn given_images_over_budget_when_encoded_then_pointer_bytes_match_stored_contract() {
    let stream = StreamName::new(b"item-stream");
    let table = TableName::new("test_table");
    let item = vec![b'n'; STREAM_EMBEDDED_MAX_BYTES];
    let indexers = vec!["new-index".to_owned()];
    let actual = StreamPointerPlan::try_new(input(&stream, &table, &item, Some(b"old"), &indexers))
        .expect("encode pointer")
        .encoded;
    let expected = storage_types::storage_serde::to_bytes(
        &StoredStreamPointer::pointer(
            storage_types::ItemHistoryAddress {
                history_id: 1u32.into(),
                stream_name: stream,
            },
            table,
            storage_types::ItemWriteVersions {
                item_revision: storage_types::ItemRevision::initial(),
                item_stream_version: ItemStreamVersion::new(7),
            },
        )
        .with_indexers(vec!["new-index".to_owned()]),
    )
    .expect("encode established pointer format");

    assert_eq!(actual, expected);
}

#[test]
fn given_images_around_embedding_boundary_when_encoded_then_declarations_survive() {
    let stream = StreamName::new(b"item-stream");
    let table = TableName::new("test_table");
    let indexers = vec!["new-index".to_owned()];
    let old_indexers = vec!["old-index".to_owned()];
    for total in [
        STREAM_EMBEDDED_MAX_BYTES - 1,
        STREAM_EMBEDDED_MAX_BYTES,
        STREAM_EMBEDDED_MAX_BYTES + 1,
    ] {
        let item = vec![b'n'; total - 3];
        let mut request = input(&stream, &table, &item, Some(b"old"), &indexers);
        request.old_indexers = Some(&old_indexers);
        let plan = StreamPointerPlan::try_new(request).expect("encode boundary pointer");
        let pointer: StoredStreamPointer = storage_types::storage_serde::from_bytes(&plan.encoded)
            .expect("decode boundary pointer");
        assert_eq!(pointer.indexers(), indexers);
        assert_eq!(pointer.old_indexers(), Some(old_indexers.as_slice()));
        let embedded = total <= STREAM_EMBEDDED_MAX_BYTES;
        assert_eq!(
            plan.referenced_version,
            (!embedded).then_some(ItemStreamVersion::new(7))
        );
        assert_eq!(
            matches!(&pointer, StoredStreamPointer::Embedded { .. }),
            embedded
        );
        if let StoredStreamPointer::Embedded { items, .. } = pointer {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0].data, item);
            assert_eq!(items[1].data, b"old");
        }
    }
}
