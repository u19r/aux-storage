#[test]
fn given_stored_item_when_observed_for_write_then_item_and_metadata_are_preserved() {
    let image = format!(
        r#"{{"pk":{{"S":"key"}},"payload":{{"S":"{}"}}}}"#,
        "x".repeat(4096)
    );
    let row = crate::SqlRow::new(vec![
        crate::SqlValue::Text(image.clone()),
        crate::SqlValue::Blob(Vec::new()),
        crate::SqlValue::Integer(42),
    ]);
    let observation = crate::item_write::decode_live(
        row,
        storage_types::StoredWireItem {
            item: storage_types::WireItem::dynamo_json(image.as_bytes().to_vec()),
            indexers: Vec::new(),
        },
    )
    .unwrap();
    let storage_provider::ItemWriteObservation::Live {
        stored,
        provenance,
        ttl_epoch,
    } = observation
    else {
        panic!("live item expected");
    };
    assert_eq!(stored.item.to_dynamo_json_string().unwrap(), image);
    assert!(stored.indexers.is_empty());
    assert!(provenance.as_bytes().is_empty());
    assert_eq!(ttl_epoch, Some(42));
}

#[test]
fn given_invalid_stored_metadata_when_observed_for_write_then_physical_errors_are_rejected() {
    for row in [
        vec![],
        vec![
            crate::SqlValue::Text("{}".to_owned()),
            crate::SqlValue::Integer(-1),
            crate::SqlValue::Null,
        ],
        vec![
            crate::SqlValue::Text("{}".to_owned()),
            crate::SqlValue::Blob(vec![0; 5]),
            crate::SqlValue::Null,
        ],
        vec![
            crate::SqlValue::Text("{}".to_owned()),
            crate::SqlValue::Blob(Vec::new()),
            crate::SqlValue::Text("invalid".to_owned()),
        ],
    ] {
        assert!(
            crate::item_write::decode_live(
                crate::SqlRow::new(row),
                storage_types::StoredWireItem {
                    item: storage_types::WireItem::dynamo_json(b"{}".to_vec()),
                    indexers: Vec::new(),
                }
            )
            .is_err()
        );
    }
}
