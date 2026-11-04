use std::collections::HashMap;

use storage_types::{
    AttributeValue, StreamRecord, SystemStreamCheckpoint, SystemStreamCheckpointToken,
    SystemStreamPosition, SystemStreamScope, TableName,
};
use stream_provider::{SystemStreamIssuedCheckpoint, SystemStreamPage};

#[test]
fn given_tombstone_when_canonical_system_page_is_converted_then_keys_and_source_are_preserved() {
    let key_image = HashMap::from([("pk".to_string(), AttributeValue::S("absent".to_string()))]);
    let scope = SystemStreamScope::try_new("catalog", 1).expect("scope");
    let position = SystemStreamPosition::try_new("shard-1", "lineage-1", "offset-1")
        .expect("logical position");
    let record = StreamRecord {
        cursor: None,
        source_table_name: Some(TableName::new("missing-delete")),
        keys: key_image.clone().into(),
        sequence_number: position.offset().as_str().to_string(),
        item: None,
        system_position: Some(position.clone()),
    };
    let checkpoint =
        SystemStreamCheckpoint::try_new(scope.clone(), vec![position]).expect("logical checkpoint");
    let token = SystemStreamCheckpointToken::try_from(format!("v1.{}", "42".repeat(16)))
        .expect("checkpoint token");
    let page = SystemStreamPage::new(
        scope,
        vec![record],
        SystemStreamIssuedCheckpoint::new(token, checkpoint).expect("issued checkpoint"),
        true,
    )
    .expect("canonical SystemStream page");

    let response = page.into_response().expect("SystemStream response");
    let record = &response.records()[0];
    assert_eq!(record.keys, key_image.into());
    assert!(record.item.is_none());
    assert_eq!(record.sequence_number, "offset-1");
    assert!(record.cursor.is_none());
    assert_eq!(
        record.source_table_name.as_ref().map(TableName::as_ref),
        Some("missing-delete")
    );
}
