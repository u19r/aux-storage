use storage_types::{ItemRevision, SystemStreamPosition};

use super::DynamoStreamIds;

#[test]
fn given_native_positions_on_different_lineages_when_encoding_ids_then_offset_order_is_preserved() {
    let ids = [
        ("z", 42 * 65_536),
        ("a", 42 * 65_536 + 1),
        ("b", 43 * 65_536),
    ]
    .map(|(lineage, offset)| {
        let position =
            SystemStreamPosition::try_new(lineage, lineage, format!("{offset:020}-ns")).unwrap();
        DynamoStreamIds::for_write(ItemRevision::initial(), &position)
            .unwrap()
            .system_pointer_id()
    });
    assert!(ids[0] < ids[1] && ids[1] < ids[2]);
    assert_eq!(&ids[0].as_bytes()[..4], &[u8::MAX; 4]);
}
