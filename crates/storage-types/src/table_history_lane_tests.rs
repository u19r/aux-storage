use crate::{SystemStreamPosition, SystemStreamScope, TableHistoryLane};

#[test]
fn given_distinct_authorities_when_table_lanes_are_encoded_then_only_offsets_share_a_lane() {
    let scope = SystemStreamScope::try_new("source/a", 1).unwrap();
    let position =
        SystemStreamPosition::try_new("shard/a", "lineage/b", "00000000000000000001").unwrap();
    let original = TableHistoryLane::try_from((1u32.into(), &scope, &position)).unwrap();
    let later =
        SystemStreamPosition::try_new("shard/a", "lineage/b", "00000000000000000009").unwrap();
    let later = TableHistoryLane::try_from((1u32.into(), &scope, &later)).unwrap();
    assert_eq!(original, later);
    assert_eq!(
        original.physical_stream_name().unwrap(),
        later.physical_stream_name().unwrap()
    );
    assert_eq!(
        original.public_shard_id().unwrap(),
        later.public_shard_id().unwrap()
    );

    for (table, source, shard, lineage, generation) in [
        (2u32, "source/a", "shard/a", "lineage/b", 1),
        (1, "source/b", "shard/a", "lineage/b", 1),
        (1, "source/a", "shard/a", "lineage/b", 2),
        (1, "source/a", "shard/b", "lineage/b", 1),
        (1, "source/a", "shard/a", "lineage/c", 1),
        (1, "source/a", "shard", "a/lineage/b", 1),
    ] {
        let scope = SystemStreamScope::try_new(source, generation).unwrap();
        let position =
            SystemStreamPosition::try_new(shard, lineage, "00000000000000000001").unwrap();
        let lane = TableHistoryLane::try_from((table.into(), &scope, &position)).unwrap();
        let physical = lane.physical_stream_name().unwrap();
        let shard = lane.public_shard_id().unwrap();
        assert_ne!(physical, original.physical_stream_name().unwrap());
        assert_ne!(shard, original.public_shard_id().unwrap());
        assert_eq!(physical.table_history_id().unwrap(), Some(table.into()));
        assert_eq!(shard.len(), 64);
        assert!(shard.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
    assert!(TableHistoryLane::try_from(([0; 16].into(), &scope, &position)).is_err());
}
