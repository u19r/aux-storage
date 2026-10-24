use std::num::NonZeroU64;

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_types::{
    IndexName, SystemStreamCheckpoint, SystemStreamPosition, SystemStreamScope, TableName,
    maintenance::TargetIncarnation, storage_serde,
};

use super::{key_layout::index_incarnation, read::decode_gsi_lifecycle};
use crate::catalog::{KvIndexIdentity, KvIndexKind, KvTableIdentity};

fn identity() -> KvTableIdentity {
    KvTableIdentity {
        table_id: 1,
        revision: NonZeroU64::new(1).unwrap(),
        table_name: TableName::new("orders"),
        indexes: vec![KvIndexIdentity {
            index_id: 1,
            index_name: IndexName::new("by-status"),
            index_kind: KvIndexKind::UserGsi,
        }],
        index_id_high_water: 1,
        deleted: false,
    }
}

fn build() -> GsiBuild<Vec<u8>> {
    let checkpoint = SystemStreamCheckpoint::try_new(
        SystemStreamScope::try_new("test", 1).unwrap(),
        vec![SystemStreamPosition::try_new("root", "root", "00000000000000000001").unwrap()],
    )
    .unwrap();
    GsiBuild::new(
        GsiBuildTarget {
            table_name: TableName::new("orders"),
            index_name: IndexName::new("by-status"),
            incarnation: TargetIncarnation::from(index_incarnation(1, 1)),
        },
        checkpoint,
    )
}

#[test]
fn given_existing_kv_build_when_decoding_lifecycle_then_projection_identity_is_preserved() {
    let build = build();
    let bytes = storage_serde::to_bytes(&build).unwrap();
    let lifecycle = decode_gsi_lifecycle(&identity(), vec![Some(bytes)]).unwrap();
    assert!(lifecycle.writable_targets() == [build.target]);
}

#[test]
fn given_invalid_kv_build_records_when_decoding_lifecycle_then_missing_and_changed_identity_are_rejected()
 {
    assert!(decode_gsi_lifecycle(&identity(), vec![]).is_err());
    assert!(decode_gsi_lifecycle(&identity(), vec![None]).is_err());
    for target in [
        GsiBuildTarget {
            table_name: TableName::new("another-table"),
            ..build().target
        },
        GsiBuildTarget {
            index_name: IndexName::new("another-index"),
            ..build().target
        },
        GsiBuildTarget {
            incarnation: TargetIncarnation::try_from(vec![1]).unwrap(),
            ..build().target
        },
    ] {
        let mut build = build();
        build.target = target;
        let bytes = storage_serde::to_bytes(&build).unwrap();
        assert!(decode_gsi_lifecycle(&identity(), vec![Some(bytes)]).is_err());
    }
}
