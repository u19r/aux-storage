use std::sync::Arc;

use crate::{
    BoundTableInfo, CreateTableRequest, PairId, PairState, RegionId, RegionalBinding, StorageEnum,
    TableBinding, TimestampMillis,
};

fn bound(state: Option<PairState>) -> BoundTableInfo {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    }))
    .unwrap();
    let info = request
        .stored_table_info_at(TimestampMillis::from_timestamp(0))
        .unwrap();
    let regional = state.map(|state| {
        Arc::new(RegionalBinding {
            pair: PairId::from_bytes([7; 16]),
            local: RegionId(std::num::NonZeroU16::new(1).unwrap()),
            peer: RegionId(std::num::NonZeroU16::new(2).unwrap()),
            state,
            peer_name: "peer".to_owned(),
        })
    });
    BoundTableInfo {
        history_id: 7_u32.into(),
        info: Arc::new(info),
        binding: TableBinding {
            regional,
            ..TableBinding::default()
        },
    }
}

#[test]
fn given_joining_binding_when_require_data_readable_then_table_not_found() {
    let error = bound(Some(PairState::Joining))
        .require_data_readable()
        .unwrap_err();
    assert!(
        matches!(error.base_error(), Some(StorageEnum::TableNotFound { .. })),
        "{error:?}"
    );
}

#[test]
fn given_other_states_or_unpaired_when_require_data_readable_then_ok() {
    for state in [
        None,
        Some(PairState::Seeding),
        Some(PairState::Active),
        Some(PairState::Leaving),
    ] {
        bound(state)
            .require_data_readable()
            .unwrap_or_else(|error| panic!("{state:?}: {error:?}"));
    }
}
