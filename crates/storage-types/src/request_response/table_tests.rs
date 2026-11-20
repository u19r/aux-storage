use std::sync::Arc;

use crate::{
    BoundTableInfo, CreateTableRequest, MultiRegionConsistency, PairId, PairState, RegionId,
    RegionalBinding, ReplicaStatus, TableBinding, TableDescription, TableStatus, TimestampMillis,
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
fn given_paired_binding_when_described_then_replicas_and_eventual() {
    for (state, expected) in [
        (PairState::Seeding, ReplicaStatus::Creating),
        (PairState::Joining, ReplicaStatus::Creating),
        (PairState::Active, ReplicaStatus::Active),
        (PairState::Leaving, ReplicaStatus::Deleting),
    ] {
        let description =
            TableDescription::from_bound_table_info(bound(Some(state)), TableStatus::Active);
        let replicas = description
            .replicas
            .unwrap_or_else(|| panic!("{state:?}: replicas"));
        assert_eq!(replicas.len(), 1, "{state:?}");
        assert_eq!(replicas[0].region_name, "peer", "{state:?}");
        assert_eq!(replicas[0].replica_status, expected, "{state:?}");
        assert_eq!(
            description.multi_region_consistency,
            Some(MultiRegionConsistency::Eventual),
            "{state:?}"
        );
    }
}

fn gsi_table() -> BoundTableInfo {
    let request: CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders",
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gpk", "AttributeType": "S"}
        ],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "GlobalSecondaryIndexes": [{
            "IndexName": "gidx",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .unwrap();
    let info = request
        .stored_table_info_at(TimestampMillis::from_timestamp(1_000))
        .unwrap();
    BoundTableInfo {
        history_id: 7_u32.into(),
        info: Arc::new(info),
        binding: TableBinding::default(),
    }
}

// AWS P13: the CreateTable response is CREATING and its GSIs are CREATING
// without `Backfilling`. Capacity is intentionally unsupported, so no
// throughput is reported.
#[test]
fn given_gsi_table_when_described_creating_then_aws_create_shape() {
    let description = TableDescription::from_bound_table_info(gsi_table(), TableStatus::Creating);
    let json = serde_json::to_value(&description).unwrap();
    assert_eq!(json["TableStatus"], "CREATING");
    assert!(json.get("ProvisionedThroughput").is_none());
    assert_eq!(
        json["BillingModeSummary"],
        serde_json::json!({"BillingMode": "PAY_PER_REQUEST"})
    );
    let index = &json["GlobalSecondaryIndexes"][0];
    assert_eq!(index["IndexStatus"], "CREATING");
    assert!(index.get("Backfilling").is_none());
    assert!(index.get("ProvisionedThroughput").is_none());
    assert_eq!(index["IndexSizeBytes"], 0);
    assert_eq!(index["ItemCount"], 0);
    assert_eq!(
        index["IndexArn"],
        format!("{}/index/gidx", json["TableArn"].as_str().unwrap())
    );
    // AWS lists attribute definitions by name.
    assert_eq!(json["AttributeDefinitions"][0]["AttributeName"], "gpk");
    assert_eq!(json["AttributeDefinitions"][1]["AttributeName"], "pk");
}

// AWS P13: an ACTIVE GSI carries no `Backfilling`.
#[test]
fn given_gsi_table_when_described_active_then_index_is_active_without_backfilling() {
    let description = TableDescription::from_bound_table_info(gsi_table(), TableStatus::Active);
    let json = serde_json::to_value(&description).unwrap();
    assert_eq!(json["GlobalSecondaryIndexes"][0]["IndexStatus"], "ACTIVE");
    assert!(
        json["GlobalSecondaryIndexes"][0]
            .get("Backfilling")
            .is_none()
    );
}

#[test]
fn given_unpaired_when_described_then_no_replicas() {
    let description = TableDescription::from_bound_table_info(bound(None), TableStatus::Active);
    assert!(description.replicas.is_none());
    assert!(description.multi_region_consistency.is_none());
}

#[test]
fn given_stream_identity_when_arn_is_built_then_label_is_the_iso_timestamp_alone() {
    let table = crate::TableName::new("orders");
    let identity = crate::TableStreamIdentity {
        history_id: crate::TableHistoryId::from(uuid::Uuid::from_u128(7)),
        enabled_at: TimestampMillis::from_timestamp(1_763_048_247_939),
    };
    let (arn, label) = crate::stream_identity(&table, identity);
    assert_eq!(label, "2025-11-13T15:37:27.939");
    assert_eq!(
        arn,
        "arn:aws:dynamodb:us-east-1:123456789012:table/orders/stream/2025-11-13T15:37:27.939"
    );
    assert_eq!(crate::stream_arn_table_name(&arn).unwrap(), table);
    assert!(crate::stream_arn_table_name("arn:aws:dynamodb:x:y:table/orders").is_err());
}
