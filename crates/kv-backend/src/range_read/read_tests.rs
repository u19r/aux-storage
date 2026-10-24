use std::{num::NonZeroU64, sync::Arc};

use storage_provider::{RangeReadPlan, RangeReadiness};
use storage_types::{
    AttributeDefinition, AttributeMap, GlobalSecondaryIndex, IndexName, ItemKey, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, ScanTableRequest, WireItem,
};

use super::{
    decode_tests::table,
    key_layout::physical_item_key,
    projection::{PublicImage, RangeContent},
    read::RangeReadInput,
};
use crate::{
    catalog::{KvIndexIdentity, KvIndexKind, KvTableIdentity},
    constants::{GSI_PROJECTION_KEY_PREFIX_BYTES, PRIMARY_ITEM_KEY_PREFIX_BYTES},
};

#[test]
fn given_a_verified_gsi_image_when_physical_leaf_changes_then_complete_key_is_rejected() {
    let mut table = table();
    let info = Arc::make_mut(&mut table.info);
    info.attribute_definitions.push(AttributeDefinition {
        attribute_name: "gsi_pk".into(),
        attribute_type: KeyAttributeType::S,
    });
    let index = GlobalSecondaryIndex {
        index_name: IndexName::new("by-group"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".into(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
    };
    info.global_secondary_indexes = Some(vec![index.clone()]);
    let identity = KvTableIdentity {
        table_id: 1,
        revision: NonZeroU64::new(1).unwrap(),
        table_name: table.table_name.clone(),
        indexes: vec![KvIndexIdentity {
            index_id: 1,
            index_name: index.index_name.clone(),
            index_kind: KvIndexKind::UserGsi,
        }],
        index_id_high_water: 1,
        deleted: false,
    };
    let plan = RangeReadPlan::try_from_scan(ScanTableRequest {
        table_name: table.table_name.clone(),
        index_name: Some(index.index_name.clone()),
        limit: None,
        exclusive_start_key: None,
        consistent_read: false,
    })
    .unwrap();
    let bound = plan
        .bind(table.info.clone(), RangeReadiness::GsiPublished)
        .unwrap();
    let page = bound.page(25, 1_048_576).unwrap();
    let input = RangeReadInput {
        table: &table,
        bound: &bound,
        page: &page,
    };
    let image = WireItem::dynamo_json(br#"{"pk":{"S":"owned"},"gsi_pk":{"S":"group"}}"#.to_vec());
    let attributes = AttributeMap::try_from(&image).unwrap();
    let key = ItemKey::from_key_schema_for_index(
        table.table_name.clone(),
        &table.key_schema,
        &index.index_name,
        &index.key_schema,
        &attributes,
    )
    .unwrap()
    .unwrap();
    let physical = physical_item_key(&identity, &table, &bound, &key).unwrap();
    let content = || {
        RangeContent::Projection(Box::new(crate::gsi::HydratedProjection {
            image: image.clone(),
            revision: 1_u64.try_into().unwrap(),
        }))
    };
    assert!(
        input
            .decode_entry::<PublicImage>(&identity, &physical, content())
            .is_ok()
    );
    for offset in [
        0,
        PRIMARY_ITEM_KEY_PREFIX_BYTES - 1,
        GSI_PROJECTION_KEY_PREFIX_BYTES - 1,
        GSI_PROJECTION_KEY_PREFIX_BYTES,
        physical.len() - 1,
    ] {
        let mut wrong = physical.clone();
        wrong[offset] ^= 1;
        let error = input
            .decode_entry::<PublicImage>(&identity, &wrong, content())
            .unwrap_err();
        use storage_types::context::WrappedError as _;
        assert!(
            matches!(error.to_enum(), storage_types::StorageEnum::InternalServerError { message }
            if message == "KV range image differs from physical key")
        );
    }
}
