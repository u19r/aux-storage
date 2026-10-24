use std::collections::{BTreeMap, HashMap};

use storage_types::{
    AttributeValue, BatchWriteItemRequest, CreateTableRequest, ItemStreamVersion, PutRequest,
    TableName, WriteRequest,
};

use crate::{
    constants::{
        LOGICAL_PROFILE_BATCH, LOGICAL_PROFILE_INDEXES, LOGICAL_PROFILE_ITEMS,
        LOGICAL_PROFILE_PAYLOAD_BYTES,
    },
    logical_profile_shape,
};

/// One independently generated expected logical record.
///
/// The key is generated directly from the fixture ordinal instead of being
/// selected or reconstructed from an observed record. `key_json` is a stable
/// full-key identity used by keyed expected/observed comparisons.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalProfileFixture {
    pub index: usize,
    pub key: HashMap<String, AttributeValue>,
    pub key_json: String,
    pub item: HashMap<String, AttributeValue>,
    pub revision: ItemStreamVersion,
}

pub fn logical_profile_fixture(index: usize) -> LogicalProfileFixture {
    let key = logical_profile_key(index);
    let key_json = storage_types::KeyAttributes::from(key.clone())
        .canonical_dynamo_json()
        .expect("logical fixture key canonical JSON");
    LogicalProfileFixture {
        index,
        key,
        key_json,
        item: logical_profile_item(index),
        revision: ItemStreamVersion::new(index as u64 + 1),
    }
}

pub fn logical_profile_fixtures() -> Vec<LogicalProfileFixture> {
    (0..LOGICAL_PROFILE_ITEMS)
        .map(logical_profile_fixture)
        .collect()
}

pub fn logical_profile_request(table_name: &TableName) -> CreateTableRequest {
    logical_profile_shape::request(table_name, false, LOGICAL_PROFILE_INDEXES)
}

pub fn logical_profile_item(index: usize) -> HashMap<String, AttributeValue> {
    logical_profile_shape::item(
        index,
        LOGICAL_PROFILE_INDEXES,
        LOGICAL_PROFILE_PAYLOAD_BYTES,
    )
}

pub fn logical_profile_key(index: usize) -> HashMap<String, AttributeValue> {
    logical_profile_shape::key(index)
}

pub fn logical_profile_batches(
    table_name: &TableName,
) -> impl Iterator<Item = BatchWriteItemRequest> + '_ {
    (0..LOGICAL_PROFILE_ITEMS)
        .step_by(LOGICAL_PROFILE_BATCH)
        .map(move |chunk_start| {
            let writes = (chunk_start
                ..LOGICAL_PROFILE_ITEMS.min(chunk_start + LOGICAL_PROFILE_BATCH))
                .map(|index| WriteRequest {
                    put_request: Some(PutRequest {
                        item: storage_types::WireItem::try_from(&logical_profile_item(index))
                            .expect("generated logical profile item must encode"),
                        indexers: None,
                        aux_item_stream_ttl_hours: None,
                    }),
                    delete_request: None,
                })
                .collect();
            BatchWriteItemRequest {
                request_items: [(table_name.clone(), writes)].into_iter().collect(),
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            }
        })
}

pub fn logical_profile_index_key(item: &HashMap<String, AttributeValue>, index: usize) -> String {
    let pk = match item.get(&format!("gsi{index}_pk")) {
        Some(AttributeValue::S(value)) => value,
        value => panic!("logical profile GSI {index} has invalid partition key: {value:?}"),
    };
    let sk = match item.get(&format!("gsi{index}_sk")) {
        Some(AttributeValue::S(value)) => value,
        value => panic!("logical profile GSI {index} has invalid sort key: {value:?}"),
    };
    format!("{pk}\0{sk}")
}

pub fn logical_profile_expected_index_rows(
    index: usize,
) -> BTreeMap<String, HashMap<String, AttributeValue>> {
    logical_profile_fixtures()
        .into_iter()
        .map(|fixture| {
            (
                logical_profile_index_key(&fixture.item, index),
                fixture.item,
            )
        })
        .collect()
}
