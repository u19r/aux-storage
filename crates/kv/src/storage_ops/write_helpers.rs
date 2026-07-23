use std::borrow::Cow;

use storage_types::{
    attribute_map_numbers_need_write_normalization, normalize_attribute_map_numbers_for_write,
};

use crate::{
    keyspace::{table_identity::TableIdentity, table_keys},
    storage_ops::imports::{
        AttributeValue, BatchItem, HashMap, IndexName, ItemKey, KeyAttributes, KeySchemaElement,
        Projection, ProjectionType, SortedKvDbStorageProvider, StorageError, StorageResult,
        StoredTableInfo, TableName, TtlConfigRecord,
    },
    ttl::{TtlIndexMutation, plan_ttl_index_mutations},
};

pub(crate) const CHANGE_INDEX_PREFIX: &str = "__change_index";
pub(crate) const CHANGE_INDEX_SLOT_COUNT: u16 = 256;

fn normalized_attribute_map_for_write(
    item: &HashMap<String, AttributeValue>,
) -> Cow<'_, HashMap<String, AttributeValue>> {
    if !attribute_map_numbers_need_write_normalization(item) {
        return Cow::Borrowed(item);
    }

    let mut normalized = item.clone();
    normalize_attribute_map_numbers_for_write(&mut normalized);
    Cow::Owned(normalized)
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) fn ttl_index_mutations_for_items(
        table_name: &TableName,
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        ttl_config: Option<&TtlConfigRecord>,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<Vec<BatchItem>> {
        Ok(plan_ttl_index_mutations(
            table_name,
            table_identity,
            table_info,
            ttl_config,
            old_item,
            new_item,
        )?
        .into_iter()
        .map(|mutation| match mutation {
            TtlIndexMutation::Delete(key) => BatchItem { key, value: None },
            TtlIndexMutation::Put(key) => BatchItem {
                key,
                value: Some(Vec::new()),
            },
        })
        .collect())
    }

    pub(super) fn prepare_batch_put_item(
        table_name: &TableName,
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        item: &HashMap<String, AttributeValue>,
        should_write_to_stream: bool,
        existing_item: Option<&HashMap<String, AttributeValue>>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<BatchItem>> {
        if item.is_empty() {
            return Err(StorageError::validation(
                "Item must have at least one attribute",
            ));
        }
        let item = normalized_attribute_map_for_write(item);
        let item = item.as_ref();

        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, item)?;
        let item_key = table_keys::item_key(table_identity, &item_key)?;

        let value = storage_types::storage_serde::to_bytes(&item)?;
        let mut batch_items = vec![BatchItem {
            key: item_key.clone(),
            value: Some(value),
        }];

        if should_write_to_stream {
            let stream_items =
                Self::prepare_stream_entries_for_batch(table_name, table_info, item, false);
            batch_items.extend(Self::change_index_entries_for_stream_items(
                table_name,
                &stream_items,
            ));
            batch_items.extend(stream_items);
        }

        if immediate_gsi_consistency {
            batch_items.extend(Self::gsi_batch_mutations_for_items(
                table_identity,
                table_info,
                existing_item,
                Some(item),
            )?);
        }

        Ok(batch_items)
    }

    pub(super) fn prepare_batch_delete_item(
        table_name: &TableName,
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        key: &KeyAttributes,
        should_write_to_stream: bool,
        existing_item: Option<&HashMap<String, AttributeValue>>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<Vec<BatchItem>> {
        if key.is_empty() {
            return Err(StorageError::validation(
                "Key must have at least one attribute",
            ));
        }

        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, key)?;
        let item_key = table_keys::item_key(table_identity, &item_key)?;

        let mut batch_items = vec![BatchItem {
            key: item_key.clone(),
            value: None,
        }];

        if should_write_to_stream && let Some(item) = existing_item {
            let stream_items =
                Self::prepare_stream_entries_for_batch(table_name, table_info, item, true);
            batch_items.extend(Self::change_index_entries_for_stream_items(
                table_name,
                &stream_items,
            ));
            batch_items.extend(stream_items);
        }

        if immediate_gsi_consistency {
            batch_items.extend(Self::gsi_batch_mutations_for_items(
                table_identity,
                table_info,
                existing_item,
                None,
            )?);
        }

        Ok(batch_items)
    }

    pub(super) fn prepare_stream_entries_for_batch(
        table_name: &TableName,
        table_metadata: &StoredTableInfo,
        item: &HashMap<String, AttributeValue>,
        is_deleted: bool,
    ) -> Vec<BatchItem> {
        if !crate::backends::common::should_write_stream_entries(table_metadata, false) {
            return Vec::new();
        }

        let mut stream_entries = Vec::new();

        let timestamp = chrono::Utc::now().timestamp_millis();
        let sequence_id = uuid::Uuid::new_v4().to_string();
        let stream_record_key = format!("stream:{table_name}:{timestamp}:{sequence_id}");

        let stream_record = Self::create_stream_record(
            table_name,
            item,
            is_deleted,
            table_metadata.stream_specification.as_ref(),
            &table_metadata.key_schema,
        );

        if let Ok(stream_record_bytes) = storage_types::storage_serde::to_bytes(&stream_record) {
            stream_entries.push(BatchItem {
                key: stream_record_key.into_bytes(),
                value: Some(stream_record_bytes),
            });
        }

        stream_entries
    }

    fn change_index_entries_for_stream_items(
        table_name: &TableName,
        stream_items: &[BatchItem],
    ) -> Vec<BatchItem> {
        if stream_items.is_empty() {
            return Vec::new();
        }

        let slot = change_index_slot(table_name);
        stream_items
            .iter()
            .filter_map(|item| {
                item.value.as_ref()?;
                Some(BatchItem {
                    key: change_index_key(slot, item.key.as_slice(), table_name),
                    value: Some(Vec::new()),
                })
            })
            .collect()
    }

    pub(super) fn create_stream_record(
        table_name: &TableName,
        item: &HashMap<String, AttributeValue>,
        is_deleted: bool,
        stream_spec: Option<&storage_types::StreamSpecification>,
        key_schema: &[KeySchemaElement],
    ) -> storage_types::StreamRecord {
        use storage_types::{StreamRecord, StreamViewType};

        let sequence_number = format!(
            "{}-{}",
            chrono::Utc::now().timestamp_millis(),
            uuid::Uuid::new_v4()
        );

        let keys = Self::extract_key_attributes(item, key_schema);

        // Default internal stream view to NewImage so internal consumers (e.g. GSI
        // maintenance) have access to full item data even if user did not
        // enable streams explicitly.
        let view_type = stream_spec
            .and_then(|spec| spec.stream_view_type.clone())
            .unwrap_or(StreamViewType::NewImage);

        let (new_image, old_image) = match view_type {
            StreamViewType::KeysOnly => (None, None),
            StreamViewType::NewImage => {
                let new_image = if is_deleted { None } else { Some(item.clone()) };
                (new_image, None)
            }
            StreamViewType::OldImage => {
                let old_image = if is_deleted { Some(item.clone()) } else { None };
                (None, old_image)
            }
            StreamViewType::NewAndOldImages => {
                let new_image = if is_deleted { None } else { Some(item.clone()) };
                let old_image = if is_deleted { Some(item.clone()) } else { None };
                (new_image, old_image)
            }
        };

        StreamRecord {
            cursor: None,
            source_table_name: Some(table_name.clone()),
            keys,
            sequence_number,
            new_image,
            old_image,
        }
    }

    pub(super) fn extract_key_attributes(
        item: &HashMap<String, AttributeValue>,
        key_schema: &[KeySchemaElement],
    ) -> HashMap<String, AttributeValue> {
        let mut keys = HashMap::new();
        for key_element in key_schema {
            if let Some(value) = item.get(&key_element.attribute_name) {
                keys.insert(key_element.attribute_name.clone(), value.clone());
            }
        }
        keys
    }

    // Removed custom projection filtering in favor of key-aware projection helper
}

pub(crate) fn change_index_slot(table_name: &TableName) -> u16 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in table_name.as_ref().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(CHANGE_INDEX_SLOT_COUNT)) as u16
}

pub(crate) fn change_index_key(slot: u16, stream_key: &[u8], table_name: &TableName) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        CHANGE_INDEX_PREFIX.len()
            + "/slot/000/".len()
            + stream_key.len()
            + 1
            + table_name.as_ref().len(),
    );
    key.extend_from_slice(CHANGE_INDEX_PREFIX.as_bytes());
    key.extend_from_slice(b"/slot/");
    key.extend_from_slice(slot.to_string().as_bytes());
    key.extend_from_slice(b"/");
    key.extend_from_slice(stream_key);
    key.extend_from_slice(b"/");
    key.extend_from_slice(table_name.as_ref().as_bytes());
    key
}

pub(crate) fn change_index_slot_prefix(slot: u16) -> Vec<u8> {
    format!("{CHANGE_INDEX_PREFIX}/slot/{slot}/").into_bytes()
}

pub(crate) fn project_gsi_item(
    item: HashMap<String, AttributeValue>,
    projection: &Projection,
    table_schema: &[KeySchemaElement],
    gsi_schema: &[KeySchemaElement],
) -> HashMap<String, AttributeValue> {
    match projection
        .projection_type
        .as_ref()
        .unwrap_or(&ProjectionType::All)
    {
        ProjectionType::All => item,
        _ => {
            storage_common::apply_gsi_projection(&item, Some(projection), table_schema, gsi_schema)
        }
    }
}

pub(crate) fn key_schema_for_gsi(
    table_info: &StoredTableInfo,
    gsi_name: &IndexName,
) -> Option<Vec<KeySchemaElement>> {
    if let Some(gsis) = table_info.global_secondary_indexes.as_ref() {
        let gsi = gsis.iter().find(|g| g.index_name == *gsi_name);
        if let Some(gsi) = gsi {
            return Some(gsi.key_schema.clone());
        }
    }
    None
}
