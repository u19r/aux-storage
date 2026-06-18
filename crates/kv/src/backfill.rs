use std::collections::HashMap;

use async_trait::async_trait;
use storage_backfill::{
    BackfillBatchOutcome, BackfillDriver, BackfillState, BackfillStatus, GsiBackfillDescriptor,
};
use storage_common::apply_gsi_projection;
use storage_types::{
    AttributeValue, IndexName, ItemKey, StorageError, TableName, TimeToLiveStatus, TimestampMillis,
};
use tracing::debug;

use crate::{
    SortedKvDbStorageProvider,
    keyspace::{compact, table_identity::StoredTableMetadata, table_keys},
    partition_family::PartitionFamilyKvStore,
    sorted_kv_store::{BatchItem, RawKey},
    storage_provider::key_schema_for_gsi,
    ttl,
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct KvBackfillRecord {
    pub state: BackfillState,
}

impl KvBackfillRecord {
    fn new(state: BackfillState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl<S> BackfillDriver for SortedKvDbStorageProvider<S>
where S: PartitionFamilyKvStore + 'static
{
    async fn enumerate_states(
        &self,
    ) -> Result<Vec<(GsiBackfillDescriptor, BackfillState)>, StorageError> {
        let range = compact::table_metadata_prefix();
        let scan_result = self
            .kv_store
            .get_range(&range.start, &range.end, None, None::<RawKey>, true)
            .await?;

        let mut records = Vec::new();
        for (raw_key, raw_value) in scan_result.items {
            if raw_key.first().copied() != Some(compact::KeyFamily::TableMetadata.code()) {
                continue;
            }

            let metadata: StoredTableMetadata =
                storage_types::storage_serde::from_bytes(&raw_value)?;
            if metadata.identity.deleted {
                continue;
            }

            for index in &metadata.identity.indexes {
                let key = compact::gsi_backfill_key(metadata.identity.table_id, index.index_id);
                let Some(raw_record) = self.kv_store.get(&key, true).await? else {
                    continue;
                };
                let record: KvBackfillRecord =
                    storage_types::storage_serde::from_bytes(&raw_record)?;
                records.push((
                    GsiBackfillDescriptor::new(
                        metadata.identity.table_name.as_ref(),
                        index.index_name.as_ref(),
                    ),
                    record.state,
                ));
            }
        }
        Ok(records)
    }

    async fn persist_state(
        &self,
        descriptor: &GsiBackfillDescriptor,
        state: &BackfillState,
    ) -> Result<(), StorageError> {
        let table = TableName::new(&descriptor.table_name);
        let index = IndexName::new(&descriptor.index_name);
        let metadata = self
            .get_table_identity_from_name(&table)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table))?;
        let key = table_keys::gsi_backfill_key(&metadata.identity, &index).ok_or_else(|| {
            StorageError::internal(&format!("missing storage identity for index {index}"))
        })?;
        let record = KvBackfillRecord::new(state.clone());
        self.kv_store
            .put(
                &key,
                &storage_types::storage_serde::to_bytes(&record)?,
                None,
            )
            .await
    }

    async fn reload_state(
        &self,
        descriptor: &GsiBackfillDescriptor,
    ) -> Result<Option<BackfillState>, StorageError> {
        let table = TableName::new(&descriptor.table_name);
        let index = IndexName::new(&descriptor.index_name);
        let Some(metadata) = self.get_table_identity_from_name(&table).await? else {
            return Ok(None);
        };
        let key = table_keys::gsi_backfill_key(&metadata.identity, &index).ok_or_else(|| {
            StorageError::internal(&format!("missing storage identity for index {index}"))
        })?;
        if let Some(bytes) = self.kv_store.get(&key, true).await? {
            let record: KvBackfillRecord = storage_types::storage_serde::from_bytes(&bytes)?;
            Ok(Some(record.state))
        } else {
            Ok(None)
        }
    }

    async fn execute_batch(
        &self,
        descriptor: &GsiBackfillDescriptor,
        state: &BackfillState,
        batch_size: usize,
    ) -> Result<BackfillBatchOutcome, StorageError> {
        let table_name = TableName::new(&descriptor.table_name);
        let index_name = IndexName::new(&descriptor.index_name);

        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let table_info = &table_metadata.table_info;
        let gsi_schema = key_schema_for_gsi(table_info, &index_name)
            .unwrap_or_else(|| table_info.key_schema.clone());

        let batch_limit = batch_size.clamp(1, 1000);
        let limit = u32::try_from(batch_limit).unwrap_or(1000);
        let data_range = table_keys::primary_item_prefix(table_metadata.identity.table_id);

        let page_token = state.scan_lek.as_ref().and_then(|token| {
            ItemKey::item_key_from_next_page_token(token, table_info, &None)
                .ok()
                .flatten()
        });
        let page_token = page_token
            .as_ref()
            .map(|token| table_keys::item_key(&table_metadata.identity, token))
            .transpose()?
            .map(RawKey);

        let range = self
            .kv_store
            .get_range(
                &data_range.start,
                &data_range.end,
                Some(limit),
                page_token,
                true,
            )
            .await?;

        if range.items.is_empty() {
            return Ok(BackfillBatchOutcome {
                items_processed: 0,
                next_token: None,
                done: state.scan_lek.is_none(),
            });
        }

        let mut ttl_config = self.load_ttl_config(&table_name).await?;
        let ttl_attribute_name = ttl_config.as_ref().map(|cfg| cfg.attribute_name.clone());
        let is_ttl_index = ttl_config
            .as_ref()
            .is_some_and(|cfg| cfg.gsi_name() == index_name);

        let projection = Self::find_gsi_projection(table_info, &index_name);
        let mut batch = Vec::with_capacity(range.items.len());
        let mut last_item: Option<HashMap<String, AttributeValue>> = None;
        let mut processed = 0usize;

        for (_raw_key, raw_value) in &range.items {
            let item: HashMap<String, AttributeValue> =
                storage_types::storage_serde::from_bytes(raw_value)?;
            last_item = Some(item.clone());

            if is_ttl_index {
                let Some(ttl_attr) = ttl_attribute_name.as_deref() else {
                    continue;
                };
                let Some(key) = ttl::compact_ttl_index_key_for_item(
                    &table_metadata.identity,
                    table_info,
                    ttl_attr,
                    &item,
                )?
                else {
                    continue;
                };
                batch.push(BatchItem {
                    key,
                    value: Some(Vec::new()),
                });
                processed += 1;
            } else {
                let key_opt = ItemKey::from_key_schema_for_index(
                    table_info.table_name.clone(),
                    &table_info.key_schema,
                    &index_name,
                    &gsi_schema,
                    &item,
                )?;
                let Some(gsi_key) = key_opt else {
                    continue;
                };
                let projected =
                    apply_gsi_projection(&item, projection, &table_info.key_schema, &gsi_schema);
                let gsi_value = storage_types::storage_serde::to_bytes(&projected)?;
                batch.push(BatchItem {
                    key: table_keys::item_key(&table_metadata.identity, &gsi_key)?,
                    value: Some(gsi_value),
                });
                processed += 1;
            }
        }

        if !batch.is_empty() {
            self.kv_store.batch_write(batch).await?;
        }

        let next_token = if range.has_more {
            if let Some(item) = last_item {
                ItemKey::last_evaluated_key_from_last_item(&item, table_info, &None)?
            } else {
                None
            }
        } else {
            None
        };

        let done = !range.has_more && next_token.is_none();
        if done
            && is_ttl_index
            && let Some(mut config) = ttl_config.take()
            && config.status != TimeToLiveStatus::Enabled
        {
            config.status = TimeToLiveStatus::Enabled;
            config.touch();
            self.save_ttl_config(&table_name, &config).await?;
        }
        debug!(
            table = %descriptor.table_name,
            index = %descriptor.index_name,
            processed,
            "backfill batch complete"
        );

        Ok(BackfillBatchOutcome {
            items_processed: processed,
            next_token,
            done,
        })
    }
}

impl<S> SortedKvDbStorageProvider<S>
where S: PartitionFamilyKvStore + 'static
{
    pub(crate) async fn initialize_backfill_record(
        &self,
        table_name: &TableName,
        index_name: &IndexName,
        captured_stream_tail: Option<String>,
    ) -> Result<(), StorageError> {
        let now = TimestampMillis::now();
        let mut state = BackfillState::new(now);
        state.status = BackfillStatus::Pending;
        state.captured_stream_tail = captured_stream_tail;
        let descriptor = GsiBackfillDescriptor::new(table_name.as_ref(), index_name.as_ref());
        self.persist_state(&descriptor, &state).await
    }
}
