use serde::{Deserialize, Serialize};
use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage, LogicalExportRequest,
};
use storage_types::StorageResult;

use super::logical_backfill_records::unchecked_checksum;
use crate::{
    SortedKvDbStorageProvider,
    helpers::increment_bytes,
    keyspace::{
        compact::{self, KeyRange},
        table_keys,
    },
    partition_family::{PartitionFamilyKind, partition_family_kind_prefix},
    stream::metadata_keys::{STREAM_CURSORS_PREFIX, STREAMS_PREFIX},
};

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn export_gsi_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let table_infos = self
            .filtered_table_infos(request.table_name.as_deref())
            .await?;
        let mut records = Vec::with_capacity(request.limit as usize);
        self.append_gsi_backfill_records(
            &mut records,
            request.table_name.as_deref(),
            request.limit,
        )
        .await?;
        for table_info in table_infos {
            let metadata = self
                .get_table_identity_from_name(&table_info.table_name)
                .await?
                .ok_or_else(|| {
                    storage_types::StorageError::table_not_found(&table_info.table_name)
                })?;
            let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
                continue;
            };
            for gsi in gsis {
                let Some(gsi_range) = table_keys::gsi_prefix(&metadata.identity, &gsi.index_name)
                else {
                    continue;
                };
                self.append_raw_range_records(
                    &mut records,
                    LogicalBackfillDomain::GsiRecords,
                    "physical_row",
                    gsi_range,
                    request.limit,
                )
                .await?;
                let Some(tombstone_range) =
                    table_keys::gsi_tombstone_prefix(&metadata.identity, &gsi.index_name)
                else {
                    continue;
                };
                self.append_raw_range_records(
                    &mut records,
                    LogicalBackfillDomain::GsiRecords,
                    "tombstone_row",
                    tombstone_range,
                    request.limit,
                )
                .await?;
                if records.len() >= request.limit as usize {
                    break;
                }
            }
            if records.len() >= request.limit as usize {
                break;
            }
        }
        records.truncate(request.limit as usize);
        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::GsiRecords,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    pub(super) async fn export_stream_records(
        &self,
        request: LogicalExportRequest,
    ) -> StorageResult<LogicalExportPage> {
        let mut records = Vec::with_capacity(request.limit as usize);
        self.append_raw_prefix_records(
            &mut records,
            LogicalBackfillDomain::StreamRecords,
            "stream_metadata",
            STREAMS_PREFIX.as_bytes().to_vec(),
            request.limit,
        )
        .await?;
        self.append_raw_prefix_records(
            &mut records,
            LogicalBackfillDomain::StreamRecords,
            "stream_cursor",
            STREAM_CURSORS_PREFIX.as_bytes().to_vec(),
            request.limit,
        )
        .await?;
        self.append_raw_range_records(
            &mut records,
            LogicalBackfillDomain::StreamRecords,
            "system_stream_item",
            compact::system_stream_prefix(),
            request.limit,
        )
        .await?;

        for table_info in self
            .filtered_table_infos(request.table_name.as_deref())
            .await?
        {
            let metadata = self
                .get_table_identity_from_name(&table_info.table_name)
                .await?
                .ok_or_else(|| {
                    storage_types::StorageError::table_not_found(&table_info.table_name)
                })?;
            self.append_raw_range_records(
                &mut records,
                LogicalBackfillDomain::StreamRecords,
                "table_stream_item",
                compact::table_stream_prefix(metadata.identity.table_id),
                request.limit,
            )
            .await?;
            self.append_raw_range_records(
                &mut records,
                LogicalBackfillDomain::StreamRecords,
                "table_item_stream_item",
                compact::item_stream_prefix(metadata.identity.table_id, b""),
                request.limit,
            )
            .await?;
            if records.len() >= request.limit as usize {
                break;
            }
        }

        self.append_raw_prefix_records(
            &mut records,
            LogicalBackfillDomain::StreamRecords,
            "stream_partition_control",
            partition_family_kind_prefix(PartitionFamilyKind::OrderedLog),
            request.limit,
        )
        .await?;

        records.truncate(request.limit as usize);
        Ok(LogicalExportPage {
            domain: LogicalBackfillDomain::StreamRecords,
            records,
            next_cursor: None,
            checksum: unchecked_checksum()?,
        })
    }

    async fn append_gsi_backfill_records(
        &self,
        records: &mut Vec<LogicalBackfillRecord>,
        table_filter: Option<&str>,
        limit: u32,
    ) -> StorageResult<()> {
        for table_info in self.filtered_table_infos(table_filter).await? {
            let metadata = self
                .get_table_identity_from_name(&table_info.table_name)
                .await?
                .ok_or_else(|| {
                    storage_types::StorageError::table_not_found(&table_info.table_name)
                })?;
            let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
                continue;
            };
            for gsi in gsis {
                let Some(key) = table_keys::gsi_backfill_key(&metadata.identity, &gsi.index_name)
                else {
                    continue;
                };
                if let Some(value) = self.kv_store.get(&key, true).await? {
                    records.push(raw_kv_record(
                        LogicalBackfillDomain::GsiRecords,
                        "backfill_state",
                        key,
                        value,
                    )?);
                }
                if records.len() >= limit as usize {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn append_raw_prefix_records(
        &self,
        records: &mut Vec<LogicalBackfillRecord>,
        domain: LogicalBackfillDomain,
        record_type: &str,
        prefix: Vec<u8>,
        limit: u32,
    ) -> StorageResult<()> {
        if records.len() >= limit as usize {
            return Ok(());
        }
        let range = self
            .kv_store
            .get_range(
                &prefix,
                &increment_bytes(prefix.clone()),
                None,
                None::<crate::sorted_kv_store::RawKey>,
                true,
            )
            .await?;
        for (key, value) in range.items {
            records.push(raw_kv_record(
                domain,
                record_type,
                key.into_vec(),
                value.into_vec(),
            )?);
            if records.len() >= limit as usize {
                break;
            }
        }
        Ok(())
    }

    async fn append_raw_range_records(
        &self,
        records: &mut Vec<LogicalBackfillRecord>,
        domain: LogicalBackfillDomain,
        record_type: &str,
        range: KeyRange,
        limit: u32,
    ) -> StorageResult<()> {
        if records.len() >= limit as usize {
            return Ok(());
        }
        let range = self
            .kv_store
            .get_range(
                &range.start,
                &range.end,
                None,
                None::<crate::sorted_kv_store::RawKey>,
                true,
            )
            .await?;
        for (key, value) in range.items {
            records.push(raw_kv_record(
                domain,
                record_type,
                key.into_vec(),
                value.into_vec(),
            )?);
            if records.len() >= limit as usize {
                break;
            }
        }
        Ok(())
    }
}

fn raw_kv_record(
    domain: LogicalBackfillDomain,
    record_type: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> StorageResult<LogicalBackfillRecord> {
    let payload = RawKvRecordPayloadRef {
        record_type,
        key: &key,
        value: &value,
    };
    Ok(LogicalBackfillRecord::DomainRecord {
        domain,
        record_key_json: serde_json::json!({
            "record_type": record_type,
            "key": &key,
        })
        .to_string(),
        payload_json: serde_json::to_string(&payload)?,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct RawKvRecordPayload {
    pub(super) record_type: String,
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct RawKvRecordPayloadRef<'a> {
    record_type: &'a str,
    key: &'a [u8],
    value: &'a [u8],
}
