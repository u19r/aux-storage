use serde::{Deserialize, Serialize};
use storage_backfill::{
    LogicalBackfillDomain, LogicalBackfillRecord, LogicalExportPage, LogicalExportRequest,
};
use storage_types::{ItemKey, StorageResult, StreamName, TableName};

use super::logical_backfill_records::unchecked_checksum;
use crate::{
    SortedKvDbStorageProvider,
    helpers::increment_bytes,
    keys::{
        STREAM_CURSORS_PREFIX, STREAMS_PREFIX, gsi_backfill_key, gsi_tombstone_prefix_from_name,
    },
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
            let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
                continue;
            };
            for gsi in gsis {
                self.append_raw_prefix_records(
                    &mut records,
                    LogicalBackfillDomain::GsiRecords,
                    "physical_row",
                    ItemKey::index_prefix_from_name(&table_info.table_name, &gsi.index_name),
                    request.limit,
                )
                .await?;
                self.append_raw_prefix_records(
                    &mut records,
                    LogicalBackfillDomain::GsiRecords,
                    "tombstone_row",
                    gsi_tombstone_prefix_from_name(&table_info.table_name, &gsi.index_name),
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
        self.append_raw_prefix_records(
            &mut records,
            LogicalBackfillDomain::StreamRecords,
            "system_stream_item",
            stream_item_prefix(&StreamName::system_table_stream()),
            request.limit,
        )
        .await?;

        for table_info in self
            .filtered_table_infos(request.table_name.as_deref())
            .await?
        {
            self.append_raw_prefix_records(
                &mut records,
                LogicalBackfillDomain::StreamRecords,
                "table_stream_item",
                stream_item_prefix(&StreamName::table_stream(&table_info.table_name)),
                request.limit,
            )
            .await?;
            self.append_raw_contains_records(
                &mut records,
                LogicalBackfillDomain::StreamRecords,
                "table_item_stream_item",
                table_stream_item_marker(&table_info.table_name),
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
            b"sys/partition-control/streams/".to_vec(),
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
            let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
                continue;
            };
            for gsi in gsis {
                let key = gsi_backfill_key(&table_info.table_name, &gsi.index_name);
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
                None::<crate::newtypes::TablePageKey>,
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

    async fn append_raw_contains_records(
        &self,
        records: &mut Vec<LogicalBackfillRecord>,
        domain: LogicalBackfillDomain,
        record_type: &str,
        needle: Vec<u8>,
        limit: u32,
    ) -> StorageResult<()> {
        if records.len() >= limit as usize {
            return Ok(());
        }
        let range = self
            .kv_store
            .get_range(
                b"",
                &[0xFF],
                None,
                None::<crate::newtypes::TablePageKey>,
                true,
            )
            .await?;
        for (key, value) in range.items {
            if key.windows(needle.len()).any(|window| window == needle) {
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

fn stream_item_prefix(stream_name: &StreamName) -> Vec<u8> {
    let mut prefix = Vec::<u8>::from(stream_name);
    prefix.push(b'/');
    prefix
}

fn table_stream_item_marker(table_name: &TableName) -> Vec<u8> {
    let mut marker = table_name.sanitized_name().as_bytes().to_vec();
    marker.extend(b"/stream-item/");
    marker
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
