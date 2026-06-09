use std::{collections::HashMap, time::Instant};

use deadpool_postgres::GenericClient;
use storage_types::{
    ReplicationEventMetadata, StorageError, StorageResult, StoredTableInfo, StreamName,
    TimestampMillis,
};
use stream_provider::{EmbeddedStreamItem, StoredStreamPointer, StreamDataType};
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use crate::backends::postgres::{
    PostgresStorageProvider, STREAM_EMBEDDED_MAX_BYTES, sql_statements,
};

#[derive(Clone, Copy)]
pub(super) struct PostgresWriteStreamEntriesInput<'a> {
    pub old_item: Option<&'a HashMap<String, storage_provider::AttributeValue>>,
    pub is_deleted: bool,
    pub item_stream_version: storage_types::ItemStreamVersion,
    pub replication: Option<&'a ReplicationEventMetadata>,
}

impl PostgresStorageProvider {
    pub(super) fn encode_stream_name(stream_name: &StreamName) -> String {
        let bytes = stream_name.as_ref();
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            encoded.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
            encoded.push(char::from(b"0123456789abcdef"[usize::from(byte & 0x0f)]));
        }
        encoded
    }

    pub(super) fn decode_hex_nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    pub(super) fn decode_stream_name(encoded: &str) -> StreamName {
        let raw = encoded.as_bytes();
        if !raw.len().is_multiple_of(2) {
            return StreamName::from(encoded);
        }

        let mut bytes = Vec::with_capacity(raw.len() / 2);
        let mut index = 0;
        while index < raw.len() {
            let hi = Self::decode_hex_nibble(raw[index]);
            let lo = Self::decode_hex_nibble(raw[index + 1]);
            let (Some(hi), Some(lo)) = (hi, lo) else {
                return StreamName::from(encoded);
            };
            bytes.push((hi << 4) | lo);
            index += 2;
        }
        StreamName::from(bytes)
    }

    pub(super) async fn insert_stream_entries_with_id<C: GenericClient + Sync>(
        &self,
        client: &C,
        entries: &[PostgresStreamEntry],
    ) -> StorageResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let sql = sql_statements::insert_stream_entries(entries.len());
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(entries.len() * 5);
        for entry in entries {
            params.push(&entry.stream_name);
            params.push(&entry.item_id);
            params.push(&entry.data);
            params.push(&entry.created_at_ms);
            params.push(&entry.data_type);
        }
        let started = Instant::now();
        client.execute(&sql, &params).await.map_err(|err| {
            let detail = err
                .as_db_error()
                .map(|db| format!("{:?}: {}", db.code(), db.message()))
                .unwrap_or_else(|| format!("{err:?}"));
            StorageError::internal(&format!("postgres insert stream entries failed: {detail}"))
        })?;
        self.record_transaction_phase("batch_write_item", "stream_write_insert", started.elapsed());
        Ok(())
    }

    pub(super) async fn write_stream_entries_for_item_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_info: &StoredTableInfo,
        item_data: &HashMap<String, storage_provider::AttributeValue>,
        input: PostgresWriteStreamEntriesInput<'_>,
    ) -> StorageResult<()> {
        if !crate::stream_writer::should_write_stream_entries_for_gsi_mode(
            table_info,
            self.immediate_gsi_consistency,
        ) {
            return Ok(());
        }
        let PostgresWriteStreamEntriesInput {
            old_item,
            is_deleted,
            item_stream_version,
            replication,
        } = input;

        let created_at = TimestampMillis::now();
        let item_key = storage_types::ItemKey::from_key_schema(
            table_info.table_name.clone(),
            &table_info.key_schema,
            item_data,
        )
        .map_err(|err| StorageError::internal(&format!("stream item key error: {err}")))?;
        let item_stream = StreamName::table_item_stream(&table_info.table_name, &item_key)
            .map_err(|err| StorageError::internal(&format!("stream name error: {err}")))?;
        let table_stream_name = StreamName::table_stream(&table_info.table_name);
        let system_stream_name = StreamName::system_table_stream();

        let data = if is_deleted {
            storage_types::storage_serde::to_bytes(item_data).map_err(|err| {
                StorageError::internal(&format!("stream delete marker encode error: {err}"))
            })?
        } else {
            storage_types::storage_serde::to_bytes(item_data).map_err(|err| {
                StorageError::internal(&format!("stream new image encode error: {err}"))
            })?
        };
        let old_bytes = match old_item {
            Some(old) if !old.is_empty() => {
                Some(storage_types::storage_serde::to_bytes(old).map_err(|err| {
                    StorageError::internal(&format!("stream old image encode error: {err}"))
                })?)
            }
            _ => None,
        };
        let embedded_bytes = data.len() + old_bytes.as_ref().map_or(0, std::vec::Vec::len);
        let item_stream_row_id = storage_types::StreamItemId::from(item_stream_version);
        let table_pointer_stream_item_id = storage_types::StreamItemId::from(Uuid::now_v7());
        let system_pointer_stream_item_id = storage_types::StreamItemId::from(Uuid::now_v7());
        let data_type = if is_deleted {
            StreamDataType::DeleteMarker
        } else {
            StreamDataType::DynamoDbJson
        };

        let stored_pointer = if embedded_bytes <= STREAM_EMBEDDED_MAX_BYTES {
            let mut items = Vec::with_capacity(1 + usize::from(old_bytes.is_some()));
            items.push(EmbeddedStreamItem {
                data: data.clone(),
                data_type,
            });
            if let Some(old) = old_bytes {
                items.push(EmbeddedStreamItem {
                    data: old,
                    data_type: StreamDataType::DynamoDbJson,
                });
            }
            StoredStreamPointer::embedded(
                item_stream.clone(),
                table_info.table_name.clone(),
                item_stream_version,
                items,
            )
        } else {
            StoredStreamPointer::pointer(
                item_stream.clone(),
                table_info.table_name.clone(),
                item_stream_version,
            )
        };
        let stored_pointer = if let Some(replication) = replication.cloned() {
            stored_pointer.with_replication_metadata(replication)
        } else {
            stored_pointer
        };
        let pointer_data = storage_types::storage_serde::to_bytes(&stored_pointer)?;

        let item_stream_row_id = item_stream_row_id.to_string();
        let table_pointer_id = table_pointer_stream_item_id.to_string();
        let system_pointer_id = system_pointer_stream_item_id.to_string();
        let created_at_ms = *created_at;
        let entries = vec![
            PostgresStreamEntry::new(
                Self::encode_stream_name(&item_stream),
                item_stream_row_id,
                data,
                created_at_ms,
                data_type,
            ),
            PostgresStreamEntry::new(
                Self::encode_stream_name(&table_stream_name),
                table_pointer_id,
                pointer_data.clone(),
                created_at_ms,
                StreamDataType::StreamPointer,
            ),
            PostgresStreamEntry::new(
                Self::encode_stream_name(&system_stream_name),
                system_pointer_id,
                pointer_data,
                created_at_ms,
                StreamDataType::StreamPointer,
            ),
        ];
        self.insert_stream_entries_with_id(client, &entries).await?;
        Self::insert_stream_pointer_index_with_client(
            client,
            &table_info.table_name,
            &item_stream,
            item_stream_version,
            table_pointer_stream_item_id,
            system_pointer_stream_item_id,
            created_at,
        )
        .await?;

        Ok(())
    }
}

pub(super) struct PostgresStreamEntry {
    stream_name: String,
    item_id: String,
    data: Vec<u8>,
    created_at_ms: i64,
    data_type: i32,
}

impl PostgresStreamEntry {
    fn new(
        stream_name: String,
        item_id: String,
        data: Vec<u8>,
        created_at_ms: i64,
        data_type: StreamDataType,
    ) -> Self {
        Self {
            stream_name,
            item_id,
            data,
            created_at_ms,
            data_type: data_type as i32,
        }
    }
}
