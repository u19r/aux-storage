//! Table stream records share catalog binding and image decisions across SQL
//! and KV.

use std::{future::Future, num::NonZeroU32};

use storage_types::{
    AttributeMap, BoundTableInfo, GetStreamRecordsRequest, GetStreamRecordsResponse,
    ItemHistoryAddress, KeyAttributes, StorageError, StorageResult, StreamItemId, StreamRecord,
    StreamViewType, TableName, TableStreamFrontier, storage_serde,
};
use stream_provider::{
    ReadDirection, StoredStreamPointer, StreamAttempt, StreamDataType, StreamItem,
    StreamPartitioningMode, StreamReadRequest, StreamRowKind, StreamRowPage, StreamRowReadKind,
};

use crate::{CachePublication, CatalogAttempt, TableMetadataCache, resolve_table_in_attempt};

/// The two physical families batch independent history ranges in the same
/// attempt that read the pointer page and table metadata.
pub trait DynamoTableStreamAttempt: CatalogAttempt + StreamAttempt {
    fn read_item_history_pages(
        &mut self,
        reads: &[(ItemHistoryAddress, StreamReadRequest)],
    ) -> impl Future<Output = StorageResult<Vec<StreamRowPage>>> + Send;
}

pub async fn latest_table_stream_pointer_in_attempt<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    name: &storage_types::TableName,
) -> StorageResult<(Option<StreamItemId>, Option<CachePublication>)> {
    let (table, publication) = resolve_table_in_attempt(attempt, cache, name).await?;
    let pointer = latest_table_stream_pointer_from_table(attempt, &table).await?;
    Ok((pointer, publication))
}

pub async fn latest_table_stream_pointer_from_table<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<Option<StreamItemId>> {
    table_stream_pointer_boundary_from_table(attempt, table, ReadDirection::Backward).await
}

async fn table_stream_pointer_boundary_from_table<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    direction: ReadDirection,
) -> StorageResult<Option<StreamItemId>> {
    table
        .info
        .stream_specification
        .as_ref()
        .filter(|spec| spec.stream_enabled)
        .ok_or_else(|| {
            StorageError::raw_validation(format!(
                "Table '{}' does not have streams enabled",
                table.info.table_name
            ))
        })?;
    let kind = table_pointer_kind(&table.info.table_name, table.history_id);
    let read = StreamReadRequest {
        partitioning_mode: StreamPartitioningMode::Single,
        exclusive_start: None,
        direction,
        limit: NonZeroU32::MIN,
    };
    let page = attempt
        .read_stream(kind.row_read(StreamRowReadKind::Page(&read)))
        .await?
        .into_items()?
        .finish(&read, &kind)?;
    Ok(page.items.first().map(|item| item.id))
}

pub async fn get_table_stream_records_in_attempt<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &GetStreamRecordsRequest,
) -> StorageResult<(GetStreamRecordsResponse, Option<CachePublication>)> {
    let name = request
        .table_name()
        .ok_or_else(|| StorageError::raw_validation("TableName is required"))?;
    let (table, publication) = resolve_table_in_attempt(attempt, cache, name).await?;
    let response = get_table_stream_records_from_table(attempt, &table, request).await?;
    Ok((response, publication))
}

pub async fn get_table_stream_records_from_table<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    request: &GetStreamRecordsRequest,
) -> StorageResult<GetStreamRecordsResponse> {
    let name = &table.info.table_name;
    if request.table_name().map(TableName::dynamodb_resource_name)
        != Some(name.dynamodb_resource_name())
    {
        return Err(StorageError::internal("stream read table binding differs"));
    }
    table
        .info
        .stream_specification
        .as_ref()
        .filter(|spec| spec.stream_enabled)
        .ok_or_else(|| {
            StorageError::raw_validation(format!("Table '{name}' does not have streams enabled"))
        })?;
    let read = parse_table_stream_read(request)?;
    let head = latest_table_stream_pointer_from_table(attempt, table).await?;
    let oldest =
        table_stream_pointer_boundary_from_table(attempt, table, ReadDirection::Forward).await?;
    let kind = table_pointer_kind(name, table.history_id);
    let rows = attempt
        .read_stream(kind.row_read(StreamRowReadKind::Page(&read)))
        .await?
        .into_items()?
        .finish(&read, &kind)?;
    let checkpoint = rows.has_more.then_some(rows.last_evaluated_key).flatten();
    let pointers = TablePointerPage::bind(table, rows.items)?;
    let pages = attempt
        .read_item_history_pages(&pointers.history_reads)
        .await?;
    let records = pointers.decode(table, pages)?;
    Ok(GetStreamRecordsResponse::table(
        name.clone(),
        records,
        checkpoint.map(|id| id.to_string()),
        TableStreamFrontier {
            table_history_id: table.history_id,
            oldest,
            head,
        },
    ))
}

fn parse_table_stream_read(request: &GetStreamRecordsRequest) -> StorageResult<StreamReadRequest> {
    let limit = request.limit().unwrap_or(100);
    let limit = NonZeroU32::new(limit)
        .filter(|limit| limit.get() <= 1_000)
        .ok_or_else(|| StorageError::raw_validation("Limit must be between 1 and 1000"))?;
    let start = request
        .last_evaluated_key()
        .map(|value| {
            value
                .parse::<StreamItemId>()
                .map_err(|_| StorageError::internal("Invalid page token"))
        })
        .transpose()?;
    Ok(StreamReadRequest {
        partitioning_mode: StreamPartitioningMode::Single,
        exclusive_start: start,
        direction: ReadDirection::Forward,
        limit,
    })
}

struct TablePointerPage {
    pointers: Vec<(StreamItem, StoredStreamPointer)>,
    history_reads: Vec<(ItemHistoryAddress, StreamReadRequest)>,
}

impl TablePointerPage {
    fn bind(table: &BoundTableInfo, items: Vec<StreamItem>) -> StorageResult<Self> {
        let mut page = Self {
            pointers: Vec::with_capacity(items.len()),
            history_reads: Vec::new(),
        };
        for item in items {
            let pointer: StoredStreamPointer = storage_serde::from_bytes(&item.data)?;
            if !pointer.matches_table_history(table.history_id) {
                continue;
            }
            if let StoredStreamPointer::Pointer {
                item_stream_version,
                ..
            } = &pointer
            {
                page.history_reads.push((
                    pointer.history_address(),
                    history_read(*item_stream_version),
                ));
            }
            page.pointers.push((item, pointer));
        }
        Ok(page)
    }

    fn decode(
        self,
        table: &BoundTableInfo,
        pages: Vec<StreamRowPage>,
    ) -> StorageResult<Vec<StreamRecord>> {
        if pages.len() != self.history_reads.len() {
            return Err(StorageError::internal("item history batch count differs"));
        }
        let mut pages = pages.into_iter();
        let mut records = Vec::with_capacity(self.pointers.len());
        for (item, pointer) in self.pointers {
            let images = decode_pointer_images(&item, &pointer, &mut pages)?;
            if let Some(record) = decode_record(table, item.id, pointer, &images)? {
                records.push(record);
            }
        }
        Ok(records)
    }
}

fn decode_pointer_images(
    item: &StreamItem,
    pointer: &StoredStreamPointer,
    pages: &mut impl Iterator<Item = StreamRowPage>,
) -> StorageResult<Vec<StreamItem>> {
    Ok(match pointer {
        StoredStreamPointer::Pointer {
            item_stream_version,
            ..
        } => {
            let page = pages
                .next()
                .ok_or_else(|| StorageError::internal("item history page is missing"))?;
            let kind = StreamRowKind::Item(pointer.history_address().physical_stream_name());
            let read = history_read(*item_stream_version);
            let page = page.finish(&read, &kind)?;
            if page.items.first().map(|image| image.id)
                != Some(StreamItemId::from(*item_stream_version))
            {
                return Err(StorageError::internal(
                    "stream pointer targets a missing item version",
                ));
            }
            page.items
        }
        StoredStreamPointer::Embedded {
            item_stream_version,
            items,
            ..
        } => items
            .iter()
            .map(|image| StreamItem {
                id: StreamItemId::from(*item_stream_version),
                stream_name: None,
                data: image.data.clone(),
                data_type: image.data_type,
                created_at: item.created_at,
            })
            .collect(),
    })
}

fn table_pointer_kind(
    name: &storage_types::TableName,
    history_id: storage_types::TableHistoryId,
) -> StreamRowKind {
    StreamRowKind::Table(
        ItemHistoryAddress {
            history_id,
            stream_name: storage_types::StreamName::table_stream(name),
        }
        .physical_stream_name(),
    )
}

fn history_read(version: storage_types::ItemStreamVersion) -> StreamReadRequest {
    StreamReadRequest {
        partitioning_mode: StreamPartitioningMode::Single,
        exclusive_start: Some(StreamItemId::from(version).increment()),
        direction: ReadDirection::Backward,
        limit: NonZeroU32::MIN.saturating_add(2 - 1),
    }
}

fn decode_record(
    table: &BoundTableInfo,
    pointer_id: StreamItemId,
    pointer: StoredStreamPointer,
    images: &[StreamItem],
) -> StorageResult<Option<StreamRecord>> {
    let (name, version) = match pointer {
        StoredStreamPointer::Pointer {
            table_name,
            item_stream_version,
            ..
        }
        | StoredStreamPointer::Embedded {
            table_name,
            item_stream_version,
            ..
        } => (table_name, item_stream_version),
    };
    if name != table.info.table_name
        || images.first().map(|image| image.id) != Some(StreamItemId::from(version))
    {
        return Err(StorageError::internal(
            "stream pointer image identity differs from table",
        ));
    }
    let mut new_image = decode_image(images.first())?;
    let mut old_image = decode_image(images.get(1))?;
    let Some(key_source) = new_image.as_ref().or(old_image.as_ref()) else {
        return Ok(None);
    };
    let keys = extract_keys(key_source, &table.info.key_schema)?;
    match table
        .info
        .stream_specification
        .as_ref()
        .and_then(|spec| spec.stream_view_type.as_ref())
    {
        Some(StreamViewType::KeysOnly) => {
            new_image = None;
            old_image = None;
        }
        Some(StreamViewType::NewImage) => old_image = None,
        Some(StreamViewType::OldImage) => new_image = None,
        Some(StreamViewType::NewAndOldImages) | None => {}
    }
    Ok(Some(StreamRecord {
        cursor: Some(pointer_id.to_string()),
        source_table_name: Some(name),
        keys,
        sequence_number: pointer_id.to_string(),
        old_image,
        new_image,
        system_position: None,
    }))
}

fn decode_image(item: Option<&StreamItem>) -> StorageResult<Option<AttributeMap>> {
    item.filter(|item| item.data_type != StreamDataType::DeleteMarker)
        .map(|item| storage_serde::from_bytes(&item.data))
        .transpose()
}

fn extract_keys(
    item: &AttributeMap,
    schema: &[storage_types::KeySchemaElement],
) -> StorageResult<KeyAttributes> {
    let mut keys = KeyAttributes::with_capacity(schema.len());
    for element in schema {
        let value = item
            .get(&element.attribute_name)
            .ok_or_else(|| StorageError::internal("stream image lacks a table key attribute"))?;
        keys.insert(element.attribute_name.clone(), value.clone());
    }
    Ok(keys)
}
