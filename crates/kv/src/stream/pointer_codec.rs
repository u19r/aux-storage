use storage_types::{
    ItemStreamVersion, ReplicationEventMetadata, StorageError, StorageResult, StreamName, TableName,
};
use stream_provider::{EmbeddedStreamItem, StreamDataType, StreamPointer};

use crate::keyspace::{compact::TableStorageId, table_identity::TableIdentity};

const COMPACT_POINTER_MAGIC: u8 = b'P';
const COMPACT_POINTER_VERSION: u8 = 1;
const FLAG_EMBEDDED: u8 = 0b0000_0001;
const FLAG_REPLICATION: u8 = 0b0000_0010;

#[derive(Debug, Clone)]
pub(crate) struct CompactStoredStreamPointer {
    pub(crate) table_id: TableStorageId,
    pub(crate) item_scope: Vec<u8>,
    pub(crate) item_stream_version: ItemStreamVersion,
    pub(crate) items: Option<Vec<EmbeddedStreamItem>>,
    pub(crate) replication: Option<ReplicationEventMetadata>,
}

impl CompactStoredStreamPointer {
    pub(crate) fn pointer(
        table: &TableIdentity,
        item_scope: Vec<u8>,
        item_stream_version: ItemStreamVersion,
        replication: Option<ReplicationEventMetadata>,
    ) -> Self {
        Self {
            table_id: table.table_id,
            item_scope,
            item_stream_version,
            items: None,
            replication,
        }
    }

    pub(crate) fn embedded(
        table: &TableIdentity,
        item_scope: Vec<u8>,
        item_stream_version: ItemStreamVersion,
        items: Vec<EmbeddedStreamItem>,
        replication: Option<ReplicationEventMetadata>,
    ) -> Self {
        Self {
            table_id: table.table_id,
            item_scope,
            item_stream_version,
            items: Some(items),
            replication,
        }
    }

    pub(crate) fn stream_pointer(
        &self,
        table: &TableIdentity,
        pointer_stream_item_id: storage_types::StreamItemId,
    ) -> StorageResult<StreamPointer> {
        if table.table_id != self.table_id {
            return Err(StorageError::internal(
                "compact stream pointer table id does not match metadata",
            ));
        }
        Ok(StreamPointer {
            stream_name: item_stream_name(&table.table_name, &self.item_scope),
            table_name: table.table_name.clone(),
            item_stream_version: self.item_stream_version,
            stream_item_id: pointer_stream_item_id,
        })
    }
}

pub(crate) fn encode_compact_pointer(
    pointer: &CompactStoredStreamPointer,
) -> StorageResult<Vec<u8>> {
    let mut flags = 0u8;
    if pointer.items.is_some() {
        flags |= FLAG_EMBEDDED;
    }
    if pointer.replication.is_some() {
        flags |= FLAG_REPLICATION;
    }

    let item_scope_len = u32::try_from(pointer.item_scope.len())
        .map_err(|_| StorageError::internal("compact stream pointer item scope is too large"))?;
    let mut output = Vec::with_capacity(24 + pointer.item_scope.len());
    output.push(COMPACT_POINTER_MAGIC);
    output.push(COMPACT_POINTER_VERSION);
    output.push(flags);
    output.extend_from_slice(&pointer.table_id.get().to_be_bytes());
    output.extend_from_slice(&pointer.item_stream_version.to_be_bytes());
    output.extend_from_slice(&item_scope_len.to_be_bytes());
    output.extend_from_slice(&pointer.item_scope);

    if let Some(items) = &pointer.items {
        let count = u32::try_from(items.len()).map_err(|_| {
            StorageError::internal("compact stream pointer embedded item count is too large")
        })?;
        output.extend_from_slice(&count.to_be_bytes());
        for item in items {
            output.push(stream_data_type_to_byte(item.data_type));
            append_len_prefixed(&mut output, &item.data)?;
        }
    }

    if let Some(replication) = &pointer.replication {
        let bytes = storage_types::storage_serde::to_bytes(replication)?;
        append_len_prefixed(&mut output, &bytes)?;
    }

    Ok(output)
}

pub(crate) fn decode_compact_pointer(bytes: &[u8]) -> StorageResult<CompactStoredStreamPointer> {
    let mut cursor = 0usize;
    let magic = read_u8(bytes, &mut cursor)?;
    if magic != COMPACT_POINTER_MAGIC {
        return Err(StorageError::internal(
            "compact stream pointer decode failed: unexpected marker",
        ));
    }
    let version = read_u8(bytes, &mut cursor)?;
    if version != COMPACT_POINTER_VERSION {
        return Err(StorageError::internal(
            "compact stream pointer decode failed: unsupported version",
        ));
    }
    let flags = read_u8(bytes, &mut cursor)?;
    let table_id = TableStorageId::new(read_u32(bytes, &mut cursor)?);
    let item_stream_version = ItemStreamVersion::from(read_exact_array::<8>(bytes, &mut cursor)?);
    let item_scope_len = usize::try_from(read_u32(bytes, &mut cursor)?)
        .map_err(|_| StorageError::internal("compact stream pointer item scope length failed"))?;
    let item_scope = read_exact_slice(bytes, &mut cursor, item_scope_len)?.to_vec();

    let items = if flags & FLAG_EMBEDDED != 0 {
        let count = usize::try_from(read_u32(bytes, &mut cursor)?).map_err(|_| {
            StorageError::internal("compact stream pointer embedded item count failed")
        })?;
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            let data_type = stream_data_type_from_byte(read_u8(bytes, &mut cursor)?)?;
            let data_len = usize::try_from(read_u32(bytes, &mut cursor)?).map_err(|_| {
                StorageError::internal("compact stream pointer embedded item length failed")
            })?;
            let data = read_exact_slice(bytes, &mut cursor, data_len)?.to_vec();
            items.push(EmbeddedStreamItem { data, data_type });
        }
        Some(items)
    } else {
        None
    };

    let replication = if flags & FLAG_REPLICATION != 0 {
        let len = usize::try_from(read_u32(bytes, &mut cursor)?).map_err(|_| {
            StorageError::internal("compact stream pointer replication length failed")
        })?;
        let bytes = read_exact_slice(bytes, &mut cursor, len)?;
        Some(storage_types::storage_serde::from_bytes(bytes)?)
    } else {
        None
    };

    if cursor != bytes.len() {
        return Err(StorageError::internal(
            "compact stream pointer decode failed: trailing bytes",
        ));
    }

    Ok(CompactStoredStreamPointer {
        table_id,
        item_scope,
        item_stream_version,
        items,
        replication,
    })
}

pub(crate) fn item_stream_name(table_name: &TableName, item_scope: &[u8]) -> StreamName {
    let mut bytes =
        Vec::with_capacity(table_name.as_ref().len() + b"/stream-item/".len() + item_scope.len());
    bytes.extend_from_slice(table_name.as_ref().as_bytes());
    bytes.extend_from_slice(b"/stream-item/");
    bytes.extend_from_slice(item_scope);
    StreamName::new(&bytes)
}

fn append_len_prefixed(output: &mut Vec<u8>, value: &[u8]) -> StorageResult<()> {
    let len = u32::try_from(value.len())
        .map_err(|_| StorageError::internal("compact stream pointer value is too large"))?;
    output.extend_from_slice(&len.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn read_exact_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> StorageResult<[u8; N]> {
    let slice = read_exact_slice(bytes, cursor, N)?;
    let mut array = [0u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn read_exact_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> StorageResult<&'a [u8]> {
    let end = cursor.saturating_add(len);
    if end > bytes.len() {
        return Err(StorageError::internal(
            "compact stream pointer decode failed: truncated bytes",
        ));
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> StorageResult<u8> {
    Ok(read_exact_slice(bytes, cursor, 1)?[0])
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> StorageResult<u32> {
    Ok(u32::from_be_bytes(read_exact_array(bytes, cursor)?))
}

fn stream_data_type_to_byte(data_type: StreamDataType) -> u8 {
    match data_type {
        StreamDataType::Binary => 0,
        StreamDataType::Json => 1,
        StreamDataType::Text => 2,
        StreamDataType::DynamoDbJson => 3,
        StreamDataType::DeleteMarker => 4,
        StreamDataType::StreamPointer => 5,
    }
}

fn stream_data_type_from_byte(value: u8) -> StorageResult<StreamDataType> {
    match value {
        0 => Ok(StreamDataType::Binary),
        1 => Ok(StreamDataType::Json),
        2 => Ok(StreamDataType::Text),
        3 => Ok(StreamDataType::DynamoDbJson),
        4 => Ok(StreamDataType::DeleteMarker),
        5 => Ok(StreamDataType::StreamPointer),
        _ => Err(StorageError::internal(
            "compact stream pointer decode failed: invalid embedded item data type",
        )),
    }
}
