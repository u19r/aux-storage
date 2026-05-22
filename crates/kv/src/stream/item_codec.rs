use storage_types::{StorageError, StorageResult, StreamItemId, StreamName, TimestampMillis};
use stream_provider::{StreamDataType, StreamItem};

use crate::stream::constants::{STREAM_ITEM_FLAG_STREAM_NAME_PRESENT, STREAM_ITEM_FORMAT_MAGIC};

#[derive(Debug, Clone)]
pub(crate) struct StoredStreamItem {
    pub(crate) stream_name: Option<StreamName>,
    pub(crate) data: Vec<u8>,
    pub(crate) data_type: StreamDataType,
    pub(crate) created_at: TimestampMillis,
}

impl StoredStreamItem {
    #[must_use]
    pub(crate) fn into_stream_item(self, id: StreamItemId) -> StreamItem {
        StreamItem {
            id,
            stream_name: self.stream_name,
            data: self.data,
            data_type: self.data_type,
            created_at: self.created_at,
        }
    }
}

pub(crate) fn encode_stored_stream_item_parts(
    stream_name: Option<&StreamName>,
    data: &[u8],
    data_type: StreamDataType,
    created_at: TimestampMillis,
) -> StorageResult<Vec<u8>> {
    let stream_name_bytes = stream_name.map(|name| {
        let bytes: Vec<u8> = name.into();
        bytes
    });
    let stream_name_len = stream_name_bytes
        .as_ref()
        .map_or(0usize, std::vec::Vec::len);
    let stream_name_len_u32 = u32::try_from(stream_name_len)
        .map_err(|_| StorageError::internal("stream item stream_name length exceeds u32::MAX"))?;
    let data_len_u32 = u32::try_from(data.len())
        .map_err(|_| StorageError::internal("stream item data length exceeds u32::MAX"))?;

    let mut bytes = Vec::with_capacity(
        1 + 1
            + 1
            + std::mem::size_of::<i64>()
            + std::mem::size_of::<u32>()
            + stream_name_len
            + std::mem::size_of::<u32>()
            + data.len(),
    );
    bytes.push(STREAM_ITEM_FORMAT_MAGIC);
    bytes.push(stream_data_type_to_byte(data_type));
    bytes.push(u8::from(stream_name.is_some()));
    bytes.extend_from_slice(&(*created_at).to_le_bytes());
    bytes.extend_from_slice(&stream_name_len_u32.to_le_bytes());
    if let Some(name) = stream_name_bytes {
        bytes.extend_from_slice(&name);
    }
    bytes.extend_from_slice(&data_len_u32.to_le_bytes());
    bytes.extend_from_slice(data);
    Ok(bytes)
}

pub(crate) fn encode_stream_item(item: &StreamItem) -> StorageResult<Vec<u8>> {
    encode_stored_stream_item_parts(
        item.stream_name.as_ref(),
        item.data.as_slice(),
        item.data_type,
        item.created_at,
    )
}

pub(crate) fn decode_stream_item(bytes: &[u8]) -> StorageResult<StoredStreamItem> {
    let mut cursor = 0usize;

    let magic = read_u8(bytes, &mut cursor)?;
    if magic != STREAM_ITEM_FORMAT_MAGIC {
        return Err(StorageError::internal(
            "stored stream row decode failed: unexpected format marker",
        ));
    }

    let data_type = stream_data_type_from_byte(read_u8(bytes, &mut cursor)?)?;
    let stream_name_present = read_u8(bytes, &mut cursor)?;
    if stream_name_present > STREAM_ITEM_FLAG_STREAM_NAME_PRESENT {
        return Err(StorageError::internal(
            "stored stream row decode failed: invalid stream_name flag",
        ));
    }

    let created_at = TimestampMillis::from(read_i64(bytes, &mut cursor)?);

    let stream_name_len_u32 = read_u32(bytes, &mut cursor)?;
    let stream_name_len = usize::try_from(stream_name_len_u32)
        .map_err(|_| StorageError::internal("stream_name length conversion failed"))?;
    let stream_name = if stream_name_present == STREAM_ITEM_FLAG_STREAM_NAME_PRESENT {
        let stream_name_bytes = read_exact_slice(bytes, &mut cursor, stream_name_len)?;
        Some(StreamName::new(stream_name_bytes))
    } else {
        if stream_name_len != 0 {
            return Err(StorageError::internal(
                "stored stream row decode failed: stream_name length must be zero when absent",
            ));
        }
        None
    };

    let data_len_u32 = read_u32(bytes, &mut cursor)?;
    let data_len = usize::try_from(data_len_u32)
        .map_err(|_| StorageError::internal("data length conversion failed"))?;
    let data = read_exact_slice(bytes, &mut cursor, data_len)?.to_vec();

    if cursor != bytes.len() {
        return Err(StorageError::internal(
            "stored stream row decode failed: trailing bytes",
        ));
    }

    Ok(StoredStreamItem {
        stream_name,
        data,
        data_type,
        created_at,
    })
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
            "invalid stream item data type in stored stream row",
        )),
    }
}

fn read_exact_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> StorageResult<&'a [u8]> {
    let end = cursor.saturating_add(len);
    if end > bytes.len() {
        return Err(StorageError::internal(
            "stored stream row decode failed: truncated data",
        ));
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> StorageResult<u8> {
    let slice = read_exact_slice(bytes, cursor, 1)?;
    Ok(slice[0])
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> StorageResult<u32> {
    let slice = read_exact_slice(bytes, cursor, std::mem::size_of::<u32>())?;
    let mut arr = [0_u8; 4];
    arr.copy_from_slice(slice);
    Ok(u32::from_le_bytes(arr))
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> StorageResult<i64> {
    let slice = read_exact_slice(bytes, cursor, std::mem::size_of::<i64>())?;
    let mut arr = [0_u8; 8];
    arr.copy_from_slice(slice);
    Ok(i64::from_le_bytes(arr))
}
