use std::{borrow::Cow, collections::HashMap};

use storage_types::{
    AttributeValue, DeleteRequest, EncodeWriteRequest, PutRequest, StorageError, StorageResult,
    WireItem, WriteRequest, attribute_map_numbers_need_write_normalization,
    normalize_attribute_map_numbers_for_write,
};

pub(crate) fn normalized_attribute_map_for_write(
    item: &HashMap<String, AttributeValue>,
) -> Cow<'_, HashMap<String, AttributeValue>> {
    if !attribute_map_numbers_need_write_normalization(item) {
        return Cow::Borrowed(item);
    }

    let mut normalized = item.clone();
    normalize_attribute_map_numbers_for_write(&mut normalized);
    Cow::Owned(normalized)
}

pub(crate) fn normalized_wire_item_for_write(item: &WireItem) -> StorageResult<Cow<'_, WireItem>> {
    if let WireItem::DynamoJson { data } = item
        && !data.iter().any(|byte| matches!(byte, b'e' | b'E'))
    {
        return Ok(Cow::Borrowed(item));
    }

    let mut attributes = item.to_attribute_map()?;
    if !normalize_attribute_map_numbers_for_write(&mut attributes) {
        return Ok(Cow::Borrowed(item));
    }

    Ok(Cow::Owned(WireItem::from_attribute_map(&attributes)?))
}

pub(crate) fn encode_requests_to_write_requests(
    requests: &[EncodeWriteRequest],
) -> StorageResult<Vec<WriteRequest>> {
    requests
        .iter()
        .map(|request| match request {
            EncodeWriteRequest {
                put_request: Some(put_request),
                delete_request: None,
            } => Ok(WriteRequest {
                put_request: Some(PutRequest {
                    item: put_request.item.clone().into_attribute_map()?,
                    aux_item_stream_ttl_hours: put_request.aux_item_stream_ttl_hours,
                }),
                delete_request: None,
            }),
            EncodeWriteRequest {
                put_request: None,
                delete_request:
                    Some(DeleteRequest {
                        key,
                        aux_item_stream_ttl_hours,
                    }),
            } => Ok(WriteRequest {
                put_request: None,
                delete_request: Some(DeleteRequest {
                    key: key.clone(),
                    aux_item_stream_ttl_hours: *aux_item_stream_ttl_hours,
                }),
            }),
            _ => Err(StorageError::validation(
                "Each WriteRequest must contain exactly one of PutRequest or DeleteRequest",
            )),
        })
        .collect()
}

pub(crate) fn encode_wire_item_storage_bytes(item: &WireItem) -> StorageResult<Vec<u8>> {
    match item {
        WireItem::DynamoJson { data } => {
            Ok(storage_types::storage_serde::compress_json_bytes(data))
        }
        WireItem::LocalSplit { .. } => {
            let map = item.to_attribute_map()?;
            storage_types::storage_serde::to_bytes(&map)
        }
    }
}

pub(crate) fn decode_wire_item_from_storage_bytes(bytes: &[u8]) -> StorageResult<WireItem> {
    let json = storage_types::storage_serde::decompress_bytes(bytes)?;
    Ok(WireItem::dynamo_json(json))
}
