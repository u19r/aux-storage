use std::{borrow::Cow, collections::HashMap};

use storage_types::{
    AttributeValue, DeleteRequest, EncodeWriteRequest, IndexedWireItem, IndexerDeclaration,
    MaxIndexers, PutRequest, StorageError, StorageResult, WireItem, WriteRequest,
    attribute_map_numbers_need_write_normalization, normalize_attribute_map_numbers_for_write,
};

use crate::sorted_kv_store::ItemValueCodec;

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
                    item: put_request.item.item().to_attribute_map()?,
                    indexers: put_request
                        .item
                        .indexer_names()
                        .map(std::borrow::Cow::into_owned),
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

pub(crate) fn encode_wire_item_storage_bytes(
    codec: ItemValueCodec,
    item: &WireItem,
    indexers: Option<&[String]>,
    capacity: MaxIndexers,
) -> StorageResult<Vec<u8>> {
    let declaration = IndexerDeclaration::try_new(indexers.unwrap_or_default().to_vec(), capacity)?;
    let logical = item.to_attribute_map()?;
    let indexed = IndexedWireItem::extract(&logical, &declaration)?;
    encode_indexed_wire_item(codec, &indexed)
}

pub(crate) fn decode_wire_item_from_storage_bytes(
    codec: ItemValueCodec,
    bytes: &[u8],
    capacity: MaxIndexers,
) -> StorageResult<WireItem> {
    decode_wire_item_with_indexers_from_storage_bytes(codec, bytes, capacity).map(|(item, _)| item)
}

pub(crate) fn decode_wire_item_with_indexers_from_storage_bytes(
    codec: ItemValueCodec,
    bytes: &[u8],
    capacity: MaxIndexers,
) -> StorageResult<(WireItem, Vec<String>)> {
    let indexed = decode_indexed_wire_item(codec, bytes)?;
    if indexed.slots().len() > capacity.as_usize() {
        return Err(StorageError::internal(
            "stored_item_corruption:declaration_exceeds_table_capacity",
        ));
    }
    let (item, declaration) = indexed.into_attribute_map_with_declaration()?;
    Ok((
        WireItem::from_attribute_map(&item)?,
        declaration.into_names(),
    ))
}

pub(crate) fn encode_indexed_wire_item(
    codec: ItemValueCodec,
    item: &IndexedWireItem,
) -> StorageResult<Vec<u8>> {
    match codec {
        ItemValueCodec::RocksDbEnvelope => item.encode_envelope(),
        #[cfg(feature = "foundationdb-backend")]
        ItemValueCodec::FoundationDbTuple => encode_foundationdb_tuple(item),
    }
}

pub(crate) fn decode_indexed_wire_item(
    codec: ItemValueCodec,
    bytes: &[u8],
) -> StorageResult<IndexedWireItem> {
    match codec {
        ItemValueCodec::RocksDbEnvelope => IndexedWireItem::decode_envelope(bytes),
        #[cfg(feature = "foundationdb-backend")]
        ItemValueCodec::FoundationDbTuple => decode_foundationdb_tuple(bytes),
    }
}

#[cfg(feature = "foundationdb-backend")]
fn encode_foundationdb_tuple(item: &IndexedWireItem) -> StorageResult<Vec<u8>> {
    use foundationdb::tuple::{Bytes, Element, pack};
    use storage_types::{INDEXED_VALUE_LZ4_HEADER, INDEXED_VALUE_RAW_HEADER};

    fn pack_candidate(item: &IndexedWireItem, header: u8, payload: &[u8]) -> Vec<u8> {
        let header = [header];
        let mut elements = Vec::with_capacity(2 + item.slots().len());
        elements.push(Element::Bytes(Bytes::from(header.as_slice())));
        elements.push(Element::Bytes(Bytes::from(payload)));
        elements.extend(item.slots().iter().map(|slot| match slot {
            Some(value) => Element::Bytes(Bytes::from(value.as_bytes())),
            None => Element::Nil,
        }));
        pack(&elements)
    }

    let compressed = item.compressed_residual_json();
    Ok(
        if foundationdb_compressed_is_smaller(item.residual_json(), &compressed) {
            pack_candidate(item, INDEXED_VALUE_LZ4_HEADER, &compressed)
        } else {
            pack_candidate(item, INDEXED_VALUE_RAW_HEADER, item.residual_json())
        },
    )
}

#[cfg(feature = "foundationdb-backend")]
fn foundationdb_bytes_element_len(bytes: &[u8]) -> usize {
    bytes.len() + bytes.iter().filter(|&&byte| byte == 0).count()
}

#[cfg(feature = "foundationdb-backend")]
pub(super) fn foundationdb_compressed_is_smaller(raw: &[u8], compressed: &[u8]) -> bool {
    foundationdb_bytes_element_len(compressed) < foundationdb_bytes_element_len(raw)
}

#[cfg(feature = "foundationdb-backend")]
fn decode_foundationdb_tuple(bytes: &[u8]) -> StorageResult<IndexedWireItem> {
    use foundationdb::tuple::{Element, unpack};

    let elements = unpack::<Vec<Element<'_>>>(bytes)
        .map_err(|_| StorageError::internal("stored_item_corruption:tuple"))?;
    let (header, payload) = match (elements.first(), elements.get(1)) {
        (Some(Element::Bytes(header)), Some(Element::Bytes(payload))) if header.len() == 1 => {
            (header[0], payload.as_ref())
        }
        _ => {
            return Err(StorageError::internal(
                "stored_item_corruption:tuple_header_payload",
            ));
        }
    };
    let mut slots = Vec::with_capacity(elements.len().saturating_sub(2));
    for element in elements.iter().skip(2) {
        match element {
            Element::Nil => slots.push(None),
            Element::Bytes(value) => {
                let value = std::str::from_utf8(value.as_ref())
                    .map_err(|_| StorageError::internal("stored_item_corruption:slot_utf8"))?;
                slots.push(Some(value.to_owned()));
            }
            _ => return Err(StorageError::internal("stored_item_corruption:slot_type")),
        }
    }
    IndexedWireItem::from_encoded_parts(header, payload, slots)
}
