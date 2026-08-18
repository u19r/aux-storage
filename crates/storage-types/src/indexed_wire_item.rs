use std::collections::HashMap;

use serde::ser::SerializeMap as _;

use crate::{
    AttributeValue, MAX_INDEXERS_CAPACITY, MaxIndexers, StorageError, StorageResult, WireItem,
};

pub const INDEXED_VALUE_FORMAT_VERSION: u8 = 1;
pub const INDEXED_VALUE_RAW_CODEC: u8 = 0;
pub const INDEXED_VALUE_LZ4_CODEC: u8 = 1;
pub const INDEXED_VALUE_RAW_HEADER: u8 = 0x10;
pub const INDEXED_VALUE_LZ4_HEADER: u8 = 0x11;
pub const INDEXER_TUPLE_OFFSET: usize = 2;

#[must_use]
pub const fn indexer_tuple_index(ordinal: usize) -> usize {
    INDEXER_TUPLE_OFFSET + ordinal
}

const ENVELOPE_SLOT_NIL: u8 = 0;
const ENVELOPE_SLOT_STRING: u8 = 1;
const ENVELOPE_FIXED_BYTES: usize = 1 + size_of::<u32>() + size_of::<u8>();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerDeclaration(Vec<String>);

impl IndexerDeclaration {
    pub fn try_new(names: Vec<String>, capacity: MaxIndexers) -> StorageResult<Self> {
        Self::validate(&names, capacity)?;
        Ok(Self(names))
    }

    pub fn validate(names: &[String], capacity: MaxIndexers) -> StorageResult<()> {
        if names.len() > capacity.as_usize() {
            return Err(StorageError::validation("Indexers:too_many"));
        }
        for (index, name) in names.iter().enumerate() {
            if name.is_empty() {
                return Err(StorageError::validation("Indexers:empty_attribute"));
            }
            if names[..index].iter().any(|seen| seen == name) {
                return Err(StorageError::validation("Indexers:duplicate_attribute"));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.0
    }

    #[must_use]
    pub fn into_names(self) -> Vec<String> {
        self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedWireItem {
    residual_json: Vec<u8>,
    slots: Vec<Option<String>>,
}

#[derive(Debug)]
pub struct DecodedIndexedWireItem {
    pub item: HashMap<String, AttributeValue>,
    pub declaration: IndexerDeclaration,
    pub slots: Vec<Option<String>>,
}

impl IndexedWireItem {
    pub fn extract(
        item: &HashMap<String, AttributeValue>,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<Self> {
        Self::extract_projected(item, item, declaration)
    }

    pub fn extract_projected(
        logical_item: &HashMap<String, AttributeValue>,
        projected_item: &HashMap<String, AttributeValue>,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<Self> {
        let mut slots = Vec::with_capacity(declaration.len());
        for name in declaration.names() {
            let slot = match logical_item.get(name) {
                Some(AttributeValue::S(value)) if !value.is_empty() => Some(value.clone()),
                Some(AttributeValue::S(_)) => {
                    return Err(StorageError::validation("Indexers:empty_string"));
                }
                Some(_) => {
                    return Err(StorageError::validation(
                        "Indexers:attribute_must_be_string",
                    ));
                }
                None => None,
            };
            slots.push(slot);
        }

        let residual_json = serde_json::to_vec(&ProjectedResidual {
            projected_item,
            declaration,
        })
        .map_err(encode_error)?;
        Ok(Self {
            residual_json,
            slots,
        })
    }

    pub fn validate_logical_item(
        item: &HashMap<String, AttributeValue>,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<()> {
        Self::validate_logical_item_names(item, declaration.names())
    }

    pub fn validate_logical_item_names(
        item: &HashMap<String, AttributeValue>,
        names: &[String],
    ) -> StorageResult<()> {
        for name in names {
            match item.get(name) {
                Some(AttributeValue::S(value)) if value.is_empty() => {
                    return Err(StorageError::validation("Indexers:empty_string"));
                }
                Some(AttributeValue::S(_)) | None => {}
                Some(_) => {
                    return Err(StorageError::validation(
                        "Indexers:attribute_must_be_string",
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn from_parts(residual_json: Vec<u8>, slots: Vec<Option<String>>) -> StorageResult<Self> {
        if slots.len() > usize::from(MAX_INDEXERS_CAPACITY) {
            return Err(corruption("slot_count"));
        }
        let item = Self {
            residual_json,
            slots,
        };
        item.validate_markers()?;
        Ok(item)
    }

    pub fn decode_padded_parts(
        residual_json: Vec<u8>,
        mut slots: Vec<Option<String>>,
    ) -> StorageResult<DecodedIndexedWireItem> {
        let mut residual =
            serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&residual_json)
                .map_err(|_| corruption("residual_json"))?;
        let declaration_len = residual
            .values()
            .filter(
                |value| matches!(value, serde_json::Value::Object(fields) if fields.contains_key("I")),
            )
            .count();
        if declaration_len > slots.len() {
            return Err(corruption("sql_slot_count"));
        }
        slots.truncate(declaration_len);
        let item = Self {
            residual_json,
            slots,
        };
        let markers = item.restore_residual(&mut residual)?;
        let declaration = item.declaration_from_markers(markers);
        Ok(DecodedIndexedWireItem {
            item: decode_logical_residual(residual)?,
            declaration,
            slots: item.slots,
        })
    }

    #[must_use]
    pub fn residual_json(&self) -> &[u8] {
        &self.residual_json
    }

    #[must_use]
    pub fn slots(&self) -> &[Option<String>] {
        &self.slots
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<u8>, Vec<Option<String>>) {
        (self.residual_json, self.slots)
    }

    #[must_use]
    pub fn compressed_residual_json(&self) -> Vec<u8> {
        lz4_flex::compress_prepend_size(&self.residual_json)
    }

    pub fn from_encoded_parts(
        header: u8,
        payload: &[u8],
        slots: Vec<Option<String>>,
    ) -> StorageResult<Self> {
        let residual_json = decode_residual(validate_header(header)?, payload)?;
        Self::from_parts(residual_json, slots)
    }

    pub fn to_attribute_map(&self) -> StorageResult<HashMap<String, AttributeValue>> {
        let mut residual = self.parse_residual()?;
        self.restore_residual(&mut residual)?;
        decode_logical_residual(residual)
    }

    pub fn into_attribute_map_with_declaration(
        self,
    ) -> StorageResult<(HashMap<String, AttributeValue>, IndexerDeclaration)> {
        let mut residual = self.parse_residual()?;
        let markers = self.restore_residual(&mut residual)?;
        Ok((
            decode_logical_residual(residual)?,
            self.declaration_from_markers(markers),
        ))
    }

    pub fn into_wire_item(self) -> StorageResult<WireItem> {
        WireItem::from_attribute_map(&self.to_attribute_map()?)
    }

    pub fn encode_envelope(&self) -> StorageResult<Vec<u8>> {
        let compressed = self.compressed_residual_json();
        if compressed.len() < self.residual_json.len() {
            self.encode_envelope_payload(INDEXED_VALUE_LZ4_HEADER, &compressed)
        } else {
            self.encode_envelope_payload(INDEXED_VALUE_RAW_HEADER, &self.residual_json)
        }
    }

    pub fn decode_envelope(bytes: &[u8]) -> StorageResult<Self> {
        let (&header, mut remaining) = bytes
            .split_first()
            .ok_or_else(|| corruption("missing_header"))?;
        let codec = validate_header(header)?;
        let residual_len = take_u32(&mut remaining)? as usize;
        let encoded_residual = take_bytes(&mut remaining, residual_len)?;
        let residual_json = decode_residual(codec, encoded_residual)?;
        let slot_count = *take_bytes(&mut remaining, 1)?
            .first()
            .ok_or_else(|| corruption("missing_slot_count"))? as usize;
        if slot_count > usize::from(MAX_INDEXERS_CAPACITY) {
            return Err(corruption("slot_count"));
        }
        let mut slots = Vec::with_capacity(slot_count);
        for _ in 0..slot_count {
            let tag = take_bytes(&mut remaining, 1)?[0];
            match tag {
                ENVELOPE_SLOT_NIL => slots.push(None),
                ENVELOPE_SLOT_STRING => {
                    let len = take_u32(&mut remaining)? as usize;
                    let value = std::str::from_utf8(take_bytes(&mut remaining, len)?)
                        .map_err(|_| corruption("slot_utf8"))?;
                    slots.push(Some(value.to_owned()));
                }
                _ => return Err(corruption("slot_tag")),
            }
        }
        if !remaining.is_empty() {
            return Err(corruption("trailing_envelope_bytes"));
        }
        Self::from_parts(residual_json, slots)
    }

    fn encode_envelope_payload(&self, header: u8, payload: &[u8]) -> StorageResult<Vec<u8>> {
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| StorageError::validation("item payload exceeds stored format"))?;
        let mut capacity = ENVELOPE_FIXED_BYTES + payload.len();
        for slot in &self.slots {
            capacity += 1 + slot
                .as_ref()
                .map_or(0, |value| size_of::<u32>() + value.len());
        }
        let mut bytes = Vec::with_capacity(capacity);
        bytes.push(header);
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes.push(
            u8::try_from(self.slots.len())
                .map_err(|_| StorageError::validation("Indexers:too_many"))?,
        );
        for slot in &self.slots {
            match slot {
                None => bytes.push(ENVELOPE_SLOT_NIL),
                Some(value) => {
                    let len = u32::try_from(value.len())
                        .map_err(|_| StorageError::validation("indexed string is too large"))?;
                    bytes.push(ENVELOPE_SLOT_STRING);
                    bytes.extend_from_slice(&len.to_be_bytes());
                    bytes.extend_from_slice(value.as_bytes());
                }
            }
        }
        Ok(bytes)
    }

    fn validate_markers(&self) -> StorageResult<()> {
        self.parse_markers().map(|_| ())
    }

    fn restore_residual(
        &self,
        residual: &mut serde_json::Map<String, serde_json::Value>,
    ) -> StorageResult<Vec<(String, usize)>> {
        let markers = self.parse_markers_from(residual)?;
        for (name, tuple_index) in &markers {
            match &self.slots[tuple_index - INDEXER_TUPLE_OFFSET] {
                Some(value) => {
                    residual.insert(name.clone(), serde_json::json!({"S": value}));
                }
                None => {
                    residual.remove(name);
                }
            }
        }
        Ok(markers)
    }

    fn parse_markers(&self) -> StorageResult<Vec<(String, usize)>> {
        let residual = self.parse_residual()?;
        self.parse_markers_from(&residual)
    }

    fn parse_markers_from(
        &self,
        residual: &serde_json::Map<String, serde_json::Value>,
    ) -> StorageResult<Vec<(String, usize)>> {
        let mut markers = Vec::with_capacity(self.slots.len());
        let mut indexes = 0_u64;
        for (name, value) in residual {
            let Some(tuple_index) = parse_marker(value)? else {
                continue;
            };
            if name.is_empty() {
                return Err(corruption("empty_marker_name"));
            }
            if tuple_index < INDEXER_TUPLE_OFFSET
                || tuple_index >= INDEXER_TUPLE_OFFSET + self.slots.len()
            {
                return Err(corruption("marker_out_of_range"));
            }
            let bit = 1_u64 << (tuple_index - INDEXER_TUPLE_OFFSET);
            if indexes & bit != 0 {
                return Err(corruption("duplicate_marker"));
            }
            indexes |= bit;
            markers.push((name.clone(), tuple_index));
        }
        if markers.len() != self.slots.len() {
            return Err(corruption("marker_slot_count"));
        }
        let expected = if self.slots.is_empty() {
            0
        } else {
            (1_u64 << self.slots.len()) - 1
        };
        if indexes != expected {
            return Err(corruption("gapped_markers"));
        }
        Ok(markers)
    }

    fn declaration_from_markers(&self, markers: Vec<(String, usize)>) -> IndexerDeclaration {
        let mut names = vec![String::new(); self.slots.len()];
        for (name, tuple_index) in markers {
            names[tuple_index - INDEXER_TUPLE_OFFSET] = name;
        }
        IndexerDeclaration(names)
    }

    fn parse_residual(&self) -> StorageResult<serde_json::Map<String, serde_json::Value>> {
        serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&self.residual_json)
            .map_err(|_| corruption("residual_json"))
    }
}

fn validate_header(header: u8) -> StorageResult<u8> {
    if header >> 4 != INDEXED_VALUE_FORMAT_VERSION {
        return Err(corruption("format_version"));
    }
    match header & 0x0f {
        INDEXED_VALUE_RAW_CODEC | INDEXED_VALUE_LZ4_CODEC => Ok(header & 0x0f),
        _ => Err(corruption("payload_codec")),
    }
}

fn decode_residual(codec: u8, payload: &[u8]) -> StorageResult<Vec<u8>> {
    match codec {
        INDEXED_VALUE_RAW_CODEC => Ok(payload.to_vec()),
        INDEXED_VALUE_LZ4_CODEC => {
            lz4_flex::decompress_size_prepended(payload).map_err(|_| corruption("payload_lz4"))
        }
        _ => unreachable!("header codec was validated"),
    }
}

fn take_u32(bytes: &mut &[u8]) -> StorageResult<u32> {
    let raw: [u8; 4] = take_bytes(bytes, 4)?
        .try_into()
        .map_err(|_| corruption("length"))?;
    Ok(u32::from_be_bytes(raw))
}

fn take_bytes<'a>(bytes: &mut &'a [u8], len: usize) -> StorageResult<&'a [u8]> {
    if bytes.len() < len {
        return Err(corruption("truncated_envelope"));
    }
    let (taken, remaining) = bytes.split_at(len);
    *bytes = remaining;
    Ok(taken)
}

struct ProjectedResidual<'a> {
    projected_item: &'a HashMap<String, AttributeValue>,
    declaration: &'a IndexerDeclaration,
}

impl serde::Serialize for ProjectedResidual<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map =
            serializer.serialize_map(Some(self.projected_item.len() + self.declaration.len()))?;
        for (name, value) in self.projected_item {
            if !self
                .declaration
                .names()
                .iter()
                .any(|indexed| indexed == name)
            {
                map.serialize_entry(name, value)?;
            }
        }
        for (ordinal, name) in self.declaration.names().iter().enumerate() {
            map.serialize_entry(
                name,
                &IndexerMarker {
                    tuple_index: indexer_tuple_index(ordinal),
                },
            )?;
        }
        map.end()
    }
}

#[derive(serde::Serialize)]
struct IndexerMarker {
    #[serde(rename = "I")]
    tuple_index: usize,
}

fn parse_marker(value: &serde_json::Value) -> StorageResult<Option<usize>> {
    let serde_json::Value::Object(fields) = value else {
        return Ok(None);
    };
    let Some(index) = fields.get("I") else {
        return Ok(None);
    };
    if fields.len() != 1 {
        return Err(corruption("marker_shape"));
    }
    let index = index.as_u64().ok_or_else(|| corruption("marker_index"))?;
    usize::try_from(index)
        .map(Some)
        .map_err(|_| corruption("marker_index"))
}

fn encode_error(error: serde_json::Error) -> StorageError {
    StorageError::internal(&format!("indexed item encode failed: {error}"))
}

fn decode_logical_residual(
    residual: serde_json::Map<String, serde_json::Value>,
) -> StorageResult<HashMap<String, AttributeValue>> {
    serde_json::from_value(serde_json::Value::Object(residual))
        .map_err(|_| corruption("residual_attribute_value"))
}

fn corruption(invariant: &str) -> StorageError {
    StorageError::internal(&format!("stored_item_corruption:{invariant}"))
}
