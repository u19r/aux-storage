use serde::de::{DeserializeSeed, Error as _, IgnoredAny, MapAccess, Visitor};

use crate::{
    AttributeDefinition, AttributeMap, AttributeValue, AttributeValueLookup, IndexerDeclaration,
    KeyAttributeType, KeyAttributes, KeySchemaElement, MaxIndexers, StorageError, StorageResult,
    StorageValidationKind, StoredTableInfo, StoredWireItem, WireItem,
    constants::{
        ENVELOPE_FIXED_BYTES, ENVELOPE_SLOT_NIL, ENVELOPE_SLOT_STRING,
        INDEXED_VALUE_FORMAT_VERSION, INDEXED_VALUE_LZ4_CODEC, INDEXED_VALUE_LZ4_HEADER,
        INDEXED_VALUE_RAW_CODEC, INDEXED_VALUE_RAW_HEADER, INDEXER_TUPLE_OFFSET,
        MAX_INDEXERS_CAPACITY,
    },
    indexer_declaration::IndexerDeclarationBytes,
    verify_key_attributes,
};

#[must_use]
pub const fn indexer_tuple_index(ordinal: usize) -> usize {
    INDEXER_TUPLE_OFFSET + ordinal
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedWireItem {
    residual_json: Vec<u8>,
    slots: Vec<Option<String>>,
}

#[derive(Debug)]
pub struct DecodedIndexedWireItem {
    pub item: AttributeMap,
    pub declaration: IndexerDeclaration,
    pub slots: Vec<Option<String>>,
}

impl IndexedWireItem {
    pub fn extract(item: &AttributeMap, declaration: &IndexerDeclaration) -> StorageResult<Self> {
        Self::extract_projected(item, item, declaration)
    }

    pub fn extract_projected(
        logical_item: &AttributeMap,
        projected_item: &AttributeMap,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<Self> {
        let slots = declaration
            .names()
            .iter()
            .map(|name| extract_indexer_slot(logical_item.get(name).cloned()))
            .collect::<StorageResult<Vec<_>>>()?;

        let residual_json = crate::wire_item::residual::encode_residual(
            projected_item
                .iter()
                .map(|(name, value)| (std::borrow::Cow::Borrowed(name), value)),
            declaration,
        )?;
        Ok(Self {
            residual_json,
            slots,
        })
    }

    pub fn extract_wire_items(
        logical_item: &WireItem,
        projected_item: &WireItem,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<Self> {
        let fields = declaration
            .names()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let values = logical_item.select_attribute_values(&fields)?;
        let slots = values
            .into_iter()
            .map(extract_indexer_slot)
            .collect::<StorageResult<Vec<_>>>()?;
        let residual_json =
            crate::wire_item::residual::encode_indexed_residual(projected_item, declaration)?;
        Ok(Self {
            residual_json,
            slots,
        })
    }

    pub fn validate_logical_item(
        item: &impl AttributeValueLookup,
        declaration: &IndexerDeclaration,
    ) -> StorageResult<()> {
        Self::validate_logical_item_names(item, declaration.names())
    }

    pub fn validate_logical_item_names(
        item: &impl AttributeValueLookup,
        names: &[String],
    ) -> StorageResult<()> {
        for name in names {
            match item.get_attribute_value(name) {
                Some(AttributeValue::S(value)) if value.is_empty() => {
                    return Err(StorageError::validation(
                        StorageValidationKind::EmptyIndexer,
                    ));
                }
                Some(AttributeValue::S(_)) | None => {}
                Some(_) => {
                    return Err(StorageError::validation(
                        StorageValidationKind::InvalidIndexerType,
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
        let declaration = item.declaration_from_markers(markers)?;
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

    /// Decode the physical payload for a read path. Marker validation is
    /// intentionally deferred until the caller reconstructs the logical map,
    /// so a normal read does not parse the residual JSON twice.
    pub fn from_encoded_parts_for_read(
        header: u8,
        payload: &[u8],
        slots: Vec<Option<String>>,
    ) -> StorageResult<Self> {
        let residual_json = decode_residual(validate_header(header)?, payload)?;
        if slots.len() > usize::from(MAX_INDEXERS_CAPACITY) {
            return Err(corruption("slot_count"));
        }
        Ok(Self {
            residual_json,
            slots,
        })
    }

    pub fn to_attribute_map(&self) -> StorageResult<AttributeMap> {
        let mut residual = self.parse_residual()?;
        self.restore_residual(&mut residual)?;
        decode_logical_residual(residual)
    }

    /// Resolve one logical attribute from the residual payload and indexer
    /// slots without reconstructing the complete item.
    pub fn resolve_attribute(&self, field: &str) -> StorageResult<Option<AttributeValue>> {
        let mut deserializer = serde_json::Deserializer::from_slice(&self.residual_json);
        let scan = IndexedAttributeSeed {
            field,
            slot_count: self.slots.len(),
        }
        .deserialize(&mut deserializer)
        .map_err(|_| corruption("residual_attribute_value"))?;
        scan.decode(field, &self.slots)
    }

    /// Verify an indexed image against its physical key and fill legacy
    /// missing key attributes. Complete matching envelopes stay untouched.
    pub fn with_verified_keys(self, key: &KeyAttributes) -> StorageResult<Self> {
        if verify_key_attributes(key, |name| self.resolve_attribute(name))? {
            return Ok(self);
        }

        let (item, declaration) = self.into_attribute_map_with_declaration()?;
        let item = item.with_verified_keys(key)?;
        Self::extract(&item, &declaration)
    }

    pub fn into_attribute_map_with_declaration(
        self,
    ) -> StorageResult<(AttributeMap, IndexerDeclaration)> {
        let mut residual = self.parse_residual()?;
        let markers = self.restore_residual(&mut residual)?;
        Ok((
            decode_logical_residual(residual)?,
            self.declaration_from_markers(markers)?,
        ))
    }

    pub fn into_wire_item(self) -> StorageResult<WireItem> {
        self.into_wire_item_with_declaration().map(|(item, _)| item)
    }

    pub fn into_wire_item_with_declaration(self) -> StorageResult<(WireItem, IndexerDeclaration)> {
        crate::wire_item::residual::restore_indexed_residual(self)
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
        Self::decode_envelope_impl(bytes, true)
    }

    /// Decode an envelope for a read path without eagerly parsing the JSON.
    /// `into_attribute_map_with_declaration` performs the same validation when
    /// an operation actually needs the logical attributes.
    pub fn decode_envelope_for_read(bytes: &[u8]) -> StorageResult<Self> {
        Self::decode_envelope_impl(bytes, false)
    }

    fn decode_envelope_impl(bytes: &[u8], validate_markers: bool) -> StorageResult<Self> {
        let (&header, mut remaining) = bytes
            .split_first()
            .ok_or_else(|| corruption("missing_header"))?;
        let codec = validate_header(header)?;
        let residual_len = take_u32(&mut remaining)? as usize;
        let encoded_residual = take_bytes(&mut remaining, residual_len)?;
        let residual_json = validate_markers
            .then(|| decode_residual(codec, encoded_residual))
            .transpose()?;
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
        if let Some(residual_json) = residual_json {
            Self::from_parts(residual_json, slots)
        } else {
            Self::from_encoded_parts_for_read(header, encoded_residual, slots)
        }
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
        let mut bytes = IndexerDeclarationBytes::default();
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
            bytes
                .include_name(name)
                .map_err(|_| corruption("indexer_declaration_bytes"))?;
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

    fn declaration_from_markers(
        &self,
        markers: Vec<(String, usize)>,
    ) -> StorageResult<IndexerDeclaration> {
        let mut names = vec![String::new(); self.slots.len()];
        for (name, tuple_index) in markers {
            names[tuple_index - INDEXER_TUPLE_OFFSET] = name;
        }
        IndexerDeclaration::try_new(names, MaxIndexers::try_from(MAX_INDEXERS_CAPACITY)?)
            .map_err(|_| corruption("indexer_declaration"))
    }

    fn parse_residual(&self) -> StorageResult<serde_json::Map<String, serde_json::Value>> {
        serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&self.residual_json)
            .map_err(|_| corruption("residual_json"))
    }
}

struct IndexedAttributeSeed<'a> {
    field: &'a str,
    slot_count: usize,
}

impl<'de> DeserializeSeed<'de> for IndexedAttributeSeed<'_> {
    type Value = IndexedAttributeScan;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(IndexedAttributeVisitor {
            field: self.field,
            slot_count: self.slot_count,
        })
    }
}

struct IndexedAttributeVisitor<'a> {
    field: &'a str,
    slot_count: usize,
}

impl<'de> Visitor<'de> for IndexedAttributeVisitor<'_> {
    type Value = IndexedAttributeScan;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an indexed DynamoDB attribute map")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut scan = IndexedAttributeScan::default();
        while let Some(name) = map.next_key::<crate::wire_item::text::BorrowedText<'de>>()? {
            if name.0 == self.field {
                scan.selected = Some(map.next_value::<serde_json::Value>()?);
                continue;
            }
            if let Some(index) = map.next_value_seed(IndexerMarkerSeed)? {
                scan.record_marker(&name.0, index, self.slot_count)?;
            }
        }
        Ok(scan)
    }
}

#[derive(Default)]
pub(crate) struct IndexedAttributeScan {
    selected: Option<serde_json::Value>,
    marker_count: usize,
    marker_indexes: u64,
    declaration_bytes: IndexerDeclarationBytes,
}

impl IndexedAttributeScan {
    pub(crate) fn record_marker<E>(
        &mut self,
        name: &str,
        index: usize,
        slot_count: usize,
    ) -> Result<(), E>
    where
        E: serde::de::Error,
    {
        if name.is_empty() || index < INDEXER_TUPLE_OFFSET {
            return Err(E::custom("invalid indexed item marker"));
        }
        let ordinal = index - INDEXER_TUPLE_OFFSET;
        if ordinal >= slot_count || self.marker_indexes & (1_u64 << ordinal) != 0 {
            return Err(E::custom("invalid indexed item marker"));
        }
        self.declaration_bytes
            .include_name(name)
            .map_err(|_| E::custom("invalid indexed item declaration bytes"))?;
        self.marker_count += 1;
        self.marker_indexes |= 1_u64 << ordinal;
        Ok(())
    }

    fn decode(
        mut self,
        field: &str,
        slots: &[Option<String>],
    ) -> StorageResult<Option<AttributeValue>> {
        let selected = self.selected.take();
        let marker = selected.as_ref().map(parse_marker).transpose()?.flatten();
        if let Some(index) = marker {
            self.record_marker::<serde_json::Error>(field, index, slots.len())
                .map_err(|_| corruption("selected_marker"))?;
        }
        self.validate_marker_set(slots.len())?;
        match (selected, marker) {
            (Some(_), Some(index)) => Ok(slots[index - INDEXER_TUPLE_OFFSET]
                .as_ref()
                .map(|value| AttributeValue::S(value.clone()))),
            (Some(value), None) => serde_json::from_value(value)
                .map(Some)
                .map_err(|_| corruption("residual_attribute_value")),
            (None, None) => Ok(None),
            (None, Some(_)) => unreachable!(),
        }
    }

    pub(crate) fn validate_marker_set(&self, slot_count: usize) -> StorageResult<()> {
        let expected = if slot_count == 0 {
            0
        } else {
            (1_u64 << slot_count) - 1
        };
        if self.marker_count != slot_count || self.marker_indexes != expected {
            return Err(corruption("marker_slot_count"));
        }
        Ok(())
    }
}

pub(crate) struct IndexerMarkerSeed;

impl<'de> DeserializeSeed<'de> for IndexerMarkerSeed {
    type Value = Option<usize>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_map(IndexerMarkerVisitor)
    }
}

struct IndexerMarkerVisitor;

impl<'de> Visitor<'de> for IndexerMarkerVisitor {
    type Value = Option<usize>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a DynamoDB attribute or indexer marker")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where M: MapAccess<'de> {
        let mut marker = None;
        let mut fields = 0;
        while let Some(name) = map.next_key::<crate::wire_item::text::BorrowedText<'de>>()? {
            fields += 1;
            if name.0 == "I" {
                marker = Some(map.next_value::<usize>()?);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        if marker.is_some() && fields != 1 {
            return Err(M::Error::custom("invalid indexed item marker shape"));
        }
        Ok(marker)
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

#[derive(serde::Serialize)]
pub(crate) struct IndexerMarker {
    #[serde(rename = "I")]
    pub(crate) tuple_index: usize,
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

fn extract_indexer_slot(value: Option<AttributeValue>) -> StorageResult<Option<String>> {
    match value {
        Some(AttributeValue::S(value)) if value.is_empty() => Err(StorageError::validation(
            StorageValidationKind::EmptyIndexer,
        )),
        Some(AttributeValue::S(value)) => Ok(Some(value)),
        None => Ok(None),
        Some(_) => Err(StorageError::validation(
            StorageValidationKind::InvalidIndexerType,
        )),
    }
}

fn decode_logical_residual(
    residual: serde_json::Map<String, serde_json::Value>,
) -> StorageResult<AttributeMap> {
    serde_json::from_value(serde_json::Value::Object(residual))
        .map_err(|_| corruption("residual_attribute_value"))
}

fn corruption(invariant: &str) -> StorageError {
    StorageError::internal(&format!("stored_item_corruption:{invariant}"))
}

/// Backend-neutral view over a single SQL row.
///
/// Mirrors the existing `TursoRowView { columns, values }` shape: backends
/// expose named columns without leaking their native value types. The two
/// required methods keep type-specific decoding backend-local:
/// - `key_column_text` converts INTEGER/REAL/TEXT/BLOB to `String`, mapping
///   missing/NULL to `invalid_or_missing_key`. Each backend preserves its
///   frozen validation strings internally (sqlite maps every failure to
///   `invalid_or_missing_key`; postgres maps binary UTF-8 failures to `invalid
///   utf-8 in column '{column}'`).
/// - `optional_text` reads nullable payload columns (`attributes_blob`,
///   `__aux_indexer_*`). Missing-column handling stays backend-local (turso
///   returns `None`; sqlite/postgres map driver errors internally).
pub trait IndexedRowView {
    fn key_column_text(
        &self,
        attribute_name: &str,
        column_prefix: Option<&str>,
    ) -> StorageResult<String>;

    fn optional_text(&self, column: &str) -> StorageResult<Option<String>>;

    /// Decode a sortable number-key payload back to canonical decimal text.
    ///
    /// sqlite/turso store `encode_dynamodb_number_key` text and must decode;
    /// postgres stores native `NUMERIC` (`::TEXT` is already plain) and
    /// returns the input unchanged. Kept on the trait so the type-specific
    /// codec stays backend-local.
    fn decode_number_key(raw: &str) -> StorageResult<String>
    where Self: Sized {
        crate::decode_dynamodb_number_key(raw).map_err(StorageError::validation)
    }
}

fn indexer_column_name(ordinal: usize) -> String {
    format!("__aux_indexer_{ordinal}")
}

impl IndexedWireItem {
    /// Read typed key attributes for one key schema over a backend-neutral row.
    ///
    /// Missing attribute definitions default to `S`, matching the
    /// sqlite/turso decoders. The postgres decoder keeps its stricter
    /// `missing attribute definition for key '{name}'` check backend-local
    /// (pre-validated before calling shared code) so its frozen string is
    /// preserved.
    pub fn row_key_attributes<R: IndexedRowView>(
        row: &R,
        key_schema: &[KeySchemaElement],
        attribute_definitions: &[AttributeDefinition],
        column_prefix: Option<&str>,
    ) -> StorageResult<KeyAttributes> {
        let mut attributes = KeyAttributes::with_capacity(key_schema.len());
        for key in key_schema {
            let raw = row.key_column_text(&key.attribute_name, column_prefix)?;
            let key_type = attribute_definitions
                .iter()
                .find(|def| def.attribute_name == key.attribute_name)
                .map_or(KeyAttributeType::S, |def| def.attribute_type);
            let value = match key_type {
                KeyAttributeType::S => AttributeValue::S(raw),
                KeyAttributeType::N => AttributeValue::N(R::decode_number_key(&raw)?),
                KeyAttributeType::B => AttributeValue::B(raw),
            };
            attributes.insert(key.attribute_name.clone(), value);
        }
        Ok(attributes)
    }

    /// Read the nullable payload columns (`attributes_blob` plus padded
    /// `__aux_indexer_*` slots) over a backend-neutral row.
    pub fn row_indexer_parts<R: IndexedRowView>(
        row: &R,
        max_indexers: MaxIndexers,
    ) -> StorageResult<(String, Vec<Option<String>>)> {
        let residual_json = row
            .optional_text("attributes_blob")?
            .unwrap_or_else(|| "{}".to_string());
        let mut slots = Vec::with_capacity(max_indexers.as_usize());
        for ordinal in 0..max_indexers.as_usize() {
            let column = indexer_column_name(ordinal);
            slots.push(row.optional_text(column.as_str())?);
        }
        Ok((residual_json, slots))
    }

    /// Reconstruct a stored item from padded parts plus key attributes.
    ///
    /// Shares `SqlIndexedItem::reconstruct_with_indexers` verbatim, including
    /// the frozen `stored_item_corruption:sql_slot_count` and
    /// `stored_item_corruption:indexed_key_mismatch` invariants.
    pub fn reconstruct_with_key_attributes(
        residual_json: String,
        slots: Vec<Option<String>>,
        key_attributes: &KeyAttributes,
        capacity: MaxIndexers,
    ) -> StorageResult<StoredWireItem> {
        if slots.len() != capacity.as_usize() {
            return Err(StorageError::internal(
                "stored_item_corruption:sql_slot_count",
            ));
        }
        let DecodedIndexedWireItem {
            mut item,
            declaration,
            ..
        } = IndexedWireItem::decode_padded_parts(residual_json.into_bytes(), slots)?;
        for (name, value) in key_attributes.iter() {
            if let Some(stored) = item.get(name)
                && stored != value
            {
                return Err(StorageError::internal(
                    "stored_item_corruption:indexed_key_mismatch",
                ));
            }
            item.insert(name.to_string(), value.clone());
        }
        Ok(StoredWireItem {
            item: WireItem::try_from(&item)?,
            indexers: declaration.into_names(),
            source: None,
        })
    }

    /// Shared sqlite-shaped decode: main keys with a prefix plus an optional
    /// index schema without prefix. Matches
    /// `sqlite_row_to_keyed_decoded_item` byte-for-byte on success.
    pub fn decode_keyed_row<R: IndexedRowView>(
        row: &R,
        table_info: &StoredTableInfo,
        key_column_prefix: Option<&str>,
        index_key_schema: Option<&[KeySchemaElement]>,
    ) -> StorageResult<(KeyAttributes, StoredWireItem)> {
        let mut key_attributes = Self::row_key_attributes(
            row,
            &table_info.key_schema,
            &table_info.attribute_definitions,
            key_column_prefix,
        )?;
        if let Some(index_key_schema) = index_key_schema {
            let index_key_attributes = Self::row_key_attributes(
                row,
                index_key_schema,
                &table_info.attribute_definitions,
                None,
            )?;
            for (name, value) in index_key_attributes.iter() {
                key_attributes.insert(name.to_string(), value.clone());
            }
        }
        let (residual_json, slots) = Self::row_indexer_parts(row, table_info.max_indexers)?;
        let decoded = Self::reconstruct_with_key_attributes(
            residual_json,
            slots,
            &key_attributes,
            table_info.max_indexers,
        )?;
        Ok((key_attributes, decoded))
    }

    /// Shared postgres/turso-shaped decode: primary keys without prefix plus
    /// an optional secondary schema with the `table_` prefix. Matches
    /// `row_to_decoded_item_for_origin` byte-for-byte on success.
    pub fn decode_row_for_origin<R: IndexedRowView>(
        row: &R,
        table_info: &StoredTableInfo,
        primary_key_schema: &[KeySchemaElement],
        secondary_key_schema: Option<&[KeySchemaElement]>,
    ) -> StorageResult<StoredWireItem> {
        let mut key_attributes = Self::row_key_attributes(
            row,
            primary_key_schema,
            &table_info.attribute_definitions,
            None,
        )?;
        if let Some(schema) = secondary_key_schema {
            let secondary = Self::row_key_attributes(
                row,
                schema,
                &table_info.attribute_definitions,
                Some("table_"),
            )?;
            for (name, value) in secondary.iter() {
                key_attributes.insert(name.to_string(), value.clone());
            }
        }
        let (residual_json, slots) = Self::row_indexer_parts(row, table_info.max_indexers)?;
        Self::reconstruct_with_key_attributes(
            residual_json,
            slots,
            &key_attributes,
            table_info.max_indexers,
        )
    }
}
