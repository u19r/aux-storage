use crate::{
    AttributeValue, IndexKey, IndexKeyPrefix, IndexName, ItemKey, KeyAttributeType,
    KeySchemaElement, SerializesToKey, StorageError, StorageResult, StoredTableInfo, StreamItemId,
    StreamKey, StreamName, TableKey, TableName,
    item_key::ItemKeyError,
    item_key_ordered::{
        serialize_attribute_value, serialize_index_key_part_values, serialize_key_part_values,
    },
};

impl ItemKey {
    #[must_use]
    pub fn all_table_prefix(table_name: &TableName) -> Vec<u8> {
        table_name.sanitized_name().as_bytes().to_vec()
    }
    #[must_use]
    pub fn table_data_prefix(&self) -> Vec<u8> {
        if let Some(index_name) = self.index_id() {
            Self::index_prefix_from_name(self.table_name(), index_name)
        } else {
            Self::table_prefix_from_name(self.table_name())
        }
    }

    #[must_use]
    pub fn table_prefix_from_name(table_name: &TableName) -> Vec<u8> {
        let mut prefix = Self::all_table_prefix(table_name);
        prefix.extend(b"/data/");
        prefix
    }

    #[must_use]
    pub fn index_prefix_from_name(table_name: &TableName, index_id: &IndexName) -> Vec<u8> {
        let mut prefix = Self::all_table_prefix(table_name);
        prefix.extend(b"/index/");
        prefix.extend(index_id.as_ref().as_bytes());
        prefix.extend(b"/data/");
        prefix
    }

    #[must_use]
    pub fn split_item_id_from_key(key: &[u8]) -> Option<Vec<u8>> {
        let index = key
            .windows(6)
            .enumerate()
            .find_map(|(i, window)| if window == b"/data/" { Some(i) } else { None })?;

        Some(key[(index + 6)..].to_vec())
    }

    pub fn serialize_attribute_value_to_bytes(
        attribute_value: &AttributeValue,
    ) -> Result<Vec<u8>, ItemKeyError> {
        serialize_attribute_value(attribute_value)
    }

    pub fn serialize_key_part(&self) -> Result<Vec<u8>, ItemKeyError> {
        serialize_key_part_values(self.hash_key(), self.range_key())
    }

    pub fn item_stream_name(&self) -> Result<StreamName, ItemKeyError> {
        StreamName::table_item_stream(self.table_name(), self)
    }

    pub fn item_stream_key(
        &self,
        stream_item_id: &StreamItemId,
    ) -> Result<StreamKey, ItemKeyError> {
        let stream_name = self.item_stream_name()?;
        let stream_item_key: StreamKey = &stream_name + stream_item_id;
        Ok(stream_item_key)
    }
}

impl SerializesToKey for ItemKey {
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError> {
        match self {
            ItemKey::Table(key) => key.serialize_to_bytes(),
            ItemKey::Index(key) => key.serialize_to_bytes(),
            ItemKey::IndexPrefix(key) => key.serialize_to_bytes(),
        }
    }
}

impl SerializesToKey for TableKey {
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut key_parts: Vec<u8> = ItemKey::table_prefix_from_name(&self.table_name);
        key_parts.extend(serialize_key_part_values(
            &self.hash_key,
            self.range_key.as_ref(),
        )?);
        Ok(key_parts)
    }
}

impl SerializesToKey for IndexKeyPrefix {
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut key_parts: Vec<u8> =
            ItemKey::index_prefix_from_name(&self.table_name, &self.index_id);
        key_parts.extend(serialize_index_key_part_values(
            &self.hash_key,
            self.range_key.as_ref(),
        )?);
        Ok(key_parts)
    }
}

impl SerializesToKey for IndexKey {
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut key_parts: Vec<u8> =
            ItemKey::index_prefix_from_name(&self.table_name, &self.index_id);
        key_parts.extend(serialize_index_key_part_values(
            &self.hash_key,
            self.range_key.as_ref(),
        )?);
        let table_key_bytes =
            serialize_key_part_values(&self.table_key.hash_key, self.table_key.range_key.as_ref())?;
        ItemKey::append_length_prefixed_key(&mut key_parts, &table_key_bytes)?;
        Ok(key_parts)
    }
}

impl ItemKey {
    /// Decode the ordered native key independently of its stored item image.
    pub fn from_sorted_storage_suffix(
        table: &StoredTableInfo,
        index: Option<&IndexName>,
        suffix: &[u8],
    ) -> StorageResult<Self> {
        let mut decoder = OrderedKeyDecoder {
            table,
            bytes: suffix,
        };
        match index {
            Some(name) => decoder.index_key(name),
            None => decoder.table_key().map(Self::Table),
        }
    }
}

struct OrderedKeyDecoder<'a> {
    table: &'a StoredTableInfo,
    bytes: &'a [u8],
}

impl<'a> OrderedKeyDecoder<'a> {
    fn table_key(&mut self) -> StorageResult<TableKey> {
        let mut schema = KeySchemaElement::ordered(&self.table.key_schema);
        let hash = schema
            .next()
            .ok_or_else(|| StorageError::internal("native table key lacks a schema"))?;
        let hash_bytes = self.length_prefixed()?;
        let hash = self.attribute(hash, hash_bytes)?;
        let range = schema
            .next()
            .map(|field| self.attribute(field, self.bytes))
            .transpose()?;
        if range.is_none() && !self.bytes.is_empty() {
            return Err(StorageError::internal(
                "native table key has trailing bytes",
            ));
        }
        self.bytes = &[];
        let key = TableKey::new(self.table.table_name.clone(), hash, range);
        key.validate_dynamodb_key()?;
        Ok(key)
    }

    fn index_key(&mut self, name: &IndexName) -> StorageResult<ItemKey> {
        let index = self
            .table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|index| &index.index_name == name)
            .ok_or_else(|| StorageError::internal("native index key lacks a schema"))?;
        let mut schema = KeySchemaElement::ordered(&index.key_schema);
        let hash = schema
            .next()
            .ok_or_else(|| StorageError::internal("native index key lacks a hash schema"))?;
        let hash_bytes = self.length_prefixed()?;
        let hash = self.attribute(hash, hash_bytes)?;
        let range = if let Some(field) = schema.next() {
            let bytes = self.index_range()?;
            Some(self.attribute(field, &bytes)?)
        } else {
            None
        };
        let table_bytes = self.length_prefixed()?;
        if !self.bytes.is_empty() {
            return Err(StorageError::internal(
                "native index key has trailing bytes",
            ));
        }
        let mut table_decoder = OrderedKeyDecoder {
            table: self.table,
            bytes: table_bytes,
        };
        let table_key = table_decoder.table_key()?;
        Ok(ItemKey::index_key(
            self.table.table_name.clone(),
            name.clone(),
            hash,
            range,
            table_key,
        ))
    }

    fn length_prefixed(&mut self) -> StorageResult<&'a [u8]> {
        let length = self
            .bytes
            .get(..2)
            .ok_or_else(|| StorageError::internal("native key length is truncated"))?;
        let length = usize::from(u16::from_be_bytes([length[0], length[1]]));
        let value = self
            .bytes
            .get(2..2 + length)
            .ok_or_else(|| StorageError::internal("native key value is truncated"))?;
        self.bytes = &self.bytes[2 + length..];
        Ok(value)
    }

    fn index_range(&mut self) -> StorageResult<Vec<u8>> {
        let mut value = Vec::new();
        loop {
            match self.bytes.first() {
                Some(0) => {
                    self.bytes = &self.bytes[1..];
                    return Ok(value);
                }
                Some(1) if self.bytes.len() >= 2 => {
                    value.push(self.bytes[1]);
                    self.bytes = &self.bytes[2..];
                }
                _ => {
                    return Err(StorageError::internal(
                        "native index range key is truncated or invalid",
                    ));
                }
            }
        }
    }

    fn attribute(&self, field: &KeySchemaElement, bytes: &[u8]) -> StorageResult<AttributeValue> {
        let kind = self
            .table
            .attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == field.attribute_name)
            .ok_or_else(|| {
                StorageError::internal("native key field lacks its attribute definition")
            })?
            .attribute_type;
        match kind {
            KeyAttributeType::S => String::from_utf8(bytes.to_vec())
                .map(AttributeValue::S)
                .map_err(|_| StorageError::internal("native string key is not UTF-8")),
            KeyAttributeType::N => crate::dynamodb_number::decode_dynamodb_number_key_bytes(bytes)
                .map(AttributeValue::N)
                .map_err(|_| StorageError::internal("native number key is invalid")),
            KeyAttributeType::B => {
                use base64::Engine as _;
                Ok(AttributeValue::B(
                    base64::engine::general_purpose::STANDARD.encode(bytes),
                ))
            }
        }
    }
}
