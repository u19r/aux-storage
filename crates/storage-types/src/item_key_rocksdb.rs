use crate::{
    AttributeValue, IndexName, ItemKey, KeyAttributeType, KeySchemaElement, StorageError,
    StorageResult, StoredTableInfo, TableKey, item_key::ItemKeyError,
    item_key_ordered::serialize_attribute_value,
};

impl ItemKey {
    pub fn serialize_attribute_value_to_bytes(
        attribute_value: &AttributeValue,
    ) -> Result<Vec<u8>, ItemKeyError> {
        serialize_attribute_value(attribute_value)
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
