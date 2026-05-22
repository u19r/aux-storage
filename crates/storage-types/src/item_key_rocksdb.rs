use rust_decimal::Decimal;

use crate::{
    AttributeValue, IndexKey, IndexKeyPrefix, IndexName, ItemKey, SerializesToKey, StreamItemId,
    StreamKey, StreamName, TableKey, TableName,
    item_key::{ItemKeyEnum, ItemKeyError},
    numeric::SortableVec as _,
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
        key_parts.extend(serialize_key_part_values(
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
        key_parts.extend(serialize_key_part_values(
            &self.hash_key,
            self.range_key.as_ref(),
        )?);
        let table_key_bytes =
            serialize_key_part_values(&self.table_key.hash_key, self.table_key.range_key.as_ref())?;
        ItemKey::add_length_prefixed_part_rocksdb(&mut key_parts, &table_key_bytes);
        Ok(key_parts)
    }
}

fn serialize_key_part_values(
    hash_key: &AttributeValue,
    range_key: Option<&AttributeValue>,
) -> Result<Vec<u8>, ItemKeyError> {
    let mut parts = Vec::new();

    // Add hash key with length prefix
    let hash_bytes = ItemKey::serialize_attribute_value_to_bytes(hash_key)?;
    ItemKey::add_length_prefixed_part_rocksdb(&mut parts, &hash_bytes);

    if let Some(range_key) = range_key {
        // Add range key with length prefix
        let range_bytes = ItemKey::serialize_attribute_value_to_bytes(range_key)?;
        // DO NOT ADD length prefix here, only hash key is prefixed for storage keys.
        parts.extend(&range_bytes);
    }

    Ok(parts)
}

impl ItemKey {
    fn add_length_prefixed_part_rocksdb(parts: &mut Vec<u8>, data: &[u8]) {
        // Use 2 bytes: 10 bits for length, 2 bits for version (00), 4 bits reserved for
        // flags. Cap length at 1023 and convert safely.
        let length_u16 = u16::try_from(data.len()).unwrap_or(u16::MAX);
        let length = length_u16.min(1023);
        // Version is 00 (0), flags are 0000 (0)
        let prefix = length << 6; // Shift length to upper 10 bits, version/flags = 0
        parts.extend(prefix.to_be_bytes());
        parts.extend(data);
    }
}

fn serialize_attribute_value(attribute_value: &AttributeValue) -> Result<Vec<u8>, ItemKeyError> {
    match attribute_value {
        AttributeValue::S(s) => Ok(s.as_bytes().to_vec()),
        AttributeValue::N(n) => {
            // Convert string number to Decimal and encode
            let decimal = Decimal::from_str_exact(n)
                .map_err(|e| ItemKeyEnum::Deserialization(e.to_string()))?;

            Ok(decimal.encode())
        }
        AttributeValue::B(b) => Ok(b.as_bytes().to_vec()),
        _ => Err(ItemKeyEnum::Validation(
            "Only S, N, and B types are supported for keys".to_string(),
        )
        .into()),
    }
}
