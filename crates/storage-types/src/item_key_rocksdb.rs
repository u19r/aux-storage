use crate::{
    AttributeValue, IndexKey, IndexKeyPrefix, IndexName, ItemKey, SerializesToKey, StreamItemId,
    StreamKey, StreamName, TableKey, TableName,
    item_key::{ItemKeyEnum, ItemKeyError},
};

const DYNAMODB_NUMBER_KEY_EXPONENT_BIAS: i32 = 1000;
const DYNAMODB_NUMBER_KEY_DIGITS: usize = 38;

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

    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        match self {
            ItemKey::Table(key) => key.sorted_storage_suffix(),
            ItemKey::Index(key) => key.sorted_storage_suffix(),
            ItemKey::IndexPrefix(key) => key.sorted_storage_suffix(),
        }
    }
}

impl TableKey {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        serialize_key_part_values(&self.hash_key, self.range_key.as_ref())
    }
}

impl IndexKeyPrefix {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        serialize_key_part_values(&self.hash_key, self.range_key.as_ref())
    }
}

impl IndexKey {
    pub fn sorted_storage_suffix(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut key_parts = serialize_key_part_values(&self.hash_key, self.range_key.as_ref())?;
        let table_key_bytes = self.table_key.sorted_storage_suffix()?;
        ItemKey::add_length_prefixed_part_rocksdb(&mut key_parts, &table_key_bytes);
        Ok(key_parts)
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
        AttributeValue::N(n) => encode_dynamodb_number_key(n),
        AttributeValue::B(b) => Ok(b.as_bytes().to_vec()),
        _ => Err(ItemKeyEnum::Validation(
            "Only S, N, and B types are supported for keys".to_string(),
        )
        .into()),
    }
}

fn encode_dynamodb_number_key(raw: &str) -> Result<Vec<u8>, ItemKeyError> {
    let parsed = ParsedDynamodbNumber::parse(raw)
        .map_err(|message| ItemKeyEnum::Deserialization(message.to_string()))?;
    let Some(parsed) = parsed else {
        return Ok(vec![1]);
    };

    let biased_exponent = parsed.adjusted_exponent + DYNAMODB_NUMBER_KEY_EXPONENT_BIAS;
    let biased_exponent = u16::try_from(biased_exponent)
        .map_err(|err| ItemKeyEnum::Deserialization(err.to_string()))?;
    let mut payload = Vec::with_capacity(2 + DYNAMODB_NUMBER_KEY_DIGITS);
    payload.extend_from_slice(&biased_exponent.to_be_bytes());
    payload.extend_from_slice(parsed.digits.as_bytes());
    payload.resize(2 + DYNAMODB_NUMBER_KEY_DIGITS, b'0');

    if parsed.negative {
        let mut encoded = Vec::with_capacity(1 + payload.len());
        encoded.push(0);
        encoded.extend(payload.into_iter().map(|byte| !byte));
        Ok(encoded)
    } else {
        let mut encoded = Vec::with_capacity(1 + payload.len());
        encoded.push(2);
        encoded.extend(payload);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedDynamodbNumber {
    negative: bool,
    adjusted_exponent: i32,
    digits: String,
}

impl ParsedDynamodbNumber {
    fn parse(raw: &str) -> Result<Option<Self>, &'static str> {
        let bytes = raw.as_bytes();
        if bytes.is_empty() {
            return Err("empty number");
        }

        let mut index = 0usize;
        let negative = match bytes.first() {
            Some(b'-') => {
                index += 1;
                true
            }
            Some(b'+') => {
                index += 1;
                false
            }
            _ => false,
        };

        let mut digits = String::new();
        let mut fractional_digits = 0i32;
        let mut saw_digit = false;

        while matches!(bytes.get(index), Some(b'0'..=b'9')) {
            saw_digit = true;
            digits.push(bytes[index] as char);
            index += 1;
        }

        if bytes.get(index) == Some(&b'.') {
            index += 1;
            while matches!(bytes.get(index), Some(b'0'..=b'9')) {
                saw_digit = true;
                digits.push(bytes[index] as char);
                fractional_digits += 1;
                index += 1;
            }
        }

        if !saw_digit {
            return Err("number must contain a digit");
        }

        let mut exponent = 0i32;
        if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
            index += 1;
            let exponent_sign = match bytes.get(index) {
                Some(b'-') => {
                    index += 1;
                    -1
                }
                Some(b'+') => {
                    index += 1;
                    1
                }
                _ => 1,
            };
            let exponent_start = index;
            while matches!(bytes.get(index), Some(b'0'..=b'9')) {
                index += 1;
            }
            if exponent_start == index {
                return Err("invalid exponent");
            }
            let exponent_value = raw
                .get(exponent_start..index)
                .ok_or("invalid exponent")?
                .parse::<i32>()
                .map_err(|_| "invalid exponent")?;
            exponent = exponent_value * exponent_sign;
        }

        if index != bytes.len() {
            return Err("invalid number");
        }

        let leading_zeroes = digits.bytes().take_while(|byte| *byte == b'0').count();
        digits.drain(..leading_zeroes);
        if digits.is_empty() {
            return Ok(None);
        }

        let trailing_zeroes = digits
            .bytes()
            .rev()
            .take_while(|byte| *byte == b'0')
            .count();
        if trailing_zeroes > 0 {
            let new_len = digits.len() - trailing_zeroes;
            digits.truncate(new_len);
        }

        let exponent = exponent - fractional_digits + i32::try_from(trailing_zeroes).unwrap_or(0);
        let adjusted_exponent = i32::try_from(digits.len()).unwrap_or(i32::MAX) + exponent - 1;

        Ok(Some(Self {
            negative,
            adjusted_exponent,
            digits,
        }))
    }
}
