use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    StorageError, StorageResult, WireAttributeDecode,
    constants::{
        CROCKFORD_114_BITS, CROCKFORD_114_LENGTH, CROCKFORD_ALPHABET, CROCKFORD_ALPHABET_BYTES,
        SYSTEM_TABLE_NAMESPACE, TABLE_NAMESPACE_HASH_OFFSET_BASIS, TABLE_NAMESPACE_HASH_PRIME,
        TABLE_NAMESPACE_PREFIX, TABLE_NAMESPACE_TYPE_NAME,
    },
};

/// Stable namespace identifier used by storage routing and shared-table
/// placement.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableNamespace(String);

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum TableNamespaceParseError {
    #[error("Invalid {type_name} format (expected `{expected_prefix}` prefix and Crockford32 key)")]
    Format {
        type_name: &'static str,
        expected_prefix: &'static str,
    },
    #[error("Invalid {type_name} length (expected {expected} characters)")]
    Length {
        type_name: &'static str,
        expected: usize,
    },
    #[error("Invalid {type_name} characters (expected Crockford32 key)")]
    Characters { type_name: &'static str },
}

impl TableNamespace {
    pub const PREFIX: &'static str = TABLE_NAMESPACE_PREFIX;
    pub const KEY_LENGTH: usize = CROCKFORD_114_LENGTH;
    pub const RANDOM_LENGTH: usize = CROCKFORD_114_LENGTH;
    pub const LENGTH: usize = TABLE_NAMESPACE_PREFIX.len() + CROCKFORD_114_LENGTH;

    #[must_use]
    pub fn new() -> Self {
        let raw = Uuid::now_v7().as_u128() >> (u128::BITS as usize - CROCKFORD_114_BITS);
        let mut namespace = String::with_capacity(Self::LENGTH);
        namespace.push_str(Self::PREFIX);
        append_crockford_u128(&mut namespace, raw, CROCKFORD_114_LENGTH);
        Self(namespace)
    }

    #[must_use]
    pub fn system() -> Self {
        Self(SYSTEM_TABLE_NAMESPACE.to_string())
    }

    #[must_use]
    pub fn from_seed(seed: impl AsRef<str>) -> Self {
        let mask = (1u128 << CROCKFORD_114_BITS) - 1;
        let mut namespace = String::with_capacity(Self::LENGTH);
        namespace.push_str(Self::PREFIX);
        append_crockford_u128(
            &mut namespace,
            stable_seed_u128("table-namespace", seed.as_ref()) & mask,
            Self::KEY_LENGTH,
        );
        Self(namespace)
    }

    pub fn parse_str(input: &str) -> Result<Self, TableNamespaceParseError> {
        Self::parse(input)
    }

    pub fn parse(input: &str) -> Result<Self, TableNamespaceParseError> {
        if input.eq_ignore_ascii_case(SYSTEM_TABLE_NAMESPACE) {
            return Ok(Self(SYSTEM_TABLE_NAMESPACE.to_string()));
        }
        parse_table_namespace(input, Self::PREFIX, Self::KEY_LENGTH).map(Self)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }

    #[must_use]
    pub fn storage_key(&self) -> &str {
        if self.0.eq_ignore_ascii_case(SYSTEM_TABLE_NAMESPACE) {
            return &self.0;
        }
        self.0.strip_prefix(Self::PREFIX).unwrap_or(&self.0)
    }

    #[must_use]
    pub const fn canonical_length() -> usize {
        Self::LENGTH
    }

    #[must_use]
    pub fn schema_pattern() -> String {
        format!(
            "^(?:{}|{}[0-9A-F][0-9A-HJKMNP-TV-Z]{{22}})$",
            SYSTEM_TABLE_NAMESPACE,
            Self::PREFIX
        )
    }

    #[must_use]
    pub fn schema_example() -> String {
        format!("{}1BCDEFGHJKMNPQRSTVWXYZ0", Self::PREFIX)
    }

    #[must_use]
    pub const fn schema_min_length() -> usize {
        SYSTEM_TABLE_NAMESPACE.len()
    }

    #[must_use]
    pub const fn schema_max_length() -> usize {
        Self::canonical_length()
    }
}

impl WireAttributeDecode for TableNamespace {
    fn decode(raw: Option<&str>, field: &str) -> StorageResult<Self> {
        let raw = raw.ok_or_else(|| StorageError::internal(&format!("missing {field} field")))?;
        Self::parse_str(raw)
            .map_err(|err| StorageError::internal(&format!("invalid {field} field: {err}")))
    }
}

impl Default for TableNamespace {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TableNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::ops::Deref for TableNamespace {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::str::FromStr for TableNamespace {
    type Err = TableNamespaceParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_str(s)
    }
}

impl utoipa::PartialSchema for TableNamespace {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, Type};

        ObjectBuilder::new()
            .schema_type(Type::String)
            .pattern(Some(Self::schema_pattern()))
            .examples([Self::schema_example()])
            .min_length(Some(Self::schema_min_length()))
            .max_length(Some(Self::schema_max_length()))
            .description(Some(TABLE_NAMESPACE_TYPE_NAME.to_string()))
            .into()
    }
}

impl utoipa::ToSchema for TableNamespace {}

impl schemars::JsonSchema for TableNamespace {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("TableNamespace")
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "title": "TableNamespace",
            "description": TABLE_NAMESPACE_TYPE_NAME,
            "pattern": Self::schema_pattern(),
            "examples": [Self::schema_example()],
            "minLength": Self::schema_min_length(),
            "maxLength": Self::schema_max_length(),
        })
    }
}

fn parse_table_namespace(
    input: &str,
    prefix: &'static str,
    key_length: usize,
) -> Result<String, TableNamespaceParseError> {
    let raw = if let Some(rest) = input.strip_prefix(prefix) {
        rest
    } else {
        input
    };

    normalize_crockford_storage(raw, prefix, key_length)
}

fn normalize_crockford_storage(
    candidate: &str,
    prefix: &str,
    expected_len: usize,
) -> Result<String, TableNamespaceParseError> {
    if candidate.len() != expected_len {
        return Err(TableNamespaceParseError::Length {
            type_name: TABLE_NAMESPACE_TYPE_NAME,
            expected: expected_len,
        });
    }
    let mut normalized = String::with_capacity(prefix.len() + expected_len);
    normalized.push_str(prefix);
    for character in candidate.chars() {
        let upper = character.to_ascii_uppercase();
        if normalize_crockford_char(upper).is_none() {
            return Err(TableNamespaceParseError::Characters {
                type_name: TABLE_NAMESPACE_TYPE_NAME,
            });
        }
        normalized.push(upper);
    }
    let Some(suffix) = normalized.get(prefix.len()..) else {
        return Err(TableNamespaceParseError::Characters {
            type_name: TABLE_NAMESPACE_TYPE_NAME,
        });
    };
    if expected_len == CROCKFORD_114_LENGTH && !is_crockford_114_storage_key(suffix) {
        return Err(TableNamespaceParseError::Characters {
            type_name: TABLE_NAMESPACE_TYPE_NAME,
        });
    }
    Ok(normalized)
}

fn is_crockford_114_storage_key(candidate: &str) -> bool {
    if candidate.len() != CROCKFORD_114_LENGTH {
        return false;
    }

    let Some(first) = candidate.chars().next() else {
        return false;
    };

    crockford_value_of(first).is_some_and(|value| value <= 0x0F)
}

fn normalize_crockford_char(c: char) -> Option<char> {
    let upper = c.to_ascii_uppercase();
    if CROCKFORD_ALPHABET.contains(&upper) {
        Some(upper)
    } else {
        None
    }
}

fn crockford_value_of(c: char) -> Option<u8> {
    let upper = normalize_crockford_char(c)?;
    CROCKFORD_ALPHABET
        .iter()
        .position(|candidate| *candidate == upper)
        .and_then(|index| u8::try_from(index).ok())
}

fn append_crockford_u128(output: &mut String, mut value: u128, length: usize) {
    let mut encoded = [0u8; 26];
    assert!(
        length <= encoded.len(),
        "Crockford output length is too large"
    );
    for slot in encoded[..length].iter_mut().rev() {
        let index = (value & 0x1F) as usize;
        *slot = CROCKFORD_ALPHABET_BYTES[index];
        value >>= 5;
    }
    output.extend(encoded[..length].iter().map(|byte| *byte as char));
}

fn stable_seed_bytes<const N: usize>(offset_basis: u64, prime: u64, parts: [&[u8]; N]) -> u64 {
    let mut hash = offset_basis;
    for part in parts {
        for byte in part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(prime);
        }
    }
    hash
}

fn stable_seed_u64(seed: &str) -> u64 {
    let mut hash = TABLE_NAMESPACE_HASH_OFFSET_BASIS;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(TABLE_NAMESPACE_HASH_PRIME);
    }
    hash
}

fn stable_seed_u128(namespace: &str, seed: &str) -> u128 {
    let upper = stable_seed_u64(seed);
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x00000100000001B3;
    let lower = stable_seed_bytes(
        OFFSET_BASIS,
        PRIME,
        [namespace.as_bytes(), b":" as &[u8], seed.as_bytes()],
    );

    (u128::from(upper) << 64) | u128::from(lower)
}
