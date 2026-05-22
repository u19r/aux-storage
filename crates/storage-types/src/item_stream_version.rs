use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::StreamItemId;

const ITEM_STREAM_VERSION_LEN: usize = 8;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, ToSchema,
)]
#[schema(value_type = u64, example = 1)]
pub struct ItemStreamVersion(u64);

impl ItemStreamVersion {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn to_be_bytes(self) -> [u8; ITEM_STREAM_VERSION_LEN] {
        self.0.to_be_bytes()
    }

    #[must_use]
    pub fn checked_increment(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl From<[u8; ITEM_STREAM_VERSION_LEN]> for ItemStreamVersion {
    fn from(value: [u8; ITEM_STREAM_VERSION_LEN]) -> Self {
        Self(u64::from_be_bytes(value))
    }
}

impl TryFrom<&[u8]> for ItemStreamVersion {
    type Error = InvalidItemStreamVersionLength;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != ITEM_STREAM_VERSION_LEN {
            return Err(InvalidItemStreamVersionLength);
        }
        let mut bytes = [0u8; ITEM_STREAM_VERSION_LEN];
        bytes.copy_from_slice(value);
        Ok(Self::from(bytes))
    }
}

impl TryFrom<i64> for ItemStreamVersion {
    type Error = InvalidItemStreamVersionValue;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        let value = u64::try_from(value).map_err(|_| InvalidItemStreamVersionValue)?;
        Ok(Self::new(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidItemStreamVersionLength;

impl std::fmt::Display for InvalidItemStreamVersionLength {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "item stream version must be {ITEM_STREAM_VERSION_LEN} bytes"
        )
    }
}

impl std::error::Error for InvalidItemStreamVersionLength {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidItemStreamVersionValue;

impl std::fmt::Display for InvalidItemStreamVersionValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "item stream version must be non-negative")
    }
}

impl std::error::Error for InvalidItemStreamVersionValue {}

impl From<InvalidItemStreamVersionValue> for crate::StorageError {
    fn from(value: InvalidItemStreamVersionValue) -> Self {
        Self::internal(&value.to_string())
    }
}

impl From<StreamItemId> for ItemStreamVersion {
    fn from(value: StreamItemId) -> Self {
        let mut bytes = [0u8; ITEM_STREAM_VERSION_LEN];
        bytes.copy_from_slice(&value.as_bytes()[4..]);
        Self::from(bytes)
    }
}

impl From<ItemStreamVersion> for Vec<u8> {
    fn from(value: ItemStreamVersion) -> Self {
        value.to_be_bytes().to_vec()
    }
}

impl std::fmt::Display for ItemStreamVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
