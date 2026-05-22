use std::str::FromStr;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ItemStreamVersion;

const STREAM_ITEM_ID_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ToSchema)]
#[schema(value_type = String, example = "000000000000000000000000")]
pub struct StreamItemId([u8; STREAM_ITEM_ID_LEN]);

impl StreamItemId {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; STREAM_ITEM_ID_LEN] {
        &self.0
    }

    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    #[must_use]
    pub fn random() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        let bytes = uuid.into_bytes();
        let mut out = [0u8; STREAM_ITEM_ID_LEN];
        out.copy_from_slice(&bytes[..STREAM_ITEM_ID_LEN]);
        Self(out)
    }

    #[must_use]
    pub fn increment(&self) -> Self {
        let mut next = self.0;
        for byte in next.iter_mut().rev() {
            if *byte == u8::MAX {
                *byte = 0;
            } else {
                *byte += 1;
                return Self(next);
            }
        }
        Self(next)
    }
}

impl TryFrom<&[u8]> for StreamItemId {
    type Error = InvalidStreamItemIdLength;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() != STREAM_ITEM_ID_LEN {
            return Err(InvalidStreamItemIdLength);
        }
        let mut out = [0u8; STREAM_ITEM_ID_LEN];
        out.copy_from_slice(bytes);
        Ok(Self(out))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidStreamItemIdLength;

impl std::fmt::Display for InvalidStreamItemIdLength {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream item id must be {STREAM_ITEM_ID_LEN} bytes")
    }
}

impl std::error::Error for InvalidStreamItemIdLength {}

impl Default for StreamItemId {
    fn default() -> Self {
        Self([0u8; STREAM_ITEM_ID_LEN])
    }
}

impl Serialize for StreamItemId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for StreamItemId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        StreamItemId::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl From<Uuid> for StreamItemId {
    fn from(value: Uuid) -> Self {
        StreamItemId::from_uuid(value)
    }
}

impl From<[u8; STREAM_ITEM_ID_LEN]> for StreamItemId {
    fn from(value: [u8; STREAM_ITEM_ID_LEN]) -> Self {
        Self(value)
    }
}

impl From<ItemStreamVersion> for StreamItemId {
    fn from(value: ItemStreamVersion) -> Self {
        let mut out = [0u8; STREAM_ITEM_ID_LEN];
        out[4..].copy_from_slice(&value.to_be_bytes());
        Self(out)
    }
}

impl From<StreamItemId> for Vec<u8> {
    fn from(value: StreamItemId) -> Self {
        value.0.to_vec()
    }
}

impl FromStr for StreamItemId {
    type Err = hex::FromHexError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != STREAM_ITEM_ID_LEN * 2 {
            return Err(hex::FromHexError::InvalidStringLength);
        }
        let mut bytes = [0u8; STREAM_ITEM_ID_LEN];
        hex::decode_to_slice(value, &mut bytes)?;
        Ok(Self(bytes))
    }
}

impl std::fmt::Display for StreamItemId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}
