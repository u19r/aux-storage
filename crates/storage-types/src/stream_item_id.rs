use std::str::FromStr;

use serde::{Deserialize, Serialize, de::Visitor};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{ItemStreamVersion, constants::STREAM_ITEM_ID_LEN};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, ToSchema)]
#[schema(value_type = String, example = "000000000000000000000000")]
pub struct StreamItemId([u8; STREAM_ITEM_ID_LEN]);

impl StreamItemId {
    /// Inclusive upper bound of the opaque, unsigned item-ID address space.
    pub const MAX: Self = Self([u8::MAX; STREAM_ITEM_ID_LEN]);

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

    /// The exclusive lower bound for an inclusive ordered stream read.
    /// The zero identity has no predecessor, so its read starts at the head.
    #[must_use]
    pub fn predecessor(&self) -> Option<Self> {
        let mut previous = self.0;
        for byte in previous.iter_mut().rev() {
            if *byte == 0 {
                *byte = u8::MAX;
            } else {
                *byte -= 1;
                return Some(Self(previous));
            }
        }
        None
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
        serializer.serialize_str(&hex::encode(self.0))
    }
}

struct StreamItemIdVisitor;

impl<'de> Visitor<'de> for StreamItemIdVisitor {
    type Value = StreamItemId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a lowercase hexadecimal stream item id")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where E: serde::de::Error {
        StreamItemId::from_str(value).map_err(E::custom)
    }
}

impl<'de> Deserialize<'de> for StreamItemId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        deserializer.deserialize_str(StreamItemIdVisitor)
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
        f.write_str(&hex::encode(self.0))
    }
}
