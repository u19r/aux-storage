//! The regional order stored with a live item; absent for an item of an
//! unpaired table and for an item stored before its table was paired.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{RegionalOrder, StorageError, StorageResult};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ItemProvenance {
    order: Option<RegionalOrder>,
    encoded: [u8; RegionalOrder::ENCODED_BYTES],
}

impl ItemProvenance {
    #[must_use]
    pub fn regional(order: RegionalOrder) -> Self {
        Self {
            order: Some(order),
            encoded: order.encode(),
        }
    }

    #[must_use]
    pub fn order(&self) -> Option<RegionalOrder> {
        self.order
    }

    #[must_use]
    pub fn is_local(&self) -> bool {
        self.order.is_none()
    }

    /// Stored bytes: empty, or the 11-byte order.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        if self.order.is_some() {
            &self.encoded
        } else {
            &[]
        }
    }

    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        match bytes.len() {
            0 => Ok(Self::default()),
            RegionalOrder::ENCODED_BYTES => Ok(Self::regional(RegionalOrder::decode(bytes)?)),
            _ => Err(StorageError::internal("invalid stored item provenance")),
        }
    }
}

impl Serialize for ItemProvenance {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.order.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ItemProvenance {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Option::<RegionalOrder>::deserialize(deserializer)?
            .map_or_else(Self::default, Self::regional))
    }
}
