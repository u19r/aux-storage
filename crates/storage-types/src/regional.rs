//! Regional pair values (design B, R1/R2): winner order, pair identity, delete
//! marker, stream mark.

use serde::{Deserialize, Serialize};

use crate::{
    BoundTableInfo, ItemKey, KeyAttributes, RegionId, StorageEnum, StorageError, StorageResult,
    TimestampMillis,
};

/// Whole-item LWW order of one key. Compared as (stamp, region, expiry).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RegionalOrder {
    #[serde(rename = "s")]
    stamp: u64,
    #[serde(rename = "r")]
    region: RegionId,
    #[serde(rename = "x", default, skip_serializing_if = "std::ops::Not::not")]
    expiry: bool,
}

impl RegionalOrder {
    pub const ENCODED_BYTES: usize = 11;
    /// Logical increments per millisecond before the stamp runs ahead of the
    /// clock.
    const LOGICAL_BITS: u32 = 10;

    #[must_use]
    pub fn from_parts(stamp: u64, region: RegionId, expiry: bool) -> Self {
        Self {
            stamp,
            region,
            expiry,
        }
    }

    #[must_use]
    pub fn stamp(self) -> u64 {
        self.stamp
    }

    #[must_use]
    pub fn region(self) -> RegionId {
        self.region
    }

    #[must_use]
    pub fn is_expiry(self) -> bool {
        self.expiry
    }

    /// The order of an item stored before its table was paired, as `region`
    /// holds it. A scan sends such an item with this order, so both copies
    /// compare equal.
    #[must_use]
    pub fn baseline(region: RegionId) -> Self {
        Self {
            stamp: 0,
            region,
            expiry: false,
        }
    }

    /// A new local mutation: above the stored winner of its key, at least the
    /// clock.
    pub fn next_local(
        now: TimestampMillis,
        winner: Option<Self>,
        region: RegionId,
    ) -> StorageResult<Self> {
        let clock = u64::try_from(now.timestamp_millis())
            .ok()
            .and_then(|millis| millis.checked_mul(1_u64 << Self::LOGICAL_BITS))
            .ok_or_else(|| StorageError::internal("regional order clock is out of range"))?;
        let stamp = match winner {
            None => clock,
            Some(winner) => clock.max(
                winner
                    .stamp
                    .checked_add(1)
                    .ok_or_else(|| StorageError::internal("regional order stamp is exhausted"))?,
            ),
        };
        Ok(Self {
            stamp,
            region,
            expiry: false,
        })
    }

    /// The TTL delete of an item with this order (P6): above the item, below
    /// any later write. Both regions expiring the same item produce the same
    /// order.
    pub fn expiry_child(self) -> StorageResult<Self> {
        if self.expiry {
            return Err(StorageError::internal(
                "an expiration order has no expiration child",
            ));
        }
        Ok(Self {
            expiry: true,
            ..self
        })
    }

    /// Big-endian stamp, region, expiry: byte order equals `Ord`.
    #[must_use]
    pub fn encode(self) -> [u8; Self::ENCODED_BYTES] {
        let mut bytes = [0; Self::ENCODED_BYTES];
        bytes[..8].copy_from_slice(&self.stamp.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.region.0.get().to_be_bytes());
        bytes[10] = u8::from(self.expiry);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        let invalid = || StorageError::internal("invalid stored regional order");
        let bytes: &[u8; Self::ENCODED_BYTES] = bytes.try_into().map_err(|_| invalid())?;
        let mut stamp = [0; 8];
        stamp.copy_from_slice(&bytes[..8]);
        let region = std::num::NonZeroU16::new(u16::from_be_bytes([bytes[8], bytes[9]]))
            .ok_or_else(invalid)?;
        let expiry = match bytes[10] {
            0 => false,
            1 => true,
            _ => return Err(invalid()),
        };
        Ok(Self {
            stamp: u64::from_be_bytes(stamp),
            region: RegionId(region),
            expiry,
        })
    }
}

/// New for each enable, replacement and restore; constant for the life of one
/// pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PairId(uuid::Uuid);

impl PairId {
    #[must_use]
    pub fn generate() -> Self {
        Self(crate::ambient::uuid_v4())
    }

    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(uuid::Uuid::from_bytes(bytes))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// Delete state of one key. `received`: kept by a Joining target for a delete
/// it applied (R2); every other marker is an origin marker of a local delete
/// (R1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionalMarker {
    pub order: RegionalOrder,
    pub received: bool,
}

impl RegionalMarker {
    pub const ENCODED_BYTES: usize = RegionalOrder::ENCODED_BYTES + 1;

    #[must_use]
    pub fn encode(self) -> [u8; Self::ENCODED_BYTES] {
        let mut bytes = [0; Self::ENCODED_BYTES];
        bytes[..RegionalOrder::ENCODED_BYTES].copy_from_slice(&self.order.encode());
        bytes[RegionalOrder::ENCODED_BYTES] = u8::from(self.received);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        let (order, received) = bytes
            .split_at_checked(RegionalOrder::ENCODED_BYTES)
            .filter(|(_, flag)| flag.len() == 1)
            .ok_or_else(|| StorageError::internal("invalid stored regional marker"))?;
        let received = match received[0] {
            0 => false,
            1 => true,
            _ => return Err(StorageError::internal("invalid stored regional marker")),
        };
        Ok(Self {
            order: RegionalOrder::decode(order)?,
            received,
        })
    }
}

/// The physical key of a marker inside its table: the sorted primary key
/// suffix, the same bytes as the KV item key suffix.
pub fn regional_marker_item_key(
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<Vec<u8>> {
    ItemKey::from_key_schema(table.info.table_name.clone(), &table.info.key_schema, key)
        .map_err(|error| StorageError::from(StorageEnum::from(error)))?
        .sorted_storage_suffix()
        .map_err(|error| StorageError::from(StorageEnum::from(error)))
}

/// Why a paired stream entry exists. `Origin` and `Echo` are sent to the peer;
/// `Applied` is covered but never sent (R1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionalRole {
    Origin,
    Applied,
    Echo,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegionalMark {
    pub order: RegionalOrder,
    pub role: RegionalRole,
    /// No public change: an absent-key origin delete (P2) or an echo that
    /// removed no item.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hidden: bool,
}

impl RegionalMark {
    #[must_use]
    pub fn origin(order: RegionalOrder) -> Self {
        Self {
            order,
            role: RegionalRole::Origin,
            hidden: false,
        }
    }
}
