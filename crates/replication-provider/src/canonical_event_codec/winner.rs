use std::{borrow::Cow, num::NonZeroUsize};

use replication_types::{
    error::ReplicationProtocolError,
    event::{
        identity::{CanonicalEventId, CanonicalItemKey, OriginEventId, VersionIdentity},
        order::{EffectRank, HybridLogicalClock, WinnerOrder, WinnerProvenance},
    },
    identity::{
        origin::{MutationOrdinal, OriginId, OriginPosition},
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::ItemProvenance;

use super::{CanonicalJsonCodec, invalid_encoding};
use crate::{bounded_json::BoundedJsonCounter, constants::NATIVE_WINNER_FORMAT};

/// Native materialization retains one exact basis for repeated winner
/// identities.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeWinner<'a> {
    format: u8,
    lineage: TableLineage,
    key_digest: CanonicalDigest,
    stamp: NativeWinnerStamp<'a>,
    rank: EffectRank,
    digest: CanonicalDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum NativeWinnerStamp<'a> {
    Baseline,
    Mutation {
        clock: HybridLogicalClock,
        origin: Cow<'a, OriginId>,
        position: OriginPosition,
        ordinal: MutationOrdinal,
    },
}

impl CanonicalJsonCodec {
    pub fn encode_winner_provenance(
        &self,
        provenance: &WinnerProvenance,
    ) -> Result<ItemProvenance, ReplicationProtocolError> {
        provenance.validate()?;
        let (stamp, rank) = NativeWinnerStamp::from_order(&provenance.order);
        let bytes = self.write_json(
            &NativeWinner {
                format: NATIVE_WINNER_FORMAT,
                lineage: provenance.lineage,
                key_digest: CanonicalDigest::of(provenance.key.as_ref()),
                stamp,
                rank,
                digest: provenance.digest,
            },
            self.winner_provenance_length_bound()?
                .min(self.bounds.event_bytes),
        )?;
        ItemProvenance::decode(&bytes).map_err(|_| invalid_encoding())
    }

    pub fn decode_winner_provenance(
        &self,
        bytes: &[u8],
        key: &CanonicalItemKey,
    ) -> Result<WinnerProvenance, ReplicationProtocolError> {
        self.validate_size(
            bytes.len(),
            self.winner_provenance_length_bound()?
                .min(self.bounds.event_bytes),
        )?;
        let native: NativeWinner<'static> =
            serde_json::from_slice(bytes).map_err(|_| invalid_encoding())?;
        if native.format != NATIVE_WINNER_FORMAT {
            return Err(invalid_encoding());
        }
        native.key_digest.verify(key.as_ref())?;
        native.into_provenance(key.clone())
    }

    fn winner_provenance_length_bound(&self) -> Result<NonZeroUsize, ReplicationProtocolError> {
        *self
            .winner_bytes
            .get_or_init(NativeWinner::bound_encoding_length)
    }
}

impl<'a> NativeWinnerStamp<'a> {
    fn from_order(order: &'a WinnerOrder) -> (Self, EffectRank) {
        match order {
            WinnerOrder::Baseline { rank, .. } => (Self::Baseline, *rank),
            WinnerOrder::Mutation {
                clock,
                origin,
                position,
                ordinal,
                rank,
            } => (
                Self::Mutation {
                    clock: *clock,
                    origin: Cow::Borrowed(origin),
                    position: *position,
                    ordinal: *ordinal,
                },
                *rank,
            ),
        }
    }

    fn into_identity(
        self,
        lineage: TableLineage,
        key: &CanonicalItemKey,
        rank: EffectRank,
    ) -> (VersionIdentity, WinnerOrder) {
        match self {
            Self::Baseline => (
                VersionIdentity::Baseline {
                    lineage,
                    key: key.clone(),
                },
                WinnerOrder::Baseline {
                    lineage,
                    key: key.clone(),
                    rank,
                },
            ),
            Self::Mutation {
                clock,
                origin,
                position,
                ordinal,
            } => {
                let origin = origin.into_owned();
                let identity = OriginEventId {
                    lineage,
                    origin: origin.clone(),
                    position,
                    ordinal,
                };
                (
                    VersionIdentity::Mutation(identity),
                    WinnerOrder::Mutation {
                        clock,
                        origin,
                        position,
                        ordinal,
                        rank,
                    },
                )
            }
        }
    }
}

impl NativeWinner<'_> {
    fn bound_encoding_length() -> Result<NonZeroUsize, ReplicationProtocolError> {
        let origin = OriginId {
            region: RegionId::try_from("a".repeat(RegionId::MAX_BYTES))?,
            database_group: DatabaseGroupId::try_from([u8::MAX; 16])?,
            journal_partition: u16::MAX.into(),
            journal_generation: u64::MAX.into(),
        };
        let largest = NativeWinner {
            format: NATIVE_WINNER_FORMAT,
            lineage: TableLineage::try_from([u8::MAX; 16])?,
            key_digest: [u8::MAX; 32].into(),
            stamp: NativeWinnerStamp::Mutation {
                clock: HybridLogicalClock {
                    physical_ms: u64::MAX,
                    logical: u64::MAX,
                },
                origin: Cow::Owned(origin),
                position: u64::MAX.into(),
                ordinal: u32::MAX.into(),
            },
            rank: EffectRank::Expiration,
            digest: [u8::MAX; 32].into(),
        };
        let mut counter = BoundedJsonCounter::new(usize::MAX);
        serde_json::to_writer(&mut counter, &largest).map_err(|_| invalid_encoding())?;
        NonZeroUsize::new(counter.length()).ok_or_else(invalid_encoding)
    }

    fn into_provenance(
        self,
        key: CanonicalItemKey,
    ) -> Result<WinnerProvenance, ReplicationProtocolError> {
        let (version, order) = self.stamp.into_identity(self.lineage, &key, self.rank);
        let event = match (&version, self.rank) {
            (_, EffectRank::Expiration) => Some(version.clone().expiration_id()),
            (VersionIdentity::Mutation(identity), EffectRank::Mutation) => {
                Some(CanonicalEventId::Origin(identity.clone()))
            }
            (VersionIdentity::Baseline { .. }, EffectRank::Mutation) => None,
        };
        let provenance = WinnerProvenance {
            lineage: self.lineage,
            key,
            version,
            event,
            order,
            digest: self.digest,
        };
        provenance.validate()?;
        Ok(provenance)
    }
}
