use std::{num::NonZeroUsize, sync::Arc};

use replication_types::{
    event::{
        identity::{CanonicalItemKey, OriginEventId, VersionIdentity},
        order::{EffectRank, HybridLogicalClock, WinnerOrder, WinnerProvenance},
    },
    identity::{origin::OriginId, resource::TableLineage},
    protocol::{CanonicalBytes, CanonicalDigest},
};
use serde::{Deserialize, Serialize};

use crate::HarnessResult;

/// Harness payload; intentionally not a DynamoDB wire API or canonical
/// production codec.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvent {
    pub winner: WinnerProvenance,
    pub value: Option<CanonicalBytes>,
}
pub struct ItemMutation<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}
impl HarnessEvent {
    pub fn mutation(
        origin: OriginId,
        position: u64,
        clock: HybridLogicalClock,
        item: ItemMutation<'_>,
    ) -> HarnessResult<Self> {
        let lineage: TableLineage = [3; 16].try_into()?;
        let key: CanonicalItemKey = bytes(item.key)?.try_into()?;
        let id = OriginEventId {
            lineage,
            origin: origin.clone(),
            position: position.into(),
            ordinal: 0.into(),
        };
        let value = item.value.map(bytes).transpose()?;
        let digest = payload_digest(&value)?;
        Ok(Self {
            winner: WinnerProvenance {
                lineage,
                key,
                version: VersionIdentity::Mutation(id.clone()),
                event: Some(id.into()),
                order: WinnerOrder::Mutation {
                    clock,
                    origin,
                    position: position.into(),
                    ordinal: 0.into(),
                    rank: EffectRank::Mutation,
                },
                digest,
            },
            value,
        })
    }
    pub fn expiration(&self) -> HarnessResult<Self> {
        let mut winner = self.winner.clone();
        winner.event = Some(winner.version.clone().expiration_id());
        winner.order = winner.order.expiration_child();
        winner.digest = payload_digest(&None)?;
        winner.validate()?;
        Ok(Self {
            winner,
            value: None,
        })
    }
    pub fn validate(&self) -> HarnessResult<()> {
        self.winner.validate()?;
        if self.winner.digest != payload_digest(&self.value)? {
            return Err("payload digest mismatch".into());
        }
        if self.winner.order.is_expiration() && self.value.is_some() {
            return Err("TTL event contains value".into());
        }
        Ok(())
    }
}
fn payload_digest(value: &Option<CanonicalBytes>) -> HarnessResult<CanonicalDigest> {
    Ok(CanonicalDigest::of(&serde_json::to_vec(value)?))
}
fn bytes(value: &[u8]) -> HarnessResult<CanonicalBytes> {
    Ok(CanonicalBytes::try_new(
        Arc::from(value),
        NonZeroUsize::MIN.saturating_add(1024 * 1024 - 1),
    )?)
}
