use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::resource::TableLineage;
use crate::{error::ReplicationProtocolError, event::identity::CanonicalItemKey};

/// A deployed journal layout, fixed for the lifetime of an origin generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct JournalPartitionCount(u32);

impl TryFrom<u32> for JournalPartitionCount {
    type Error = ReplicationProtocolError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if !(64..=65_536).contains(&value) || !value.is_power_of_two() {
            return Err(ReplicationProtocolError::InvalidRange {
                context: "journal partition count",
            });
        }
        Ok(Self(value))
    }
}

impl Default for JournalPartitionCount {
    fn default() -> Self {
        Self(1_024)
    }
}

impl From<JournalPartitionCount> for u32 {
    fn from(value: JournalPartitionCount) -> Self {
        value.0
    }
}

impl JournalPartitionCount {
    pub fn select(self, lineage: TableLineage, key: &CanonicalItemKey) -> JournalPartition {
        let mut hash = Sha256::new();
        hash.update(crate::constants::JOURNAL_PARTITION_HASH_PREFIX);
        hash.update(lineage.as_ref());
        hash.update(key.as_ref());
        let digest: [u8; 32] = hash.finalize().into();
        let value = u64::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
        ]);
        JournalPartition((value & u64::from(self.0 - 1)) as u16)
    }

    pub fn require_partition(
        self,
        partition: JournalPartition,
    ) -> Result<(), ReplicationProtocolError> {
        if u32::from(partition.0) >= self.0 {
            return Err(ReplicationProtocolError::InvalidRange {
                context: "partition outside fixed journal layout",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalPartition(u16);

impl From<u16> for JournalPartition {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<JournalPartition> for u16 {
    fn from(value: JournalPartition) -> Self {
        value.0
    }
}
