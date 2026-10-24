use serde::{Deserialize, Serialize};

use super::{
    journal_partition::JournalPartition,
    replica::{DatabaseGroupId, RegionId},
};
use crate::error::ReplicationProtocolError;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalGeneration(u64);

impl From<u64> for JournalGeneration {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<JournalGeneration> for u64 {
    fn from(value: JournalGeneration) -> Self {
        value.0
    }
}

impl JournalGeneration {
    pub fn checked_next(self) -> Result<Self, ReplicationProtocolError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ReplicationProtocolError::CounterOverflow {
                context: "origin.checked_next",
            })
    }
}

impl std::fmt::Display for JournalGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OriginPosition(u64);
impl From<u64> for OriginPosition {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<OriginPosition> for u64 {
    fn from(value: OriginPosition) -> Self {
        value.0
    }
}

impl OriginPosition {
    pub fn checked_next(self) -> Result<Self, ReplicationProtocolError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ReplicationProtocolError::CounterOverflow {
                context: "origin.checked_next",
            })
    }
}

impl std::fmt::Display for OriginPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MutationOrdinal(u32);

impl From<u32> for MutationOrdinal {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<MutationOrdinal> for u32 {
    fn from(value: MutationOrdinal) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OriginId {
    pub region: RegionId,
    pub database_group: DatabaseGroupId,
    pub journal_partition: JournalPartition,
    pub journal_generation: JournalGeneration,
}
