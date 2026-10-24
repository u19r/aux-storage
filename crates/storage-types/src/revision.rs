//! Zero-based CAS counters shared by physical records and maintenance progress.

use serde::{Deserialize, Serialize};

use crate::{InfrastructureContext, InfrastructureResultKind, StorageError, StorageResult};

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Revision(u64);

impl Revision {
    pub const INITIAL: Self = Self(0);

    pub fn checked_next(self) -> StorageResult<Self> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            StorageError::from(InfrastructureResultKind::InvalidBound {
                context: InfrastructureContext::Revision,
            })
        })
    }
}

impl From<u64> for Revision {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Revision> for u64 {
    fn from(value: Revision) -> Self {
        value.0
    }
}
