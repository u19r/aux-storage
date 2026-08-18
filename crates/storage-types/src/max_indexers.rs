use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{StorageError, StorageResult};

pub const MAX_INDEXERS_CAPACITY: u8 = 32;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, ToSchema)]
#[serde(transparent)]
pub struct MaxIndexers(u8);

impl MaxIndexers {
    pub const ZERO: Self = Self(0);

    pub fn try_new(value: u8) -> StorageResult<Self> {
        if value > MAX_INDEXERS_CAPACITY {
            return Err(StorageError::validation("MaxIndexers:too_many"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }

    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[must_use]
    pub const fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl<'de> Deserialize<'de> for MaxIndexers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let value = u8::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<u8> for MaxIndexers {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<MaxIndexers> for u8 {
    fn from(value: MaxIndexers) -> Self {
        value.get()
    }
}
