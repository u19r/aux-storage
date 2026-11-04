//! DynamoDB sequence numbers order native keys within one qualified lineage.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{StorageError, StorageResult, StreamItemId, SystemStreamOffset};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableStreamSequence(StreamItemId);

impl TableStreamSequence {
    pub fn native_key(self) -> StreamItemId {
        self.0
    }
}

impl From<StreamItemId> for TableStreamSequence {
    fn from(key: StreamItemId) -> Self {
        Self(key)
    }
}

impl TryFrom<&SystemStreamOffset> for TableStreamSequence {
    type Error = StorageError;

    fn try_from(offset: &SystemStreamOffset) -> StorageResult<Self> {
        let bytes = if let Some(stamp) = offset.versionstamp() {
            stamp
        } else {
            let order = offset.order().map_err(|_| invalid_sequence())?;
            let mut bytes = [0; 12];
            bytes[4..].copy_from_slice(&order.to_be_bytes());
            bytes
        };
        Ok(Self(bytes.into()))
    }
}

impl fmt::Display for TableStreamSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = [0; 16];
        bytes[4..].copy_from_slice(self.0.as_bytes());
        write!(formatter, "{:040}", u128::from_be_bytes(bytes))
    }
}

impl FromStr for TableStreamSequence {
    type Err = StorageError;

    fn from_str(value: &str) -> StorageResult<Self> {
        if !(21..=40).contains(&value.len()) || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid_sequence());
        }
        let bytes = value
            .parse::<u128>()
            .map_err(|_| invalid_sequence())?
            .to_be_bytes();
        if bytes[..4] != [0; 4] {
            return Err(invalid_sequence());
        }
        let mut native = [0; 12];
        native.copy_from_slice(&bytes[4..]);
        Ok(Self(native.into()))
    }
}

impl Serialize for TableStreamSequence {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for TableStreamSequence {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

fn invalid_sequence() -> StorageError {
    StorageError::raw_validation("Invalid table stream sequence number")
}
