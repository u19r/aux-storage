use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use storage_types::{StorageError, TableName};

use super::{TtlConfigRecord, TtlIndexKey, TtlSweepPageInput};

#[derive(Clone)]
pub enum TtlIndexWrite {
    Delete(TtlIndexKey),
    Upsert(TtlIndexKey),
}

#[derive(Clone)]
pub struct TtlIndexWriteBatch(pub(super) Vec<TtlIndexWrite>);

impl TtlIndexWriteBatch {
    #[must_use]
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &TtlIndexWrite> {
        self.0.iter()
    }
}

impl IntoIterator for TtlIndexWriteBatch {
    type Item = TtlIndexWrite;
    type IntoIter = std::vec::IntoIter<TtlIndexWrite>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone)]
pub struct TtlSweepRequest {
    pub table_name: TableName,
    pub input: TtlSweepPageInput,
}

pub struct TtlConfigActivation {
    pub table_name: TableName,
    pub expected: TtlConfigRecord,
    pub activated: TtlConfigRecord,
}

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TtlMaintenanceCursor {
    pub token: String,
}

impl TryFrom<String> for TtlMaintenanceCursor {
    type Error = StorageError;

    fn try_from(token: String) -> Result<Self, Self::Error> {
        if token.trim().is_empty() {
            return Err(StorageError::validation(
                "ttl maintenance cursor token must not be empty",
            ));
        }
        Ok(Self { token })
    }
}

impl TryFrom<&str> for TtlMaintenanceCursor {
    type Error = StorageError;

    fn try_from(token: &str) -> Result<Self, Self::Error> {
        Self::try_from(token.to_owned())
    }
}

impl<'de> Deserialize<'de> for TtlMaintenanceCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            token: String,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::try_from(wire.token).map_err(D::Error::custom)
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtlSweepPageResult {
    pub deleted_items: u64,
    pub next_cursor: Option<TtlMaintenanceCursor>,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtlMaintenanceCheckpoint {
    pub cursor: Option<TtlMaintenanceCursor>,
}
