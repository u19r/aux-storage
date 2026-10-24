use serde::{Deserialize, Serialize};

use crate::{ImmutableImageId, StorageError, StorageResult, TableHistoryId};

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ImageAddressParts", into = "ImageAddressParts")]
pub struct ImageAddress(ImageAddressParts);

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageAddressParts {
    pub history: TableHistoryId,
    pub identity: ImmutableImageId,
}

impl TryFrom<ImageAddressParts> for ImageAddress {
    type Error = StorageError;
    fn try_from(parts: ImageAddressParts) -> StorageResult<Self> {
        if parts.history.as_ref() == &[0; 16] {
            return Err(StorageError::internal("Immutable image scope is invalid"));
        }
        Ok(Self(parts))
    }
}

impl From<ImageAddress> for ImageAddressParts {
    fn from(value: ImageAddress) -> Self {
        value.0
    }
}

impl ImageAddress {
    pub fn try_new(history: TableHistoryId, identity: ImmutableImageId) -> StorageResult<Self> {
        ImageAddressParts { history, identity }.try_into()
    }
    pub fn history(self) -> TableHistoryId {
        self.0.history
    }
    pub fn identity(self) -> ImmutableImageId {
        self.0.identity
    }
}
