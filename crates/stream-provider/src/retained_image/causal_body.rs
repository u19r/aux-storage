//! Causal content shares only the journal root represented by its original
//! locator.

use infrastructure_provider::record::DurableRecordKey;
use sha2::{Digest as _, Sha256};
use storage_types::{ImageAddress, ImmutableImageId, StorageResult, TableHistoryId};

use super::error::invalid_image;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct CausalBodyScope(TableHistoryId);

impl TryFrom<&DurableRecordKey> for CausalBodyScope {
    type Error = storage_types::StorageError;

    fn try_from(locator: &DurableRecordKey) -> StorageResult<Self> {
        let address = locator.as_ref();
        let mut digest = Sha256::new();
        digest.update(crate::constants::CAUSAL_BODY_SCOPE_DOMAIN);
        for bytes in [address.namespace.as_bytes(), address.resource.as_slice()] {
            digest.update(
                u64::try_from(bytes.len())
                    .map_err(|_| invalid_image())?
                    .to_be_bytes(),
            );
            digest.update(bytes);
        }
        let digest = digest.finalize();
        Self::try_from(<[u8; 16]>::try_from(&digest[..16]).map_err(|_| invalid_image())?)
    }
}

impl TryFrom<[u8; 16]> for CausalBodyScope {
    type Error = storage_types::StorageError;

    fn try_from(bytes: [u8; 16]) -> StorageResult<Self> {
        if bytes == [0; 16] {
            return Err(invalid_image());
        }
        Ok(Self(bytes.into()))
    }
}

impl AsRef<[u8; 16]> for CausalBodyScope {
    fn as_ref(&self) -> &[u8; 16] {
        self.0.as_ref()
    }
}

impl CausalBodyScope {
    pub(super) fn address(self, identity: ImmutableImageId) -> StorageResult<ImageAddress> {
        ImageAddress::try_new(self.0, identity)
    }
}
