use std::num::NonZeroUsize;

use crate::{ImageAddress, StorageError, StorageResult, WireItem};

/// Exact primary manifest binding retained after verified native hydration.
#[derive(Clone, Copy)]
pub struct StoredPrimaryDescriptor {
    digest: [u8; 32],
    declaration: (ImageAddress, NonZeroUsize),
    payload: (ImageAddress, NonZeroUsize),
}

impl StoredPrimaryDescriptor {
    pub fn try_new(
        digest: [u8; 32],
        declaration: (ImageAddress, usize),
        payload: (ImageAddress, usize),
    ) -> StorageResult<Self> {
        let invalid = || StorageError::internal("Invalid primary descriptor proof");
        if declaration.0.history() != payload.0.history()
            || (declaration.0 == payload.0 && declaration.1 != payload.1)
        {
            return Err(invalid());
        }
        Ok(Self {
            digest,
            declaration: (
                declaration.0,
                NonZeroUsize::new(declaration.1).ok_or_else(invalid)?,
            ),
            payload: (payload.0, NonZeroUsize::new(payload.1).ok_or_else(invalid)?),
        })
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn references(&self) -> [(ImageAddress, usize); 2] {
        [
            (self.declaration.0, self.declaration.1.get()),
            (self.payload.0, self.payload.1.get()),
        ]
    }
}

/// Native immutable body location and admitted byte lengths, without item data.
#[derive(Clone, Copy)]
pub struct StoredImageSource {
    pub address: ImageAddress,
    pub raw_bytes: NonZeroUsize,
    pub encoded_bytes: NonZeroUsize,
    pub descriptor: StoredPrimaryDescriptor,
}

impl StoredImageSource {
    pub fn try_new(
        address: ImageAddress,
        raw_bytes: usize,
        encoded_bytes: usize,
        descriptor: StoredPrimaryDescriptor,
    ) -> StorageResult<Self> {
        let invalid = || StorageError::internal("Invalid stored image lengths");
        if descriptor.payload != (address, NonZeroUsize::new(raw_bytes).ok_or_else(invalid)?) {
            return Err(invalid());
        }
        Ok(Self {
            address,
            descriptor,
            raw_bytes: NonZeroUsize::new(raw_bytes).ok_or_else(invalid)?,
            encoded_bytes: NonZeroUsize::new(encoded_bytes).ok_or_else(invalid)?,
        })
    }
}

impl std::fmt::Debug for StoredImageSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("StoredImageSource")
    }
}

/// A stored logical item and the indexer declaration persisted with it.
///
/// Native value codecs decode into this shape before shared read, write,
/// replication, GSI, or maintenance logic consumes the item.
pub struct StoredWireItem {
    pub item: WireItem,
    pub indexers: Vec<String>,
    pub source: Option<StoredImageSource>,
}
