//! Qualified output and physical dimensions for complete image inventories.

use storage_types::StorageResult;

use super::error::{capacity_rejected, invalid_image};

#[derive(Clone, Copy)]
pub(super) enum ImageProjectionShape {
    Item,
    History,
    Pointer,
    System,
}

impl ImageProjectionShape {
    pub(super) fn bound_projection_bytes(self, metadata_bytes: usize) -> StorageResult<usize> {
        let declaration = 6 * storage_types::MAX_INDEXER_DECLARATION_BYTES
            + 3 * usize::from(storage_types::MAX_INDEXERS_CAPACITY)
            + 2;
        let (images, auxiliary) = match self {
            Self::Item => (
                1usize,
                declaration.max(storage_types::SourceOrdinalProjection::encoded_length_bound()),
            ),
            Self::History => (1, 0),
            Self::Pointer => (2, 2 * declaration),
            Self::System => (2, 0),
        };
        storage_types::LogicalItemSize::maximum_json_projection_bytes()
            .checked_mul(images)
            .and_then(|bytes| bytes.checked_add(auxiliary.checked_mul(2)?))
            .and_then(|bytes| bytes.checked_add(metadata_bytes.checked_mul(2)?))
            .ok_or_else(invalid_image)
    }
}

#[derive(Default)]
pub(super) struct ImageReadWork {
    physical_bytes: usize,
    projected_bytes: usize,
}

impl ImageReadWork {
    pub(super) fn new(physical_bytes: usize, projected_bytes: usize) -> Self {
        Self {
            physical_bytes,
            projected_bytes,
        }
    }

    pub(super) fn page_bytes(&self) -> usize {
        self.physical_bytes.max(self.projected_bytes)
    }

    pub(super) fn admit_batch(
        work: impl Iterator<Item = StorageResult<Self>>,
        maximum: usize,
    ) -> StorageResult<Self> {
        let mut admitted = Self::default();
        for next in work {
            let next = next?;
            admitted.physical_bytes = admitted
                .physical_bytes
                .checked_add(next.physical_bytes)
                .ok_or_else(capacity_rejected)?;
            admitted.projected_bytes = admitted
                .projected_bytes
                .checked_add(next.projected_bytes)
                .ok_or_else(capacity_rejected)?;
            if admitted.physical_bytes > maximum {
                return Err(capacity_rejected());
            }
        }
        Ok(admitted)
    }
}
