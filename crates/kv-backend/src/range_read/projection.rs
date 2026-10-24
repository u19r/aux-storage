//! Static value projections reuse one bounded native scanner.

use storage_provider::{BoundRangeReadPlan, ItemWriteObservation, PrimaryStateRow, RangeReadItem};
use storage_types::{BoundTableInfo, ItemKey, LogicalItemSize, StorageResult};

use super::decode::{decode_range_entry, range_evidence};
use crate::item_read::HydratedPrimaryItem;

pub(super) enum RangeContent {
    Primary(Box<HydratedPrimaryItem>),
    Projection(Box<crate::gsi::HydratedProjection>),
}

pub(super) trait RangeProjection {
    type Item: Send;

    fn decode(
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        physical: &[u8],
        content: RangeContent,
    ) -> StorageResult<(Self::Item, ItemKey, LogicalItemSize)>;
}

pub(super) struct PublicImage;

impl RangeProjection for PublicImage {
    type Item = RangeReadItem;

    fn decode(
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        physical: &[u8],
        content: RangeContent,
    ) -> StorageResult<(RangeReadItem, ItemKey, LogicalItemSize)> {
        let (item, key, size) = decode_range_entry(table, bound, physical, content)?;
        Ok((
            RangeReadItem {
                key: key.clone(),
                item,
            },
            key,
            size,
        ))
    }
}

pub(super) struct PrimaryState;

impl RangeProjection for PrimaryState {
    type Item = PrimaryStateRow;

    fn decode(
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        physical: &[u8],
        content: RangeContent,
    ) -> StorageResult<(PrimaryStateRow, ItemKey, LogicalItemSize)> {
        let RangeContent::Primary(primary) = content else {
            return Err(storage_types::StorageError::internal(
                "Primary state lacks hydrated content",
            ));
        };
        let stored = primary.stored;
        let (key, size) = range_evidence(table, bound, physical, &stored.item)?;
        let observation = ItemWriteObservation::Live {
            stored: Box::new(stored),
            revision: primary.revision,
            provenance: primary.provenance,
            ttl_epoch: primary.ttl_epoch,
        };
        let row = PrimaryStateRow {
            key: key.key_attributes(&table.info)?,
            state: observation,
        };
        Ok((row, key, size))
    }
}

pub(super) struct GsiState;

impl RangeProjection for GsiState {
    type Item = storage_provider::GsiProjectionRow;

    fn decode(
        table: &BoundTableInfo,
        bound: &BoundRangeReadPlan<'_>,
        physical: &[u8],
        content: RangeContent,
    ) -> StorageResult<(Self::Item, ItemKey, LogicalItemSize)> {
        let RangeContent::Projection(projection) = content else {
            return Err(storage_types::StorageError::internal(
                "GSI state lacks hydrated content",
            ));
        };
        let crate::gsi::HydratedProjection { image, revision } = *projection;
        let (key, size) = range_evidence(table, bound, physical, &image)?;
        let row = storage_provider::GsiProjectionRow {
            key: key.clone(),
            image,
            revision,
        };
        Ok((row, key, size))
    }
}
