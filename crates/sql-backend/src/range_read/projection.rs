//! Static projections share the native range scanner without runtime modes.

use storage_provider::{PrimaryStateRow, RangeReadItem};
use storage_types::{BoundTableInfo, ItemKey, StorageError, StorageResult};

use crate::{ColumnIndex, SqlRow, SqlValue, item_write};

pub(super) enum RangeContent {
    Image {
        row: SqlRow,
        stored: Box<storage_types::StoredWireItem>,
    },
    Tombstone(SqlRow),
}

pub(super) trait RangeProjection {
    type Item: Send;
    const COLUMNS: &'static str;
    const IMAGE_COLUMN: Option<usize> = Some(0);
    const EVALUATED_BYTES: &'static str = "__aux_evaluated_bytes";
    const TOMBSTONE_PREDICATE: &'static str = "__aux_tombstone = 0";

    fn decode(
        table: &BoundTableInfo,
        content: RangeContent,
        offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<Self::Item>;
}

pub(super) struct PublicImage;

impl RangeProjection for PublicImage {
    type Item = RangeReadItem;
    const COLUMNS: &'static str = "item_descriptor";

    fn decode(
        _table: &BoundTableInfo,
        content: RangeContent,
        _offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<RangeReadItem> {
        let RangeContent::Image { stored, .. } = content else {
            return Err(StorageError::internal("Range image lacks retained content"));
        };
        Ok(RangeReadItem {
            key: key.cloned().ok_or_else(|| {
                StorageError::internal("native range image lacks its physical key")
            })?,
            item: stored.item,
        })
    }
}

pub(super) struct PrimaryState;

impl RangeProjection for PrimaryState {
    type Item = PrimaryStateRow;
    const COLUMNS: &'static str = item_write::WRITE_COLUMNS;

    fn decode(
        table: &BoundTableInfo,
        content: RangeContent,
        offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<PrimaryStateRow> {
        let RangeContent::Image { row, stored } = content else {
            return Err(StorageError::internal(
                "Primary state lacks verified content",
            ));
        };
        let mut values = row.into_values();
        if offset > values.len() {
            return Err(StorageError::internal("SQL primary range row is truncated"));
        }
        values.drain(..offset);
        let observation = item_write::decode_live(SqlRow::new(values), *stored)?;
        Ok(PrimaryStateRow {
            key: key
                .ok_or_else(|| StorageError::internal("primary range row lacks its key"))?
                .key_attributes(&table.info)?,
            state: observation,
        })
    }
}

pub(super) struct GsiState;

impl RangeProjection for GsiState {
    type Item = storage_provider::GsiProjectionRow;
    const COLUMNS: &'static str = "item_descriptor, __aux_item_version";

    fn decode(
        table: &BoundTableInfo,
        content: RangeContent,
        offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<Self::Item> {
        let RangeContent::Image { row, stored } = content else {
            return Err(StorageError::internal("GSI state lacks retained content"));
        };
        let revision = match row.value(ColumnIndex::new(offset + 1))? {
            SqlValue::Integer(value) => storage_types::ItemRevision::try_from(*value)?,
            _ => return Err(StorageError::internal("SQL GSI scan has invalid revision")),
        };
        let image = stored.item;
        let key = key
            .ok_or_else(|| StorageError::internal("SQL GSI scan lacks physical key"))?
            .clone();
        if !image.has_verified_keys(&key.key_attributes(&table.info)?)? {
            return Err(StorageError::internal(
                "SQL GSI image differs from physical key",
            ));
        }
        Ok(storage_provider::GsiProjectionRow {
            key,
            image,
            revision,
        })
    }
}

pub(super) struct GsiTombstone;

impl RangeProjection for GsiTombstone {
    type Item = (ItemKey, storage_types::ItemRevision);
    const COLUMNS: &'static str = "__aux_item_version";
    const IMAGE_COLUMN: Option<usize> = None;
    const EVALUATED_BYTES: &'static str = "8";
    const TOMBSTONE_PREDICATE: &'static str = "__aux_tombstone = 1";
    fn decode(
        _table: &BoundTableInfo,
        content: RangeContent,
        offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<Self::Item> {
        let RangeContent::Tombstone(row) = content else {
            return Err(StorageError::internal(
                "GSI tombstone has unexpected image content",
            ));
        };
        let revision = match row.value(ColumnIndex::new(offset))? {
            SqlValue::Integer(value) => storage_types::ItemRevision::try_from(*value)?,
            _ => {
                return Err(StorageError::internal(
                    "invalid SQL projection tombstone revision",
                ));
            }
        };
        Ok((
            key.ok_or_else(|| StorageError::internal("SQL projection tombstone lacks key"))?
                .clone(),
            revision,
        ))
    }
}
