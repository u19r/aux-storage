//! Static projections share the native range scanner without runtime modes.

use storage_provider::{PrimaryStateRow, RangeReadItem};
use storage_types::{BoundTableInfo, ItemKey, StorageError, StorageResult};

use crate::{SqlRow, item_write};

pub(crate) struct RangeContent {
    pub row: SqlRow,
    pub stored: Box<storage_types::StoredWireItem>,
}

pub(crate) trait RangeProjection {
    type Item: Send;
    const COLUMNS: &'static str;
    const EVALUATED_BYTES: &'static str = "__aux_evaluated_bytes";

    fn decode(
        table: &BoundTableInfo,
        content: RangeContent,
        offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<Self::Item>;
}

pub(crate) struct PublicImage;

impl RangeProjection for PublicImage {
    type Item = RangeReadItem;
    const COLUMNS: &'static str = "item_value";

    fn decode(
        _table: &BoundTableInfo,
        content: RangeContent,
        _offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<RangeReadItem> {
        let RangeContent { stored, .. } = content;
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
    const COLUMNS: &'static str = crate::constants::WRITE_COLUMNS;

    fn decode(
        table: &BoundTableInfo,
        content: RangeContent,
        offset: usize,
        key: Option<&ItemKey>,
    ) -> StorageResult<PrimaryStateRow> {
        let RangeContent { row, stored } = content;
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
