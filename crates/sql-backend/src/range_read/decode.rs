//! Decode physical SQL range rows into shared Dynamo item maps.

use storage_provider::{RangeAdmission, RangeReadPage};
use storage_types::{
    AttributeValue, ItemKey, KeyAttributes, LogicalItemSize, StorageError, StorageResult, WireItem,
};

use super::{
    projection::{RangeContent, RangeProjection},
    sql_statements::{GsiRowShape, KeyColumn, RangeStatementInput},
};
use crate::{ColumnIndex, SqlLayout, SqlQueryPage, SqlValue};

pub(super) enum RangeDecode {
    Exhausted,
    PhysicalMore,
    BudgetStop,
}

pub(super) fn decode_range_rows<P: RangeProjection>(
    engine: impl SqlLayout,
    input: &RangeStatementInput<'_, '_>,
    fetched: (SqlQueryPage, Vec<Option<storage_types::StoredWireItem>>),
    result: &mut RangeReadPage<P::Item>,
) -> StorageResult<RangeDecode> {
    let RangeStatementInput {
        table,
        page,
        columns,
        ..
    } = input;
    let (fetched, hydrated) = fetched;
    let has_more = fetched.more;
    for (row, stored) in fetched.rows.into_iter().zip(hydrated) {
        if result.progress.evaluated_count >= page.budget.max_items {
            return Ok(RangeDecode::BudgetStop);
        }
        let (item_key, keys) = decode_row_key(engine, input, &row)?;
        if let Some(stored) = &stored
            && !stored
                .item
                .has_verified_keys(&item_key.key_attributes(&table.info)?)?
        {
            return Err(StorageError::internal(
                "SQL retained content differs from physical range evidence",
            ));
        }
        let (stored, size) = match input.gsi {
            Some(shape) => {
                let item = project_gsi_row(shape, stored, &keys)?;
                let size = item.logical_size()?;
                (
                    storage_types::StoredWireItem {
                        item,
                        indexers: Vec::new(),
                    },
                    size,
                )
            }
            None => {
                let stored = stored.ok_or_else(|| {
                    StorageError::internal("SQL range row lacks retained content")
                })?;
                let size = match row.value(ColumnIndex::new(columns.len()))? {
                    SqlValue::Integer(value) => LogicalItemSize::try_from(*value)?,
                    _ => {
                        return Err(StorageError::internal(
                            "SQL range row has invalid evaluated bytes",
                        ));
                    }
                };
                if stored.item.logical_size()? != size {
                    return Err(StorageError::internal(
                        "SQL retained content differs from physical range evidence",
                    ));
                }
                (stored, size)
            }
        };
        if page.budget.admit(&mut result.progress, item_key, size)? == RangeAdmission::Stop {
            return Ok(RangeDecode::BudgetStop);
        }
        let item = P::decode(
            table,
            RangeContent {
                row,
                stored: Box::new(stored),
            },
            columns.len() + 1,
            result.progress.last_evaluated_key.as_ref(),
        )?;
        result.items.push(item);
    }
    Ok(if has_more {
        RangeDecode::PhysicalMore
    } else {
        RangeDecode::Exhausted
    })
}

/// The GSI projection of one row: `KEYS_ONLY` from the key columns alone,
/// `INCLUDE` and `ALL` from the item image. Size, filter and response use
/// the projected item, as for a DynamoDB index read.
fn project_gsi_row(
    shape: &GsiRowShape,
    stored: Option<storage_types::StoredWireItem>,
    keys: &KeyAttributes,
) -> StorageResult<WireItem> {
    match (shape, stored) {
        (GsiRowShape::KeysOnly, _) => {
            let data = serde_json::to_vec(keys)
                .map_err(|_| StorageError::internal("SQL GSI key image encoding failed"))?;
            Ok(WireItem::dynamo_json(data))
        }
        (GsiRowShape::Include(fields), Some(stored)) => {
            let fields = fields.iter().map(String::as_str).collect::<Vec<_>>();
            stored.item.project_wire_attributes(&fields)
        }
        (GsiRowShape::All, Some(stored)) => Ok(stored.item),
        (GsiRowShape::Include(_) | GsiRowShape::All, None) => {
            Err(StorageError::internal("SQL GSI row lacks its item image"))
        }
    }
}

fn decode_row_key(
    engine: impl SqlLayout,
    input: &RangeStatementInput<'_, '_>,
    row: &crate::SqlRow,
) -> StorageResult<(ItemKey, KeyAttributes)> {
    let RangeStatementInput {
        table,
        bound,
        columns,
        ..
    } = input;
    let mut key = KeyAttributes::new();
    for (ordinal, column) in columns.iter().enumerate() {
        let value = decode_key(engine, column, row.value(ColumnIndex::new(ordinal))?)?;
        if key
            .get(&column.logical)
            .is_some_and(|previous| previous != &value)
        {
            return Err(StorageError::internal(
                "SQL physical range key columns disagree",
            ));
        }
        key.insert(column.logical.clone(), value);
    }
    let item_key = if let Some(index) = bound.index.as_ref() {
        ItemKey::from_key_schema_for_index(
            table.info.table_name.clone(),
            &table.info.key_schema,
            &index.index_name,
            &index.key_schema,
            &key,
        )?
        .ok_or_else(|| StorageError::internal("SQL GSI row is missing an index key"))?
    } else {
        ItemKey::from_key_schema(table.info.table_name.clone(), &table.info.key_schema, &key)?
    };
    Ok((item_key, key))
}

fn decode_key(
    engine: impl SqlLayout,
    column: &KeyColumn,
    value: &SqlValue,
) -> StorageResult<AttributeValue> {
    crate::item_read::decode_key_value(engine, column.kind, value)
}
