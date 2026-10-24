//! Decode physical SQL range rows into shared Dynamo item maps.

use storage_provider::{RangeAdmission, RangeReadPage};
use storage_types::{
    AttributeValue, ItemKey, KeyAttributes, LogicalItemSize, StorageError, StorageResult,
};

use super::{
    projection::{RangeContent, RangeProjection},
    sql_statements::{KeyColumn, RangeStatementInput},
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
    if fetched.rows.len() != hydrated.len() {
        return Err(StorageError::internal("SQL range hydration count differs"));
    }
    let has_more = fetched.more;
    for (row, stored) in fetched.rows.into_iter().zip(hydrated) {
        if result.progress.evaluated_count >= page.budget.max_items {
            return Ok(RangeDecode::BudgetStop);
        }
        let (item_key, size) = decode_row_evidence(engine, input, &row)?;
        if let Some(stored) = &stored
            && (!stored
                .item
                .has_verified_keys(&item_key.key_attributes(&table.info)?)?
                || stored.item.logical_size()? != size)
        {
            return Err(StorageError::internal(
                "SQL retained content differs from physical range evidence",
            ));
        }
        if page.budget.admit(&mut result.progress, item_key, size)? == RangeAdmission::Stop {
            return Ok(RangeDecode::BudgetStop);
        }
        let item = P::decode(
            table,
            match stored {
                Some(stored) => RangeContent::Image {
                    row,
                    stored: Box::new(stored),
                },
                None => RangeContent::Tombstone(row),
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

fn decode_row_evidence(
    engine: impl SqlLayout,
    input: &RangeStatementInput<'_, '_>,
    row: &crate::SqlRow,
) -> StorageResult<(ItemKey, LogicalItemSize)> {
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
    let size = match row.value(ColumnIndex::new(columns.len()))? {
        SqlValue::Integer(value) => LogicalItemSize::try_from(*value)?,
        _ => {
            return Err(StorageError::internal(
                "SQL range row has invalid evaluated bytes",
            ));
        }
    };
    Ok((item_key, size))
}

fn decode_key(
    engine: impl SqlLayout,
    column: &KeyColumn,
    value: &SqlValue,
) -> StorageResult<AttributeValue> {
    crate::item_read::decode_key_value(engine, column.kind, value)
}
