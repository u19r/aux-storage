//! Native key-column fill pages on the caller's protected SQL attempt.

use storage_backfill::{GsiBuildTarget, GsiPageLimit};
use storage_provider::{
    CATALOG_READ_BYTES,
    maintenance::{NativeGsiFill, NativeGsiPage, NativeGsiRow, NativeGsiRowVersion},
};
use storage_types::{BoundTableInfo, KeyAttributes, StorageError, StorageResult, storage_serde};

use super::{scan::decode_source_key, sql_statements};
use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlValue,
    catalog::SqlCatalogAttempt,
};

pub(super) async fn load_fill_page<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    source: (&GsiBuildTarget, Option<&Vec<u8>>, GsiPageLimit),
) -> StorageResult<NativeGsiPage> {
    let (target, after, limit) = source;
    let index = table
        .binding
        .gsi(&target.index_name)
        .ok_or_else(|| StorageError::internal("Native GSI binding is absent"))?;
    let after = after
        .map(|bytes| storage_serde::from_bytes::<KeyAttributes>(bytes))
        .transpose()?;
    let mut page = catalog
        .attempt
        .query(
            sql_statements::render_source_scan(
                catalog.engine,
                sql_statements::ScanSourceStatement {
                    table,
                    after: after.as_ref(),
                    limit,
                    index: Some(index),
                },
            )?,
            ReadProtection::Snapshot,
            CATALOG_READ_BYTES,
        )
        .await?;
    page.more |= page.rows.len() > usize::from(limit);
    page.rows.truncate(usize::from(limit));
    crate::item_read::select_image_prefix(table, &mut page, 0, CATALOG_READ_BYTES.get())?;
    let images = crate::item_read::decode_item_images(
        catalog,
        page.rows
            .iter()
            .map(|row| {
                Ok((
                    table,
                    Some(row.value(ColumnIndex::new(0))?),
                    table.max_indexers,
                ))
            })
            .collect::<StorageResult<Vec<_>>>()?,
        CATALOG_READ_BYTES.get(),
    )?;
    let rows = page
        .rows
        .into_iter()
        .zip(images)
        .map(|(row, image)| {
            FillDecoder {
                engine: catalog.engine,
                table,
                index,
            }
            .decode_row(row, image)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(NativeGsiPage {
        rows,
        more: page.more,
    })
}

struct FillDecoder<'a, L> {
    engine: L,
    table: &'a BoundTableInfo,
    index: &'a storage_types::BoundGsi,
}

impl<L: SqlLayout> FillDecoder<'_, L> {
    fn decode_row(
        &self,
        row: crate::SqlRow,
        image: Option<storage_types::StoredWireItem>,
    ) -> StorageResult<NativeGsiRow> {
        let Self {
            engine,
            table,
            index,
        } = *self;
        let values = row.into_values();
        let primary_end = 1 + table.key_schema.len();
        let native_columns = usize::from(engine.engine() == SqlEngine::Postgres) * 2;
        if values.len() != primary_end + native_columns + index.columns.len() {
            return Err(StorageError::internal("Native GSI source width differs"));
        }
        let key = decode_source_key(engine, table, &values[1..primary_end])?;
        let image =
            image.ok_or_else(|| StorageError::internal("Native GSI source image is absent"))?;
        if !image.item.has_verified_keys(&key)? {
            return Err(StorageError::internal("Native GSI source key differs"));
        }
        let version = if engine.engine() == SqlEngine::Postgres {
            let (SqlValue::Text(ctid), SqlValue::Text(xmin)) =
                (&values[primary_end], &values[primary_end + 1])
            else {
                return Err(StorageError::internal("Native GSI row version is invalid"));
            };
            NativeGsiRowVersion::Postgres {
                ctid: ctid.clone(),
                xmin: xmin.clone(),
            }
        } else {
            NativeGsiRowVersion::Serialized
        };
        let mut current = KeyAttributes::new();
        for (column, value) in index
            .columns
            .iter()
            .zip(&values[primary_end + native_columns..])
        {
            if !matches!(value, SqlValue::Null) {
                current.insert(
                    column.attribute.clone(),
                    crate::item_read::decode_key_value(engine, column.kind, value)?,
                );
            }
        }
        Ok(NativeGsiRow {
            key,
            current,
            version,
            item: image.item,
        })
    }
}

pub(super) async fn store_fills<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    target: &GsiBuildTarget,
    fills: Vec<NativeGsiFill>,
) -> StorageResult<Vec<bool>> {
    let index = table
        .binding
        .gsi(&target.index_name)
        .ok_or_else(|| StorageError::internal("Native GSI binding is absent"))?;
    let mut statements = Vec::new();
    let mut slots = Vec::new();
    let mut changed = vec![true; fills.len()];
    for (slot, fill) in fills.into_iter().enumerate() {
        if fill.values.is_some() {
            statements.push(sql_statements::fill_row_statement(
                catalog.engine,
                table,
                index,
                fill,
            )?);
            slots.push(slot);
        }
    }
    if statements.is_empty() {
        return Ok(changed);
    }
    let counts = catalog.attempt.execute_batch(statements).await?;
    if counts.len() != slots.len() || counts.iter().any(|count| *count > 1) {
        return Err(StorageError::internal("Native GSI fill row counts differ"));
    }
    for (slot, count) in slots.into_iter().zip(counts) {
        changed[slot] = count == 1;
    }
    Ok(changed)
}
