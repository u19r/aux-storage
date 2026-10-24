//! Protected SQL base-table scan for one GSI build page.

use storage_backfill::{GsiScanPageEnd, GsiScanSourcePage, GsiScannedSourceItem};
use storage_provider::{CATALOG_READ_BYTES, maintenance::GsiScanSource};
use storage_types::{
    BoundTableInfo, ItemRevision, KeyAttributes, StorageError, StorageResult, StoredWireItem,
    storage_serde,
};

use super::sql_statements;
use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlValue,
    stream_history::capture_high,
};

pub(crate) async fn read_scan_source_page<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut crate::catalog::SqlCatalogAttempt<'_, A, L>,
    source: GsiScanSource<'_>,
) -> StorageResult<GsiScanSourcePage<Vec<u8>>> {
    let engine = catalog.engine;
    let GsiScanSource {
        table,
        target: _,
        after,
        limit,
    } = source;
    let start = after
        .map(|bytes| storage_serde::from_bytes::<KeyAttributes>(bytes))
        .transpose()?;
    let page = catalog
        .attempt
        .query(
            sql_statements::scan_source_statement(engine, table, start.as_ref(), limit)?,
            ReadProtection::Conflict,
            CATALOG_READ_BYTES,
        )
        .await?;
    let count = usize::try_from(u32::from(limit))
        .map_err(|_| StorageError::internal("GSI scan limit exceeds platform capacity"))?;
    let (items, next, more) = decode_scan_page(catalog, table, page, count).await?;
    let end = if more {
        GsiScanPageEnd::Continue(
            next.ok_or_else(|| StorageError::internal("SQL GSI scan page has no cursor"))?,
        )
    } else {
        GsiScanPageEnd::Complete(capture_high(catalog.attempt, engine).await?)
    };
    Ok(GsiScanSourcePage {
        table: table.info.clone(),
        expected_after: after.cloned(),
        end,
        items,
    })
}

type ScanRows = (Vec<GsiScannedSourceItem>, Option<Vec<u8>>, bool);

async fn decode_scan_page<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut crate::catalog::SqlCatalogAttempt<'_, A, L>,
    table: &BoundTableInfo,
    mut page: SqlQueryPage,
    count: usize,
) -> StorageResult<ScanRows> {
    page.more |= page.rows.len() > count;
    page.rows.truncate(count);
    crate::item_read::select_image_prefix(table, &mut page, 0, CATALOG_READ_BYTES.get())?;
    let requests = page
        .rows
        .iter()
        .map(|row| {
            Ok((
                table,
                Some(row.value(crate::ColumnIndex::new(0))?),
                table.max_indexers,
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let hydrated =
        crate::item_read::hydrate_item_images(catalog, requests, CATALOG_READ_BYTES.get()).await?;
    let more = page.more;
    let mut items = Vec::with_capacity(page.rows.len().min(count));
    let mut next = None;
    for (row, stored) in page.rows.into_iter().zip(hydrated) {
        let (item, key) = decode_scan_row(
            catalog.engine,
            table,
            row,
            stored.ok_or_else(|| StorageError::internal("Missing GSI source content"))?,
        )?;
        next = Some(storage_serde::to_bytes(&key)?);
        items.push(item);
    }
    Ok((items, next, more))
}

fn decode_scan_row(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    row: SqlRow,
    stored: StoredWireItem,
) -> StorageResult<(GsiScannedSourceItem, KeyAttributes)> {
    let mut values = row.into_values();
    if values.len() != 3 + table.key_schema.len() {
        return Err(StorageError::internal(
            "SQL GSI source row has invalid width",
        ));
    }
    let physical = decode_source_key(engine, table, &values[3..])?;
    let [_descriptor, revision, indexers]: [SqlValue; 3] = values
        .drain(..3)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| StorageError::internal("SQL GSI source row is truncated"))?;
    let (SqlValue::Integer(revision), SqlValue::Text(indexers)) = (revision, indexers) else {
        return Err(StorageError::internal(
            "SQL GSI source row has invalid types",
        ));
    };
    let indexers: Vec<String> = serde_json::from_str(&indexers)?;
    if indexers != stored.indexers {
        return Err(StorageError::internal(
            "SQL GSI source indexers differ from retained content",
        ));
    }
    if !stored.item.has_verified_keys(&physical)? {
        return Err(StorageError::internal(
            "SQL GSI source content differs from physical key",
        ));
    }
    let key = table.info.table_key_from_wire(&stored.item)?;
    Ok((
        GsiScannedSourceItem {
            item: stored.item,
            indexers: stored.indexers,
            revision: ItemRevision::try_from(revision)?,
        },
        key,
    ))
}

fn decode_source_key(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    values: &[SqlValue],
) -> StorageResult<KeyAttributes> {
    let mut key = KeyAttributes::new();
    for (element, value) in storage_types::KeySchemaElement::ordered(&table.key_schema).zip(values)
    {
        let kind = table
            .attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == element.attribute_name)
            .map_or(storage_types::KeyAttributeType::S, |definition| {
                definition.attribute_type
            });
        key.insert(
            element.attribute_name.clone(),
            crate::item_read::decode_key_value(engine, kind, value)?,
        );
    }
    Ok(key)
}
