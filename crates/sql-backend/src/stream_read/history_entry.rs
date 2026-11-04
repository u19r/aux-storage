//! Exact immutable history reads preserve the operation's native snapshot.

use storage_types::{
    ItemHistoryEntry, ItemHistoryRecordHeader, ItemHistoryReference, StorageError, StorageResult,
    StreamItemId, TableHistoryId, TableHistoryState,
};
use stream_provider::{ItemHistoryReadAttempt, ItemHistoryReadBatch, StreamRowKind};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlValue,
    catalog::SqlCatalogAttempt,
};

impl<A: SqlAttempt, L: SqlLayout> ItemHistoryReadAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_item_history_table_states(
        &mut self,
        histories: &[TableHistoryId],
    ) -> StorageResult<Vec<Option<TableHistoryState>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = load_history_table_states(
            catalog.attempt,
            catalog.engine,
            catalog.protection,
            histories,
        )
        .await;
        operation.complete(result)
    }

    async fn load_item_history_headers(
        &mut self,
        read: ItemHistoryReadBatch<'_>,
    ) -> StorageResult<Vec<Option<ItemHistoryRecordHeader>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result =
            load_history_headers(catalog.attempt, catalog.engine, catalog.protection, read).await;
        operation.complete(result)
    }

    async fn load_item_history_entries(
        &mut self,
        read: ItemHistoryReadBatch<'_>,
    ) -> StorageResult<Vec<Option<ItemHistoryEntry>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result =
            load_history_entries(catalog.attempt, catalog.engine, catalog.protection, read).await;
        operation.complete(result)
    }
}

async fn load_history_table_states<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    histories: &[TableHistoryId],
) -> StorageResult<Vec<Option<TableHistoryState>>> {
    if histories.is_empty() {
        return Ok(Vec::new());
    }
    let pages = attempt
        .query_batch(
            Vec::from(super::sql_statements::read_history_table_states(
                engine, histories,
            )),
            protection,
            storage_provider::CATALOG_READ_BYTES,
        )
        .await?;
    if pages.len() != 2
        || pages.iter().any(|page| page.more)
        || pages.iter().map(|page| page.rows.len()).sum::<usize>() > histories.len()
    {
        return Err(invalid_history_read());
    }
    let mut states = histories
        .iter()
        .map(|history| (*history, None))
        .collect::<std::collections::HashMap<_, _>>();
    for row in pages.into_iter().flat_map(|page| page.rows) {
        let (history, state) = decode_history_table_state(row)?;
        let slot = states.get_mut(&history).ok_or_else(invalid_history_read)?;
        if slot.replace(state).is_some() {
            return Err(invalid_history_read());
        }
    }
    Ok(histories
        .iter()
        .map(|history| states.get(history).cloned().flatten())
        .collect())
}

fn decode_history_table_state(row: SqlRow) -> StorageResult<(TableHistoryId, TableHistoryState)> {
    let SqlValue::Text(id) = row.value(ColumnIndex::new(0))? else {
        return Err(invalid_history_read());
    };
    let history = uuid::Uuid::parse_str(id)
        .map_err(|_| invalid_history_read())?
        .into();
    let state = match row.value(ColumnIndex::new(1))? {
        SqlValue::Integer(0) => {
            let (SqlValue::Text(name), SqlValue::Text(schema)) = (
                row.value(ColumnIndex::new(2))?,
                row.value(ColumnIndex::new(3))?,
            ) else {
                return Err(invalid_history_read());
            };
            let schema = storage_types::TableHistorySchema::try_from((
                storage_types::TableName::new(name),
                serde_json::from_str(schema)?,
            ))?;
            TableHistoryState::Active(std::sync::Arc::new(schema))
        }
        SqlValue::Integer(1) => TableHistoryState::Deleting,
        _ => return Err(invalid_history_read()),
    };
    Ok((history, state))
}

async fn load_history_headers<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    read: ItemHistoryReadBatch<'_>,
) -> StorageResult<Vec<Option<ItemHistoryRecordHeader>>> {
    let statements = read
        .references
        .iter()
        .map(|reference| super::sql_statements::read_history_header(engine, reference))
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt
        .query_batch(statements, protection, read.maximum_bytes)
        .await?;
    if pages.len() != read.references.len() {
        return Err(invalid_history_read());
    }
    pages
        .into_iter()
        .zip(read.references)
        .map(|(page, reference)| {
            take_exact_row(page)?
                .map(|row| decode_exact_header(row, reference))
                .transpose()
        })
        .collect()
}

async fn load_history_entries<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    protection: ReadProtection,
    read: ItemHistoryReadBatch<'_>,
) -> StorageResult<Vec<Option<ItemHistoryEntry>>> {
    let statements = read
        .references
        .iter()
        .map(|reference| {
            let stream = StreamRowKind::Item(reference.address().physical_stream_name()?);
            Ok(super::sql_statements::read_item(
                engine,
                &stream,
                StreamItemId::from(reference.version()),
            ))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt
        .query_batch(statements, protection, read.maximum_bytes)
        .await?;
    if pages.len() != read.references.len() {
        return Err(invalid_history_read());
    }
    let mut remaining = read.maximum_bytes.get();
    pages
        .into_iter()
        .zip(read.references)
        .map(|(page, reference)| {
            take_exact_row(page)?
                .map(|row| decode_exact_entry(row, reference, &mut remaining))
                .transpose()
        })
        .collect()
}

fn take_exact_row(mut page: SqlQueryPage) -> StorageResult<Option<SqlRow>> {
    if page.more || page.rows.len() > 1 {
        return Err(invalid_history_read());
    }
    Ok(page.rows.pop())
}

fn decode_exact_header(
    row: SqlRow,
    reference: &ItemHistoryReference,
) -> StorageResult<ItemHistoryRecordHeader> {
    let header = super::history_metadata::decode_history_header_row(row, reference.address())?;
    if header.metadata().reference() != reference {
        return Err(invalid_history_read());
    }
    Ok(header)
}

fn decode_exact_entry(
    row: SqlRow,
    reference: &ItemHistoryReference,
    remaining: &mut usize,
) -> StorageResult<ItemHistoryEntry> {
    let (id, bytes) = super::row::decode_stream_address(row)?;
    if id != StreamItemId::from(reference.version()) {
        return Err(invalid_history_read());
    }
    let maximum = *remaining;
    *remaining = remaining.checked_sub(bytes.len()).ok_or_else(|| {
        StorageError::from(storage_types::InfrastructureResultKind::NativeCapacityRejected)
    })?;
    stream_provider::decode_item_history_entry(&bytes, reference, maximum)
}

fn invalid_history_read() -> StorageError {
    StorageError::internal("Invalid SQL exact item-history read")
}
