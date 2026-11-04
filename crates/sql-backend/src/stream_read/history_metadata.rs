//! Bounded item-history metadata, read without transferring image payloads.

use storage_provider::maintenance::{ItemHistoryMetadataAttempt, ItemHistoryMetadataPage};
use storage_types::{
    ItemHistoryAddress, ItemStreamVersion, StorageError, StorageResult, StreamItemId,
};
use stream_provider::StreamRowValue;

use crate::{
    ReadProtection, SqlAttempt, SqlLayout, SqlQueryPage, SqlRow, SqlValue,
    catalog::SqlCatalogAttempt, constants::STREAM_READ_BYTES,
};

impl<A: SqlAttempt, L: SqlLayout> ItemHistoryMetadataAttempt for SqlCatalogAttempt<'_, A, L> {
    async fn load_table_history_namespace(
        &mut self,
        history: storage_types::TableHistoryId,
        after: Option<&[u8]>,
    ) -> StorageResult<Option<storage_provider::maintenance::TableHistoryNamespace>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = super::history_namespace::load_namespace(
            catalog.attempt,
            catalog.engine,
            history,
            after,
        )
        .await;
        operation.complete(result)
    }

    async fn load_item_history_metadata_pages(
        &mut self,
        histories: &[ItemHistoryAddress],
    ) -> StorageResult<Vec<ItemHistoryMetadataPage>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = load_history_metadata_pages(catalog.attempt, catalog.engine, histories).await;
        operation.complete(result)
    }
}

async fn load_history_metadata_pages<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    histories: &[ItemHistoryAddress],
) -> StorageResult<Vec<ItemHistoryMetadataPage>> {
    let statements = histories
        .iter()
        .map(|history| super::sql_statements::read_history_metadata(engine, history))
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt
        .query_batch(statements, ReadProtection::Conflict, STREAM_READ_BYTES)
        .await?;
    if pages.len() != histories.len() {
        return Err(invalid_metadata_page());
    }
    pages
        .into_iter()
        .zip(histories)
        .map(|(page, history)| decode_metadata_page(page, history))
        .collect()
}

fn decode_metadata_page(
    mut page: SqlQueryPage,
    history: &ItemHistoryAddress,
) -> StorageResult<ItemHistoryMetadataPage> {
    if page.rows.len() > ItemHistoryMetadataPage::MAX_ENTRIES + 1 {
        return Err(invalid_metadata_page());
    }
    let reached_tail = !page.more && page.rows.len() <= ItemHistoryMetadataPage::MAX_ENTRIES;
    page.rows.truncate(ItemHistoryMetadataPage::MAX_ENTRIES);
    let entries = page
        .rows
        .into_iter()
        .map(|row| {
            decode_history_header_row(row, history)
                .map(storage_types::ItemHistoryRecordHeader::into_metadata)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(ItemHistoryMetadataPage {
        entries,
        reached_tail,
    })
}

pub(super) fn decode_history_header_row(
    row: SqlRow,
    history: &ItemHistoryAddress,
) -> StorageResult<storage_types::ItemHistoryRecordHeader> {
    let [id, length, prefix] = row
        .into_values()
        .try_into()
        .map_err(|_| invalid_metadata_page())?;
    let (SqlValue::Blob(id), SqlValue::Integer(length), SqlValue::Blob(prefix)) =
        (id, length, prefix)
    else {
        return Err(invalid_metadata_page());
    };
    let id = StreamItemId::try_from(id.as_slice()).map_err(|_| invalid_metadata_page())?;
    let version = ItemStreamVersion::from(id);
    if StreamItemId::from(version) != id {
        return Err(invalid_metadata_page());
    }
    let length = usize::try_from(length).map_err(|_| invalid_metadata_page())?;
    StreamRowValue::decode_history_header_prefix(&prefix, length, (history, version))
}

fn invalid_metadata_page() -> StorageError {
    StorageError::internal("Invalid SQL item-history metadata page")
}
