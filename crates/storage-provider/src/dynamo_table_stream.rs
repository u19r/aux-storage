//! Table stream policy and exact resolution are shared across physical
//! families.

use std::{future::Future, num::NonZeroU32};

use storage_types::{
    BoundTableInfo, GetStreamRecordsRequest, GetStreamRecordsResponse, StorageError, StorageResult,
    TableName, TableStreamCheckpoint, TableStreamFrontier, TimestampMillis,
};
use stream_provider::{StreamAttempt, TableHistoryIndexAttempt};

use crate::{
    CachePublication, CatalogAttempt, TableMetadataCache,
    dynamodb_streams::record::DynamoStreamRecord, resolve_table_in_attempt,
    table_history_read::TableHistoryReadState,
};

pub trait TableHistoryRetentionAttempt {
    /// Writers request conflict protection; readers use their coherent native
    /// snapshot.
    fn load_table_history_retention(
        &mut self,
        table: &BoundTableInfo,
        protection: storage_types::ReadProtection,
    ) -> impl Future<Output = StorageResult<Option<crate::StreamTrimState>>> + Send;
}

pub trait DynamoTableStreamAttempt:
    CatalogAttempt + StreamAttempt + TableHistoryIndexAttempt + TableHistoryRetentionAttempt
{
}

pub(crate) struct DynamoTableStreamPage {
    pub records: Vec<DynamoStreamRecord>,
    pub checkpoint: Option<String>,
    pub frontier: TableStreamFrontier,
    pub sealed_end: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum TableStreamImageSelection {
    Snapshot,
    DynamoDbImages,
}

pub(crate) struct TableStreamReadOptions<'a> {
    pub now: TimestampMillis,
    pub selection: TableStreamImageSelection,
    pub shard_id: Option<&'a str>,
}

pub async fn table_stream_checkpoint_in_attempt<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    target: &storage_types::TableHistoryTarget,
) -> StorageResult<(TableStreamCheckpoint, Option<CachePublication>)> {
    let (table, publication) = resolve_table_in_attempt(attempt, cache, &target.table_name).await?;
    if table.history_id != target.history_id {
        return Err(StorageError::table_not_found(&target.table_name));
    }
    Ok((
        table_stream_checkpoint_from_table(attempt, &table).await?,
        publication,
    ))
}

pub async fn table_stream_checkpoint_from_table<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<TableStreamCheckpoint> {
    require_enabled_table_stream(table)?;
    TableHistoryReadState::load(attempt, table)
        .await?
        .checkpoint(table)
}

pub async fn get_table_stream_records_in_attempt<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &GetStreamRecordsRequest,
    now: TimestampMillis,
) -> StorageResult<(GetStreamRecordsResponse, Option<CachePublication>)> {
    let name = request
        .table_name()
        .ok_or_else(|| StorageError::raw_validation("TableName is required"))?;
    let (table, publication) = resolve_table_in_attempt(attempt, cache, name).await?;
    Ok((
        get_table_stream_records_from_table(attempt, &table, request, now).await?,
        publication,
    ))
}

pub async fn get_table_stream_records_from_table<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    request: &GetStreamRecordsRequest,
    now: TimestampMillis,
) -> StorageResult<GetStreamRecordsResponse> {
    let page = read_table_stream_page(
        attempt,
        table,
        request,
        TableStreamReadOptions {
            now,
            selection: TableStreamImageSelection::Snapshot,
            shard_id: None,
        },
    )
    .await?;
    Ok(GetStreamRecordsResponse::table(
        table.info.table_name.clone(),
        page.records.into_iter().map(Into::into).collect(),
        page.checkpoint,
        page.frontier,
    ))
}

pub(crate) async fn read_table_stream_page<A: DynamoTableStreamAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
    request: &GetStreamRecordsRequest,
    options: TableStreamReadOptions<'_>,
) -> StorageResult<DynamoTableStreamPage> {
    require_table_stream_request(table, request)?;
    let limit = NonZeroU32::new(request.limit().unwrap_or(100))
        .filter(|limit| limit.get() <= 1000)
        .ok_or_else(|| StorageError::raw_validation("Limit must be between 1 and 1000"))?;
    let state = TableHistoryReadState::load(attempt, table).await?;
    let frontier = state.frontier(attempt, table).await?;
    let assembly = crate::table_history_page::TableHistoryPage::prepare(
        table,
        &state,
        request.last_evaluated_key(),
        options,
    )?;
    assembly.read(attempt, limit, frontier).await
}

fn require_table_stream_request(
    table: &BoundTableInfo,
    request: &GetStreamRecordsRequest,
) -> StorageResult<()> {
    if request
        .table_history_id()
        .is_some_and(|id| id != table.history_id)
    {
        return Err(StorageError::table_not_found(&table.info.table_name));
    }
    if request.table_name().map(TableName::dynamodb_resource_name)
        != Some(table.info.table_name.dynamodb_resource_name())
    {
        return Err(StorageError::internal("Stream read table binding differs"));
    }
    require_enabled_table_stream(table)
}

pub fn require_enabled_table_stream(table: &BoundTableInfo) -> StorageResult<()> {
    if table.info.has_enabled_stream() {
        return Ok(());
    }
    Err(StorageError::raw_validation(format!(
        "Table '{}' does not have streams enabled",
        table.info.table_name
    )))
}
