use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::{
    CatalogNamePage, CatalogStreamPage, DynamoAttempt, DynamoPort, ListStreamsAttempt,
    ListTablesAttempt, TableMetadataCache, list_streams_in_attempt, list_tables_in_attempt,
    resolve_table_in_attempt,
};
use storage_types::{
    AttemptMode, ListStreamsRequest, ListStreamsResponse, ListTablesRequest, ListTablesResponse,
    StorageError, StorageResult, TableName, TimestampMillis,
};

use super::runtime::RegionalDynamo;
use crate::journal::JournalStore;

struct VisibleCatalog<'a, A> {
    attempt: &'a mut A,
    journal: &'a JournalStore,
    cache: &'a TableMetadataCache,
}

impl<A: DynamoAttempt + PhysicalRecordAttempt> VisibleCatalog<'_, A> {
    async fn visible(&mut self, name: &TableName) -> StorageResult<bool> {
        if !storage_provider::is_public_table_name(name) {
            return Ok(false);
        }
        let (table, _) = resolve_table_in_attempt(self.attempt, self.cache, name).await?;
        self.journal.is_listed(self.attempt, &table).await
    }
}

impl<A: DynamoAttempt + PhysicalRecordAttempt> ListTablesAttempt for VisibleCatalog<'_, A> {
    async fn list_name_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> StorageResult<CatalogNamePage> {
        let mut cursor = after.cloned();
        let mut names = Vec::new();
        loop {
            let remaining = limit
                .checked_sub(u32::try_from(names.len()).map_err(|_| invalid_page())?)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(invalid_page)?;
            let page = self
                .attempt
                .list_name_page(cursor.as_ref(), remaining)
                .await?;
            if page.names.len() > remaining as usize || (page.more && page.names.is_empty()) {
                return Err(invalid_page());
            }
            for name in page.names {
                if cursor
                    .as_ref()
                    .is_some_and(|previous| previous.as_ref() >= name.as_ref())
                {
                    return Err(invalid_page());
                }
                cursor = Some(name.clone());
                if self.visible(&name).await? {
                    names.push(name);
                }
            }
            if !page.more || names.len() == limit as usize {
                return Ok(CatalogNamePage {
                    names,
                    more: page.more,
                });
            }
        }
    }
}

impl<A: DynamoAttempt + PhysicalRecordAttempt> ListStreamsAttempt for VisibleCatalog<'_, A> {
    async fn read_stream_entry(
        &mut self,
        name: &TableName,
    ) -> StorageResult<Option<TimestampMillis>> {
        let entry = self.attempt.read_stream_entry(name).await?;
        if entry.is_some() && !self.visible(name).await? {
            return Ok(None);
        }
        Ok(entry)
    }

    async fn list_stream_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> StorageResult<CatalogStreamPage> {
        let mut cursor = after.cloned();
        let mut entries = Vec::new();
        loop {
            let remaining = limit
                .checked_sub(u32::try_from(entries.len()).map_err(|_| invalid_page())?)
                .filter(|remaining| *remaining > 0)
                .ok_or_else(invalid_page)?;
            let page = self
                .attempt
                .list_stream_page(cursor.as_ref(), remaining)
                .await?;
            if page.entries.len() > remaining as usize || (page.more && page.entries.is_empty()) {
                return Err(invalid_page());
            }
            for (name, created_at) in page.entries {
                if cursor
                    .as_ref()
                    .is_some_and(|previous| previous.as_ref() >= name.as_ref())
                {
                    return Err(invalid_page());
                }
                cursor = Some(name.clone());
                if self.visible(&name).await? {
                    entries.push((name, created_at));
                }
            }
            if !page.more || entries.len() == limit as usize {
                return Ok(CatalogStreamPage {
                    entries,
                    more: page.more,
                });
            }
        }
    }
}

impl<P: DynamoPort> RegionalDynamo<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(super) async fn list_tables(
        &self,
        request: ListTablesRequest,
    ) -> StorageResult<ListTablesResponse> {
        let journal = Arc::clone(&self.journal);
        let cache = self.port.metadata_cache();
        self.port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    list_tables_in_attempt(
                        &mut VisibleCatalog {
                            attempt,
                            journal: &journal,
                            cache: &cache,
                        },
                        &request,
                    )
                    .await
                })
            })
            .await
    }

    pub(super) async fn list_streams(
        &self,
        request: ListStreamsRequest,
    ) -> StorageResult<ListStreamsResponse> {
        let journal = Arc::clone(&self.journal);
        let cache = self.port.metadata_cache();
        self.port
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    list_streams_in_attempt(
                        &mut VisibleCatalog {
                            attempt,
                            journal: &journal,
                            cache: &cache,
                        },
                        &request,
                    )
                    .await
                })
            })
            .await
    }
}

fn invalid_page() -> StorageError {
    StorageError::internal("regional catalog page did not advance within its bound")
}
