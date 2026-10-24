//! One ListTables pagination and visibility decision for SQL and KV.

use std::future::Future;

use storage_types::{
    ListTablesRequest, ListTablesResponse, MAX_LIST_TABLES_LIMIT, StorageError, StorageResult,
    TableName,
};

use crate::constants::INTERNAL_TABLE_NAMES;

/// Read ordered catalog names inside the caller's native read attempt.
/// `more` means the physical byte budget stopped this page before the SQL
/// statement or KV range was exhausted.
pub struct CatalogNamePage {
    pub names: Vec<TableName>,
    pub more: bool,
}

/// One byte-bounded physical page, including private system tables.
pub trait ListTablesAttempt: Send {
    fn list_name_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> impl Future<Output = StorageResult<CatalogNamePage>> + Send;
}

/// The three internal tables are API policy, not SQL or KV layout policy.
pub fn is_public_table_name(name: &TableName) -> bool {
    !INTERNAL_TABLE_NAMES.contains(&name.as_ref())
}

async fn collect_names<A: ListTablesAttempt>(
    attempt: &mut A,
    after: Option<&TableName>,
    limit: u32,
) -> StorageResult<Vec<TableName>> {
    let mut cursor = after.cloned();
    let mut names = Vec::with_capacity(limit as usize);
    while names.len() < limit as usize {
        let remaining = limit - names.len() as u32;
        let page = {
            let _call = metrics_facade::begin_database_call("dynamo.list_tables.name_page");
            attempt.list_name_page(cursor.as_ref(), remaining).await?
        };
        if page.names.len() > remaining as usize || (page.more && page.names.is_empty()) {
            return Err(StorageError::internal(
                "catalog name page did not advance within its limit",
            ));
        }
        for name in page.names {
            if cursor
                .as_ref()
                .is_some_and(|previous| name.as_ref() <= previous.as_ref())
            {
                return Err(StorageError::internal(
                    "catalog names are not strictly ordered",
                ));
            }
            cursor = Some(name.clone());
            names.push(name);
        }
        if !page.more {
            break;
        }
    }
    Ok(names)
}

/// Extra physical names cover the internal tables and one
/// continuation candidate. Every supported backend uses the same ordering.
pub async fn list_tables_in_attempt<A: ListTablesAttempt>(
    attempt: &mut A,
    request: &ListTablesRequest,
) -> StorageResult<ListTablesResponse> {
    let limit = request.limit.unwrap_or(MAX_LIST_TABLES_LIMIT);
    if limit == 0 || limit > MAX_LIST_TABLES_LIMIT {
        return Err(StorageError::validation(format!(
            "Limit must be between 1 and {MAX_LIST_TABLES_LIMIT}"
        )));
    }
    let physical_limit = limit + INTERNAL_TABLE_NAMES.len() as u32 + 1;
    let names = collect_names(
        attempt,
        request.exclusive_start_table_name.as_ref(),
        physical_limit,
    )
    .await?;
    let mut visible = names
        .into_iter()
        .filter(is_public_table_name)
        .collect::<Vec<_>>();
    let has_next = visible.len() > limit as usize;
    visible.truncate(limit as usize);
    let last_evaluated_table_name = has_next.then(|| visible.last().cloned()).flatten();
    Ok(ListTablesResponse {
        table_names: visible,
        last_evaluated_table_name,
    })
}
