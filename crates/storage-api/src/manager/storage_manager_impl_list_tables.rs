use http_error::HttpApiError;
use storage::Tables;
use storage_types::{ListTablesRequest, ListTablesResponse, TableName};

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn list_tables_internal(
        &self,
        request: ListTablesRequest,
    ) -> Result<Response, HttpApiError> {
        let limit = request.limit;
        let (table_names, last_evaluated_table_name) = if let Some(limit) = limit {
            let limit_usize = limit as usize;
            let mut collected: Vec<TableName> = Vec::new();
            let mut exclusive_start_table_name = request.exclusive_start_table_name;

            loop {
                let (tables, last_evaluated) = self
                    .db()
                    .list_tables(Some(limit), exclusive_start_table_name)
                    .await?;

                if tables.is_empty() {
                    break;
                }

                for table in tables {
                    if Tables::should_hide_from_list_tables(&table.table_name) {
                        continue;
                    }
                    if collected.len() >= limit_usize {
                        break;
                    }
                    collected.push(table.table_name);
                }

                if collected.len() >= limit_usize {
                    break;
                }

                let Some(next_start) = last_evaluated else {
                    break;
                };
                exclusive_start_table_name = Some(next_start);
            }

            let last_evaluated_table_name = if collected.len() >= limit_usize {
                collected.last().cloned()
            } else {
                None
            };

            (collected, last_evaluated_table_name)
        } else {
            let (tables, last_evaluated_table_name) = self
                .db()
                .list_tables(None, request.exclusive_start_table_name)
                .await?;

            let table_names: Vec<TableName> = tables
                .into_iter()
                .filter_map(|table| {
                    if Tables::should_hide_from_list_tables(&table.table_name) {
                        None
                    } else {
                        Some(table.table_name)
                    }
                })
                .collect();

            let last_visible = table_names.last().cloned();
            let last_evaluated_table_name = match last_evaluated_table_name {
                Some(name) if Tables::should_hide_from_list_tables(&name) => last_visible,
                other => other,
            };

            (table_names, last_evaluated_table_name)
        };

        let response = ListTablesResponse {
            table_names,
            last_evaluated_table_name,
        };

        Ok(Response::ListTables(response))
    }
}
