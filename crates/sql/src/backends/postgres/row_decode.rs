use std::collections::HashMap;

use queue_provider::{
    MessageAttributeValue, MessageId, QueueError, QueueResult, QueueValidationKind,
};
use storage_types::{StorageResult, StoredTableInfo, StreamName, WireItem};
use stream_provider::{StreamDataType, StreamError, StreamItem, StreamResult};
use tokio_postgres::{Row, types::ToSql};

use crate::backends::postgres::{PostgresStorageProvider, sql_statements};

impl PostgresStorageProvider {
    #[expect(
        clippy::too_many_arguments,
        reason = "Unified paginated read needs schemas, predicates, and pagination inputs"
    )]
    pub(super) async fn load_paginated_wire_items(
        &self,
        physical_name: &str,
        table_info: &StoredTableInfo,
        primary_key_schema: &[storage_types::KeySchemaElement],
        secondary_key_schema: Option<&[storage_types::KeySchemaElement]>,
        where_clauses: &[String],
        bind_values: &[String],
        scan_forward: bool,
        effective_limit: u32,
    ) -> StorageResult<(Vec<WireItem>, bool)> {
        let select_projection = Self::build_select_projection_for_origin(
            table_info,
            primary_key_schema,
            secondary_key_schema,
        )?;
        let ordered_columns = Self::ordered_key_columns_for_origin(
            table_info,
            primary_key_schema,
            secondary_key_schema,
        )?;
        let direction = if scan_forward { "ASC" } else { "DESC" };
        let where_sql = (!where_clauses.is_empty()).then(|| where_clauses.join(" AND "));
        let order_by = if ordered_columns.is_empty() {
            None
        } else {
            Some(
                ordered_columns
                    .iter()
                    .map(|column| format!("{} {direction}", column.column))
                    .collect::<Vec<_>>()
                    .join(", "),
            )
        };
        let sql = sql_statements::select_ordered_rows(
            &select_projection,
            physical_name,
            where_sql.as_deref(),
            order_by.as_deref(),
            effective_limit,
        );

        let params: Vec<&(dyn ToSql + Sync)> = bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let rows = {
            let client = self.acquire_client("query_table").await?;
            let _connection_hold = self.connection_hold_timer("query_table");
            client
                .query(&sql, &params)
                .await
                .map_err(|err| Self::map_postgres_error("load ordered rows", err))?
        };

        let mut items: Vec<WireItem> = rows
            .into_iter()
            .map(|row| {
                Self::row_to_wire_item_for_origin(
                    &row,
                    table_info,
                    primary_key_schema,
                    secondary_key_schema,
                )
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let has_more = items.len() > effective_limit as usize;
        if has_more {
            items.truncate(effective_limit as usize);
        }
        Ok((items, has_more))
    }

    pub(super) fn parse_stream_item_id(
        value: &str,
        field: &str,
    ) -> StreamResult<storage_types::StreamItemId> {
        value.parse::<storage_types::StreamItemId>().map_err(|_| {
            StreamError::validation(format!("invalid stream item id in {field}: {value}"))
        })
    }

    pub(super) fn parse_message_id(value: &str, field: &str) -> QueueResult<MessageId> {
        value.parse::<MessageId>().map_err(|_| {
            QueueError::validation_with_detail(
                QueueValidationKind::MessageNotFound,
                format!("invalid message id in {field}: {value}"),
            )
        })
    }

    pub(super) fn parse_stream_item_row(
        row: &Row,
        stream_name: &StreamName,
    ) -> StreamResult<StreamItem> {
        let id: String = row
            .try_get("item_id")
            .map_err(|err| Self::map_stream_error("decode stream item_id", err))?;
        let data: Vec<u8> = row
            .try_get("data")
            .map_err(|err| Self::map_stream_error("decode stream data", err))?;
        let created_at: i64 = row
            .try_get("created_at")
            .map_err(|err| Self::map_stream_error("decode stream created_at", err))?;
        let data_type: i32 = row
            .try_get("data_type")
            .map_err(|err| Self::map_stream_error("decode stream data_type", err))?;
        Ok(StreamItem {
            id: Self::parse_stream_item_id(&id, "item_id")?,
            stream_name: Some(stream_name.clone()),
            data,
            data_type: StreamDataType::from(data_type),
            created_at: created_at.into(),
        })
    }

    pub(super) fn parse_queue_message_attributes(
        raw: Option<String>,
    ) -> QueueResult<Option<HashMap<String, MessageAttributeValue>>> {
        match raw {
            Some(raw) if !Self::is_json_null_like(&raw) => serde_json::from_str(&raw)
                .map(Some)
                .map_err(QueueError::from),
            _ => Ok(None),
        }
    }
}
