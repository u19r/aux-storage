//! Stream-enabled catalog listing is one bounded ordered index walk.

use std::future::Future;

use storage_types::{
    ListStreamsRequest, ListStreamsResponse, StorageError, StorageResult, StreamDescriptor,
    TableName, TimestampMillis, stream_identity,
};

use crate::constants::INTERNAL_TABLE_NAMES;

/// The physical index contains only currently stream-enabled tables.
pub struct CatalogStreamPage {
    pub entries: Vec<(TableName, TimestampMillis)>,
    pub more: bool,
}

pub trait ListStreamsAttempt: Send {
    fn read_stream_entry(
        &mut self,
        name: &TableName,
    ) -> impl Future<Output = StorageResult<Option<TimestampMillis>>> + Send;

    fn list_stream_page(
        &mut self,
        after: Option<&TableName>,
        limit: u32,
    ) -> impl Future<Output = StorageResult<CatalogStreamPage>> + Send;
}

pub async fn list_streams_in_attempt<A: ListStreamsAttempt>(
    attempt: &mut A,
    request: &ListStreamsRequest,
) -> StorageResult<ListStreamsResponse> {
    let limit = request.limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(StorageError::validation("Limit must be between 1 and 100"));
    }
    let after = match request.exclusive_start_stream_arn.as_deref() {
        Some(arn) => {
            let name = stream_arn_table_name(arn)?;
            let created_at = attempt
                .read_stream_entry(&name)
                .await?
                .ok_or_else(|| StorageError::validation("Invalid ExclusiveStartStreamArn"))?;
            if stream_identity(&name, created_at).0 != arn {
                return Err(StorageError::validation("Invalid ExclusiveStartStreamArn"));
            }
            Some(name)
        }
        None => None,
    };
    if let Some(name) = request.table_name.as_ref() {
        if after.as_ref().is_some_and(|after| after != name) {
            return Ok(ListStreamsResponse {
                streams: Vec::new(),
                last_evaluated_stream_arn: None,
            });
        }
        let streams = if after.is_some() || INTERNAL_TABLE_NAMES.contains(&name.as_ref()) {
            Vec::new()
        } else {
            attempt
                .read_stream_entry(name)
                .await?
                .map(|created_at| descriptor(name.clone(), created_at))
                .into_iter()
                .collect()
        };
        return Ok(ListStreamsResponse {
            streams,
            last_evaluated_stream_arn: None,
        });
    }

    let mut cursor = after;
    let mut streams = Vec::with_capacity(limit as usize + 1);
    while streams.len() <= limit as usize {
        let remaining = limit + 1 + INTERNAL_TABLE_NAMES.len() as u32 - streams.len() as u32;
        let page = attempt.list_stream_page(cursor.as_ref(), remaining).await?;
        if page.entries.len() > remaining as usize || (page.more && page.entries.is_empty()) {
            return Err(StorageError::internal(
                "stream catalog page did not advance",
            ));
        }
        for (name, created_at) in page.entries {
            if cursor
                .as_ref()
                .is_some_and(|previous| name.as_ref() <= previous.as_ref())
            {
                return Err(StorageError::internal(
                    "stream catalog names are not strictly ordered",
                ));
            }
            cursor = Some(name.clone());
            if !INTERNAL_TABLE_NAMES.contains(&name.as_ref()) {
                streams.push(descriptor(name, created_at));
            }
        }
        if !page.more {
            break;
        }
    }
    let has_next = streams.len() > limit as usize;
    streams.truncate(limit as usize);
    let last_evaluated_stream_arn = has_next
        .then(|| streams.last().map(|stream| stream.stream_arn.clone()))
        .flatten();
    Ok(ListStreamsResponse {
        streams,
        last_evaluated_stream_arn,
    })
}

fn descriptor(table_name: TableName, created_at: TimestampMillis) -> StreamDescriptor {
    let (stream_arn, stream_label) = stream_identity(&table_name, created_at);
    StreamDescriptor {
        stream_arn,
        table_name,
        stream_label,
    }
}

fn stream_arn_table_name(arn: &str) -> StorageResult<TableName> {
    let (_, suffix) = arn
        .split_once(":table/")
        .ok_or_else(|| StorageError::validation("Invalid ExclusiveStartStreamArn"))?;
    let (name, _) = suffix
        .split_once("/stream/")
        .ok_or_else(|| StorageError::validation("Invalid ExclusiveStartStreamArn"))?;
    if name.is_empty() {
        return Err(StorageError::validation("Invalid ExclusiveStartStreamArn"));
    }
    Ok(TableName::new(name))
}
