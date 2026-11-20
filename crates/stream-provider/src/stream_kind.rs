//! Resolve ordinary user and explicit table-append streams in one native
//! attempt.
use storage_types::{StorageResult, StreamName};

use crate::{
    StreamAddress, StreamAttempt, StreamCatalogLookup, StreamError, StreamPartitioningMode,
};

pub(crate) async fn resolve_stream_layout<A: StreamAttempt>(
    attempt: &mut A,
    stream: &StreamName,
) -> StorageResult<StreamPartitioningMode> {
    if let Some(history) = stream.table_history_id()? {
        let states = attempt.load_table_history_states(&[history]).await?;
        return match states.as_slice() {
            [Some(storage_types::TableHistoryState::Active(_))] => {
                Ok(StreamPartitioningMode::Single)
            }
            [None | Some(storage_types::TableHistoryState::Deleting)] => {
                Err(storage_types::StorageEnum::ResourceNotFound {
                    resource_type: "table history",
                    resource_id: uuid::Uuid::from_bytes(*history.as_ref()).to_string(),
                }
                .into())
            }
            _ => Err(storage_types::StorageError::internal(
                "History catalog returned another state count",
            )),
        };
    }
    let stream_name = stream;
    attempt
        .read_stream_record(StreamCatalogLookup::Id(stream_name))
        .await?
        .filter(|record| !record.deleted)
        .map(|record| record.stream.partitioning_mode)
        .ok_or_else(|| {
            StreamError::stream_not_found(String::from_utf8_lossy(stream_name.as_ref()))
                .into_storage_error()
        })
}

/// Resolve the addressed stream to its internal name and layout on this
/// native attempt. A user stream reads its catalog row by name (one read);
/// an internal name resolves as `resolve_stream_layout` does.
pub(crate) async fn resolve_stream<A: StreamAttempt>(
    attempt: &mut A,
    address: &StreamAddress,
) -> StorageResult<(StreamName, StreamPartitioningMode)> {
    match address {
        StreamAddress::Internal(stream) => {
            let partitioning_mode = resolve_stream_layout(attempt, stream).await?;
            Ok((stream.clone(), partitioning_mode))
        }
        StreamAddress::User(name) => attempt
            .read_stream_record(StreamCatalogLookup::Name(name.as_str()))
            .await?
            .filter(|record| !record.deleted)
            .map(|record| (record.stream.internal_id, record.stream.partitioning_mode))
            .ok_or_else(|| StreamError::stream_not_found(name.as_str()).into_storage_error()),
    }
}
