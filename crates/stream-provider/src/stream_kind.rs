//! One stream-kind vocabulary for rows and the operations that use them.

use storage_types::{StorageResult, StreamName};

use crate::{StreamAttempt, StreamCatalogLookup, StreamError, StreamPartitioningMode};

/// The payload differs between an owned stream target, a physical read, and
/// a planned write. The four stream kinds stay consistent across those uses.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum StreamKind<Row, System = Row> {
    System(System),
    Table(Row),
    Item(Row),
    User(Row),
}

pub type StreamRowKind = StreamKind<StreamName>;
pub type StreamWriteKind<Row = StreamName> = StreamKind<Row>;

impl<Row> StreamKind<Row> {
    pub fn value(&self) -> &Row {
        match self {
            Self::System(value) | Self::Table(value) | Self::Item(value) | Self::User(value) => {
                value
            }
        }
    }

    pub fn into_value(self) -> Row {
        match self {
            Self::System(value) | Self::Table(value) | Self::Item(value) | Self::User(value) => {
                value
            }
        }
    }
}

impl StreamRowKind {
    pub(crate) fn stream_name(&self) -> &StreamName {
        self.value()
    }

    pub(crate) fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }
}

pub(crate) async fn read_partitioning_mode<A: StreamAttempt>(
    attempt: &mut A,
    stream: &StreamRowKind,
) -> StorageResult<StreamPartitioningMode> {
    if !stream.is_user() {
        return Ok(StreamPartitioningMode::Single);
    }
    let stream_name = stream.stream_name();
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
