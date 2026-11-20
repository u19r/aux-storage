//! User streams: the library entry over the default connection. Each method
//! is one admitted stream operation; the stream provider validates the
//! request and resolves the user stream name on the operation's own native
//! attempt.

use storage_types::{DurationSeconds, StorageResult, StreamItemId, StreamName, UserStreamName};
use stream_provider::{
    CursorName, CursorPage, CursorPosition, ReadDirection, STREAM_LIMITS, Stream, StreamAddress,
    StreamCursor, StreamError, StreamPage, StreamPartitioningMode,
};

use crate::{
    admission::AdmissionClass,
    database_manager::{constants::ROUTED_DEFAULT_CONNECTION_ID, core::DatabaseManager},
};

impl DatabaseManager {
    /// Create a user stream; returns its internal name.
    pub async fn create_user_stream(
        &self,
        name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StorageResult<StreamName> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .create_stream(name, ttl_seconds, partitioning_mode)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn get_user_stream(&self, name: UserStreamName) -> StorageResult<Option<Stream>> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            move |stream| async move {
                stream
                    .get_stream(name)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn delete_user_stream(&self, name: UserStreamName) -> StorageResult<()> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .delete_stream(name)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn append_user_stream_item(
        &self,
        name: UserStreamName,
        data: Vec<u8>,
        partition_key: Option<String>,
    ) -> StorageResult<StreamItemId> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .append_item(name, &data, partition_key.as_deref())
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    /// One page in `direction`; `limit` `None` reads the default page (100).
    pub async fn read_user_stream(
        &self,
        name: UserStreamName,
        direction: ReadDirection,
        exclusive_start: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StorageResult<StreamPage> {
        let limit = limit.unwrap_or(STREAM_LIMITS.default_limit());
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            move |stream| async move {
                let address = StreamAddress::User(name);
                match direction {
                    ReadDirection::Forward => {
                        stream.read_forward(address, exclusive_start, limit).await
                    }
                    ReadDirection::Backward => {
                        stream.read_backward(address, exclusive_start, limit).await
                    }
                }
                .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn create_user_stream_cursor(
        &self,
        name: UserStreamName,
        cursor: CursorName,
        position: CursorPosition,
    ) -> StorageResult<()> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .create_cursor(StreamAddress::User(name), cursor, position)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn get_user_stream_cursor(
        &self,
        name: UserStreamName,
        cursor: CursorName,
    ) -> StorageResult<Option<StreamCursor>> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::PointRead,
            move |stream| async move {
                stream
                    .get_cursor(StreamAddress::User(name), cursor)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn delete_user_stream_cursor(
        &self,
        name: UserStreamName,
        cursor: CursorName,
    ) -> StorageResult<()> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .delete_cursor(StreamAddress::User(name), cursor)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    pub async fn advance_user_stream_cursor(
        &self,
        name: UserStreamName,
        cursor: CursorName,
        to_item_id: StreamItemId,
    ) -> StorageResult<()> {
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .advance_cursor(StreamAddress::User(name), cursor, to_item_id)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }

    /// Read a page from the cursor and advance it in the same attempt (a
    /// write, so it takes a write permit). `limit` `None` reads 100.
    pub async fn read_user_stream_cursor(
        &self,
        name: UserStreamName,
        cursor: CursorName,
        limit: Option<u32>,
    ) -> StorageResult<CursorPage> {
        let limit = limit.unwrap_or(STREAM_LIMITS.default_limit());
        self.run_stream_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::Write,
            move |stream| async move {
                stream
                    .read_from_cursor(StreamAddress::User(name), cursor, limit)
                    .await
                    .map_err(StreamError::into_storage_error)
            },
        )
        .await
    }
}
