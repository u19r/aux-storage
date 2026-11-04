//! Stream provider for DynamoDB-compatible change streams.
use async_trait::async_trait;
use storage_types::{DurationSeconds, StreamItemId, StreamName, UserStreamName};

use crate::{
    constants::STREAM_LIMITS,
    errors::{StreamError, StreamResult, StreamValidationKind},
    newtypes::CursorName,
    system_stream::{
        SystemStreamLineage, SystemStreamPage, SystemStreamPageRequest, SystemStreamSplitRequest,
        SystemStreamSplitResult,
    },
    types::{CursorPage, CursorPosition, Stream, StreamCursor, StreamPage, StreamPartitioningMode},
};

/// Validate that a limit is between 1 and 1000 inclusive
pub fn validate_limit(limit: u32) -> StreamResult<()> {
    STREAM_LIMITS
        .validate(limit)
        .map_err(|_| StreamError::validation(StreamValidationKind::InvalidLimit))
        .map(|_| ())
}

#[async_trait]
pub trait StreamProvider: Send + Sync {
    /// Initialize the stream storage backend
    async fn initialize_stream(&self) -> StreamResult<()>;

    /// Create a new stream with optional TTL
    async fn create_stream(
        &self,
        stream_name: UserStreamName,
        ttl_seconds: Option<DurationSeconds>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName>;

    /// Delete a stream and all its items and cursors
    async fn delete_stream(&self, stream_name: UserStreamName) -> StreamResult<()>;

    /// Get stream information
    async fn get_stream(&self, stream_name: UserStreamName) -> StreamResult<Option<Stream>>;

    /// Append an item to the stream
    async fn append_item(
        &self,
        stream_name: StreamName,
        item_data: &[u8],
        partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId>;

    /// Read items forward (chronological order - oldest first)
    async fn read_forward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage>;

    /// Read items backward (reverse chronological order - newest first)
    async fn read_backward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage>;

    /// Read one logical SystemStream page.  Providers that do not own a
    /// logical scope/frontier inherit the fail-closed contract; the SQLite
    /// reference provider implements the transaction and issued-token catalog
    /// behind this boundary.
    async fn get_system_stream_page(
        &self,
        _request: SystemStreamPageRequest,
    ) -> StreamResult<SystemStreamPage> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical lineage is unavailable from this backend",
        )))
    }

    /// Return the provider's logical lineage catalog.  This is primarily a
    /// conformance/readiness seam; public records contain positions, never
    /// physical provider identifiers.
    async fn system_stream_lineage_catalog(
        &self,
        _source_scope: storage_types::SystemStreamScope,
    ) -> StreamResult<Vec<SystemStreamLineage>> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical lineage is unavailable from this backend",
        )))
    }

    /// Return the database-owned logical SystemStream scope. This is a
    /// discovery seam for internal clients; records still validate the scope
    /// supplied by each page request.
    async fn system_stream_scope(&self) -> StreamResult<storage_types::SystemStreamScope> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical scope is unavailable from this backend",
        )))
    }

    /// Atomically split one logical SystemStream lineage.
    ///
    /// A provider must compare the complete request with the durable
    /// operation receipt before reading or writing split state. An exact
    /// receipt is returned as `Replayed`; an unknown operation with any
    /// partial child publication is rejected. A fresh operation must use one
    /// native transaction that records the receipt before applying effects,
    /// seals the exact active parent with a scope/generation/lineage write
    /// fence, creates both child catalog rows and initialized sequences, and
    /// publishes private routing/topology.
    /// Existing checkpoints remain immutable and a sealed parent is never
    /// appendable. The result may contain only logical identities and must be
    /// returned after the transaction commits; page requests are unchanged.
    async fn split_system_stream(
        &self,
        _request: SystemStreamSplitRequest,
    ) -> StreamResult<SystemStreamSplitResult> {
        Err(StreamError::from(storage_types::StorageError::unsupported(
            "SystemStream logical split is unavailable from this backend",
        )))
    }

    /// Create a cursor at the specified position
    async fn create_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        position: CursorPosition,
    ) -> StreamResult<()>;

    /// Delete a cursor
    async fn delete_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<()>;

    /// Read from a cursor without advancing the cursor position
    async fn read_from_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        limit: u32,
    ) -> StreamResult<CursorPage>;

    /// Advance the cursor position to a specific item
    async fn advance_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
        to_item_id: StreamItemId,
    ) -> StreamResult<()>;

    /// Get cursor information
    async fn get_cursor(
        &self,
        stream_name: StreamName,
        cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>>;
}
