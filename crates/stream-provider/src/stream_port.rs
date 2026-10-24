//! Native-attempt capabilities lent to shared stream operations.

use std::future::Future;

use futures::future::BoxFuture;
use storage_common::partition::{
    PartitionInfo, PartitionLoadSampleRecord, ResolvedPartitionFamily,
};
use storage_types::{
    AttemptMode, Clock, StorageError, StorageResult, StreamItemId, StreamName,
    SystemStreamCheckpoint, SystemStreamCheckpointToken,
};

use crate::{
    CursorName, StreamCatalogLookup, StreamCatalogRecord, StreamCursor, StreamItem, StreamReadKind,
    StreamReadRequest, StreamReadResult, StreamRowKind, StreamRowPage, StreamRowValue,
    SystemStreamCheckpointIssue, SystemStreamCheckpointReceipt, SystemStreamHistory,
    SystemStreamHistoryRead, SystemStreamHistoryReadPage, SystemStreamSplitOperationId,
    SystemStreamSplitReceipt,
};

/// The selected physical stream layout's behavior when a caller requests
/// key-ordered partitioning. A family may normalize the request only when
/// that matches its established public behavior.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StreamPartitioningSupport {
    KeyOrdered,
    NormalizeToSingle,
}

/// How a single-partition user append obtains its public item ID.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StreamItemIdAllocation {
    AttemptAllocated,
    CommitVersionstamped,
}

pub trait StreamAttempt:
    Send + infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    /// The physical layout is a property of the attempt family, not a flag
    /// supplied by a domain caller or stored on each request.
    const PARTITIONING_SUPPORT: StreamPartitioningSupport;

    fn store_append_receipt(
        &mut self,
        receipt: &crate::StreamAppendReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn load_append_receipt(
        &mut self,
        operation: crate::StreamAppendOperationId,
    ) -> impl Future<Output = StorageResult<Option<crate::StreamAppendReceipt>>> + Send;

    fn read_stream_record(
        &mut self,
        lookup: StreamCatalogLookup<'_>,
    ) -> impl Future<Output = StorageResult<Option<StreamCatalogRecord>>> + Send;

    fn write_stream_record(
        &mut self,
        name: &str,
        record: StreamCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_stream(
        &mut self,
        request: StreamReadKind<'_>,
    ) -> impl Future<Output = StorageResult<StreamReadResult>> + Send;

    /// Read the Single user-row layout through an inclusive caller-owned
    /// bound, with an exclusive continuation. The caller must resolve the
    /// exact fresh stream incarnation and verify its committed prefix.
    fn read_user_page_through(
        &mut self,
        stream: &StreamName,
        request: &StreamReadRequest,
        through: StreamItemId,
    ) -> impl Future<Output = StorageResult<StreamRowPage>> + Send;

    /// Read one addressed Single user row only when its ID is within the
    /// caller's verified inclusive bound; ordinary native frontiers differ.
    fn read_user_item_through(
        &mut self,
        stream: &StreamName,
        id: StreamItemId,
        through: StreamItemId,
    ) -> impl Future<Output = StorageResult<Option<StreamItem>>> + Send;

    /// Read the protected history and an optional checkpoint in one native
    /// batch. Both observations must belong to this attempt's snapshot.
    fn read_system_page_start(
        &mut self,
        token: Option<&SystemStreamCheckpointToken>,
    ) -> impl Future<
        Output = StorageResult<(SystemStreamHistory, Option<SystemStreamCheckpointReceipt>)>,
    > + Send;

    fn read_system_history_pages(
        &mut self,
        reads: &[SystemStreamHistoryRead],
    ) -> impl Future<Output = StorageResult<Vec<SystemStreamHistoryReadPage>>> + Send;

    /// Protect the topology and read an optional idempotency receipt in one
    /// native attempt before the shared split decision.
    fn read_system_split_state(
        &mut self,
        operation_id: &SystemStreamSplitOperationId,
    ) -> impl Future<Output = StorageResult<(SystemStreamHistory, Option<SystemStreamSplitReceipt>)>>
    + Send;

    /// Persist the shared split decision and all physical topology rows on
    /// this attempt. A partial publication must abort with the attempt.
    fn write_system_split(
        &mut self,
        receipt: &SystemStreamSplitReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Issue a logical-history checkpoint with its token and frontier in this
    /// same native attempt. Ordinary stream cursors cannot express its lineage.
    fn write_system_stream_checkpoint(
        &mut self,
        issue: &SystemStreamCheckpointIssue,
        checkpoint: &SystemStreamCheckpoint,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Store the caller's fully addressed row in this native attempt. The
    /// caller owns its ID and a fresh compatible stream generation.
    fn store_user_item(
        &mut self,
        row: StreamItem,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Initialize the existing per-stream sequence in this attempt. The
    /// caller must own a fresh generation; this does not adopt existing rows.
    fn initialize_user_sequence(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Delete only this stream generation's physical sequence.
    fn delete_user_sequence(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    /// Allocate an ID and write its row in this native attempt. Readers must
    /// protect cursors from earlier IDs whose transactions can still commit.
    fn append_attempt_allocated_user_item(
        &mut self,
        row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<StreamItemId>> + Send;

    fn load_partition_family(
        &mut self,
        _stream: &storage_types::StreamName,
    ) -> impl Future<Output = StorageResult<ResolvedPartitionFamily>> + Send {
        // Single-only families do not have ordered partition authority.
        self.reject_settlement();
        std::future::ready(Err(StorageError::internal(
            "stream backend has no partition family",
        )))
    }

    fn store_partition_sample(
        &mut self,
        _stream: &storage_types::StreamName,
        _sample: &PartitionLoadSampleRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        // Single-only families do not record ordered partition samples.
        self.reject_settlement();
        std::future::ready(Err(StorageError::internal(
            "stream backend has no partition samples",
        )))
    }

    /// Only a catalog whose family reports KeyOrdered can reach this call.
    /// Families that normalize to Single reject a persisted KeyOrdered row.
    fn append_partitioned_user_item(
        &mut self,
        _partition: &PartitionInfo,
        _row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<[u8; 2]>> + Send {
        self.reject_settlement();
        std::future::ready(Err(StorageError::internal(
            "stream catalog requests unsupported key-ordered partitioning",
        )))
    }

    fn append_attempt_allocated_partitioned_user_item(
        &mut self,
        _partition: &PartitionInfo,
        _row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<StreamItemId>> + Send {
        self.reject_settlement();
        std::future::ready(Err(StorageError::internal(
            "stream backend cannot allocate a keyed item ID in its attempt",
        )))
    }

    fn append_versionstamped_user_item(
        &mut self,
        _row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<[u8; 2]>> + Send {
        // Attempt-allocated families have no native commit versionstamp.
        self.reject_settlement();
        std::future::ready(Err(StorageError::internal(
            "stream backend cannot append a versionstamped user item",
        )))
    }

    fn read_cursor(
        &mut self,
        stream: &StreamRowKind,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<Option<StreamCursor>>> + Send;

    fn write_cursor(
        &mut self,
        cursor: StreamCursor,
        expected_position: Option<StreamItemId>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_cursor(
        &mut self,
        stream: &StreamRowKind,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

/// One customer stream operation owns one native attempt.
pub trait StreamPort: Clock + Send + Sync {
    type Attempt<'native>: StreamAttempt
    where Self: 'native;

    type AppendCapture: for<'native> TableAppendCapture<Self::Attempt<'native>>;

    fn table_append_capture(&self) -> Self::AppendCapture;

    fn partitioning_support(&self) -> StreamPartitioningSupport;

    fn item_id_allocation(&self) -> StreamItemIdAllocation {
        StreamItemIdAllocation::AttemptAllocated
    }

    fn with_stream_attempt<R, F>(
        &self,
        mode: AttemptMode,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static;

    fn with_topology_attempt<R, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.with_stream_attempt(AttemptMode::ReadWrite, operation)
    }

    fn with_committed_stream_attempt<R, F>(
        &self,
        operation: F,
    ) -> impl Future<Output = StorageResult<(R, Option<[u8; 10]>)>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        async move {
            self.with_stream_attempt(AttemptMode::ReadWrite, operation)
                .await
                .map(|value| (value, None))
        }
    }
}

/// Owned capture capability travels into the same native attempt as the append.
/// It can outlive a canceled waiter without retaining a borrowed provider.
pub trait TableAppendCapture<A: StreamAttempt>: Clone + Send + Sync + 'static {
    type Captured: Send;

    fn capture(
        &self,
        attempt: &mut A,
        request: &crate::TableAppendRequest,
        append: &crate::UserStreamAppend,
    ) -> impl Future<Output = StorageResult<Option<Self::Captured>>> + Send;

    fn store_response(
        &self,
        attempt: &mut A,
        captured: Self::Captured,
        response: crate::StreamAppendAllocation,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn reconcile(
        &self,
        attempt: &mut A,
        request: &crate::TableAppendRequest,
    ) -> impl Future<Output = StorageResult<crate::TableAppendReconciliation>> + Send;
}

#[derive(Clone, Copy)]
pub struct NoTableAppendCapture;

impl<A: StreamAttempt> TableAppendCapture<A> for NoTableAppendCapture {
    type Captured = std::convert::Infallible;

    async fn capture(
        &self,
        _attempt: &mut A,
        _request: &crate::TableAppendRequest,
        _append: &crate::UserStreamAppend,
    ) -> StorageResult<Option<Self::Captured>> {
        Ok(None)
    }

    async fn store_response(
        &self,
        _attempt: &mut A,
        captured: Self::Captured,
        _response: crate::StreamAppendAllocation,
    ) -> StorageResult<()> {
        match captured {}
    }

    async fn reconcile(
        &self,
        _attempt: &mut A,
        _request: &crate::TableAppendRequest,
    ) -> StorageResult<crate::TableAppendReconciliation> {
        Ok(crate::TableAppendReconciliation::Unresolved)
    }
}
