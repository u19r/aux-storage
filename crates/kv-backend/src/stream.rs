//! One KV stream attempt shared by catalog and row operations.

use std::future::Future;

use futures::future::BoxFuture;
use storage_common::partition::{
    PartitionInfo, PartitionLoadSampleRecord, ResolvedPartitionFamily,
};
use storage_types::{AttemptRequest, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    CursorName, StreamAttempt, StreamCatalogLookup, StreamCatalogRecord, StreamCursor, StreamItem,
    StreamItemIdAllocation, StreamPartitioningSupport, StreamPort, StreamReadKind,
    StreamReadRequest, StreamReadResult, StreamRowPage, StreamRowValue,
    SystemStreamHistoryReadPage,
};

use crate::{
    KvAttempt, KvBackend, KvStore, KvTopology, catalog::KvCatalogAttempt, stream_catalog,
    stream_cursor, stream_partition, stream_read, stream_split, stream_write,
};

impl<A: KvAttempt> StreamAttempt for KvCatalogAttempt<'_, A> {
    const PARTITIONING_SUPPORT: StreamPartitioningSupport = StreamPartitioningSupport::KeyOrdered;

    async fn store_append_receipt(
        &mut self,
        receipt: &stream_provider::StreamAppendReceipt,
    ) -> StorageResult<()> {
        stream_write::store_append_receipt(self.attempt, receipt).await
    }

    async fn load_append_receipt(
        &mut self,
        key: stream_provider::TableAppendReceiptKey,
    ) -> StorageResult<Option<stream_provider::StreamAppendReceipt>> {
        stream_write::load_append_receipt(self.attempt, self.protection, key).await
    }

    fn read_stream_record(
        &mut self,
        lookup: StreamCatalogLookup<'_>,
    ) -> impl Future<Output = StorageResult<Option<StreamCatalogRecord>>> + Send {
        stream_catalog::read_stream_record(self.attempt, self.protection, lookup)
    }

    fn write_stream_record(
        &mut self,
        name: &str,
        record: StreamCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_catalog::write_stream_record(self.attempt, name, record)
    }

    async fn read_stream(
        &mut self,
        request: StreamReadKind<'_>,
    ) -> StorageResult<StreamReadResult> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = stream_read::read_stream(catalog.attempt, catalog.protection, request).await;
        settlement.complete(result)
    }

    async fn read_user_page_through(
        &mut self,
        stream: &StreamName,
        request: &StreamReadRequest,
        through: StreamItemId,
    ) -> StorageResult<StreamRowPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = stream_read::read_user_page_through(catalog, stream, request, through).await;
        settlement.complete(result)
    }

    async fn read_user_item_through(
        &mut self,
        stream: &StreamName,
        id: StreamItemId,
        through: StreamItemId,
    ) -> StorageResult<Option<StreamItem>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = stream_read::read_user_item_through(catalog, stream, id, through).await;
        settlement.complete(result)
    }

    fn read_system_split_state(
        &mut self,
        _parent: &storage_types::SystemStreamLineageId,
        operation_id: &stream_provider::SystemStreamSplitOperationId,
    ) -> impl Future<
        Output = StorageResult<(
            stream_provider::SystemStreamHistory,
            Option<stream_provider::SystemStreamSplitReceipt>,
        )>,
    > + Send {
        stream_split::read_split_state(self.attempt, operation_id)
    }

    fn write_system_split(
        &mut self,
        receipt: &stream_provider::SystemStreamSplitPlan,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_split::write_split(self.attempt, receipt)
    }

    async fn store_user_item(&mut self, row: StreamItem) -> StorageResult<()> {
        stream_write::store_user_item(self.attempt, row)
    }

    fn initialize_user_sequence(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_write::counter::initialize_user_sequence(self.attempt, stream)
    }

    async fn delete_user_sequence(&mut self, stream: &StreamName) -> StorageResult<()> {
        stream_write::counter::delete_user_sequence(self.attempt, stream)
    }

    fn append_attempt_allocated_user_item(
        &mut self,
        row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<StreamItemId>> + Send {
        stream_write::append_attempt_allocated_user_item(self.attempt, row)
    }

    fn load_partition_family(
        &mut self,
        stream: &storage_types::StreamName,
    ) -> impl Future<Output = StorageResult<ResolvedPartitionFamily>> + Send {
        stream_partition::state::load_state(self.attempt, stream, crate::ReadProtection::Conflict)
    }

    async fn store_partition_sample(
        &mut self,
        stream: &storage_types::StreamName,
        sample: &PartitionLoadSampleRecord,
    ) -> StorageResult<()> {
        self.attempt.stage(vec![crate::KvMutation::Put {
            key: stream_partition::key_layout::sample_key(stream, sample)?,
            value: storage_types::storage_serde::to_bytes(sample)?,
        }])
    }

    fn append_partitioned_user_item(
        &mut self,
        partition: &PartitionInfo,
        row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<[u8; 2]>> + Send {
        stream_partition::write::append_partitioned_user_item(self.attempt, partition, row)
    }

    fn append_attempt_allocated_partitioned_user_item(
        &mut self,
        partition: &PartitionInfo,
        row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<StreamItemId>> + Send {
        stream_partition::write::append_attempt_allocated_partitioned_user_item(
            self.attempt,
            partition,
            row,
        )
    }

    fn append_versionstamped_user_item(
        &mut self,
        row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<[u8; 2]>> + Send {
        std::future::ready(stream_write::append_versionstamped_user_item(
            self.attempt,
            row,
        ))
    }

    fn read_cursor(
        &mut self,
        stream: &storage_types::StreamName,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<Option<StreamCursor>>> + Send {
        stream_cursor::read_cursor(self.attempt, self.protection, stream, cursor_name)
    }

    fn write_cursor(
        &mut self,
        cursor: StreamCursor,
        expected_position: Option<StreamItemId>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_cursor::write_cursor(self.attempt, cursor, expected_position)
    }

    fn delete_cursor(
        &mut self,
        stream: &storage_types::StreamName,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_cursor::delete_cursor(self.attempt, stream, cursor_name)
    }

    async fn load_table_history_states(
        &mut self,
        histories: &[storage_types::TableHistoryId],
    ) -> StorageResult<Vec<Option<storage_types::TableHistoryState>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = async {
            let keys = histories
                .iter()
                .map(|history| {
                    crate::catalog::table_id_from_history(*history)
                        .map(crate::catalog::metadata_key)
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let records = catalog.attempt.get_batch(keys, catalog.protection).await?;
            if records.len() != histories.len() {
                return Err(storage_types::StorageError::internal(
                    "Invalid table history catalog read",
                ));
            }
            records
                .into_iter()
                .zip(histories)
                .map(|(record, history)| {
                    record
                        .map(|bytes| crate::catalog::decode_table_history_state(&bytes, *history))
                        .transpose()
                })
                .collect()
        }
        .await;
        operation.complete(result)
    }

    async fn load_system_retired_frontiers(
        &mut self,
        origins: &[storage_types::SystemStreamPosition],
    ) -> StorageResult<storage_types::Exact<Option<storage_types::SystemStreamPosition>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::load_retired_positions(catalog.attempt, catalog.protection, origins).await;
        settlement.complete(result)
    }

    fn read_system_page_start(
        &mut self,
    ) -> impl Future<Output = StorageResult<stream_provider::SystemStreamHistory>> + Send {
        crate::stream_history::read_page_start(self.attempt, self.protection)
    }

    fn read_system_page_start_snapshot(
        &mut self,
    ) -> impl Future<Output = StorageResult<stream_provider::SystemStreamHistory>> + Send {
        crate::stream_history::read_page_start_snapshot(self.attempt, self.protection)
    }

    async fn read_system_history_pages(
        &mut self,
        reads: &[stream_provider::SystemStreamHistoryRead],
    ) -> StorageResult<Vec<SystemStreamHistoryReadPage>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::read_system_history_pages(catalog.attempt, catalog.protection, reads)
                .await;
        settlement.complete(result)
    }

    fn load_trim_receipts(
        &mut self,
        operation: &stream_provider::SystemStreamTrimOperationId,
        sequences: &[u64],
    ) -> impl Future<
        Output = StorageResult<
            storage_types::Exact<Option<stream_provider::SystemStreamTrimPageReceipt>>,
        >,
    > + Send {
        crate::stream_trim::load_receipts(self.attempt, operation, sequences)
    }

    fn scan_trim_records(
        &mut self,
        starts: &[storage_types::SystemStreamPosition],
    ) -> impl Future<Output = StorageResult<(Vec<stream_provider::SystemStreamTrimRecord>, bool)>> + Send
    {
        crate::stream_trim::scan_records(self.attempt, starts)
    }

    fn apply_trim_page(
        &mut self,
        selected: &[stream_provider::SystemStreamTrimRecord],
        frontiers: &[storage_types::SystemStreamPosition],
        receipt: &stream_provider::SystemStreamTrimPageReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        crate::stream_trim::apply_page(self.attempt, selected, frontiers, receipt)
    }
}

impl<B: KvBackend + 'static> StreamPort for KvStore<B> {
    type Attempt<'native>
        = KvCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    type AppendCapture = stream_provider::NoTableAppendCapture;

    fn table_append_capture(&self) -> Self::AppendCapture {
        stream_provider::NoTableAppendCapture
    }

    fn partitioning_support(&self) -> StreamPartitioningSupport {
        <Self::Attempt<'_> as StreamAttempt>::PARTITIONING_SUPPORT
    }

    fn item_id_allocation(&self) -> StreamItemIdAllocation {
        match self.backend.topology() {
            KvTopology::Local => StreamItemIdAllocation::AttemptAllocated,
            KvTopology::Distributed => StreamItemIdAllocation::CommitVersionstamped,
        }
    }

    fn with_attempt<R, F>(
        &self,
        request: AttemptRequest,
        step: F,
    ) -> impl Future<Output = StorageResult<R>> + Send
    where
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut Self::Attempt<'native>,
            ) -> BoxFuture<'borrow, StorageResult<R>>
            + Send
            + 'static,
    {
        self.with_catalog_attempt(request, step)
    }

    fn with_committed_stream_attempt<R, F>(
        &self,
        request: AttemptRequest,
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
        self.with_versionstamped_catalog_attempt(request, operation)
    }
}
