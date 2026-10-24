//! One KV stream attempt shared by catalog and row operations.

use std::future::Future;

use futures::future::BoxFuture;
use storage_common::partition::{
    PartitionInfo, PartitionLoadSampleRecord, ResolvedPartitionFamily,
};
use storage_provider::DynamoTableStreamAttempt;
use storage_types::{AttemptMode, StorageResult, StreamItemId, StreamName, SystemStreamCheckpoint};
use stream_provider::{
    CursorName, StreamAttempt, StreamCatalogLookup, StreamCatalogRecord, StreamCursor, StreamItem,
    StreamItemIdAllocation, StreamPartitioningSupport, StreamPort, StreamReadKind,
    StreamReadRequest, StreamReadResult, StreamRowKind, StreamRowPage, StreamRowValue,
    SystemStreamCheckpointIssue, SystemStreamHistoryReadPage,
};

use crate::{
    KvAttempt, KvBackend, KvStore, KvTopology, catalog::KvCatalogAttempt, stream_catalog,
    stream_checkpoint, stream_cursor, stream_partition, stream_read, stream_split, stream_write,
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
        operation: stream_provider::StreamAppendOperationId,
    ) -> StorageResult<Option<stream_provider::StreamAppendReceipt>> {
        stream_write::load_append_receipt(self.attempt, self.protection, operation).await
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

    fn read_system_page_start(
        &mut self,
        token: Option<&storage_types::SystemStreamCheckpointToken>,
    ) -> impl Future<
        Output = StorageResult<(
            stream_provider::SystemStreamHistory,
            Option<stream_provider::SystemStreamCheckpointReceipt>,
        )>,
    > + Send {
        crate::stream_history::read_page_start(self.attempt, token)
    }

    async fn read_system_history_pages(
        &mut self,
        reads: &[stream_provider::SystemStreamHistoryRead],
    ) -> StorageResult<Vec<SystemStreamHistoryReadPage>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = stream_read::read_system_history_pages(catalog.attempt, reads).await;
        settlement.complete(result)
    }

    fn read_system_split_state(
        &mut self,
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
        receipt: &stream_provider::SystemStreamSplitReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_split::write_split(self.attempt, receipt)
    }

    fn write_system_stream_checkpoint(
        &mut self,
        issue: &SystemStreamCheckpointIssue,
        checkpoint: &SystemStreamCheckpoint,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_checkpoint::write_checkpoint(self.attempt, issue, checkpoint)
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
        stream: &StreamRowKind,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<Option<StreamCursor>>> + Send {
        stream_cursor::read_cursor(self.attempt, self.protection, stream.value(), cursor_name)
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
        stream: &StreamRowKind,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_cursor::delete_cursor(self.attempt, stream.value(), cursor_name)
    }
}

impl<A: KvAttempt> DynamoTableStreamAttempt for KvCatalogAttempt<'_, A> {
    async fn read_item_history_pages(
        &mut self,
        reads: &[(
            storage_types::ItemHistoryAddress,
            stream_provider::StreamReadRequest,
        )],
    ) -> StorageResult<Vec<stream_provider::StreamRowPage>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::read_item_history_pages(catalog.attempt, catalog.protection, reads).await;
        settlement.complete(result)
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
            + 'static,
    {
        self.backend.with_attempt(mode, move |native| {
            Box::pin(async move {
                let mut attempt = KvCatalogAttempt::new(native, mode.read_protection());
                let result = operation(&mut attempt).await?;
                stream_provider::retained_image::RetainedOwnershipAttempt::finalize_ownership(
                    &mut attempt,
                )
                .await?;
                Ok(result)
            })
        })
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
        let distributed = self.backend.topology() == KvTopology::Distributed;
        async move {
            if distributed {
                self.backend
                    .with_versionstamped_attempt(move |native| {
                        Box::pin(async move {
                            let mut attempt = KvCatalogAttempt::new(native, storage_types::ReadProtection::Conflict);
                            let result = operation(&mut attempt).await?;
                    stream_provider::retained_image::RetainedOwnershipAttempt::finalize_ownership(&mut attempt).await?;
                    Ok(result)
                        })
                    })
                    .await
            } else {
                self.with_stream_attempt(AttemptMode::ReadWrite, operation)
                    .await
                    .map(|value| (value, None))
            }
        }
    }
}

impl<A: KvAttempt> stream_provider::DynamoStreamEffectAttempt for KvCatalogAttempt<'_, A> {
    async fn load_stream_effects(
        &mut self,
        effects: &[storage_types::SystemStreamEffectDigest],
    ) -> StorageResult<Vec<Option<stream_provider::DynamoStreamEffectRow>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::load_stream_effects(catalog.attempt, catalog.protection, effects).await;
        settlement.complete(result)
    }

    async fn load_stream_records(
        &mut self,
        positions: &[storage_types::SystemStreamPosition],
    ) -> StorageResult<
        Vec<
            Option<
                stream_provider::retained_image::verified_system_stream::VerifiedSystemStreamRecord,
            >,
        >,
    > {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::load_stream_records(catalog.attempt, catalog.protection, positions).await;
        settlement.complete(result)
    }
    async fn load_stream_items(
        &mut self,
        addresses: &[(&storage_types::StreamName, storage_types::StreamItemId)],
    ) -> StorageResult<Vec<Option<stream_provider::StreamItem>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::load_stream_items(catalog.attempt, catalog.protection, addresses).await;
        settlement.complete(result)
    }

    async fn load_stream_references(
        &mut self,
        addresses: &[(&storage_types::StreamName, storage_types::StreamItemId)],
    ) -> StorageResult<Vec<Option<stream_provider::StreamPointerReference>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::load_stream_references(catalog.attempt, catalog.protection, addresses)
                .await;
        settlement.complete(result)
    }
}
