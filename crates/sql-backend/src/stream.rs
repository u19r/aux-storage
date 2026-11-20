//! One SQL stream attempt shared by catalog and row operations.

use std::{future::Future, sync::Arc};

use futures::future::BoxFuture;
use storage_types::{AttemptRequest, StorageResult, StreamItemId, StreamName};
use stream_provider::{
    CursorName, StreamAttempt, StreamCatalogLookup, StreamCatalogRecord, StreamCursor, StreamItem,
    StreamPartitioningSupport, StreamPort, StreamReadKind, StreamReadRequest, StreamReadResult,
    StreamRowPage, StreamRowValue, SystemStreamHistoryReadPage,
};

use crate::{
    SqlAttempt, SqlBackend, SqlLayout, SqlStore, catalog::SqlCatalogAttempt, stream_catalog,
    stream_cursor, stream_history, stream_read, stream_split, stream_write,
};

impl<A: SqlAttempt, L: SqlLayout> StreamAttempt for SqlCatalogAttempt<'_, A, L> {
    const PARTITIONING_SUPPORT: StreamPartitioningSupport =
        StreamPartitioningSupport::NormalizeToSingle;

    async fn store_append_receipt(
        &mut self,
        receipt: &stream_provider::StreamAppendReceipt,
    ) -> StorageResult<()> {
        stream_write::store_append_receipt(self.attempt, self.engine, receipt).await
    }

    async fn load_append_receipt(
        &mut self,
        key: stream_provider::TableAppendReceiptKey,
    ) -> StorageResult<Option<stream_provider::StreamAppendReceipt>> {
        stream_write::load_append_receipt(self.attempt, self.engine, self.protection, key).await
    }

    fn read_stream_record(
        &mut self,
        lookup: StreamCatalogLookup<'_>,
    ) -> impl Future<Output = StorageResult<Option<StreamCatalogRecord>>> + Send {
        stream_catalog::read_stream_record(self.attempt, self.engine, self.protection, lookup)
    }

    fn write_stream_record(
        &mut self,
        name: &str,
        record: StreamCatalogRecord,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_catalog::write_stream_record(self.attempt, self.engine, name, record)
    }

    async fn read_stream(
        &mut self,
        request: StreamReadKind<'_>,
    ) -> StorageResult<StreamReadResult> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result =
            stream_read::read_stream(catalog.attempt, catalog.engine, catalog.protection, request)
                .await;
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
        parent: &storage_types::SystemStreamLineageId,
        operation_id: &stream_provider::SystemStreamSplitOperationId,
    ) -> impl Future<
        Output = StorageResult<(
            stream_provider::SystemStreamHistory,
            Option<stream_provider::SystemStreamSplitReceipt>,
        )>,
    > + Send {
        stream_split::read_split_state(self.attempt, self.engine, parent, operation_id)
    }

    fn write_system_split(
        &mut self,
        receipt: &stream_provider::SystemStreamSplitPlan,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_split::write_split(self.attempt, self.engine, receipt)
    }

    fn store_user_item(
        &mut self,
        row: StreamItem,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_write::store_user_item(self.attempt, self.engine, row)
    }

    fn initialize_user_sequence(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_write::initialize_user_sequence(self.attempt, self.engine, stream)
    }

    fn delete_user_sequence(
        &mut self,
        stream: &StreamName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_write::delete_user_sequence(self.attempt, self.engine, stream)
    }

    fn append_attempt_allocated_user_item(
        &mut self,
        row: StreamRowValue,
    ) -> impl Future<Output = StorageResult<StreamItemId>> + Send {
        stream_write::append_attempt_allocated_user_item(self.attempt, self.engine, row)
    }

    fn read_cursor(
        &mut self,
        stream: &storage_types::StreamName,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<Option<StreamCursor>>> + Send {
        stream_cursor::read_cursor(
            self.attempt,
            self.engine,
            self.protection,
            stream,
            cursor_name,
        )
    }

    fn write_cursor(
        &mut self,
        cursor: StreamCursor,
        expected_position: Option<StreamItemId>,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_cursor::write_cursor(self.attempt, self.engine, cursor, expected_position)
    }

    fn delete_cursor(
        &mut self,
        stream: &storage_types::StreamName,
        cursor_name: &CursorName,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        stream_cursor::delete_cursor(self.attempt, self.engine, stream, cursor_name)
    }

    async fn load_table_history_states(
        &mut self,
        histories: &[storage_types::TableHistoryId],
    ) -> StorageResult<Vec<Option<storage_types::TableHistoryState>>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = stream_read::load_history_table_states(
            catalog.attempt,
            catalog.engine,
            catalog.protection,
            histories,
        )
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
        let result = crate::stream_allocation::load_retired_offsets(
            catalog.attempt,
            catalog.engine,
            origins,
        )
        .await;
        settlement.complete(result)
    }

    fn read_system_page_start(
        &mut self,
    ) -> impl Future<Output = StorageResult<stream_provider::SystemStreamHistory>> + Send {
        // The SQL history read is already a snapshot read.
        stream_history::read_history(self.attempt, self.engine)
    }

    async fn read_system_history_pages(
        &mut self,
        reads: &[stream_provider::SystemStreamHistoryRead],
    ) -> StorageResult<Vec<SystemStreamHistoryReadPage>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = stream_read::read_system_history_pages(
            catalog.attempt,
            catalog.engine,
            catalog.protection,
            reads,
        )
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
        crate::stream_trim::load_receipts(self.attempt, self.engine, operation, sequences)
    }

    fn scan_trim_records(
        &mut self,
        starts: &[storage_types::SystemStreamPosition],
    ) -> impl Future<Output = StorageResult<(Vec<stream_provider::SystemStreamTrimRecord>, bool)>> + Send
    {
        crate::stream_trim::scan_records(self.attempt, self.engine, starts)
    }

    fn apply_trim_page(
        &mut self,
        _selected: &[stream_provider::SystemStreamTrimRecord],
        frontiers: &[storage_types::SystemStreamPosition],
        receipt: &stream_provider::SystemStreamTrimPageReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send {
        crate::stream_trim::apply_page(self.attempt, self.engine, frontiers, receipt)
    }
}

impl<B: SqlBackend + 'static> StreamPort for SqlStore<B> {
    type Attempt<'native>
        = SqlCatalogAttempt<'native, B::Attempt>
    where Self: 'native;

    type AppendCapture = stream_provider::NoTableAppendCapture;

    fn table_append_capture(&self) -> Self::AppendCapture {
        stream_provider::NoTableAppendCapture
    }

    fn partitioning_support(&self) -> StreamPartitioningSupport {
        <Self::Attempt<'_> as StreamAttempt>::PARTITIONING_SUPPORT
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
        let request = request.with_default_scope(storage_types::AttemptScope::Data);
        let backend = Arc::clone(&self.backend);
        let engine = backend.engine();
        let attempt = async move {
            backend
                .with_attempt(request, move |native| {
                    Box::pin(async move {
                        let mut attempt = SqlCatalogAttempt::new(native, engine, request);
                        let result = step(&mut attempt).await?;
                        Ok(result)
                    })
                })
                .await
        };
        self.admission.settle(request, attempt)
    }
}
