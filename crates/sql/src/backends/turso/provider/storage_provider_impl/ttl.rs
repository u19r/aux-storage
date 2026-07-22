use crate::backends::turso::provider::storage_provider_impl::*;

pub(crate) async fn run_custom_stream_trim_once(
    provider: &TursoStorageProvider,
) -> StorageResult<bool> {
    let stats = StreamDurationTrimWorker::new(
        provider.clone(),
        StreamDurationTrimConfig {
            marker_page_size: 250,
            stream_page_size: 1_000,
        },
    )
    .run_due_page(TimestampMillis::now(), TimestampMillis::now())
    .await?;
    Ok(stats.did_work())
}

#[async_trait]
impl StreamDurationTrimBackend for TursoStorageProvider {
    async fn list_due_stream_trim_markers(
        &self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        let conn = self.connect().await?;
        self.list_due_stream_trim_markers(&conn, due_before, limit)
            .await
    }

    async fn load_stream_trim_state(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        let conn = self.connect().await?;
        self.load_stream_trim_state_by_scope(&conn, scope).await
    }

    async fn load_stream_trim_boundaries(
        &self,
        scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        let conn = self.connect().await?;
        self.load_stream_trim_boundaries(&conn, scope).await
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page(request).await
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_stream_page(request).await
    }

    async fn finish_stream_trim_marker(
        &self,
        marker: StreamTrimDueMarker,
        write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        self.finish_stream_trim_marker(marker, write).await
    }
}
