use std::sync::Arc;

use storage_types::{DurationSeconds, StreamItemId, StreamName, TimestampMillis, UserStreamName};
use stream_provider::{
    CursorName, CursorPage, CursorPosition, Stream, StreamCursor, StreamError, StreamPage,
    StreamPartitioningMode, StreamProvider, StreamResult, SubscriptionMessageSender,
};
use tracing::instrument;

use crate::validation::{
    CursorNameValidation, UserStreamNameValidation, validate_item_data_size, validate_ttl_seconds,
};

/// Stream service that wraps a `StreamProvider` with additional business logic
pub struct StreamManager {
    provider: Arc<dyn StreamProvider>,
    subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
}

impl std::fmt::Debug for StreamManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamManager")
            .field("provider", &"<StreamProvider>")
            .field(
                "subscription_message_sender",
                &self.subscription_message_sender.is_some(),
            )
            .finish()
    }
}

impl StreamManager {
    pub fn new(provider: Arc<dyn StreamProvider>) -> Self {
        Self {
            provider,
            subscription_message_sender: None,
        }
    }

    pub fn new_with_subscription_message_sender(
        provider: Arc<dyn StreamProvider>,
        subscription_message_sender: Arc<dyn SubscriptionMessageSender>,
    ) -> Self {
        Self {
            provider,
            subscription_message_sender: Some(subscription_message_sender),
        }
    }

    #[must_use]
    pub fn subscription_message_sender(&self) -> Option<Arc<dyn SubscriptionMessageSender>> {
        self.subscription_message_sender.clone()
    }

    #[must_use]
    pub fn has_subscription_message_sender(&self) -> bool {
        self.subscription_message_sender.is_some()
    }

    /// Create a new stream with validation
    #[instrument(skip_all, fields(feature = "stream", user_stream_name, ttl_seconds = %ttl_seconds.unwrap_or(0)))]
    pub async fn create_stream(
        &self,
        user_stream_name: &str,
        ttl_seconds: Option<u32>,
    ) -> StreamResult<StreamName> {
        self.create_stream_with_partitioning(
            user_stream_name,
            ttl_seconds,
            StreamPartitioningMode::Single,
        )
        .await
    }

    pub async fn create_stream_with_partitioning(
        &self,
        user_stream_name: &str,
        ttl_seconds: Option<u32>,
        partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        let user_stream_name = UserStreamName::new(user_stream_name);

        user_stream_name.validate_stream_name()?;

        // Validate TTL bounds (1 second to 1 year)
        if let Some(ttl) = ttl_seconds {
            validate_ttl_seconds(ttl)?;
        }

        let result = self
            .provider
            .create_stream(
                user_stream_name,
                ttl_seconds.map(Into::into),
                partitioning_mode,
            )
            .await?;

        Ok(result)
    }

    /// Delete a stream
    pub async fn delete_stream(&self, user_stream_name: &str) -> StreamResult<()> {
        let user_stream_name = UserStreamName::new(user_stream_name);
        self.provider.delete_stream(user_stream_name).await
    }

    /// Get stream information
    pub async fn get_stream(&self, user_stream_name: &str) -> StreamResult<Option<Stream>> {
        let user_stream_name = UserStreamName::new(user_stream_name);
        self.provider.get_stream(user_stream_name).await
    }

    /// Append an item to a stream with size validation
    #[instrument(
        skip_all,
        fields(feature = "stream", user_stream_name, item_size, item_id)
    )]
    pub async fn append_item(
        &self,
        user_stream_name: &str,
        item_data: &[u8],
    ) -> StreamResult<StreamItemId> {
        self.append_item_with_partition_key(user_stream_name, item_data, None)
            .await
    }

    pub async fn append_item_with_partition_key(
        &self,
        user_stream_name: &str,
        item_data: &[u8],
        partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        tracing::Span::current().record("item_size", item_data.len());

        // Validate item data size
        validate_item_data_size(item_data)?;

        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;

        let result = self
            .provider
            .append_item(stream_name, item_data, partition_key)
            .await?;

        tracing::Span::current().record("item_id", result.to_string());
        Ok(result)
    }

    /// Read items forward with pagination
    #[instrument(
        skip_all,
        fields(feature = "stream", has_page_token, items_returned, has_more)
    )]
    pub async fn read_forward(
        &self,
        user_stream_name: UserStreamName,
        page_token: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<StreamPage> {
        let limit = limit.unwrap_or(100).clamp(1, 1000);

        tracing::Span::current().record("has_page_token", page_token.is_some());

        let stream_name = self
            .get_stream_name_from_user_name(&user_stream_name)
            .await?;

        let result = self
            .provider
            .read_forward(stream_name, page_token, limit)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "stream.read_forward.failed");
                e
            })?;

        tracing::Span::current().record("items_returned", result.items.len());
        tracing::Span::current().record("has_more", result.has_more);
        Ok(result)
    }

    /// Read items backward with pagination
    #[instrument(
        skip(self, page_token),
        fields(feature = "stream", has_page_token, items_returned, has_more)
    )]
    pub async fn read_backward(
        &self,
        user_stream_name: &str,
        page_token: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<StreamPage> {
        tracing::Span::current().record("has_page_token", page_token.is_some());
        let limit = limit.unwrap_or(100).clamp(1, 1000);
        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;

        self.provider
            .read_backward(stream_name, page_token, limit)
            .await
            .inspect(|r| {
                tracing::Span::current().record("has_more", r.has_more);
                tracing::Span::current().record("items_returned", r.items.len());
            })
    }

    /// Create a cursor with validation
    #[instrument(skip(self), fields(feature = "stream", cursor_name, position))]
    pub async fn create_cursor(
        &self,
        user_stream_name: &str,
        cursor_name: &str,
        position: CursorPosition,
    ) -> StreamResult<()> {
        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;

        let cursor_name = CursorName::new(cursor_name);

        cursor_name.validate_cursor_name()?;

        self.provider
            .create_cursor(stream_name, cursor_name, position)
            .await
    }

    /// Delete a cursor
    #[instrument(skip(self), fields(feature = "stream"))]
    pub async fn delete_cursor(
        &self,
        user_stream_name: &str,
        cursor_name: &str,
    ) -> StreamResult<()> {
        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;
        self.provider
            .delete_cursor(stream_name, CursorName::new(cursor_name))
            .await
    }

    /// Read from a cursor with automatic advancement
    #[instrument(skip(self), fields(feature = "stream", items_returned))]
    pub async fn read_from_cursor(
        &self,
        user_stream_name: &str,
        cursor_name: &str,
        limit: Option<u32>,
    ) -> StreamResult<CursorPage> {
        let limit = limit.unwrap_or(100).clamp(1, 1000);
        let cursor_name = CursorName::new(cursor_name);

        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;

        let page = self
            .provider
            .read_from_cursor(stream_name.clone(), cursor_name.clone(), limit)
            .await?;

        tracing::Span::current().record("items_returned", page.items.len());

        // Automatically advance cursor to the last read item
        if let Some(last_item) = page.items.last() {
            self.provider
                .advance_cursor(stream_name, cursor_name, last_item.id)
                .await?;
        }

        Ok(page)
    }

    /// Advance cursor to a specific item
    #[instrument(skip(self), fields(feature = "stream"))]
    pub async fn advance_cursor(
        &self,
        user_stream_name: &str,
        cursor_name: &str,
        to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        let cursor_name = CursorName::new(cursor_name);

        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;

        self.provider
            .advance_cursor(stream_name, cursor_name, to_item_id)
            .await
    }

    /// Get cursor information
    pub async fn get_cursor(
        &self,
        user_stream_name: &str,
        cursor_name: &str,
    ) -> StreamResult<Option<StreamCursor>> {
        let cursor_name = CursorName::new(cursor_name);

        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;

        self.provider.get_cursor(stream_name, cursor_name).await
    }

    /// Start the background cleanup task
    pub async fn start_cleanup_task(&self, parallelism: Option<usize>) -> StreamResult<()> {
        let parallelism = parallelism.unwrap_or(4).clamp(1, 32);
        self.provider.start_cleanup_task(parallelism).await
    }

    /// Stop the background cleanup task
    pub async fn stop_cleanup_task(&self) -> StreamResult<()> {
        self.provider.stop_cleanup_task().await
    }

    /// Manually trigger cleanup of expired items
    pub async fn cleanup_expired_items(&self) -> StreamResult<u64> {
        self.provider.cleanup_expired_items().await
    }

    /// Get statistics about streams
    pub async fn get_stream_stats(&self, stream_name: &str) -> StreamResult<StreamStats> {
        // This could be enhanced to provide more detailed statistics
        // For now, just check if stream exists
        let stream = self.get_stream(stream_name).await?;
        match stream {
            Some(s) => Ok(StreamStats {
                stream_name: s.name.as_str().to_string(),
                item_count: 0, // Would need additional queries to get accurate counts
                cursor_count: 0,
                created_at: s.created_at,
                ttl_seconds: s.ttl_seconds,
            }),
            None => Err(StreamError::stream_not_found(stream_name)),
        }
    }

    async fn get_stream_name_from_user_name(
        &self,
        user_stream_name: &str,
    ) -> StreamResult<StreamName> {
        let user_stream_name = UserStreamName::new(user_stream_name);
        let user_stream_name_for_error = user_stream_name.as_str().to_string();
        let stream = self
            .provider
            .get_stream(user_stream_name)
            .await?
            .ok_or_else(|| StreamError::stream_not_found(user_stream_name_for_error))?;
        Ok(stream.internal_id)
    }
}

/// Statistics about a stream
#[derive(Debug, Clone)]
pub struct StreamStats {
    pub stream_name: String,
    pub item_count: u64,
    pub cursor_count: u64,
    pub created_at: TimestampMillis,
    pub ttl_seconds: Option<DurationSeconds>,
}
