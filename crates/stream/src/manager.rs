use std::sync::Arc;

use storage_types::{StreamItemId, StreamName, UserStreamName};
use stream_provider::{
    CursorName, CursorPage, CursorPosition, Stream, StreamCursor, StreamError, StreamOperations,
    StreamPage, StreamPartitioningMode, StreamResult, StreamRowKind, SubscriptionMessageSender,
};
use tracing::instrument;

use crate::validation::{
    CursorNameValidation, UserStreamNameValidation, validate_item_data_size, validate_ttl_seconds,
};

/// Stream service that wraps shared stream operations with request validation.
pub struct StreamManager {
    operations: Arc<dyn StreamOperations>,
    subscription_message_sender: Option<Arc<dyn SubscriptionMessageSender>>,
}

impl std::fmt::Debug for StreamManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamManager")
            .field("operations", &"<StreamOperations>")
            .field(
                "subscription_message_sender",
                &self.subscription_message_sender.is_some(),
            )
            .finish()
    }
}

impl StreamManager {
    pub fn new(operations: Arc<dyn StreamOperations>) -> Self {
        Self {
            operations,
            subscription_message_sender: None,
        }
    }

    pub fn new_with_subscription_message_sender(
        operations: Arc<dyn StreamOperations>,
        subscription_message_sender: Arc<dyn SubscriptionMessageSender>,
    ) -> Self {
        Self {
            operations,
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
    #[instrument(skip_all, fields(feature = "stream", ttl_seconds = %ttl_seconds.unwrap_or(0)))]
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
            .operations
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
        self.operations.delete_stream(user_stream_name).await
    }

    /// Get stream information
    pub async fn get_stream(&self, user_stream_name: &str) -> StreamResult<Option<Stream>> {
        let user_stream_name = UserStreamName::new(user_stream_name);
        self.operations.get_stream(user_stream_name).await
    }

    /// Append an item to a stream with size validation
    #[instrument(skip_all, fields(feature = "stream", item_size))]
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

        let result = self
            .operations
            .append_item(
                UserStreamName::new(user_stream_name),
                item_data,
                partition_key,
            )
            .await?;

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
            .operations
            .read_forward(StreamRowKind::User(stream_name), page_token, limit)
            .await
            .inspect_err(|_| {
                tracing::error!("stream.read_forward.failed");
            })?;

        tracing::Span::current().record("items_returned", result.items.len());
        tracing::Span::current().record("has_more", result.has_more);
        Ok(result)
    }

    /// Read items backward with pagination
    #[instrument(
        skip_all,
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

        self.operations
            .read_backward(StreamRowKind::User(stream_name), page_token, limit)
            .await
            .inspect(|r| {
                tracing::Span::current().record("has_more", r.has_more);
                tracing::Span::current().record("items_returned", r.items.len());
            })
    }

    /// Create a cursor with validation
    #[instrument(skip_all, fields(feature = "stream"))]
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

        self.operations
            .create_cursor(StreamRowKind::User(stream_name), cursor_name, position)
            .await
    }

    /// Delete a cursor
    #[instrument(skip_all, fields(feature = "stream"))]
    pub async fn delete_cursor(
        &self,
        user_stream_name: &str,
        cursor_name: &str,
    ) -> StreamResult<()> {
        let stream_name = self
            .get_stream_name_from_user_name(user_stream_name)
            .await?;
        self.operations
            .delete_cursor(
                StreamRowKind::User(stream_name),
                CursorName::new(cursor_name),
            )
            .await
    }

    /// Read from a cursor with automatic advancement
    #[instrument(skip_all, fields(feature = "stream", items_returned))]
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
            .operations
            .read_from_cursor(StreamRowKind::User(stream_name), cursor_name, limit)
            .await?;

        tracing::Span::current().record("items_returned", page.items.len());

        Ok(page)
    }

    /// Advance cursor to a specific item
    #[instrument(skip_all, fields(feature = "stream"))]
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

        self.operations
            .advance_cursor(StreamRowKind::User(stream_name), cursor_name, to_item_id)
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

        self.operations
            .get_cursor(StreamRowKind::User(stream_name), cursor_name)
            .await
    }

    async fn get_stream_name_from_user_name(
        &self,
        user_stream_name: &str,
    ) -> StreamResult<StreamName> {
        let stream = self
            .operations
            .get_stream(UserStreamName::new(user_stream_name))
            .await?
            .ok_or_else(|| StreamError::stream_not_found(user_stream_name))?;
        Ok(stream.internal_id)
    }
}
