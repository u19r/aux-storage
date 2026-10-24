//! Stable stream provider traits and types.
//!
//! Downstream libraries should depend on this crate for stream abstractions
//! instead of backend implementation crates.

mod append_receipt;
mod change_index;
#[cfg(test)]
mod change_index_tests;
mod constants;
mod dynamo_stream_effect;
mod dynamo_stream_write;
#[cfg(test)]
mod dynamo_stream_write_tests;
mod errors;
#[cfg(test)]
mod errors_tests;
mod models;
mod newtypes;
#[cfg(test)]
mod newtypes_tests;
mod pointer_owner;
mod protected_cursor;
#[cfg(test)]
mod protected_cursor_alloc_tests;
#[cfg(test)]
mod protected_cursor_tests;
pub mod retained_image;
mod stream_append;
mod stream_catalog;
mod stream_cursor;
mod stream_cursor_operation;
mod stream_cursor_read;
mod stream_embedding;
#[cfg(test)]
mod stream_embedding_tests;
mod stream_hold;
mod stream_images;
mod stream_item_id;
mod stream_kind;
mod stream_operations;
mod stream_pointer_plan;
#[cfg(test)]
mod stream_pointer_plan_tests;
mod stream_port;
mod stream_provider;
mod stream_read;
mod stream_row;
mod subscription_sender;
mod system_stream;
#[cfg(test)]
mod system_stream_tests;
mod types;
#[cfg(test)]
mod types_tests;

pub use constants::{
    DYNAMO_SYSTEM_LINEAGE_ID, DYNAMO_SYSTEM_SHARD_ID, MAX_RETAINED_MANIFEST_BODIES,
    MAX_STREAM_ITEM_DATA_BYTES, MAX_SYSTEM_STREAM_CURSOR_SCAN_ROWS,
    MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES, MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT,
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SYSTEM_STREAM_HISTORY_READ_BATCH,
};

pub use crate::{
    append_receipt::{
        StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt,
        TableAppendReconciliation, TableAppendRequest,
    },
    change_index::{CHANGE_INDEX_SLOT_COUNT, change_index_slot, sortable_version},
    dynamo_stream_effect::DynamoStreamEffectAttempt,
    dynamo_stream_write::{
        DynamoStreamEffectRow, DynamoStreamIds, DynamoStreamWriteInput, DynamoStreamWritePlan,
        StreamPointerReference, dynamo_item_history_address,
    },
    errors::{
        StreamContinuityKind, StreamEnum, StreamError, StreamInternalKind, StreamResult,
        StreamValidationKind,
    },
    models::DynamoDbStreamImages,
    newtypes::CursorName,
    pointer_owner::PointerOwner,
    protected_cursor::{
        BootstrapCursorName, ProtectedCursorAdvanceRequest, ProtectedCursorPage,
        ProtectedCursorPageEnd, ProtectedCursorReadRequest,
    },
    stream_append::{AcceptedUserAppend, UserStreamAppend, accept_user_append_in_attempt},
    stream_catalog::{
        StreamCatalogLookup, StreamCatalogRecord, create_stream_in_attempt,
        delete_stream_in_attempt, load_user_stream_in_attempt,
    },
    stream_cursor::{
        PendingCursorAdvance, accept_cursor_creation, accept_cursor_page, prepare_cursor_advance,
        protected_cursor_floor_in_attempt, validate_user_cursor_name,
    },
    stream_cursor_operation::StreamCursorOperation,
    stream_cursor_read::{CommittedUserStreamRead, NativeStreamRead, StreamCursorRead},
    stream_embedding::{
        STREAM_EMBEDDED_MAX_BYTES, can_embed_new_image, embedded_stream_items,
        should_embed_stream_images,
    },
    stream_item_id::sequenced_stream_item_id,
    stream_kind::{StreamKind, StreamRowKind, StreamWriteKind},
    stream_operations::StreamOperations,
    stream_pointer_plan::{StreamPointerInput, StreamPointerPlan},
    stream_port::{
        NoTableAppendCapture, StreamAttempt, StreamItemIdAllocation, StreamPartitioningSupport,
        StreamPort, TableAppendCapture,
    },
    stream_provider::{StreamProvider, validate_limit},
    stream_read::{
        AddressedStreamRead, StreamReadKind, StreamReadRequest, StreamReadResult, StreamRowPage,
        StreamRowReadKind, SystemStreamHistoryReadPage, SystemStreamReadKind,
        verify_stream_item_identity,
    },
    stream_row::{StreamRowValue, decode_stream_item_row},
    subscription_sender::{SubscriptionMessageSender, SubscriptionSendFuture},
    system_stream::{
        DynamoMutationRoute, LogicalSystemStreamMutation, LogicalSystemStreamMutationInput,
        PreparedStreamImage, SystemStreamCheckpointIssue, SystemStreamCheckpointLookup,
        SystemStreamCheckpointReceipt, SystemStreamHistory, SystemStreamHistoryBounds,
        SystemStreamHistoryObservation, SystemStreamHistoryOrdering, SystemStreamHistoryPage,
        SystemStreamHistoryRead, SystemStreamHistoryRecord, SystemStreamIssuedCheckpoint,
        SystemStreamLineage, SystemStreamLineageCatalog, SystemStreamPage, SystemStreamPagePlan,
        SystemStreamPageRequest, SystemStreamSplitChild, SystemStreamSplitDisposition,
        SystemStreamSplitOperationId, SystemStreamSplitReceipt, SystemStreamSplitRequest,
        SystemStreamSplitResult, SystemStreamStoredRecord, SystemStreamTrimAttempt,
        SystemStreamTrimCandidate, SystemStreamTrimCandidateObservation,
        SystemStreamTrimContinuation, SystemStreamTrimCursor, SystemStreamTrimKey,
        SystemStreamTrimOperationId, SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest,
        SystemStreamTrimPageResult, SystemStreamTrimRecord, SystemStreamWriteAllocation,
        canonical_route_key, execute_trim_page,
    },
    types::{
        CursorPage, CursorPosition, EmbeddedStreamItem, PointerRecordsResult, ReadDirection,
        StoredStreamPointer, Stream, StreamCursor, StreamDataType, StreamItem, StreamPage,
        StreamPartitioningMode, StreamPointer, SubscriptionDestination, SubscriptionMessage,
        SubscriptionSendOutcome,
    },
};
