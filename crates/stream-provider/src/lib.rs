//! Stable stream provider traits and types.
//!
//! Downstream libraries should depend on this crate for stream abstractions
//! instead of backend implementation crates.

mod append_receipt;
mod change_index;
#[cfg(test)]
mod change_index_tests;
mod constants;
mod dynamo_stream_write;
#[cfg(test)]
mod dynamo_stream_write_tests;
mod errors;
#[cfg(test)]
mod errors_tests;
mod history_resolution;
mod table_history_index;
pub use table_history_index::{
    TableHistoryIndexAttempt, TableHistoryIndexPage, TableHistoryIndexRead,
    read_table_history_indexes,
};
mod item_history;
pub use history_resolution::{ItemHistoryImages, ResolvedItemHistory, resolve_item_history};
pub use item_history::{
    HistoryRowDelete, HistoryRowDeleteAttempt, ItemHistoryReadAttempt, ItemHistoryReadBatch,
    decode_item_history_entry,
};
#[cfg(test)]
mod item_history_tests;
mod newtypes;
#[cfg(test)]
mod newtypes_tests;
mod stream_append;
mod stream_catalog;
mod stream_cursor;
mod stream_cursor_operation;
mod stream_cursor_read;
mod stream_item_id;
mod stream_kind;
mod stream_operations;
mod stream_port;
mod stream_provider;
mod stream_read;
mod stream_row;
mod subscription_sender;
mod system_stream;
#[cfg(test)]
mod system_stream_tests;
mod types;

pub use constants::{
    DYNAMO_SYSTEM_LINEAGE_ID, DYNAMO_SYSTEM_SHARD_ID, MAX_STREAM_ITEM_DATA_BYTES,
    MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES, MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT,
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, SYSTEM_STREAM_HISTORY_READ_BATCH,
};

pub use crate::{
    append_receipt::{
        StreamAppendAllocation, StreamAppendOperationId, StreamAppendReceipt,
        TableAppendReceiptKey, TableAppendReconciliation, TableAppendRequest,
    },
    change_index::{CHANGE_INDEX_SLOT_COUNT, change_index_slot, sortable_version},
    dynamo_stream_write::{
        DynamoStreamWriteInput, DynamoStreamWritePlan, dynamo_item_history_address,
    },
    errors::{StreamEnum, StreamError, StreamInternalKind, StreamResult, StreamValidationKind},
    newtypes::CursorName,
    stream_append::{
        AcceptedUserAppend, UserStreamAppend, accept_user_append_in_attempt,
        append_table_row_in_attempt,
    },
    stream_catalog::{
        StreamCatalogLookup, StreamCatalogRecord, create_stream_in_attempt,
        delete_stream_in_attempt, load_user_stream_in_attempt,
    },
    stream_cursor::{
        PendingCursorAdvance, accept_cursor_creation, accept_cursor_page, prepare_cursor_advance,
    },
    stream_cursor_operation::StreamCursorOperation,
    stream_cursor_read::{CommittedUserStreamRead, NativeStreamRead, StreamCursorRead},
    stream_item_id::sequenced_stream_item_id,
    stream_kind::{StreamKind, StreamRowKind, StreamWriteKind},
    stream_operations::StreamOperations,
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
    stream_row::{
        StreamRowValue, decode_materialized_stream_row, decode_stream_item_row,
        decode_table_history_index_row,
    },
    subscription_sender::{SubscriptionMessageSender, SubscriptionSendFuture},
    system_stream::{
        DynamoMutationRoute, LogicalSystemStreamMutation, LogicalSystemStreamMutationInput,
        SystemHistoryReadAttempt, SystemStreamHistory, SystemStreamHistoryBounds,
        SystemStreamHistoryObservation, SystemStreamHistoryOrdering, SystemStreamHistoryPage,
        SystemStreamHistoryRead, SystemStreamHistoryRecord, SystemStreamIssuedCheckpoint,
        SystemStreamLineage, SystemStreamLineageCatalog, SystemStreamPage, SystemStreamPageRequest,
        SystemStreamSplitChild, SystemStreamSplitDisposition, SystemStreamSplitOperationId,
        SystemStreamSplitReceipt, SystemStreamSplitRequest, SystemStreamSplitResult,
        SystemStreamTrimAttempt, SystemStreamTrimContinuation, SystemStreamTrimOperationId,
        SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest, SystemStreamTrimPageResult,
        SystemStreamTrimRecord, SystemStreamWriteAllocation, canonical_route_key,
        execute_trim_page,
    },
    types::{
        CursorPage, CursorPosition, ReadDirection, Stream, StreamCursor, StreamDataType,
        StreamItem, StreamPage, StreamPartitioningMode, SubscriptionDestination,
        SubscriptionMessage, SubscriptionSendOutcome,
    },
};
