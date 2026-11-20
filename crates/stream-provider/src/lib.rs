//! Stable stream provider traits and types.
//!
//! Downstream libraries should depend on this crate for stream abstractions
//! instead of backend implementation crates.

mod append_receipt;
#[cfg(test)]
mod append_receipt_tests;
mod constants;
#[cfg(test)]
mod constants_tests;
mod dynamo_stream_write;
mod errors;
#[cfg(test)]
mod errors_tests;
mod table_history_index;
pub use table_history_index::{
    TableHistoryBoundaryRead, TableHistoryIndexAttempt, TableHistoryIndexPage,
    TableHistoryIndexRead, TableHistoryObservation, TableHistoryObservationPage,
    read_table_history_boundaries, read_table_history_indexes, read_table_history_observations,
};
mod entry_delete;
pub use entry_delete::{StreamEntryAddress, StreamEntryDeleteAttempt};
mod newtypes;
#[cfg(test)]
mod newtypes_tests;
mod stream_address;
mod stream_append;
mod stream_catalog;
mod stream_cursor;
mod stream_cursor_operation;
mod stream_cursor_read;
mod stream_entry;
#[cfg(test)]
mod stream_entry_tests;
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
mod validation;
#[cfg(test)]
mod validation_tests;

pub use constants::{
    DYNAMO_SYSTEM_LINEAGE_ID, DYNAMO_SYSTEM_SHARD_ID, MAX_STREAM_ITEM_DATA_BYTES,
    MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES, MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT,
    MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS, STREAM_APPEND_PROOF_BYTES,
    STREAM_APPEND_RECEIPT_ENCODED_BYTES, STREAM_LIMITS, SYSTEM_STREAM_HISTORY_READ_BATCH,
};

pub use crate::{
    append_receipt::{
        StreamAppendAllocation, StreamAppendOperationId, StreamAppendProof, StreamAppendReceipt,
        TableAppendReceiptKey, TableAppendReconciliation, TableAppendRequest,
    },
    dynamo_stream_write::PositionedStreamEntry,
    errors::{StreamEnum, StreamError, StreamInternalKind, StreamResult, StreamValidationKind},
    newtypes::CursorName,
    stream_address::StreamAddress,
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
    stream_cursor_read::StreamCursorRead,
    stream_entry::{StreamChange, StreamEntry, StreamEntryInput},
    stream_item_id::sequenced_stream_item_id,
    stream_operations::StreamOperations,
    stream_port::{
        NoTableAppendCapture, StreamAttempt, StreamItemIdAllocation, StreamPartitioningSupport,
        StreamPort, TableAppendCapture,
    },
    stream_provider::validate_limit,
    stream_read::{
        StreamReadKind, StreamReadRequest, StreamReadResult, StreamRowPage, StreamRowReadKind,
        SystemStreamHistoryReadPage, verify_stream_item_identity,
    },
    stream_row::{StreamRowValue, decode_stream_item_row},
    subscription_sender::{SubscriptionMessageSender, SubscriptionSendFuture},
    system_stream::{
        DynamoMutationRoute, SystemStreamHistory, SystemStreamHistoryBounds,
        SystemStreamHistoryObservation, SystemStreamHistoryOrdering, SystemStreamHistoryPage,
        SystemStreamHistoryRead, SystemStreamHistoryRecord, SystemStreamIssuedCheckpoint,
        SystemStreamLineage, SystemStreamLineageCatalog, SystemStreamPage, SystemStreamPageRequest,
        SystemStreamSplitChild, SystemStreamSplitDisposition, SystemStreamSplitOperationId,
        SystemStreamSplitPlan, SystemStreamSplitReceipt, SystemStreamSplitRequest,
        SystemStreamSplitResult, SystemStreamTrimContinuation, SystemStreamTrimOperationId,
        SystemStreamTrimPageReceipt, SystemStreamTrimPageRequest, SystemStreamTrimPageResult,
        SystemStreamTrimRecord, SystemStreamWriteAllocation, canonical_route_key,
        execute_trim_page, lineage_frontier_high, retired_frontiers,
    },
    types::{
        CursorPage, CursorPosition, ReadDirection, Stream, StreamCursor, StreamDataType,
        StreamItem, StreamPage, StreamPartitioningMode, SubscriptionDestination,
        SubscriptionMessage, SubscriptionSendOutcome,
    },
};

#[cfg(test)]
mod stream_row_tests;
