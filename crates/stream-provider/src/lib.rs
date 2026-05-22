//! Stable stream provider traits and types.
//!
//! Downstream libraries should depend on this crate for stream abstractions
//! instead of backend implementation crates.

mod constants;
mod errors;
#[cfg(test)]
mod errors_tests;
mod newtypes;
#[cfg(test)]
mod newtypes_tests;
mod stream_provider;
mod subscription_sender;
mod types;
#[cfg(test)]
mod types_tests;

pub use crate::{
    errors::{StreamEnum, StreamError, StreamInternalKind, StreamResult, StreamValidationKind},
    newtypes::CursorName,
    stream_provider::{StreamProvider, validate_limit},
    subscription_sender::{SubscriptionMessageSender, SubscriptionSendFuture},
    types::{
        AppendItemRequest, AppendItemResponse, CreateCursorRequest, CreateCursorResponse,
        CreateStreamRequest, CreateStreamResponse, CursorPage, CursorPosition, DeleteCursorRequest,
        DeleteStreamRequest, EmbeddedStreamItem, PointerRecordsResult, ReadDirection,
        ReadFromCursorRequest, ReadFromCursorResponse, ReadStreamRequest, ReadStreamResponse,
        StoredStreamPointer, Stream, StreamCursor, StreamDataType, StreamItem, StreamItemResponse,
        StreamPage, StreamPartitioningMode, StreamPointer, SubscriptionDestination,
        SubscriptionMessage, SubscriptionSendOutcome,
    },
};
