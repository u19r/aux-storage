use storage_types::StorageEnum;

use crate::{StreamEnum, StreamError, StreamInternalKind, StreamValidationKind};

#[test]
fn stream_validation_kind_exposes_client_facing_messages() {
    assert_eq!(
        StreamValidationKind::EmptyName.message(),
        "name cannot be empty"
    );
    assert_eq!(
        StreamValidationKind::MissingPartitionKey.message(),
        "partition key is required for key-ordered streams"
    );
}

#[test]
fn stream_validation_error_preserves_kind_and_default_message() {
    let error = StreamError::validation(StreamValidationKind::InvalidLimit);

    assert!(
        matches!(
            error,
            StreamError::Base(StreamEnum::Validation {
                kind: StreamValidationKind::InvalidLimit,
                ref message,
            }) if message == "invalid limit"
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn stream_validation_error_accepts_custom_business_message() {
    let error = StreamError::validation("limit must be less than shard count");

    assert!(
        matches!(
            error,
            StreamError::Base(StreamEnum::Validation {
                kind: StreamValidationKind::Message,
                ref message,
            }) if message == "limit must be less than shard count"
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn stream_internal_error_adds_operation_detail_to_default_message() {
    let error = StreamError::internal_with_detail(StreamInternalKind::CleanupTask, "cursor-a");

    assert!(
        matches!(
            error,
            StreamError::Base(StreamEnum::Internal {
                kind: StreamInternalKind::CleanupTask,
                ref message,
            }) if message == "cleanup task failed: cursor-a"
        ),
        "unexpected error: {error:?}"
    );
}

#[test]
fn stream_resource_helpers_preserve_resource_type_for_storage_mapping() {
    let not_found = StreamError::cursor_not_found("cursor-a").into_storage_enum();
    let exists = StreamError::stream_already_exists("events").into_storage_enum();

    assert!(
        matches!(
            not_found,
            StorageEnum::ResourceNotFound {
                resource_type: "cursor",
                ref resource_id,
            } if resource_id == "cursor-a"
        ),
        "unexpected storage error: {not_found:?}"
    );
    assert!(
        matches!(
            exists,
            StorageEnum::ResourceExists {
                resource_type: "stream",
                ref resource_id,
            } if resource_id == "events"
        ),
        "unexpected storage error: {exists:?}"
    );
}

#[test]
fn stream_validation_and_internal_errors_map_to_storage_categories() {
    let validation = StreamError::validation_with_detail(
        StreamValidationKind::InvalidNameCharacters,
        "bad name",
    )
    .into_storage_enum();
    let internal = StreamError::serialization_error("append item").into_storage_enum();

    assert!(
        matches!(
            validation,
            StorageEnum::Validation { ref message } if message == "bad name"
        ),
        "unexpected storage error: {validation:?}"
    );
    assert!(
        matches!(
            internal,
            StorageEnum::InternalServerError { ref message }
                if message == "serialization failed: append item"
        ),
        "unexpected storage error: {internal:?}"
    );
}
