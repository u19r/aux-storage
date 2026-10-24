use super::stream_embedding::{
    STREAM_EMBEDDED_MAX_BYTES, can_embed_new_image, embedded_stream_items,
    should_embed_stream_images,
};
use crate::types::StreamDataType;

#[test]
fn given_boundary_sizes_when_embed_is_checked_then_combined_budget_applies() {
    assert!(should_embed_stream_images(STREAM_EMBEDDED_MAX_BYTES, None));
    assert!(!should_embed_stream_images(
        STREAM_EMBEDDED_MAX_BYTES + 1,
        None
    ));
    assert!(should_embed_stream_images(512, Some(512)));
    assert!(!should_embed_stream_images(512, Some(513)));
}

#[test]
fn given_large_new_image_when_old_encode_is_checked_then_encoding_is_skipped() {
    assert!(!can_embed_new_image(STREAM_EMBEDDED_MAX_BYTES + 1));
    assert!(can_embed_new_image(STREAM_EMBEDDED_MAX_BYTES));
}

#[test]
fn given_images_when_embedded_items_are_built_then_old_image_uses_json_type() {
    let items = embedded_stream_items(vec![1], Some(vec![2]), StreamDataType::DeleteMarker);

    assert_eq!(items.len(), 2);
    assert_eq!(items[0].data_type, StreamDataType::DeleteMarker);
    assert_eq!(items[1].data_type, StreamDataType::DynamoDbJson);
}
