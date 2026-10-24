use storage_types::{AttributeValue, StorageError};

use crate::{DynamoDbStreamImages, StreamDataType, StreamItem};

impl TryFrom<&[StreamItem]> for DynamoDbStreamImages {
    type Error = StorageError;

    fn try_from(items: &[StreamItem]) -> Result<Self, Self::Error> {
        let Some(first) = items.first() else {
            return Ok(Self {
                old: None,
                new: None,
            });
        };
        let first_image = decode(first, "new")?;
        if first.data_type == StreamDataType::DeleteMarker {
            let old = if items.len() == 1 {
                first_image
            } else {
                decode(
                    items
                        .last()
                        .ok_or_else(|| StorageError::internal("GSI old image is missing"))?,
                    "old",
                )?
            };
            return Ok(Self {
                old: Some(old),
                new: None,
            });
        }
        let old = items
            .get(1)
            .filter(|item| item.data_type != StreamDataType::DeleteMarker)
            .map(|item| decode(item, "old"))
            .transpose()?;
        Ok(Self {
            old,
            new: Some(first_image),
        })
    }
}

fn decode(
    item: &StreamItem,
    image: &str,
) -> Result<std::collections::HashMap<String, AttributeValue>, StorageError> {
    storage_types::storage_serde::from_bytes(&item.data).map_err(|error| {
        StorageError::internal(&format!("GSI {image} image is malformed: {error}"))
    })
}
