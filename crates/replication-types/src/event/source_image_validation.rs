use storage_types::{AttributeValue, IndexerDeclaration, KeyAttributes, MaxIndexers, WireItem};

use super::content::{MutationCause, SourceImagesRef};
use crate::error::ReplicationProtocolError;

impl SourceImagesRef<'_> {
    pub fn require_item_shape(
        &self,
        key: &KeyAttributes,
        cause: MutationCause,
    ) -> Result<(), ReplicationProtocolError> {
        let valid = match cause {
            MutationCause::Put | MutationCause::Update => self.new.is_some(),
            MutationCause::Delete => self.new.is_none(),
            MutationCause::Expiration => self.old.is_some() && self.new.is_none(),
        };
        if !valid {
            return Err(invalid_image());
        }
        self.require_valid_images(key)
    }

    /// Validate full source images without rewriting their immutable
    /// representation.
    pub fn require_valid_images(
        &self,
        key: &KeyAttributes,
    ) -> Result<(), ReplicationProtocolError> {
        key.validate_source_components()
            .map_err(|_| invalid_image())?;
        let capacity = MaxIndexers::try_new(storage_types::MAX_INDEXERS_CAPACITY)
            .map_err(|_| invalid_image())?;
        for (image, names) in [(self.old, self.old_indexers), (self.new, self.new_indexers)] {
            if let Some(names) = names {
                IndexerDeclaration::validate(names, capacity).map_err(|_| invalid_image())?;
            }
            let Some(image) = image else {
                continue;
            };
            image
                .validate_for_write("Item")
                .map_err(|_| invalid_image())?;
            if !image.has_verified_keys(key).map_err(|_| invalid_image())? {
                return Err(invalid_image());
            }
            if let Some(names) = names {
                require_indexer_values(image, names)?;
            }
        }
        Ok(())
    }
}

fn require_indexer_values(
    image: &WireItem,
    names: &[String],
) -> Result<(), ReplicationProtocolError> {
    if names.is_empty() {
        return Ok(());
    }
    let fields = names.iter().map(String::as_str).collect::<Vec<_>>();
    let values = image
        .select_attribute_values(&fields)
        .map_err(|_| invalid_image())?;
    for value in values {
        match value {
            None => {}
            Some(AttributeValue::S(value)) if !value.is_empty() => {}
            Some(_) => return Err(invalid_image()),
        }
    }
    Ok(())
}

fn invalid_image() -> ReplicationProtocolError {
    ReplicationProtocolError::IdentityContentMismatch {
        context: "canonical source images",
    }
}
