use crate::{
    AttributeValue, AttributeValueLookup, IndexerDeclaration, StorageError, StorageResult,
    StorageValidationKind,
};

pub fn validate_logical_item(
    item: &impl AttributeValueLookup,
    declaration: &IndexerDeclaration,
) -> StorageResult<()> {
    validate_logical_item_names(item, declaration.names())
}

pub fn validate_logical_item_names(
    item: &impl AttributeValueLookup,
    names: &[String],
) -> StorageResult<()> {
    for name in names {
        match item.get_attribute_value(name) {
            Some(AttributeValue::S(value)) if value.is_empty() => {
                return Err(StorageError::validation(
                    StorageValidationKind::EmptyIndexer,
                ));
            }
            Some(AttributeValue::S(_)) | None => {}
            Some(_) => {
                return Err(StorageError::validation(
                    StorageValidationKind::InvalidIndexerType,
                ));
            }
        }
    }
    Ok(())
}
