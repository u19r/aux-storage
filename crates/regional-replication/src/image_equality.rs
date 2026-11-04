//! Compare retained originals with materialized logical images without storage
//! I/O.

use storage_types::{AttributeMap, AttributeValue, StorageResult, WireItem};

use crate::native_record::corrupt_record as invalid_image;

/// Compare projected logical content while retaining the exact source spelling.
pub fn require_materialized_image(exact: &WireItem, observed: &WireItem) -> StorageResult<()> {
    let mut expected = project_attributes(exact)?;
    let mut actual = project_attributes(observed)?;
    if expected == actual {
        return Ok(());
    }
    for value in expected.values_mut().chain(actual.values_mut()) {
        canonicalize_numbers(value);
    }
    if expected != actual {
        return Err(invalid_image());
    }
    Ok(())
}

fn project_attributes(item: &WireItem) -> StorageResult<AttributeMap> {
    let bytes = item.dynamo_json_bytes()?;
    // Received spelling may be longer than the normalized winner. Bound each
    // temporary projection by its already admitted source, not the other item.
    let mut maximum = bytes.len().checked_mul(2).ok_or_else(invalid_image)?;
    let reader = crate::json_projection::CanonicalImageProjection::for_item(
        std::io::Cursor::new(bytes.as_ref()),
        &mut maximum,
    );
    Ok(serde_json::from_reader(reader)?)
}

fn canonicalize_numbers(value: &mut AttributeValue) {
    match value {
        AttributeValue::M(values) => {
            for value in values.values_mut() {
                canonicalize_numbers(value);
            }
        }
        AttributeValue::L(values) => {
            for value in values {
                canonicalize_numbers(value);
            }
        }
        _ => storage_types::canonicalize_top_level_number_value(value),
    }
}
