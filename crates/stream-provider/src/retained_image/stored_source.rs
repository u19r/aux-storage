//! Exact source bodies use the same complete hydration verifier as originals.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{AttributeMap, AttributeValue, StorageResult, StoredImageSource, WireItem};

use super::{
    ImageHydrationBatch, RetainedImageManifest, error::invalid_image, manifest::ImageSegment,
    preparation::RetainedImageBodyWork,
};

pub async fn load_stored_image<A: PhysicalRecordAttempt>(
    attempt: &mut A,
    source: StoredImageSource,
) -> StorageResult<WireItem> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let identity = source.address.identity();
        let length = source.raw_bytes.get();
        let manifest = RetainedImageManifest {
            length,
            history: Some(source.address.history()),
            digest: identity.as_ref().try_into().map_err(|_| invalid_image())?,
            segments: vec![ImageSegment::Image { identity, length }],
        };
        let work = vec![RetainedImageBodyWork {
            identity,
            encoded_bytes: source.encoded_bytes.get(),
        }];
        let mut bodies = ImageHydrationBatch::try_projected(vec![(manifest, work)])?
            .load(attempt, RetainedImageManifest::image_work_limits(length)?)
            .await?;
        if bodies.len() != 1 {
            return Err(invalid_image());
        }
        Ok(WireItem::DynamoJson {
            data: bodies.pop().ok_or_else(invalid_image)?,
        })
    }
    .await;
    settlement.complete(result)
}

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
    let reader = super::json_projection::RetainedJsonProjection::for_item(
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
