use infrastructure_provider::record::{DurableRecordKey, RecordAddress};
use storage_types::{ImageAddress, StorageResult};

use crate::constants::RETAINED_IMAGE_NAMESPACE;

pub(super) fn image_record_key(image: ImageAddress) -> StorageResult<DurableRecordKey> {
    let mut namespace = String::new();
    let mut resource = Vec::new();
    let mut suffix = Vec::new();
    namespace
        .try_reserve_exact(RETAINED_IMAGE_NAMESPACE.len())
        .map_err(|_| super::error::capacity_rejected())?;
    resource
        .try_reserve_exact(image.history().as_ref().len())
        .map_err(|_| super::error::capacity_rejected())?;
    suffix
        .try_reserve_exact(image.identity().as_ref().len())
        .map_err(|_| super::error::capacity_rejected())?;
    namespace.push_str(RETAINED_IMAGE_NAMESPACE);
    resource.extend_from_slice(image.history().as_ref());
    suffix.extend_from_slice(image.identity().as_ref());
    RecordAddress {
        namespace,
        resource,
        generation: 0,
        suffix,
    }
    .try_into()
}
