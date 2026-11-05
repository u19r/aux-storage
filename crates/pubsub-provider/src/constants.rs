use std::{num::NonZeroU32, time::Duration};

use storage_types::WriteRetryPolicy;

pub(crate) const COMPAT_PRODUCT_NAME: &str = concat!("Amazon ", "SN", "S");
pub(crate) const SNS_NOT_FOUND_ERROR_TYPE: &str = "NotFound";
pub(crate) const SNS_INVALID_PARAMETER_ERROR_TYPE: &str = "InvalidParameter";
pub(crate) const SNS_INTERNAL_ERROR_TYPE: &str = "InternalError";

pub const PUBLISH_INTENT_LEASE_MILLIS: i64 = 30_000;

pub(crate) const PUBSUB_PAGE_LIMIT: NonZeroU32 = NonZeroU32::MIN.saturating_add(100 - 1);
pub(crate) const PUBSUB_SNAPSHOT_CHUNK_SIZE: usize = 10;
pub(crate) const PUBSUB_CLAIM_BATCH_SIZE: NonZeroU32 = NonZeroU32::MIN.saturating_add(32 - 1);
pub(crate) const PUBSUB_WRITE_RETRY: WriteRetryPolicy =
    WriteRetryPolicy::new(5, Duration::from_millis(2));

pub(crate) const SNS_DEFAULT_MAX_MESSAGE_SIZE_BYTES: usize = 256 * 1024;
pub(crate) const SNS_SUBJECT_CHARACTER_LIMIT: usize = 100;

pub(crate) const SNS_MAX_ATTRIBUTE_NAME_BYTES: usize = 256;
