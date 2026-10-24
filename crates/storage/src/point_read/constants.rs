pub(super) const POINT_READ_BATCH_KEYS: usize = 100;
pub(super) const POINT_READ_BATCH_BYTES: usize = 1_048_576;
pub(super) const POINT_READ_BATCH_ATTEMPTS: u32 = 3;
pub(super) const POINT_READ_SOURCE_KEYS: usize = 128;
pub(super) const POINT_READ_SOURCE_SUBSCRIBERS: usize = 4096;
pub(super) const POINT_READ_REQUEST_KEYS: usize = 1024;
pub(super) const POINT_READ_RETAINED_BYTES: usize = 32 * 1024 * 1024;
pub(super) const POINT_READ_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub(super) const POINT_READ_SOURCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
pub(super) const POINT_READ_WORKERS: usize = 2;
pub(super) const POINT_READ_REQUEST_BYTES: usize = 4 * 1024 * 1024;
pub(super) const POINT_READ_ITEM_OVERHEAD_BYTES: usize = 256;
pub(super) const POINT_READ_SOURCE_OVERHEAD_BYTES: usize = 2048;
pub(super) const POINT_READ_KEY_COPY_CHARGE: usize = 6;
pub(super) const POINT_READ_IDENTITY_COPY_CHARGE: usize = 4;
pub(super) const POINT_READ_PLANNED_BYTES: usize = 8 * 1024 * 1024;

pub(super) const POINT_READ_BACKGROUND_COLLECTION: std::time::Duration =
    std::time::Duration::from_millis(5);
