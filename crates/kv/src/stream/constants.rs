/// Marker byte for the current stream item storage codec.
pub(crate) const STREAM_ITEM_FORMAT_MAGIC: u8 = 0xA5;

/// Stored stream item flag indicating the optional stream name field is
/// present.
pub(crate) const STREAM_ITEM_FLAG_STREAM_NAME_PRESENT: u8 = 0x01;

/// Stream TTL cleanup fallback cadence used when the job manager schedules the
/// per-stream cleanup task.
pub(crate) const STREAM_TTL_CLEANUP_SLEEP_SECONDS: u64 = 3_600;

/// Maximum combined old/new image payload size embedded directly in stream
/// pointer records. Larger images stay in the item stream to avoid duplicating
/// write-path bytes and allocations.
pub(crate) const STREAM_EMBEDDED_MAX_BYTES: usize = 1024;
