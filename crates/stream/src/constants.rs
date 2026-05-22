/// Maximum user stream name length accepted by the stream manager.
pub(crate) const MAX_STREAM_NAME_LEN: usize = 255;

/// Maximum cursor name length accepted by the stream manager.
pub(crate) const MAX_CURSOR_NAME_LEN: usize = 64;

/// Stream and cursor names allow separators between alphanumeric boundary
/// characters.
pub(crate) const STREAM_NAME_ALLOWED_MIDDLE_CHARS: &[char] = &['_', '-', '.'];
pub(crate) const CURSOR_NAME_ALLOWED_MIDDLE_CHARS: &[char] = &['_', '-'];

/// Stream item payloads are capped at 1 MiB.
pub(crate) const MAX_STREAM_ITEM_DATA_BYTES: usize = 1_048_576;

/// TTL accepts one second through one non-leap year.
pub(crate) const MAX_STREAM_TTL_SECONDS: u32 = 31_536_000;
