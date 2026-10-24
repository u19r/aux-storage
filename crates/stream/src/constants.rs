/// Maximum user stream name length accepted by the stream manager.
pub(crate) const MAX_STREAM_NAME_LEN: usize = 255;

/// Maximum cursor name length accepted by the stream manager.
pub(crate) const MAX_CURSOR_NAME_LEN: usize = 64;

/// Stream and cursor names allow separators between alphanumeric boundary
/// characters.
pub(crate) const STREAM_NAME_ALLOWED_MIDDLE_CHARS: &[char] = &['_', '-', '.'];
pub(crate) const CURSOR_NAME_ALLOWED_MIDDLE_CHARS: &[char] = &['_', '-'];

/// TTL accepts one second through one non-leap year.
pub(crate) const MAX_STREAM_TTL_SECONDS: u32 = 31_536_000;
