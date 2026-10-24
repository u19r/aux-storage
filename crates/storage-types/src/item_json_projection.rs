//! JSON output bounds derived from DynamoDB logical sizing, not request bytes.

use crate::{
    LogicalItemSize,
    constants::{DYNAMODB_WRITE_NUMBER_BYTES, MAX_ITEM_SIZE_BYTES},
};

// A positive logical byte pays a bounded number token, an escaped name byte,
// entry punctuation, a scalar wrapper and a possible zero-length set member.
// Twice the framing also covers preserved whitespace between JSON tokens.
const PROJECTED_BYTES_PER_LOGICAL_BYTE: usize = DYNAMODB_WRITE_NUMBER_BYTES
    + b"\"\", ".len()
    + 6
    + 2 * b"\"\":,".len()
    + 2 * b"{\"BOOL\":false}".len()
    + 2 * b"{\"SS\":[\"\"]}".len();

impl LogicalItemSize {
    /// Upper bound for a legal item's retained compact JSON projection.
    /// Names and collection membership pay framing; set uniqueness permits only
    /// one empty member. Escaped base64 costs at most 24 bytes per decoded
    /// byte. Compact numbers are no longer than their bounded canonical
    /// decimal form.
    pub const fn maximum_json_projection_bytes() -> usize {
        MAX_ITEM_SIZE_BYTES * PROJECTED_BYTES_PER_LOGICAL_BYTE + 2 * b"{}".len()
    }
}
