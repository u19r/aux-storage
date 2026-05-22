pub(super) const CONFLICTING_KEYS_PREFIX: &[u8] = b"\xff\xff/transaction/conflicting_keys/";
pub(super) const READ_CONFLICT_RANGE_PREFIX: &[u8] = b"\xff\xff/transaction/read_conflict_range/";
pub(super) const WRITE_CONFLICT_RANGE_PREFIX: &[u8] = b"\xff\xff/transaction/write_conflict_range/";
pub(super) const CONFLICT_LOG_MAX_KEYS: usize = 64;
pub(super) const CONFLICT_LOG_MAX_RANGES: usize = 32;
