use storage_types::PaginationLimit;

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
pub const STREAM_LIMITS: PaginationLimit = PaginationLimit::new(DEFAULT_LIMIT, MAX_LIMIT);
