/// Scan overfetch factor because expired or already-claimed deliveries can
/// remain in the claim index until cleanup.
pub(crate) const CLAIM_SCAN_MULTIPLIER: usize = 32;

/// Hard cap for one delivery claim scan to bound memory and transaction size.
pub(crate) const CLAIM_SCAN_MAX_LIMIT: u32 = 1024;
