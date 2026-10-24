pub(super) const CUTOVER_PK: &str = "SYS#CUTOVER";
pub(super) const CUTOVER_STATUS_SCHEDULED: &str = "scheduled";
pub(super) const CUTOVER_STATUS_APPLIED: &str = "applied";
pub(super) const UPDATED_AT_ATTR: &str = "updated_at";
pub(super) const BACKFILL_RECENCY_CONDITION: &str =
    "attribute_not_exists(#updated_at) OR #updated_at <= :updated_at";
