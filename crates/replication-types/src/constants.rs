pub(crate) const JOURNAL_PARTITION_HASH_PREFIX: &[u8] = b"aux-storage/regional-journal/v1\0";
pub(crate) const MAX_REGION_ID_BYTES: usize = 64;
/// Transaction observations plus the item's predecessor and configuration
/// parent.
pub(crate) const MAX_ITEM_EVENT_PARENTS: usize = storage_types::MAX_TRANSACTION_MEMBERS + 2;
