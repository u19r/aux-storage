//! Reservation-set revision is independent of ordinary retention epoch churn.
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ReservationRevision(Revision);

impl ReservationRevision {
    pub(crate) const GENESIS: Self = Self(Revision::INITIAL);

    pub(crate) fn checked_next(self) -> StorageResult<Self> {
        self.0.checked_next().map(Self)
    }

    pub(crate) fn require_barrier(self, native: Revision, count: u64) -> StorageResult<()> {
        let transitions = u64::from(self.0);
        if self.0 > native || count > transitions || !(transitions - count).is_multiple_of(2) {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}
