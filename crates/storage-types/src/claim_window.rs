//! The slot rule of a claim over a snapshot window of due entries, shared by
//! the SQS claim and the SNS delivery and publish-chunk claims (W2 D3,
//! W3 design 7.1).

use std::num::NonZeroU32;

/// One claim attempt's choice of entries in an observed window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClaimSlot {
    /// Entries one claim takes at most.
    pub limit: NonZeroU32,
    pub first_attempt: bool,
    /// The caller's random value, so tests are deterministic.
    pub rotation: u32,
}

impl ClaimSlot {
    /// Aligned slots of `limit` entries: two claims that observed the same
    /// entries select the same slot or disjoint slots, never a partial
    /// overlap. Claims with different observations (a write between their
    /// scans shifts the entries) can overlap partially; the conflict read of
    /// the chosen keys then decides. The first attempt with fewer than two
    /// full slots takes slot 0 (exact order). With two or more full slots, a
    /// full window, or on a retry, the claim takes the slot of `rotation`, so
    /// that concurrent first attempts do not all collide on slot 0; order is
    /// then best effort.
    #[must_use]
    pub fn positions(&self, observed: usize, window_full: bool) -> Vec<usize> {
        let limit = self.limit.get() as usize;
        if observed == 0 {
            return Vec::new();
        }
        let slots = (observed / limit).max(1);
        let slot = if self.first_attempt && !window_full && slots < 2 {
            0
        } else {
            self.rotation as usize % slots
        };
        (slot * limit..((slot + 1) * limit).min(observed)).collect()
    }
}
