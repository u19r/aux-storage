//! Qualified age is transient, revocable process evidence, never wire
//! authority.
use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use super::{clocks::ClockObservation, source::QualifiedTimeSample};
use crate::journal::public_trim_scan::CompleteTrimPrefix;

pub(crate) struct QualifiedPrefixAge {
    prefix: CompleteTrimPrefix,
    first: QualifiedTimeSample,
    current: QualifiedTimeSample,
    minimum: NonZeroU64,
    live: Arc<AtomicBool>,
}

impl QualifiedPrefixAge {
    pub(super) fn try_qualify(
        prefix: CompleteTrimPrefix,
        samples: (QualifiedTimeSample, QualifiedTimeSample),
        binding: (NonZeroU64, Arc<AtomicBool>),
    ) -> Option<Self> {
        let age = Self {
            prefix,
            first: samples.0,
            current: samples.1,
            minimum: binding.0,
            live: binding.1,
        };
        age.has_native_age(&age.prefix, age.minimum).then_some(age)
    }

    pub(crate) fn has_native_age(&self, prefix: &CompleteTrimPrefix, minimum: NonZeroU64) -> bool {
        self.prefix == *prefix
            && self.minimum == minimum
            && self.live.load(Ordering::Acquire)
            && tokio::time::Instant::now() < self.current.deadline
            && self.require_full_age().is_some()
            && tokio::time::Instant::now() < self.current.deadline
            && self.live.load(Ordering::Acquire)
    }

    pub(crate) fn grant_binding(
        &self,
    ) -> storage_types::StorageResult<replication_types::protocol::CanonicalDigest> {
        replication_types::protocol::CanonicalDigest::of_json(&(
            self.prefix.time_binding(),
            self.minimum,
            self.first.clock.wall,
            self.first.error,
            self.current.clock.wall,
            self.current.error,
            self.current.frequency,
            self.current.binding,
        ))
        .map_err(|_| crate::native_record::corrupt_record())
    }

    fn require_full_age(&self) -> Option<()> {
        if self.first.binding != self.current.binding {
            return None;
        }
        let now = ClockObservation::sample()?;
        let (_, elapsed) = now.elapsed_since(self.current.clock)?;
        let drift = elapsed
            .checked_mul(self.current.frequency)?
            .checked_add(999_999_999)?
            .checked_div(1_000_000_000)?;
        // Continuity admits an error envelope around the last qualified sample.
        // Include both that envelope and its original error in the current
        // bound.
        let error = self.current.error.checked_mul(3)?.checked_add(drift)?;
        now.require_continuity(
            self.current.clock,
            self.current.error.checked_mul(2)?,
            self.current.frequency,
        )?;
        let wall_lower = now.wall.checked_sub(error)?;
        let publisher = i128::from(self.prefix.time_binding().1) * 1_000_000;
        let minimum = i128::from(self.minimum.get()) * 1_000_000;
        let qualified_elapsed =
            wall_lower.checked_sub(self.first.clock.wall.checked_add(self.first.error)?)?;
        let elapsed = now.require_continuity(
            self.first.clock,
            self.first.error.checked_add(error)?,
            self.first.frequency.max(self.current.frequency),
        )?;
        if wall_lower.checked_sub(publisher)? < minimum || elapsed.min(qualified_elapsed) < minimum
        {
            return None;
        }
        Some(())
    }
}
