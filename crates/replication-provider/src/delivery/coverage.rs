use std::num::NonZeroUsize;

use replication_types::{
    error::ReplicationProtocolError, event::interval::ClosedSourceInterval,
    identity::origin::OriginPosition,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCoverage {
    through: OriginPosition,
    pending: Vec<ClosedSourceInterval>,
}

impl DeliveryCoverage {
    pub fn through(&self) -> OriginPosition {
        self.through
    }
    pub fn pending(&self) -> &[ClosedSourceInterval] {
        &self.pending
    }

    /// Complete acceptance may be contiguous or held behind an earlier gap.
    pub fn has_accepted(&self, range: ClosedSourceInterval) -> bool {
        range.through() <= self.through
            || self.pending.iter().any(|pending| {
                pending.after() <= range.after() && pending.through() >= range.through()
            })
    }

    pub fn validate(&self, limit: NonZeroUsize) -> Result<(), ReplicationProtocolError> {
        if self.pending.len() > limit.get()
            || self
                .pending
                .first()
                .is_some_and(|range| range.after() <= self.through)
            || self
                .pending
                .windows(2)
                .any(|ranges| ranges[1].after() <= ranges[0].through())
        {
            return Err(ReplicationProtocolError::InvalidRange {
                context: "durable delivery coverage",
            });
        }
        Ok(())
    }

    pub fn accept(
        &self,
        range: ClosedSourceInterval,
        limit: NonZeroUsize,
    ) -> Result<Self, ReplicationProtocolError> {
        self.validate(limit)?;
        let insertion = self
            .pending
            .partition_point(|pending| pending.after() <= range.after());
        let mut next = Self {
            through: self.through,
            pending: Vec::new(),
        };
        for range in self.pending[..insertion]
            .iter()
            .chain(std::iter::once(&range))
            .chain(&self.pending[insertion..])
        {
            next.merge_range(*range)?;
        }
        next.validate(limit)?;
        Ok(next)
    }

    fn merge_range(&mut self, range: ClosedSourceInterval) -> Result<(), ReplicationProtocolError> {
        if range.after() <= self.through {
            self.through = self.through.max(range.through());
        } else if let Some(last) = self
            .pending
            .last_mut()
            .filter(|last| range.after() <= last.through())
        {
            *last = ClosedSourceInterval::try_from((
                last.after(),
                last.through().max(range.through()),
            ))?;
        } else {
            self.pending.push(range);
        }
        Ok(())
    }
}

impl Default for DeliveryCoverage {
    fn default() -> Self {
        Self {
            through: 0.into(),
            pending: Vec::new(),
        }
    }
}
