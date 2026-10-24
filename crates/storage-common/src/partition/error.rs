use std::fmt::{Display, Formatter};

use super::PartitionState;

#[derive(Debug)]
pub struct PartitionTransitionError {
    from: PartitionState,
    to: PartitionState,
}

impl PartitionTransitionError {
    pub(super) const fn new(from: PartitionState, to: PartitionState) -> Self {
        Self { from, to }
    }
}

impl Display for PartitionTransitionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "partition state cannot transition from {} to {}",
            self.from, self.to
        )
    }
}

impl std::error::Error for PartitionTransitionError {}
