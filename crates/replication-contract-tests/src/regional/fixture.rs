use super::{event::HarnessEvent, replica::RegionalReplica};
use crate::HarnessResult;

pub struct RegionalFixture {
    pub us_east_1: RegionalReplica,
    pub eu_central_1: RegionalReplica,
}
impl RegionalFixture {
    pub fn new() -> HarnessResult<Self> {
        Ok(Self {
            us_east_1: RegionalReplica::new("us-east-1")?,
            eu_central_1: RegionalReplica::new("eu-central-1")?,
        })
    }
    /// Heal by exchanging all immutable accepted events; replay order and
    /// multiplicity vary.
    pub fn heal(&mut self, reverse: bool, copies: usize) -> HarnessResult<()> {
        if copies == 0 {
            return Err("zero delivery copies".into());
        }
        let mut left = self
            .us_east_1
            .history()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut right = self
            .eu_central_1
            .history()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if reverse {
            left.reverse();
            right.reverse();
        }
        deliver(&mut self.eu_central_1, &left, copies)?;
        deliver(&mut self.us_east_1, &right, copies)?;
        Ok(())
    }
    pub fn assert_converged(&self) -> HarnessResult<()> {
        if self.us_east_1.history() != self.eu_central_1.history()
            || self.us_east_1.winners() != self.eu_central_1.winners()
        {
            return Err("regional divergence".into());
        }
        Ok(())
    }
}
fn deliver(
    target: &mut RegionalReplica,
    events: &[HarnessEvent],
    copies: usize,
) -> HarnessResult<()> {
    for event in events {
        for _ in 0..copies {
            target.accept(serde_json::from_slice(&serde_json::to_vec(event)?)?)?;
        }
    }
    Ok(())
}
