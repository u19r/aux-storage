use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixtureGap {
    operation: &'static str,
}

impl fmt::Display for FixtureGap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "stateful fixture operation is not connected: {}",
            self.operation
        )
    }
}

impl std::error::Error for FixtureGap {}
