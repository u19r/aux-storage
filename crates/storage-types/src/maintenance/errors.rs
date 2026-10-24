use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TargetIncarnationError {
    #[error("maintenance target incarnation must not be empty")]
    Empty,
    #[error("maintenance target incarnation exceeds 256 bytes")]
    TooLong,
}
