#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanTailError {
    ProgressConflict,
    InvalidBoundary,
}
