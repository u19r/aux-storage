mod error;
mod models;
mod progress;

pub use error::ScanTailError;
pub use models::{ScanPageEnd, ScanProgress, ScanTransition, TailPageEnd, TailProgress};
