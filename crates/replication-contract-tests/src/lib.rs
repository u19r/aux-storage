//! Regional algorithm fixtures and independent real-provider contracts.
mod harness_result;
#[cfg(feature = "native")]
pub mod native;

pub use harness_result::HarnessResult;
