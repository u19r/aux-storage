//! Regional algorithm fixtures and independent real-provider contracts.
pub type HarnessResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
#[cfg(feature = "native")]
pub mod native;
pub mod regional;
