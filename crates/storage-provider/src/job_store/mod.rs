//! Backend-independent checked-commit decision and provider boundary.

mod atomic_commit;
mod attempt;
mod error;
mod executor;
mod limits;
mod range;
mod retry;
mod settlement;

pub use atomic_commit::AtomicCommit;
pub use attempt::{ContractErrorSlot, run_native_job_attempt};
pub use error::JobCommitError;
pub use executor::execute_commit;
pub use limits::scheduled_job_capabilities;
pub use range::finish_range_page;
pub use retry::settle_native_commit;
pub use settlement::run_native_job_read;
