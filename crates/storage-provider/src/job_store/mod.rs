//! Backend-independent checked-commit decision and provider boundary.

mod attempt;
mod error;
mod executor;
mod limits;
mod models;
mod port;
mod range;
mod retry;
mod settlement;
mod transaction;

pub use attempt::{ContractErrorSlot, run_native_job_attempt};
pub use error::JobCommitError;
pub use executor::execute_commit;
pub use limits::scheduled_job_capabilities;
pub use models::{
    CheckObservation, CommitExecution, CommittedTransaction, ConflictedTransaction,
    EstablishedCommit, EstablishedCommitOutcome, ScheduledJobMutations, ScheduledJobPut,
    TransactionCheckObservations,
};
pub use port::AtomicCommit;
pub use range::finish_range_page;
pub use retry::settle_native_commit;
pub use settlement::run_native_job_read;
pub use transaction::encode_transaction;
