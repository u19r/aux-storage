//! Postgres native driver; domain SQL text and row layout live above it.

mod attempt;
mod driver;
mod error;
mod input_limits;
mod network;
mod record_limits;
mod statements;
mod value;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod transaction_tests;

pub use attempt::PostgresAttempt;
pub use driver::PostgresBackend;

#[cfg(test)]
mod attempt_tests;

#[cfg(test)]
mod error_tests;

#[cfg(test)]
mod item_lock_tests;
