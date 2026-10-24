//! SQLite native driver; item and table algorithms live above this module.

mod attempt;
mod driver;
mod durability;
mod error;
mod record_limits;
mod statements;
mod transaction;
mod value;

#[cfg(test)]
mod durability_tests;
#[cfg(test)]
mod transaction_tests;

pub use attempt::SqliteAttempt;
pub use driver::SqliteBackend;
