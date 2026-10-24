//! Durable trim operation admission; grants and installed floors require later
//! native proof.
pub mod executor;
pub mod grant_turn;
pub mod proposal;
pub mod record;

pub(crate) mod time;

pub(crate) mod original;

pub mod commit;

pub mod installation;

pub mod completion;

#[cfg(test)]
pub(crate) mod native_trim_tests;
pub(crate) mod reservation;
