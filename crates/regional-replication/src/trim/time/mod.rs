//! Local time evidence is private process state, separate from native grants.
mod authentication;
mod clocks;
mod decimal;
mod reports;
mod source;
pub(crate) mod state;

pub(crate) mod age;
#[cfg(all(test, target_os = "linux"))]
mod qualified_trim_tests;
