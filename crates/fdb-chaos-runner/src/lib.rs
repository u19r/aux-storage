mod aggregate;
mod artifact_io;
mod cli;
mod constants;
mod entrypoint;
mod minimize;
mod profiles;
mod simulation;
mod trace_digest;

#[cfg(test)]
mod profile_tests;
#[cfg(test)]
mod trace_digest_tests;

pub use entrypoint::run;
