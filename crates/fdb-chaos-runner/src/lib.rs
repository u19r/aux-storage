mod aggregate;
mod artifact_io;
mod cli;
mod constants;
mod entrypoint;
mod minimize;
mod profiles;
mod simulation;

#[cfg(test)]
mod profile_tests;

pub use entrypoint::run;
