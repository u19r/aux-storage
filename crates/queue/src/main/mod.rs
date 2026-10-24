mod app;
mod constants;
mod entrypoint;
mod protocol;

pub use entrypoint::main;

#[cfg(test)]
mod backend_builder_tests;
#[cfg(test)]
mod main_tests;
#[cfg(test)]
mod protocol_tests;
