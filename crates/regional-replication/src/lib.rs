//! Regional pair replication (design B): messages, region configuration, peer
//! transport.
pub mod client;
pub mod config;
#[cfg(test)]
mod config_tests;
mod constants;
pub mod message;
#[cfg(test)]
mod message_tests;
pub mod server;
#[cfg(test)]
mod server_tests;
pub mod transport;

pub use client::RegionalPeerClient;
pub use config::{RegionName, RegionalConfig};
pub use server::{RegionalRequestHandler, spawn_server};
