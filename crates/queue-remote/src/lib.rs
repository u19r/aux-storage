//! Internal remote SQS-compatible queue provider implementation.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

mod provider;
mod settings;

pub use provider::RemoteQueueProvider;
pub use settings::{
    RemoteCredentialStrategy, RemoteQueueSettings, RemoteSigv4Settings, RemoteStaticCredentials,
    RemoteTimeoutOverrides,
};

#[cfg(test)]
mod settings_tests;
