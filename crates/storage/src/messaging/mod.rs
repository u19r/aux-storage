//! The queue and pubsub services of one process and the store values they
//! run on.
mod services;
mod settings;
mod stores;

pub use services::MessagingServices;
pub use settings::{MessagingConfig, PubsubServiceConfig};
pub(crate) use stores::MessagingStores;
pub use stores::{MessagingLane, MessagingStore};

#[cfg(all(test, feature = "sqlite"))]
mod services_tests;
