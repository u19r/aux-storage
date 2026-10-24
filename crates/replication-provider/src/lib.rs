//! Regional replication decisions over caller-owned native attempts.
mod bounded_json;
pub mod canonical_event_codec;
pub mod canonical_item_original;
mod constants;
pub mod control;
pub mod delivery;
pub mod error;
pub mod history;
pub mod ordering;
pub mod record;
pub mod recovery;
pub mod regional_application;
pub mod runtime_configuration;
pub use constants::CANONICAL_EVENT_SEMANTIC_VERSION;

#[cfg(test)]
mod canonical_event_codec_tests;
