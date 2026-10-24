//! Durable bounded collection of active source facts. Completion grants no
//! target activation.
mod collector;
pub(crate) mod executor;
mod record;

pub(crate) use collector::HistoryCollector;
pub(crate) use record::{
    AncestryCollectionStage, HistoryCollectionChange, HistoryCollectionObservation,
    HistoryCollectionRecord,
};

mod definition;
pub use definition::HistoryAdoptionDefinition;

mod ancestry;

mod observation;

mod adoption;
pub(crate) mod adoption_executor;
mod source_adoption;
pub(crate) mod source_executor;
