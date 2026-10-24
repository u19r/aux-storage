//! Bounded, per-connection foreground admission control.
//!
//! The controller is deliberately independent of a provider implementation.  A
//! caller acquires a permit before starting a provider future and completes the
//! permit with the provider outcome.  This keeps the adaptive state machine and
//! the queue useful while the individual provider call sites are migrated.

mod admission_transition;
mod admission_types;
mod constants;
mod control;
mod controller;
mod drainage;
mod metrics;
mod native;
mod pool_capacity;
mod registry;
mod settlement;
mod state;
mod waiters;

pub(crate) use admission_types::is_admission_pressure;

#[cfg(test)]
pub(crate) use crate::admission::{
    admission_transition::Window,
    controller::PermitGrant,
    state::{ControllerState, lock_state},
    waiters::{Waiter, grant_waiters_from_locked},
};
pub use crate::admission::{
    admission_types::{
        AdmissionClass, AdmissionConfig, AdmissionConfigError, AdmissionOutcome,
        AdmissionRejection, AdmissionRejectionReason, AdmissionSnapshot, AdmissionState,
    },
    control::ControlPermit,
    controller::{AdmissionController, AdmissionPermit},
    registry::AdmissionRegistry,
};

#[cfg(test)]
mod admission_conformance_tests;
#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod metrics_tests;
#[cfg(test)]
mod quint_admission_tests;
