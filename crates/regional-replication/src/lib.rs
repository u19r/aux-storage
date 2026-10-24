//! Native regional replication over the existing regional database boundary.
pub mod bootstrap;
mod canonical_original;
mod constants;
pub mod control;
pub mod delivery;
mod history_capacity;
mod history_image_charge;
pub mod import;
pub mod import_receipt;
mod job_recovery;
pub mod journal;
mod native_read;
mod native_record;
pub(crate) mod origin;
pub mod runtime;
pub mod serving;
pub mod source_frame;
pub mod transport;

#[cfg(test)]
mod bootstrap_native_tests;
#[cfg(test)]
mod fragment_native_tests;
#[cfg(test)]
mod native_admission_test_support;
#[cfg(test)]
mod native_slice_tests;
#[cfg(test)]
mod source_frame_performance_tests;
#[cfg(test)]
mod test_support;

pub mod enable;

pub mod lifecycle;

pub mod history_capture;

pub mod history_collection;

pub mod addition;
pub mod streams;

pub mod history;

mod archive;

pub mod trim;
