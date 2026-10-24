//! Shared range-read request, binding, and page contracts.
//!
//! Providers keep SQL predicates and KV key encoding at their native owners.
//! This module owns request identity, index binding, and page invariants so a
//! filtered page cannot lose its continuation or exceed its evaluated bound.

mod budget;
mod operation;
mod page;

#[cfg(test)]
mod budget_tests;

#[cfg(test)]
mod range_read_tests;

pub use budget::{RangeAdmission, RangeReadBudget, RangeReadProgress};
pub use operation::{
    BoundRangeReadPage, BoundRangeReadPlan, RangeReadPlan, RangeReadRequest, RangeReadiness,
};
pub use page::{RangeReadItem, RangeReadPage};
