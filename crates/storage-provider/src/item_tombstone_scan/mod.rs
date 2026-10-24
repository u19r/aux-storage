//! Bounded deleted-item authority reads. These are native observations;
//! snapshot holds and mutation-tail reconciliation belong to the caller.

mod page;
mod request;
mod row;

pub use page::ItemTombstonePage;
pub use request::{ItemTombstoneScan, ItemTombstoneScanAttempt};
pub use row::ItemTombstone;
