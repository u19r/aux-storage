//! Active global source preparation; these facts do not authorize membership
//! activation.

mod definition;
mod executor;
mod request;
mod vector;

pub use definition::HistoryCaptureDefinition;
pub use executor::{
    advance_history_capture, cancel_history_capture, load_history_capture_cuts,
    load_history_capture_definition, load_retained_baseline_definition,
    load_retained_baseline_page, load_retired_history_page, load_retired_source_catalog,
    observe_history_capture, release_history_capture,
};
pub use request::HistoryCaptureRequest;
pub use vector::{HistoryCaptureStatus, HistoryCaptureVectorPage};

mod retired;
pub use retired::{RetiredHistoryDefinition, RetiredHistoryPage};

mod baseline;

pub use baseline::{RetainedBaselineDefinition, RetainedBaselinePage, RetainedBaselineScope};

mod catalog;
pub use catalog::{HistoryCatalogScope, RetiredHistoryCatalogPage};
