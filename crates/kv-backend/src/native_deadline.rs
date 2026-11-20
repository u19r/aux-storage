//! The one absolute operation deadline of a native attempt and the clock that
//! sleeps toward it. FoundationDB simulation and tests inject their clock.

use std::sync::Arc;

use storage_types::{AttemptDeadline, Clock};

pub(crate) struct NativeDeadline {
    pub(crate) deadline: AttemptDeadline,
    pub(crate) clock: Arc<dyn Clock>,
}
