//! PERF-FDB-1: the catalog-version read that rides in an attempt's next data
//! wave, and the gate that keeps pipelined data undecoded until that
//! observation matches the candidates' version. Data waves take `&self`.

use std::sync::{
    Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};

use storage_types::CatalogVersion;

#[derive(Default)]
pub(super) struct CatalogVersionPipeline {
    requested: AtomicBool,
    state: Mutex<PipelineState>,
}

#[derive(Default)]
struct PipelineState {
    /// The candidates' cached version of the pending request.
    expected: Option<CatalogVersion>,
    /// `Some(observation)` once a data wave read it.
    observed: Option<Option<CatalogVersion>>,
}

impl CatalogVersionPipeline {
    pub(super) fn request(&self, expected: CatalogVersion) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = PipelineState {
            expected: Some(expected),
            observed: None,
        };
        self.requested.store(true, Ordering::SeqCst);
    }

    /// The wave that calls this carries the read; at most one wave does.
    pub(super) fn take_request(&self) -> bool {
        self.requested.swap(false, Ordering::SeqCst)
    }

    pub(super) fn record(&self, observation: Option<CatalogVersion>) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .observed = Some(observation);
    }

    /// Ends the gate: after this the attempt decodes normally.
    pub(super) fn take_observed(&self) -> Option<Option<CatalogVersion>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.expected = None;
        state.observed.take()
    }

    /// A data wave observed a version other than the expected one, and
    /// shared code has not taken it yet: pipelined data must not be decoded.
    pub(super) fn moved(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        matches!(
            (state.expected, state.observed),
            (Some(expected), Some(observation)) if observation != Some(expected)
        )
    }

    /// A catalog change in the attempt ends any pending or recorded read.
    pub(super) fn clear(&self) {
        self.requested.store(false, Ordering::SeqCst);
        self.take_observed();
    }
}
