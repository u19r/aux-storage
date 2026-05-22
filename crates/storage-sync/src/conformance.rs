use crate::{ResolvedSyncMutationBatch, SyncMutationResponse};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncConformanceCase<Request> {
    pub name: &'static str,
    pub request: Request,
    pub expected: SyncConformanceExpectation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncConformanceExpectation {
    pub resolved_batch: ResolvedSyncMutationBatch,
    pub responses: Vec<SyncMutationResponse>,
}

impl SyncConformanceExpectation {
    #[must_use]
    pub fn new(
        resolved_batch: ResolvedSyncMutationBatch,
        responses: Vec<SyncMutationResponse>,
    ) -> Self {
        Self {
            resolved_batch,
            responses,
        }
    }
}
