//! Shared condition-check membership (phase 4, §4.1).
//!
//! Immutable condition-check request with its source ordinal. Carries no
//! TTL context: checks are read-only.

use storage_types::TransactConditionCheckRequest;

/// One transaction condition check with its retained source ordinal.
#[derive(Clone)]
pub struct TableCheckMember {
    ordinal: usize,
    request: TransactConditionCheckRequest,
}

impl TableCheckMember {
    /// Store already-framed data; parsing stays with the current owners.
    pub fn new(ordinal: usize, request: TransactConditionCheckRequest) -> Self {
        Self { ordinal, request }
    }

    /// Source ordinal of the check in the original member order.
    pub fn ordinal(&self) -> usize {
        self.ordinal
    }

    /// Borrowed retained check request with every expression field.
    pub fn request(&self) -> &TransactConditionCheckRequest {
        &self.request
    }

    /// Consume the retained check without rebuilding its request map.
    pub fn into_parts(self) -> (usize, TransactConditionCheckRequest) {
        (self.ordinal, self.request)
    }
}
