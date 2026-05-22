use std::collections::BTreeSet;

use crate::{
    BatchGetDecision, CacheReadOutcome, CacheState, Epoch, QueryDecision, QueryRequest, Slot,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadRequest {
    Get {
        slot: Slot,
        strong: bool,
        request_epoch: Epoch,
    },
    BatchGet {
        requested_keys: BTreeSet<Slot>,
        strong: bool,
        request_epoch: Epoch,
    },
    Query {
        query: QueryRequest,
        strong: bool,
        request_epoch: Epoch,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedRead {
    Get {
        outcome: CacheReadOutcome,
        slot_present: bool,
    },
    BatchGet {
        outcome: CacheReadOutcome,
        served_keys: BTreeSet<Slot>,
        fallback_keys: BTreeSet<Slot>,
        returned_present_keys: BTreeSet<Slot>,
        returned_absent_keys: BTreeSet<Slot>,
    },
    Query {
        outcome: CacheReadOutcome,
        serve_whole_page: bool,
        cache_evaluated_keys: Vec<Slot>,
        returned_page: Vec<Slot>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DifferentialMismatch {
    pub field: &'static str,
    pub expected: String,
    pub observed: String,
}

impl CacheState {
    #[must_use]
    pub fn expected_read(&self, request: &ReadRequest) -> ObservedRead {
        match request {
            ReadRequest::Get {
                slot,
                strong,
                request_epoch,
            } => {
                let outcome = if *strong {
                    self.strong_get_decision(*slot, *request_epoch)
                } else {
                    self.eventual_get_decision(*slot, *request_epoch)
                };

                ObservedRead::Get {
                    outcome,
                    slot_present: self.db_present.contains(slot),
                }
            }
            ReadRequest::BatchGet {
                requested_keys,
                strong,
                request_epoch,
            } => {
                let BatchGetDecision {
                    outcome,
                    served_keys,
                    fallback_keys,
                    ..
                } = self.batch_get_decision(*strong, requested_keys, *request_epoch);
                let returned_present_keys = requested_keys
                    .iter()
                    .copied()
                    .filter(|slot| self.db_present.contains(slot))
                    .collect();
                let returned_absent_keys = requested_keys
                    .iter()
                    .copied()
                    .filter(|slot| !self.db_present.contains(slot))
                    .collect();

                ObservedRead::BatchGet {
                    outcome,
                    served_keys,
                    fallback_keys,
                    returned_present_keys,
                    returned_absent_keys,
                }
            }
            ReadRequest::Query {
                query,
                strong,
                request_epoch,
            } => {
                let QueryDecision {
                    outcome,
                    serve_whole_page,
                    cache_evaluated_keys,
                    ..
                } = self.query_decision(query, *strong, *request_epoch);

                ObservedRead::Query {
                    outcome,
                    serve_whole_page,
                    cache_evaluated_keys,
                    returned_page: self.source_returned_page(query),
                }
            }
        }
    }
}

pub fn compare_observed_read(
    state: &CacheState,
    request: &ReadRequest,
    observed: &ObservedRead,
) -> Result<(), DifferentialMismatch> {
    let expected = state.expected_read(request);
    if expected == *observed {
        return Ok(());
    }

    match (&expected, observed) {
        (
            ObservedRead::Get {
                outcome: expected_outcome,
                slot_present: expected_present,
            },
            ObservedRead::Get {
                outcome: observed_outcome,
                slot_present: observed_present,
            },
        ) => compare_get(
            *expected_outcome,
            *observed_outcome,
            *expected_present,
            *observed_present,
        ),
        (
            ObservedRead::BatchGet {
                outcome: expected_outcome,
                served_keys: expected_served,
                fallback_keys: expected_fallback,
                returned_present_keys: expected_present,
                returned_absent_keys: expected_absent,
            },
            ObservedRead::BatchGet {
                outcome: observed_outcome,
                served_keys: observed_served,
                fallback_keys: observed_fallback,
                returned_present_keys: observed_present,
                returned_absent_keys: observed_absent,
            },
        ) => compare_batch(
            *expected_outcome,
            *observed_outcome,
            expected_served,
            observed_served,
            expected_fallback,
            observed_fallback,
            expected_present,
            observed_present,
            expected_absent,
            observed_absent,
        ),
        (
            expected_query @ ObservedRead::Query { .. },
            observed_query @ ObservedRead::Query { .. },
        ) => compare_query(expected_query, observed_query),
        _ => Err(DifferentialMismatch {
            field: "request_kind",
            expected: format!("{expected:?}"),
            observed: format!("{observed:?}"),
        }),
    }
}

fn compare_get(
    expected_outcome: CacheReadOutcome,
    observed_outcome: CacheReadOutcome,
    expected_present: bool,
    observed_present: bool,
) -> Result<(), DifferentialMismatch> {
    if expected_outcome != observed_outcome {
        return Err(DifferentialMismatch {
            field: "outcome",
            expected: format!("{expected_outcome:?}"),
            observed: format!("{observed_outcome:?}"),
        });
    }
    if expected_present != observed_present {
        return Err(DifferentialMismatch {
            field: "slot_present",
            expected: expected_present.to_string(),
            observed: observed_present.to_string(),
        });
    }
    Ok(())
}

#[expect(clippy::too_many_arguments)]
fn compare_batch(
    expected_outcome: CacheReadOutcome,
    observed_outcome: CacheReadOutcome,
    expected_served: &BTreeSet<Slot>,
    observed_served: &BTreeSet<Slot>,
    expected_fallback: &BTreeSet<Slot>,
    observed_fallback: &BTreeSet<Slot>,
    expected_present: &BTreeSet<Slot>,
    observed_present: &BTreeSet<Slot>,
    expected_absent: &BTreeSet<Slot>,
    observed_absent: &BTreeSet<Slot>,
) -> Result<(), DifferentialMismatch> {
    if expected_outcome != observed_outcome {
        return Err(DifferentialMismatch {
            field: "outcome",
            expected: format!("{expected_outcome:?}"),
            observed: format!("{observed_outcome:?}"),
        });
    }
    if expected_served != observed_served {
        return Err(DifferentialMismatch {
            field: "served_keys",
            expected: format!("{expected_served:?}"),
            observed: format!("{observed_served:?}"),
        });
    }
    if expected_fallback != observed_fallback {
        return Err(DifferentialMismatch {
            field: "fallback_keys",
            expected: format!("{expected_fallback:?}"),
            observed: format!("{observed_fallback:?}"),
        });
    }
    if expected_present != observed_present {
        return Err(DifferentialMismatch {
            field: "returned_present_keys",
            expected: format!("{expected_present:?}"),
            observed: format!("{observed_present:?}"),
        });
    }
    if expected_absent != observed_absent {
        return Err(DifferentialMismatch {
            field: "returned_absent_keys",
            expected: format!("{expected_absent:?}"),
            observed: format!("{observed_absent:?}"),
        });
    }
    Ok(())
}

fn compare_query(
    expected: &ObservedRead,
    observed: &ObservedRead,
) -> Result<(), DifferentialMismatch> {
    let (
        ObservedRead::Query {
            outcome: expected_outcome,
            serve_whole_page: expected_serve,
            cache_evaluated_keys: expected_keys,
            returned_page: expected_page,
        },
        ObservedRead::Query {
            outcome: observed_outcome,
            serve_whole_page: observed_serve,
            cache_evaluated_keys: observed_keys,
            returned_page: observed_page,
        },
    ) = (expected, observed)
    else {
        return Err(DifferentialMismatch {
            field: "request_kind",
            expected: format!("{expected:?}"),
            observed: format!("{observed:?}"),
        });
    };

    if expected_outcome != observed_outcome {
        return Err(DifferentialMismatch {
            field: "outcome",
            expected: format!("{expected_outcome:?}"),
            observed: format!("{observed_outcome:?}"),
        });
    }
    if expected_serve != observed_serve {
        return Err(DifferentialMismatch {
            field: "serve_whole_page",
            expected: expected_serve.to_string(),
            observed: observed_serve.to_string(),
        });
    }
    if expected_keys != observed_keys {
        return Err(DifferentialMismatch {
            field: "cache_evaluated_keys",
            expected: format!("{expected_keys:?}"),
            observed: format!("{observed_keys:?}"),
        });
    }
    if expected_page != observed_page {
        return Err(DifferentialMismatch {
            field: "returned_page",
            expected: format!("{expected_page:?}"),
            observed: format!("{observed_page:?}"),
        });
    }
    Ok(())
}
