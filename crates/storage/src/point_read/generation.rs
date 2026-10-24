use std::{
    hash::{Hash, Hasher},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use super::{PointReadRequest, constants::POINT_READ_SOURCE_KEYS};
use crate::types::{StorageError, StorageResult};

/// Weak entries retain no request data after the last physical consumer
/// settles.
#[derive(Default)]
pub(super) struct SourceGenerations {
    state: Mutex<GenerationState>,
}

#[derive(Default)]
struct GenerationState {
    active: Vec<Weak<SourceGeneration>>,
    next_read_id: u64,
}

struct SourceGeneration {
    request: Arc<PointReadRequest>,
    invalidated: AtomicBool,
}

impl SourceGenerations {
    pub(super) fn capture(
        &self,
        request: Arc<PointReadRequest>,
        deadline: Instant,
    ) -> StorageResult<ReadValidity> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let existing = state
            .active
            .iter()
            .filter_map(Weak::upgrade)
            .find(|generation| {
                !generation.invalidated.load(Ordering::Acquire)
                    && generation.request.fingerprint == request.fingerprint
            });
        let generation = match existing {
            Some(generation) => generation,
            None => {
                if state.active.len() >= POINT_READ_SOURCE_KEYS {
                    state
                        .active
                        .retain(|generation| generation.strong_count() != 0);
                }
                if state.active.len() >= POINT_READ_SOURCE_KEYS {
                    return Err(StorageError::service_unavailable(1));
                }
                let generation = Arc::new(SourceGeneration {
                    request,
                    invalidated: AtomicBool::new(false),
                });
                state.active.push(Arc::downgrade(&generation));
                generation
            }
        };
        Ok(ReadValidity {
            generation,
            deadline,
        })
    }

    pub(super) fn start_read(&self, validity: ReadValidity) -> StorageResult<SourceRead> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let id = state.next_read_id;
        state.next_read_id = id
            .checked_add(1)
            .ok_or_else(|| StorageError::service_unavailable(1))?;
        Ok(SourceRead { id, validity })
    }

    pub(super) fn invalidate(&self, fingerprint: Option<&str>) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        for generation in state.active.iter().filter_map(Weak::upgrade) {
            if fingerprint.is_none_or(|key| key == generation.request.fingerprint) {
                generation.invalidated.store(true, Ordering::Release);
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct ReadValidity {
    generation: Arc<SourceGeneration>,
    deadline: Instant,
}

impl ReadValidity {
    pub(super) fn is_obsolete(&self) -> bool {
        self.generation.invalidated.load(Ordering::Acquire) || Instant::now() >= self.deadline
    }
}

impl PartialEq for ReadValidity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.generation, &other.generation)
    }
}

impl Eq for ReadValidity {}

impl Hash for ReadValidity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.generation).hash(state);
    }
}

/// One elected physical owner; retained native work keeps its unique identity.
#[derive(Clone)]
pub(super) struct SourceRead {
    id: u64,
    validity: ReadValidity,
}

impl SourceRead {
    pub(super) fn id(&self) -> u64 {
        self.id
    }

    pub(super) fn is_obsolete(&self) -> bool {
        self.validity.is_obsolete()
    }
}
