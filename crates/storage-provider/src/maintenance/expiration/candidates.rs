use storage_types::{StorageError, StorageResult};

use super::{
    ExpirationCandidateBatch, ExpirationCandidateObservation, ExpirationDelete,
    ExpirationDeletionBatch,
};

impl ExpirationCandidateBatch {
    pub(crate) fn plan_deletions(
        self,
        observation: ExpirationCandidateObservation,
    ) -> StorageResult<ExpirationDeletionBatch> {
        let candidate_count = self.candidates.len();
        if observation.candidate_count != candidate_count {
            return Err(StorageError::internal(
                "expiration observation does not match its requested candidates",
            ));
        }
        let mut present = observation.present.iter().peekable();
        let mut items = Vec::with_capacity(present.len());
        let mut index_entries = Vec::with_capacity(candidate_count);
        let mut previous_index = None;
        while let Some(observed) = present.peek() {
            if previous_index.is_some_and(|index| index >= observed.candidate_index)
                || observed.candidate_index >= candidate_count
            {
                return Err(StorageError::internal(
                    "expiration observations are not a valid ordered candidate subset",
                ));
            }
            previous_index = Some(observed.candidate_index);
            present.next();
        }
        let mut present = observation.present.into_iter().peekable();
        for (candidate_index, candidate) in self.candidates.into_iter().enumerate() {
            if candidate.index_locator.indexed_expiration_seconds > self.cutoff_seconds {
                return Err(StorageError::internal(
                    "expiration candidate is newer than the requested cutoff",
                ));
            }
            if let Some(observed) =
                present.next_if(|observed| observed.candidate_index == candidate_index)
                && observed.item.number_attribute_i64(&self.ttl_attribute)?
                    == Some(candidate.index_locator.indexed_expiration_seconds)
            {
                items.push(ExpirationDelete { key: candidate.key });
            }
            index_entries.push(candidate.index_locator);
        }
        Ok(ExpirationDeletionBatch {
            index_entries,
            items,
        })
    }
}
