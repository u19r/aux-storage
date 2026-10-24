use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecordKey, RecordAddress},
};
use replication_provider::ordering::winner::{WinnerDecision, WinnerEffectPlan};
use replication_types::{
    event::{identity::CanonicalItemKey, order::WinnerProvenance},
    identity::resource::ControlOperationId,
    protocol::CanonicalDigest,
};
use storage_types::{KeyAttributes, ReadProtection, StorageResult};

use super::record::ExpectedWinner;
use crate::{
    constants::EXPECTED_WINNER_SUFFIX, journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) fn expected_winner_key(
        &self,
        operation: ControlOperationId,
        key: &CanonicalItemKey,
    ) -> StorageResult<DurableRecordKey> {
        let mut address: RecordAddress = self
            .identity_key(EXPECTED_WINNER_SUFFIX, &operation)?
            .into();
        address
            .suffix
            .extend_from_slice(CanonicalDigest::of(key.as_ref()).as_ref());
        address.try_into()
    }

    pub(in crate::journal) async fn fold_expected_winner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        input: (&KeyAttributes, WinnerProvenance),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (key_attributes, incoming) = input;
            let key = self.expected_winner_key(operation, &incoming.key)?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let observed = rows[0]
                .as_ref()
                .map(|row| self.decode_expected_winner(row, operation))
                .transpose()?;
            if observed
                .as_ref()
                .is_some_and(|current| &current.key != key_attributes)
            {
                return Err(corrupt_record());
            }
            let decision = WinnerEffectPlan {
                incoming: &incoming,
                observed: observed.as_ref().map(|current| &current.provenance),
                known_identity: None,
            }
            .decide_winner()
            .map_err(|_| corrupt_record())?;
            if decision != WinnerDecision::Replace {
                return Ok(());
            }
            let expected = ExpectedWinner {
                operation,
                key: key_attributes.clone(),
                provenance: incoming,
            };
            let change =
                self.records
                    .prepare(key, rows[0].as_ref().map(|row| row.revision), &expected)?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement.complete(result)
    }
}
