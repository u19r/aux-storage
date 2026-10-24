use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::identity::resource::ControlOperationId;
use storage_types::{ReadProtection, StorageResult};

use super::record::ExpectedWinner;
use crate::{
    constants::EXPECTED_WINNER_SUFFIX, journal::JournalStore, native_record::corrupt_record,
};

pub(in crate::journal) struct ExpectedWinnersPage {
    pub winners: Vec<ExpectedWinner>,
    pub continuation: Option<DurableRecordKey>,
}

impl JournalStore {
    pub(in crate::journal) async fn load_expected_winners<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        after: Option<DurableRecordKey>,
    ) -> StorageResult<ExpectedWinnersPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let first = self.identity_key(EXPECTED_WINNER_SUFFIX, &operation)?;
            let request = infrastructure_provider::page::RecordPageRequest {
                range: self.record_prefix_range(&first)?,
                after,
                max_bytes: self.records.limits.max_bytes(),
                protection: ReadProtection::Snapshot,
            };
            let page = self.records.scan(attempt, request).await?;
            let winners = page
                .records
                .iter()
                .map(|row| self.decode_expected_winner(row, operation))
                .collect::<StorageResult<Vec<_>>>()?;
            Ok(ExpectedWinnersPage {
                winners,
                continuation: page.continuation,
            })
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_expected_winner<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        operation: ControlOperationId,
        key: &replication_types::event::identity::CanonicalItemKey,
    ) -> StorageResult<Option<ExpectedWinner>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let address = self.expected_winner_key(operation, key)?;
            let rows = self
                .records
                .load(attempt, &[address], ReadProtection::Snapshot)
                .await?;
            let expected = rows[0]
                .as_ref()
                .map(|row| self.decode_expected_winner(row, operation))
                .transpose()?;
            if expected
                .as_ref()
                .is_some_and(|expected| &expected.provenance.key != key)
            {
                return Err(corrupt_record());
            }
            Ok(expected)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn decode_expected_winner(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
        operation: ControlOperationId,
    ) -> StorageResult<ExpectedWinner> {
        let expected: ExpectedWinner = self.records.decode(row)?;
        expected
            .provenance
            .validate()
            .map_err(|_| corrupt_record())?;
        if expected.operation != operation
            || self.expected_winner_key(operation, &expected.provenance.key)? != row.key
        {
            return Err(corrupt_record());
        }
        Ok(expected)
    }
}
