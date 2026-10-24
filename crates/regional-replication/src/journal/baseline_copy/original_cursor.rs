use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey},
};
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::BaselineCopyScope, constants::ADOPTED_TRANSFER_SUFFIX, journal::JournalStore,
    native_record::corrupt_record,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginalCursor {
    pub source: usize,
    pub stage: OriginalStage,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum OriginalStage {
    Active {
        page: u32,
        index: usize,
    },
    Catalog {
        after: Option<DurableRecordKey>,
    },
    Retired {
        definition: CanonicalDigest,
        page: u32,
        index: usize,
        next_catalog: Option<DurableRecordKey>,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalTraversalRecord {
    scope: CanonicalDigest,
    definition: CanonicalDigest,
    cursor: OriginalCursor,
}

impl JournalStore {
    pub(super) async fn load_original_cursor<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        definition: CanonicalDigest,
    ) -> StorageResult<(Option<DurableRecord>, OriginalCursor)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(
                ADOPTED_TRANSFER_SUFFIX,
                &(scope.proposal.operation().id, &scope.destination.region),
            )?;
            let row = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?
                .pop()
                .flatten();
            let cursor = if let Some(row) = &row {
                let record: OriginalTraversalRecord = self.records.decode(row)?;
                if record.scope != scope.content()? || record.definition != definition {
                    return Err(corrupt_record());
                }
                record.cursor
            } else {
                OriginalCursor {
                    source: 0,
                    stage: OriginalStage::Active { page: 0, index: 0 },
                }
            };
            let source = replication_provider::control::borrow_addition_source(
                &scope.proposal,
                &scope.proposal.operation().home_region,
            )
            .map_err(|_| corrupt_record())?;
            let replication_types::configuration::table::RegionalMode::Global { members, .. } =
                &source.configuration.regional
            else {
                return Err(corrupt_record());
            };
            if cursor.source > members.len()
                || (cursor.source == members.len()
                    && cursor.stage != OriginalStage::Active { page: 0, index: 0 })
            {
                return Err(corrupt_record());
            }
            Ok((row, cursor))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn store_original_cursor<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scope: &BaselineCopyScope,
        input: (Option<DurableRecord>, CanonicalDigest, OriginalCursor),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (row, definition, cursor) = input;
            let key = self.identity_key(
                ADOPTED_TRANSFER_SUFFIX,
                &(scope.proposal.operation().id, &scope.destination.region),
            )?;
            let record = OriginalTraversalRecord {
                scope: scope.content()?,
                definition,
                cursor,
            };
            self.records
                .store(
                    attempt,
                    &[self
                        .records
                        .prepare(key, row.map(|row| row.revision), &record)?],
                )
                .await
        }
        .await;
        settlement.complete(result)
    }
}
