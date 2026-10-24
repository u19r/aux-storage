use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use storage_provider::DynamoAttempt;
use storage_types::{BoundTableInfo, StorageResult};

use super::record::TargetRecord;
use crate::{
    journal::{BaselineAuthority, JournalStore},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn materialize_baseline<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        input: (&TargetRecord, Arc<BoundTableInfo>, &BaselineAuthority),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (record, table, baseline) = input;
            let item = baseline.item();
            record.require_baseline_scope(baseline)?;
            storage_types::validate_transact_key(&table.info, &item.key)
                .map_err(|_| corrupt_record())?;
            self.retain_received_baseline(attempt, (baseline, table.history_id), codec)
                .await?;
            let (old, lifecycle) = attempt.observe_for_write(&table, &item.key).await?;
            let incoming = baseline.provenance();
            if let Some(winner) = crate::import::materialized_winner(
                &old,
                (item.lineage, &item.canonical_key),
                codec,
            )? {
                if winner.order == incoming.order && winner != incoming {
                    return Err(corrupt_record());
                }
                if winner.order >= incoming.order {
                    return Ok(());
                }
            }
            let provenance = codec
                .encode_winner_provenance(&incoming)
                .map_err(|_| corrupt_record())?;
            self.apply_baseline_effects(
                attempt,
                (table, baseline, provenance),
                (old, lifecycle),
                codec,
            )
            .await
        }
        .await;
        settlement.complete(result)
    }
}
