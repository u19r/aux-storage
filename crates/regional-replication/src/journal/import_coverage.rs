use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_provider::delivery::coverage::DeliveryCoverage;
use replication_types::{
    identity::{
        origin::OriginId,
        resource::{TableIdentity, TableLineage},
    },
    regional_import::RegionalImport,
};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::{
    JournalStore,
    delivery_activity::{DeliveryActivity, DeliveryActivityObservation},
};
use crate::{constants::IMPORT_COVERAGE_SUFFIX, native_record::corrupt_record};

struct ImportCoverageObservation {
    key: DurableRecordKey,
    revision: Option<Revision>,
    coverage: DeliveryCoverage,
    activity: DeliveryActivityObservation,
}

impl JournalStore {
    pub(in crate::journal) async fn prepare_import_coverage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let scopes = self.prepare_import_scopes(import)?;
            let count = scopes
                .len()
                .checked_mul(2)
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut changes = Vec::new();
            changes
                .try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let observed = self
                .observe_import_coverages(attempt, scopes, ReadProtection::Conflict)
                .await?;
            for observed in observed {
                let next = observed
                    .coverage
                    .accept(import.manifest().range, self.records.limits.max_records())
                    .map_err(|_| corrupt_record())?;
                if observed.revision.is_none() || next != observed.coverage {
                    changes.push(
                        self.records
                            .prepare(observed.key, observed.revision, &next)?,
                    );
                }
                if let Some(change) =
                    self.prepare_delivery_activity(observed.activity, next.through())?
                {
                    changes.push(change);
                }
            }
            Ok(changes)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_received_coverage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        tables: (TableIdentity, TableIdentity, TableLineage),
        origin: &OriginId,
        protection: ReadProtection,
    ) -> StorageResult<DeliveryCoverage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.load_received_coverages(attempt, tables, std::slice::from_ref(origin), protection)
                .await?
                .pop()
                .ok_or_else(corrupt_record)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn load_received_coverages<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        tables: (TableIdentity, TableIdentity, TableLineage),
        origins: &[OriginId],
        protection: ReadProtection,
    ) -> StorageResult<Vec<DeliveryCoverage>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut scopes = Vec::new();
            let mut coverages = Vec::new();
            scopes
                .try_reserve_exact(origins.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            coverages
                .try_reserve_exact(origins.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for origin in origins {
                scopes.push(self.import_coverage_scope(tables, origin)?);
            }
            let observed = self
                .observe_import_coverages(attempt, scopes, protection)
                .await?;
            coverages.extend(observed.into_iter().map(|observed| observed.coverage));
            Ok(coverages)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_complete_import_coverage<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        import: &RegionalImport,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let scopes = self.prepare_import_scopes(import)?;
            let observed = self
                .observe_import_coverages(attempt, scopes, ReadProtection::Snapshot)
                .await?;
            if observed
                .iter()
                .any(|observed| !observed.coverage.has_accepted(import.manifest().range))
            {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_import_scopes(
        &self,
        import: &RegionalImport,
    ) -> StorageResult<Vec<(DurableRecordKey, DeliveryActivity)>> {
        let mut scopes = Vec::new();
        scopes
            .try_reserve_exact(import.bindings().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for binding in import.bindings() {
            scopes.push(self.import_coverage_scope(
                (binding.local_table, binding.source_table, binding.lineage),
                &import.manifest().origin,
            )?);
        }
        Ok(scopes)
    }

    fn import_coverage_scope(
        &self,
        tables: (TableIdentity, TableIdentity, TableLineage),
        origin: &OriginId,
    ) -> StorageResult<(DurableRecordKey, DeliveryActivity)> {
        let key = self.identity_key(
            IMPORT_COVERAGE_SUFFIX,
            &(tables.0, tables.1, tables.2, origin),
        )?;
        let activity = DeliveryActivity {
            table: tables.0,
            source_table: tables.1,
            lineage: tables.2,
            origin: origin.clone(),
            through: 0.into(),
        };
        Ok((key, activity))
    }

    async fn observe_import_coverages<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        scopes: Vec<(DurableRecordKey, DeliveryActivity)>,
        protection: ReadProtection,
    ) -> StorageResult<Vec<ImportCoverageObservation>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let count = scopes
                .len()
                .checked_mul(2)
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut keys = Vec::new();
            let mut observed = Vec::new();
            keys.try_reserve_exact(count)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            observed
                .try_reserve_exact(scopes.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            for (key, activity) in &scopes {
                keys.push(DurableRecordKey::try_from(key)?);
                keys.push(self.delivery_activity_key(activity)?);
            }
            let mut rows = self
                .records
                .load(attempt, &keys, protection)
                .await?
                .into_iter();
            for (key, activity) in scopes {
                let coverage = rows.next().ok_or_else(corrupt_record)?;
                let marker = rows.next().ok_or_else(corrupt_record)?;
                observed.push(self.decode_import_coverage(key, activity, [coverage, marker])?);
            }
            if rows.next().is_some() {
                return Err(corrupt_record());
            }
            Ok(observed)
        }
        .await;
        settlement.complete(result)
    }

    fn decode_import_coverage(
        &self,
        key: DurableRecordKey,
        expected: DeliveryActivity,
        rows: [Option<DurableRecord>; 2],
    ) -> StorageResult<ImportCoverageObservation> {
        let revision = rows[0].as_ref().map(|row| row.revision);
        let coverage: DeliveryCoverage = rows[0]
            .as_ref()
            .map(|row| self.records.decode(row))
            .transpose()?
            .unwrap_or_default();
        coverage
            .validate(self.records.limits.max_records())
            .map_err(|_| corrupt_record())?;
        let is_native_source =
            expected.table == expected.source_table && self.is_local_origin(&expected.origin);
        let activity = self.observe_delivery_activity(expected, rows[1].as_ref())?;
        match (revision, activity.revision) {
            (None, Some(_)) if !is_native_source => return Err(corrupt_record()),
            (Some(_), None) => return Err(corrupt_record()),
            (_, Some(_))
                if activity.activity.through < coverage.through()
                    || (!is_native_source && activity.activity.through != coverage.through()) =>
            {
                return Err(corrupt_record());
            }
            _ => {}
        }
        Ok(ImportCoverageObservation {
            key,
            revision,
            coverage,
            activity,
        })
    }
}
