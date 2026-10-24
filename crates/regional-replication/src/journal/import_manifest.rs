//! Retain complete original coverage alongside native table disposition
//! receipts.

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::interval::{IntervalCodec, SourceIntervalManifest};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{constants::IMPORT_MANIFEST_SUFFIX, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) fn decode_import_manifest_record(
        &self,
        row: &DurableRecord,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<SourceIntervalManifest> {
        let manifest = self.decode_import_manifest_identity(row)?;
        require_manifest_content(&manifest, codec)?;
        Ok(manifest)
    }

    pub(in crate::journal) fn decode_import_manifest_identity(
        &self,
        row: &DurableRecord,
    ) -> StorageResult<SourceIntervalManifest> {
        let manifest: SourceIntervalManifest = self.records.decode(row)?;
        manifest.validate_shape().map_err(|_| corrupt_record())?;
        if row.revision != Revision::INITIAL
            || row.key != self.identity_key(IMPORT_MANIFEST_SUFFIX, &manifest.digest)?
        {
            return Err(corrupt_record());
        }
        Ok(manifest)
    }

    pub(in crate::journal) async fn prepare_import_manifest<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        manifest: &SourceIntervalManifest,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Option<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            require_manifest_content(manifest, codec)?;
            let key = self.identity_key(IMPORT_MANIFEST_SUFFIX, &manifest.digest)?;
            let rows = self
                .load_immutable_waves(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            if let Some(row) = &rows[0] {
                let existing = self.decode_import_manifest_record(row, codec)?;
                if existing != *manifest {
                    return Err(corrupt_record());
                }
                return Ok(None);
            }
            self.records.prepare(key, None, manifest).map(Some)
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_import_manifest<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        manifest: &SourceIntervalManifest,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let retained = self
                .load_import_manifest(attempt, manifest.digest, codec)
                .await?;
            if retained != *manifest {
                return Err(corrupt_record());
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
    pub(in crate::journal) async fn load_import_manifest<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        digest: replication_types::protocol::CanonicalDigest,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<SourceIntervalManifest> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(IMPORT_MANIFEST_SUFFIX, &digest)?;
            let rows = self
                .load_immutable_waves(attempt, &[key], ReadProtection::Snapshot)
                .await?;
            let row = rows[0].as_ref().ok_or_else(corrupt_record)?;
            let retained = self.decode_import_manifest_record(row, codec)?;
            if retained.digest != digest {
                return Err(corrupt_record());
            }
            Ok(retained)
        }
        .await;
        settlement.complete(result)
    }
}

fn require_manifest_content(
    manifest: &SourceIntervalManifest,
    codec: &CanonicalJsonCodec,
) -> StorageResult<()> {
    manifest.validate_shape().map_err(|_| corrupt_record())?;
    let bytes = codec
        .encode_interval(manifest)
        .map_err(|_| corrupt_record())?;
    manifest
        .digest
        .verify(bytes.as_ref())
        .map_err(|_| corrupt_record())
}
