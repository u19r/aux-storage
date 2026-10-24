use std::sync::Arc;

use serde::{Deserialize, Serialize};
use storage_types::{
    InfrastructureContext, InfrastructureResultKind, Revision, StorageError, StorageResult,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
#[serde(try_from = "RecordAddress")]
pub struct DurableRecordKey(RecordAddress);

impl Serialize for DurableRecordKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().serialize(serializer)
    }
}

/// Namespace/resource/generation select one range; suffix compares as raw
/// bytes. DurableRecordKey validates the address before it crosses a native
/// boundary.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordAddress {
    pub namespace: String,
    pub resource: Vec<u8>,
    pub generation: u64,
    pub suffix: Vec<u8>,
}

impl TryFrom<RecordAddress> for DurableRecordKey {
    type Error = StorageError;
    fn try_from(value: RecordAddress) -> Result<Self, Self::Error> {
        validate_record_address(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&DurableRecordKey> for DurableRecordKey {
    type Error = StorageError;

    fn try_from(key: &DurableRecordKey) -> StorageResult<Self> {
        let address = key.as_ref();
        let mut namespace = String::new();
        let mut resource = Vec::new();
        let mut suffix = Vec::new();
        namespace
            .try_reserve_exact(address.namespace.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        resource
            .try_reserve_exact(address.resource.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        suffix
            .try_reserve_exact(address.suffix.len())
            .map_err(|_| InfrastructureResultKind::NativeCapacityRejected)?;
        namespace.push_str(&address.namespace);
        resource.extend_from_slice(&address.resource);
        suffix.extend_from_slice(&address.suffix);
        Ok(Self(RecordAddress {
            namespace,
            resource,
            generation: address.generation,
            suffix,
        }))
    }
}

impl From<DurableRecordKey> for RecordAddress {
    fn from(value: DurableRecordKey) -> Self {
        value.0
    }
}

impl AsRef<RecordAddress> for DurableRecordKey {
    fn as_ref(&self) -> &RecordAddress {
        &self.0
    }
}

/// Native records preserve opaque envelope bytes with their CAS
/// address/revision. WireItem decodes Dynamo attributes; StoredWireItem also
/// retains indexers.
#[derive(Clone, Serialize, Deserialize)]
pub struct DurableRecord {
    pub key: DurableRecordKey,
    pub revision: Revision,
    pub bytes: Arc<[u8]>,
}

impl TryFrom<&DurableRecord> for DurableRecord {
    type Error = StorageError;

    fn try_from(record: &DurableRecord) -> StorageResult<Self> {
        Ok(Self {
            key: DurableRecordKey::try_from(&record.key)?,
            revision: record.revision,
            bytes: Arc::clone(&record.bytes),
        })
    }
}

impl std::fmt::Debug for DurableRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableRecord")
            .field("revision", &self.revision)
            .field("byte_count", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordExpectation {
    Absent,
    Revision(Revision),
}

#[derive(Clone)]
pub enum RecordChange {
    Store {
        expected: RecordExpectation,
        record: DurableRecord,
    },
    Delete {
        expected: Revision,
        key: DurableRecordKey,
    },
}

impl std::fmt::Debug for RecordAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordAddress")
            .field("generation", &self.generation)
            .field("resource_bytes", &self.resource.len())
            .field("suffix_bytes", &self.suffix.len())
            .finish_non_exhaustive()
    }
}

fn validate_record_address(value: &RecordAddress) -> StorageResult<()> {
    if value.namespace.is_empty() || value.namespace.contains('\0') || value.resource.is_empty() {
        return Err(InfrastructureResultKind::Encoding {
            context: InfrastructureContext::RecordAddress,
        }
        .into());
    }
    Ok(())
}

/// Result of one native conditional record batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConditionalWriteResult {
    Applied,
    Conflict { observed: Option<Revision> },
}
