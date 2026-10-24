use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::HistoryCaptureRequest;
use crate::{bootstrap::CaptureSchema, native_record::corrupt_record};

/// Frozen facts from one ready native source. This is not a membership
/// certificate.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCaptureDefinition {
    request: HistoryCaptureRequest,
    schema: CaptureSchema,
    vector: CanonicalDigest,
    content: CanonicalDigest,
}

impl HistoryCaptureDefinition {
    pub(crate) fn from_ready(
        request: HistoryCaptureRequest,
        schema: CaptureSchema,
        vector: CanonicalDigest,
    ) -> StorageResult<Self> {
        let content =
            CanonicalDigest::of_json(&(&request, &schema, vector)).map_err(|_| corrupt_record())?;
        let definition = Self {
            request,
            schema,
            vector,
            content,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.request.validate()?;
        self.schema.validate(
            &self
                .request
                .proposal
                .participant(&self.request.region)
                .map_err(|_| corrupt_record())?
                .proposed
                .name,
        )?;
        let serialized_digest =
            CanonicalDigest::of_json(&(&self.request, &self.schema, self.vector))
                .map_err(|_| corrupt_record())?;
        if self.content != serialized_digest {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn schema_digest(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&self.schema).map_err(|_| corrupt_record())
    }

    pub(crate) fn vector_seed(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(self.request.content()?, self.schema_digest()?))
            .map_err(|_| corrupt_record())
    }

    pub(crate) fn policy_digest(&self) -> StorageResult<CanonicalDigest> {
        let logical = crate::bootstrap::definition::logical_table_content(&self.schema.table)?;
        CanonicalDigest::of_json(&(logical, &self.schema.ttl)).map_err(|_| corrupt_record())
    }

    pub(crate) fn require_page(
        &self,
        page: &super::HistoryCaptureVectorPage,
        after: u32,
    ) -> StorageResult<()> {
        self.request.require_page(page, after)?;
        if page.schema != self.schema_digest()? || page.vector != self.vector {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn schema(&self) -> &CaptureSchema {
        &self.schema
    }

    pub fn request(&self) -> &HistoryCaptureRequest {
        &self.request
    }
    pub fn table(&self) -> &storage_types::StoredTableInfo {
        &self.schema.table
    }
    pub fn ttl(&self) -> Option<&crate::bootstrap::CaptureTtlPolicy> {
        self.schema.ttl.as_ref()
    }
    pub fn vector(&self) -> CanonicalDigest {
        self.vector
    }
    pub fn content(&self) -> CanonicalDigest {
        self.content
    }
}
