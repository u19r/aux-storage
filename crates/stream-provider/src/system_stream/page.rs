//! Logical SystemStream pages retain a lineage frontier and issued checkpoint.
//! Ordinary stream pages use `StreamRowKind`; both reach the same physical
//! `StreamAttempt::read_stream` entry point.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use storage_types::{
    GetStreamRecordsResponse, KeyAttributes, StreamRecord, SystemStreamCheckpoint,
    SystemStreamCheckpointToken, SystemStreamContractError, SystemStreamPosition,
    SystemStreamScope, SystemStreamSourceTableName, TableName, WireItem,
};

use crate::constants::SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES;

/// A validated checkpoint and its opaque, server-issued identifier.
///
/// Providers persist the checkpoint state and token atomically.  The frontier
/// is included at this boundary so conformance adapters can compare the
/// canonical state without decoding provider-specific rows; API adapters only
/// expose [`Self::token`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamIssuedCheckpoint {
    token: SystemStreamCheckpointToken,
    checkpoint: SystemStreamCheckpoint,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuedCheckpointWire {
    token: SystemStreamCheckpointToken,
    checkpoint: Vec<u8>,
}

impl Serialize for SystemStreamIssuedCheckpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let checkpoint = self
            .checkpoint
            .encode_state()
            .map_err(serde::ser::Error::custom)?;
        IssuedCheckpointWire {
            token: self.token.clone(),
            checkpoint,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamIssuedCheckpoint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = IssuedCheckpointWire::deserialize(deserializer)?;
        let checkpoint =
            SystemStreamCheckpoint::decode_state(&wire.checkpoint).map_err(D::Error::custom)?;
        Self::new(wire.token, checkpoint).map_err(D::Error::custom)
    }
}

impl SystemStreamIssuedCheckpoint {
    pub fn new(
        token: SystemStreamCheckpointToken,
        checkpoint: SystemStreamCheckpoint,
    ) -> Result<Self, SystemStreamContractError> {
        checkpoint.encode_state()?;
        Ok(Self { token, checkpoint })
    }

    #[must_use]
    pub fn token(&self) -> &SystemStreamCheckpointToken {
        &self.token
    }

    #[must_use]
    pub fn checkpoint(&self) -> &SystemStreamCheckpoint {
        &self.checkpoint
    }
}

/// A validated provider page request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamPageRequest {
    source_scope: SystemStreamScope,
    checkpoint: Option<SystemStreamCheckpointToken>,
    limit: u32,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct PageRequestWire<'a> {
    source_scope: &'a SystemStreamScope,
    checkpoint: &'a Option<SystemStreamCheckpointToken>,
    limit: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedPageRequestWire {
    source_scope: SystemStreamScope,
    checkpoint: Option<SystemStreamCheckpointToken>,
    limit: u32,
}

impl SystemStreamPageRequest {
    pub fn try_new(
        source_scope: SystemStreamScope,
        checkpoint: Option<SystemStreamCheckpointToken>,
        limit: u32,
    ) -> Result<Self, SystemStreamContractError> {
        if limit == 0 || limit > storage_types::SYSTEM_STREAM_RECORDS_LIMIT_MAX {
            return Err(SystemStreamContractError::BackendContract);
        }
        Ok(Self {
            source_scope,
            checkpoint,
            limit,
        })
    }

    #[must_use]
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }

    #[must_use]
    pub fn checkpoint(&self) -> Option<&SystemStreamCheckpointToken> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub const fn limit(&self) -> u32 {
        self.limit
    }
}

impl Serialize for SystemStreamPageRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        PageRequestWire {
            source_scope: &self.source_scope,
            checkpoint: &self.checkpoint,
            limit: self.limit,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamPageRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OwnedPageRequestWire::deserialize(deserializer)?;
        Self::try_new(wire.source_scope, wire.checkpoint, wire.limit).map_err(D::Error::custom)
    }
}

/// A provider-owned logical SystemStream page.
#[derive(Debug, Clone)]
pub struct SystemStreamPage {
    source_scope: SystemStreamScope,
    records: Vec<StreamRecord>,
    issued_checkpoint: SystemStreamIssuedCheckpoint,
    terminal: bool,
}

/// Provider-neutral selection of decoded records for one bounded page.
pub struct SystemStreamPagePlan {
    pub records: Vec<StreamRecord>,
    pub next_frontier: Vec<SystemStreamPosition>,
    pub has_more: bool,
}

impl SystemStreamPagePlan {
    pub fn try_new(
        frontier: &SystemStreamCheckpoint,
        candidates: Vec<StreamRecord>,
        provider_has_more: bool,
        limit: u32,
    ) -> storage_types::StorageResult<Self> {
        if limit == 0 || limit > storage_types::SYSTEM_STREAM_RECORDS_LIMIT_MAX {
            return Err(storage_types::StorageError::validation(
                "SystemStream page limit is outside its bound",
            ));
        }
        let mut records = Vec::with_capacity(limit as usize);
        let mut next_frontier = frontier
            .frontier()
            .iter()
            .map(|position| (position.shard_id().as_str().to_string(), position.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut encoded_bytes = 0usize;
        let mut has_more = provider_has_more || candidates.len() > limit as usize;
        for record in candidates.into_iter().take(limit as usize) {
            let record_bytes = serde_json::to_vec(&record)?.len();
            let encoded_page_bytes = encoded_bytes
                .saturating_add(record_bytes)
                .saturating_add(usize::from(!records.is_empty()))
                .saturating_add(SYSTEM_STREAM_PAGE_ENVELOPE_RESERVE_BYTES);
            if encoded_page_bytes > storage_types::STREAM_RECORDS_MAX_ENCODED_BYTES as usize {
                if records.is_empty() {
                    return Err(storage_types::StorageError::validation(
                        "The first SystemStream record exceeds the response byte limit",
                    ));
                }
                has_more = true;
                break;
            }
            encoded_bytes = encoded_bytes
                .saturating_add(record_bytes)
                .saturating_add(usize::from(!records.is_empty()));
            let position = record.system_position.clone().ok_or_else(|| {
                storage_types::StorageError::validation("SystemStream record is missing position")
            })?;
            next_frontier.insert(position.shard_id().as_str().to_string(), position);
            records.push(record);
        }
        Ok(Self {
            records,
            next_frontier: next_frontier.into_values().collect(),
            has_more,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageWire {
    source_scope: SystemStreamScope,
    records: Vec<StreamRecord>,
    issued_checkpoint: SystemStreamIssuedCheckpoint,
    terminal: bool,
}

impl SystemStreamPage {
    pub fn new(
        source_scope: SystemStreamScope,
        records: Vec<StreamRecord>,
        issued_checkpoint: SystemStreamIssuedCheckpoint,
        terminal: bool,
    ) -> Result<Self, SystemStreamContractError> {
        issued_checkpoint
            .checkpoint()
            .validate_scope(&source_scope)?;
        if records.iter().any(|record| record.cursor.is_some()) {
            return Err(SystemStreamContractError::PhysicalSystemStreamFields);
        }
        for record in &records {
            if record.system_position.is_none() {
                return Err(SystemStreamContractError::MissingRecordPosition);
            }
            let Some(source_table_name) = record.source_table_name.as_ref() else {
                return Err(SystemStreamContractError::MissingSourceTableName);
            };
            SystemStreamSourceTableName::try_from(source_table_name)?;
        }
        Ok(Self {
            source_scope,
            records,
            issued_checkpoint,
            terminal,
        })
    }

    #[must_use]
    pub fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }

    #[must_use]
    pub fn records(&self) -> &[StreamRecord] {
        &self.records
    }

    #[must_use]
    pub fn into_records(self) -> Vec<StreamRecord> {
        self.records
    }

    #[must_use]
    pub fn issued_checkpoint(&self) -> &SystemStreamIssuedCheckpoint {
        &self.issued_checkpoint
    }

    #[must_use]
    pub const fn terminal(&self) -> bool {
        self.terminal
    }

    /// Convert the provider result to the accepted public API response.
    /// A terminal page means caught up now, not permanent EOF; its issued
    /// checkpoint remains resumable.
    pub fn into_response(self) -> Result<GetStreamRecordsResponse, SystemStreamContractError> {
        GetStreamRecordsResponse::system_stream(
            self.source_scope,
            self.records,
            Some(self.issued_checkpoint.token),
        )
    }
}

impl Serialize for SystemStreamPage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        PageWire {
            source_scope: self.source_scope.clone(),
            records: self.records.clone(),
            issued_checkpoint: self.issued_checkpoint.clone(),
            terminal: self.terminal,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamPage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = PageWire::deserialize(deserializer)?;
        Self::new(
            wire.source_scope,
            wire.records,
            wire.issued_checkpoint,
            wire.terminal,
        )
        .map_err(D::Error::custom)
    }
}

/// Provider-neutral description of one stored logical record before it is
/// assembled into the public `StreamRecord` shape.  The source table identity
/// is required and logical; physical pointers remain provider-private.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStreamStoredRecord {
    pub source_table_name: SystemStreamSourceTableName,
    pub keys: KeyAttributes,
    #[serde(with = "storage_types::dynamo_json::optional")]
    pub old_image: Option<WireItem>,
    #[serde(with = "storage_types::dynamo_json::optional")]
    pub new_image: Option<WireItem>,
    pub position: SystemStreamPosition,
}

impl SystemStreamStoredRecord {
    pub fn effect_digest(
        &self,
        history_id: storage_types::TableHistoryId,
        version: storage_types::ItemStreamVersion,
    ) -> storage_types::StorageResult<storage_types::SystemStreamEffectDigest> {
        use sha2::{Digest as _, Sha256};
        let bytes = serde_json::to_vec(&(
            history_id,
            version,
            &self.source_table_name,
            &self.keys,
            self.old_image
                .as_ref()
                .map(storage_types::dynamo_json::ItemRef),
            self.new_image
                .as_ref()
                .map(storage_types::dynamo_json::ItemRef),
        ))?;
        storage_types::SystemStreamEffectDigest::try_new(hex::encode(Sha256::digest(bytes)))
    }

    pub fn into_stream_record(self) -> storage_types::StorageResult<StreamRecord> {
        Ok(StreamRecord {
            cursor: None,
            source_table_name: Some(TableName::new(self.source_table_name.as_str())),
            keys: self.keys,
            sequence_number: self.position.offset().as_str().to_string(),
            old_image: self
                .old_image
                .map(WireItem::into_attribute_map)
                .transpose()?,
            new_image: self
                .new_image
                .map(WireItem::into_attribute_map)
                .transpose()?,
            system_position: Some(self.position),
        })
    }
}
