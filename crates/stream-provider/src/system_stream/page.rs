//! Logical SystemStream pages retain a lineage frontier and issued checkpoint.
//! Ordinary stream pages use `StreamRowKind`; both reach the same physical
//! `StreamAttempt::read_stream` entry point.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use storage_types::{
    GetStreamRecordsResponse, StreamRecord, SystemStreamCheckpoint, SystemStreamCheckpointToken,
    SystemStreamContractError, SystemStreamScope, SystemStreamSourceTableName,
};

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
