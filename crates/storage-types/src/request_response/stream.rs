use serde::{Deserialize, Deserializer, Serialize, Serializer};
use utoipa::ToSchema;

use crate::{
    AttributeMap, KeyAttributes, KeySchemaElement, StreamItemId, StreamRecord, StreamViewType,
    SystemStreamCheckpointToken, SystemStreamContractError, SystemStreamScope,
    SystemStreamSourceTableName, TableHistoryId, TableName, TimestampSecondsFractional,
};

/// Table incarnation and retained pointer range observed in the same read
/// attempt as a table-stream page. An empty head certifies an empty stream
/// for that incarnation at the attempt's snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct TableStreamFrontier {
    #[schema(value_type = Vec<u8>)]
    pub table_history_id: TableHistoryId,
    pub oldest: Option<StreamItemId>,
    pub head: Option<StreamItemId>,
}

impl TableStreamFrontier {
    fn validate(&self) -> Result<(), SystemStreamContractError> {
        match (self.oldest, self.head) {
            (None, None) => Ok(()),
            (Some(oldest), Some(head)) if oldest <= head => Ok(()),
            _ => Err(SystemStreamContractError::InvalidTableFrontier),
        }
    }

    /// A returning subscriber may resume only when its previously verified
    /// checkpoint is within this incarnation's retained range. Bounds do not
    /// prove that an arbitrary caller-supplied item ID ever existed. A missing
    /// checkpoint needs a new snapshot rather than an incremental read.
    #[must_use]
    pub fn can_resume_from(
        &self,
        table_history_id: TableHistoryId,
        checkpoint: StreamItemId,
    ) -> bool {
        self.table_history_id == table_history_id
            && self
                .oldest
                .zip(self.head)
                .is_some_and(|(oldest, head)| oldest <= checkpoint && checkpoint <= head)
    }
}

#[derive(Debug, Clone, ToSchema)]
pub struct GetStreamRecordsRequest {
    table_name: Option<TableName>,
    system_stream: bool,
    last_evaluated_key: Option<String>,

    /// Logical source authority required by every SystemStream request.
    source_scope: Option<SystemStreamScope>,

    /// Opaque per-shard checkpoint frontier used only by SystemStream.
    checkpoint: Option<SystemStreamCheckpointToken>,

    #[schema(default = 100, minimum = 1, maximum = 8192)]
    limit: Option<u32>,
}

#[derive(Debug, Clone, ToSchema)]
pub struct GetStreamRecordsResponse {
    table_name: Option<TableName>,
    records: Vec<StreamRecord>,
    last_evaluated_key: Option<String>,
    table_frontier: Option<TableStreamFrontier>,

    /// Logical source authority returned by SystemStream.
    source_scope: Option<SystemStreamScope>,

    /// Opaque per-shard checkpoint frontier returned by SystemStream.
    checkpoint: Option<SystemStreamCheckpointToken>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct GetStreamRecordsRequestWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<TableName>,
    #[serde(default)]
    system_stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_scope: Option<SystemStreamScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<SystemStreamCheckpointToken>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
struct GetStreamRecordsResponseWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<TableName>,
    records: Vec<StreamRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_frontier: Option<TableStreamFrontier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_scope: Option<SystemStreamScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<SystemStreamCheckpointToken>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetStreamRecordsRequestWireRef<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<&'a TableName>,
    system_stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_scope: Option<&'a SystemStreamScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<&'a SystemStreamCheckpointToken>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
struct GetStreamRecordsResponseWireRef<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    table_name: Option<&'a TableName>,
    records: &'a [StreamRecord],
    #[serde(skip_serializing_if = "Option::is_none")]
    last_evaluated_key: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_frontier: Option<&'a TableStreamFrontier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_scope: Option<&'a SystemStreamScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<&'a SystemStreamCheckpointToken>,
}

impl Serialize for GetStreamRecordsRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.validate()
            .map_err(<S::Error as serde::ser::Error>::custom)?;
        GetStreamRecordsRequestWireRef {
            table_name: self.table_name.as_ref(),
            system_stream: self.system_stream,
            last_evaluated_key: self.last_evaluated_key.as_ref(),
            source_scope: self.source_scope.as_ref(),
            checkpoint: self.checkpoint.as_ref(),
            limit: self.limit,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GetStreamRecordsRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        Self::from_wire(GetStreamRecordsRequestWire::deserialize(deserializer)?)
            .map_err(<D::Error as serde::de::Error>::custom)
    }
}

impl Serialize for GetStreamRecordsResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        self.validate()
            .map_err(<S::Error as serde::ser::Error>::custom)?;
        GetStreamRecordsResponseWireRef {
            table_name: self.table_name.as_ref(),
            records: &self.records,
            last_evaluated_key: self.last_evaluated_key.as_ref(),
            table_frontier: self.table_frontier.as_ref(),
            source_scope: self.source_scope.as_ref(),
            checkpoint: self.checkpoint.as_ref(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GetStreamRecordsResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let wire = GetStreamRecordsResponseWire::deserialize(deserializer)?;
        let response = Self {
            table_name: wire.table_name,
            records: wire.records,
            last_evaluated_key: wire.last_evaluated_key,
            table_frontier: wire.table_frontier,
            source_scope: wire.source_scope,
            checkpoint: wire.checkpoint,
        };
        response
            .validate()
            .map(|()| response)
            .map_err(<D::Error as serde::de::Error>::custom)
    }
}

impl GetStreamRecordsRequest {
    /// Build an ordinary table-stream request.
    #[must_use]
    pub fn table(
        table_name: TableName,
        last_evaluated_key: Option<String>,
        limit: Option<u32>,
    ) -> Self {
        Self {
            table_name: Some(table_name),
            system_stream: false,
            last_evaluated_key,
            source_scope: None,
            checkpoint: None,
            limit,
        }
    }

    /// Build a logical SystemStream request.  The scope is typed and all
    /// physical table-stream fields are absent by construction.
    #[must_use]
    pub fn system_stream(
        source_scope: SystemStreamScope,
        checkpoint: Option<SystemStreamCheckpointToken>,
        limit: Option<u32>,
    ) -> Self {
        Self {
            table_name: None,
            system_stream: true,
            last_evaluated_key: None,
            source_scope: Some(source_scope),
            checkpoint,
            limit,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> Option<&TableName> {
        self.table_name.as_ref()
    }

    #[must_use]
    pub const fn is_system_stream(&self) -> bool {
        self.system_stream
    }

    #[must_use]
    pub fn last_evaluated_key(&self) -> Option<&str> {
        self.last_evaluated_key.as_deref()
    }

    #[must_use]
    pub fn source_scope(&self) -> Option<&SystemStreamScope> {
        self.source_scope.as_ref()
    }

    #[must_use]
    pub fn checkpoint(&self) -> Option<&SystemStreamCheckpointToken> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub const fn limit(&self) -> Option<u32> {
        self.limit
    }

    fn from_wire(wire: GetStreamRecordsRequestWire) -> Result<Self, SystemStreamContractError> {
        let request = Self {
            table_name: wire.table_name,
            system_stream: wire.system_stream,
            last_evaluated_key: wire.last_evaluated_key,
            source_scope: wire.source_scope,
            checkpoint: wire.checkpoint,
            limit: wire.limit,
        };
        request.validate().map(|()| request)
    }

    pub(crate) fn from_json_value(value: serde_json::Value) -> Result<Self, String> {
        let wire: GetStreamRecordsRequestWire = serde_json::from_value(value)
            .map_err(|error| format!("Invalid request format: {error}"))?;
        Self::from_wire(wire).map_err(|error| error.to_string())
    }

    /// Validate the request shape before any provider operation is admitted.
    pub fn validate(&self) -> Result<(), SystemStreamContractError> {
        if self.system_stream {
            if self.table_name.is_some() {
                return Err(SystemStreamContractError::ConflictingStreamSources);
            }
            if self.source_scope.is_none() && self.checkpoint.is_some() {
                return Err(SystemStreamContractError::MissingCheckpointScope);
            }
            if self.source_scope.is_none() {
                return Err(SystemStreamContractError::MissingSourceScope);
            }
            if self.last_evaluated_key.is_some() {
                return Err(SystemStreamContractError::LegacyLastEvaluatedKey);
            }
            return Ok(());
        }

        if self.source_scope.is_some() || self.checkpoint.is_some() {
            return Err(SystemStreamContractError::UnexpectedSystemStreamFields);
        }
        Ok(())
    }
}

impl GetStreamRecordsResponse {
    /// Build the ordinary table-stream response while retaining its legacy
    /// DynamoDB-compatible `LastEvaluatedKey` semantics.
    #[must_use]
    pub fn table(
        table_name: TableName,
        records: Vec<StreamRecord>,
        last_evaluated_key: Option<String>,
        table_frontier: TableStreamFrontier,
    ) -> Self {
        Self {
            table_name: Some(table_name),
            records,
            last_evaluated_key,
            table_frontier: Some(table_frontier),
            source_scope: None,
            checkpoint: None,
        }
    }

    /// Build a SystemStream response and require logical metadata on every
    /// record.  Backend lanes should call this only after producing lineage
    /// identities; this constructor never fabricates them.
    pub fn system_stream(
        source_scope: SystemStreamScope,
        records: Vec<StreamRecord>,
        checkpoint: Option<SystemStreamCheckpointToken>,
    ) -> Result<Self, SystemStreamContractError> {
        let response = Self {
            table_name: None,
            records,
            last_evaluated_key: None,
            table_frontier: None,
            source_scope: Some(source_scope),
            checkpoint,
        };
        response.validate()?;
        Ok(response)
    }

    #[must_use]
    pub fn table_name(&self) -> Option<&TableName> {
        self.table_name.as_ref()
    }

    #[must_use]
    pub fn records(&self) -> &[StreamRecord] {
        &self.records
    }

    #[must_use]
    pub fn last_evaluated_key(&self) -> Option<&str> {
        self.last_evaluated_key.as_deref()
    }

    #[must_use]
    pub fn table_frontier(&self) -> Option<&TableStreamFrontier> {
        self.table_frontier.as_ref()
    }

    #[must_use]
    pub fn source_scope(&self) -> Option<&SystemStreamScope> {
        self.source_scope.as_ref()
    }

    #[must_use]
    pub fn checkpoint(&self) -> Option<&SystemStreamCheckpointToken> {
        self.checkpoint.as_ref()
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<StreamRecord>, Option<String>) {
        (self.records, self.last_evaluated_key)
    }

    /// Validate response invariants independently of construction.  This is
    /// useful at serialization boundaries where a response may be assembled by
    /// a provider adapter.
    pub fn validate(&self) -> Result<(), SystemStreamContractError> {
        match (&self.source_scope, &self.checkpoint) {
            (Some(_), _) => {
                if self.table_name.is_some()
                    || self.last_evaluated_key.is_some()
                    || self.table_frontier.is_some()
                {
                    return Err(SystemStreamContractError::MixedSystemStreamFields);
                }
                if self.records.iter().any(|record| record.cursor.is_some()) {
                    return Err(SystemStreamContractError::PhysicalSystemStreamFields);
                }
                for record in &self.records {
                    if record.system_position.is_none() {
                        return Err(SystemStreamContractError::MissingRecordPosition);
                    }
                    let Some(source_table_name) = record.source_table_name.as_ref() else {
                        return Err(SystemStreamContractError::MissingSourceTableName);
                    };
                    SystemStreamSourceTableName::try_from(source_table_name)?;
                }
                Ok(())
            }
            (None, Some(_)) => Err(SystemStreamContractError::MissingCheckpointScope),
            (None, None) => {
                if self.table_name.is_some() != self.table_frontier.is_some() {
                    return Err(SystemStreamContractError::MissingTableFrontier);
                }
                if let Some(frontier) = &self.table_frontier {
                    frontier.validate()?;
                }
                if self.last_evaluated_key.is_some() && self.table_name.is_none() {
                    return Err(SystemStreamContractError::LegacyLastEvaluatedKey);
                }
                if self
                    .records
                    .iter()
                    .any(|record| record.system_position.is_some())
                {
                    return Err(SystemStreamContractError::UnexpectedSystemStreamFields);
                }
                Ok(())
            }
        }
    }
}

/// Public request for discovering the database-owned logical SystemStream
/// source scope.  The scope is intentionally obtained from the provider;
/// callers never supply a physical table, partition, or checkpoint here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DescribeSystemStreamRequest {}

/// The versioned logical source authority used by subsequent SystemStream
/// page requests.  Physical topology and issued checkpoints are not exposed
/// by this discovery action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DescribeSystemStreamResponse {
    pub source_scope: SystemStreamScope,
}

impl DescribeSystemStreamResponse {
    #[must_use]
    pub const fn new(source_scope: SystemStreamScope) -> Self {
        Self { source_scope }
    }

    #[must_use]
    pub const fn source_scope(&self) -> &SystemStreamScope {
        &self.source_scope
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ListStreamsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<TableName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_stream_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 100)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ListStreamsResponse {
    pub streams: Vec<StreamDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_stream_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamDescriptor {
    pub stream_arn: String,
    pub table_name: TableName,
    pub stream_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeStreamRequest {
    pub stream_arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_shard_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 100)]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_filter: Option<ShardFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ShardFilter {
    #[serde(rename = "Type")]
    pub filter_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeStreamResponse {
    pub stream_description: StreamDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamDescription {
    pub stream_arn: String,
    pub stream_label: String,
    pub stream_status: String,
    pub stream_view_type: StreamViewType,
    pub creation_request_date_time: TimestampSecondsFractional,
    pub table_name: TableName,
    pub key_schema: Vec<KeySchemaElement>,
    pub shards: Vec<Shard>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct Shard {
    pub shard_id: String,
    pub sequence_number_range: SequenceNumberRange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct SequenceNumberRange {
    pub starting_sequence_number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ending_sequence_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetShardIteratorRequest {
    pub stream_arn: String,
    pub shard_id: String,
    pub shard_iterator_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetShardIteratorResponse {
    pub shard_iterator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetRecordsRequest {
    pub shard_iterator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 1000)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetRecordsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_shard_iterator: Option<String>,
    pub records: Vec<DynamoDbStreamsRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DynamoDbStreamsRecord {
    #[serde(rename = "eventID")]
    pub event_id: String,
    pub event_name: String,
    pub event_version: String,
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    pub dynamodb: StreamRecordDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamRecordDetails {
    pub keys: KeyAttributes,
    pub sequence_number: String,
    pub stream_view_type: StreamViewType,
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_image: Option<AttributeMap>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_image: Option<AttributeMap>,
}
