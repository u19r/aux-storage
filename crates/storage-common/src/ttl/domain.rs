use std::{collections::HashMap, convert::TryFrom, num::NonZeroU64};

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use storage_types::{
    AttributeValue, HIDDEN_TTL_INDEX_PREFIX, IndexName, ItemKey, StorageError, StorageResult,
    StoredTableInfo, TableName, TimeToLiveStatus, TimestampMillis, TtlBinding, WireItem,
};

use super::{TtlIndexWrite, TtlIndexWriteBatch, TtlMaintenanceCheckpoint, TtlMaintenanceCursor};

impl TtlIndexWriteBatch {
    fn from_keys(old: Option<TtlIndexKey>, new: Option<TtlIndexKey>) -> Self {
        if old == new {
            return Self::empty();
        }
        let mut writes = Vec::with_capacity(2);
        writes.extend(old.map(TtlIndexWrite::Delete));
        writes.extend(new.map(TtlIndexWrite::Upsert));
        Self(writes)
    }

    pub fn for_wire_items(
        table: &StoredTableInfo,
        config: Option<&TtlBinding>,
        old_item: Option<&WireItem>,
        new_item: Option<&WireItem>,
    ) -> StorageResult<Self> {
        let Some(config) = config.filter(|config| config.tracks()) else {
            return Ok(Self(Vec::new()));
        };
        let old_key = old_item
            .map(|item| TtlIndexKey::for_wire_item(table, &config.attribute, item))
            .transpose()?
            .flatten();
        let new_key = new_item
            .map(|item| TtlIndexKey::for_wire_item(table, &config.attribute, item))
            .transpose()?
            .flatten();
        Ok(Self::from_keys(old_key, new_key))
    }

    /// Attribute-map variant of [`Self::for_wire_items`] for sqlite/postgres
    /// write paths. Same status gate, diff, and no-op short-circuit;
    /// backends keep only their DELETE/INSERT strings.
    pub fn for_items(
        table: &StoredTableInfo,
        config: Option<&TtlBinding>,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<Self> {
        let Some(config) = config.filter(|config| config.tracks()) else {
            return Ok(Self(Vec::new()));
        };
        let old_key = old_item
            .filter(|item| !item.is_empty())
            .map(|item| TtlIndexKey::for_item(table, &config.attribute, item))
            .transpose()?
            .flatten();
        let new_key = new_item
            .filter(|item| !item.is_empty())
            .map(|item| TtlIndexKey::for_item(table, &config.attribute, item))
            .transpose()?
            .flatten();
        Ok(Self::from_keys(old_key, new_key))
    }
}

/// New-image TTL work completed against the operation's bound TTL policy.
pub enum PreparedTtlPut {
    Disabled,
    NewItem {
        attribute_name: String,
        token: TtlIndexKeyToken,
        key: Option<TtlIndexKey>,
    },
}

impl PreparedTtlPut {
    pub fn try_new(
        table: &StoredTableInfo,
        ttl: Option<&TtlBinding>,
        item: &WireItem,
    ) -> StorageResult<Self> {
        let Some(ttl) = ttl.filter(|ttl| ttl.tracks()) else {
            return Ok(Self::Disabled);
        };
        let key = TtlIndexKey::for_wire_item(table, &ttl.attribute, item)?;
        let token = match &key {
            Some(key) => key.token.clone(),
            None => TtlIndexKeyToken::from_wire_item(table, item)?,
        };
        Ok(Self::NewItem {
            attribute_name: ttl.attribute.clone(),
            token,
            key,
        })
    }

    pub fn new_key(&self) -> Option<&TtlIndexKey> {
        match self {
            Self::Disabled => None,
            Self::NewItem { key, .. } => key.as_ref(),
        }
    }

    pub fn finish(
        &self,
        ttl: Option<&TtlBinding>,
        old: Option<&WireItem>,
    ) -> StorageResult<TtlIndexWriteBatch> {
        let ttl = ttl.filter(|ttl| ttl.tracks());
        match (self, ttl) {
            (Self::Disabled, None) => Ok(TtlIndexWriteBatch::empty()),
            (
                Self::NewItem {
                    attribute_name,
                    token,
                    key,
                },
                Some(ttl),
            ) if *attribute_name == ttl.attribute => {
                let old_key = old
                    .map(|item| item.scalar_attributes(&[attribute_name.as_str()]))
                    .transpose()?
                    .and_then(|values| {
                        values[0]
                            .as_deref()
                            .and_then(|value| value.parse::<i64>().ok())
                    })
                    .map(|seconds| TtlIndexKey::new(seconds, token.clone()));
                Ok(TtlIndexWriteBatch::from_keys(old_key, key.clone()))
            }
            _ => Err(storage_types::StorageError::logical_conflict(
                "prepared TTL policy changed",
            )),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TtlPolicyGeneration(NonZeroU64);

impl TtlPolicyGeneration {
    #[must_use]
    pub const fn one() -> Self {
        Self(NonZeroU64::MIN)
    }

    pub fn next(self) -> StorageResult<Self> {
        self.0
            .get()
            .checked_add(1)
            .ok_or_else(|| StorageError::validation("ttl policy generation overflow"))
            .and_then(Self::try_from)
    }
}

impl TryFrom<u64> for TtlPolicyGeneration {
    type Error = StorageError;

    fn try_from(value: u64) -> StorageResult<Self> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or_else(|| StorageError::validation("ttl policy generation must be nonzero"))
    }
}

impl From<TtlPolicyGeneration> for u64 {
    fn from(generation: TtlPolicyGeneration) -> Self {
        generation.0.get()
    }
}

/// The logical, deterministic input to one bounded TTL sweep. The command
/// intentionally carries no physical table, route, proposal, or read-set
/// information; a native executor resolves those details from its transaction.
#[derive(Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TtlSweepPageInput {
    pub ttl_attribute: String,
    pub scan_time: TimestampMillis,
    pub limit: usize,
    pub cursor: Option<TtlMaintenanceCursor>,
}

impl TtlSweepPageInput {
    pub fn try_new(
        ttl_attribute: impl Into<String>,
        scan_time: TimestampMillis,
        limit: usize,
        cursor: Option<TtlMaintenanceCursor>,
    ) -> StorageResult<Self> {
        let ttl_attribute = ttl_attribute.into();
        if ttl_attribute.trim().is_empty() {
            return Err(StorageError::validation(
                "ttl maintenance attribute must not be empty",
            ));
        }
        if scan_time.timestamp_millis() <= 0 {
            return Err(StorageError::validation(
                "ttl maintenance scan time must be positive",
            ));
        }
        if limit == 0 || limit > crate::MAX_TTL_SWEEP_PAGE_LIMIT {
            return Err(StorageError::validation(format!(
                "ttl maintenance page limit must be between 1 and {}",
                crate::MAX_TTL_SWEEP_PAGE_LIMIT
            )));
        }
        Ok(Self {
            ttl_attribute,
            scan_time,
            limit,
            cursor,
        })
    }
}

impl<'de> Deserialize<'de> for TtlSweepPageInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            ttl_attribute: String,
            scan_time: TimestampMillis,
            limit: usize,
            cursor: Option<TtlMaintenanceCursor>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::try_new(wire.ttl_attribute, wire.scan_time, wire.limit, wire.cursor)
            .map_err(D::Error::custom)
    }
}

/// A policy observation and its protected immutable table identity.
#[derive(Clone)]
pub struct TtlSweepPolicy {
    pub target: storage_types::TableHistoryTarget,
    pub config: TtlConfigRecord,
}

/// One deterministic semantic TTL sweep command. The expected generation is
/// mandatory so a stale scheduler command cannot apply under a new policy.
#[derive(Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TtlSweepPageCommand {
    pub target: storage_types::TableHistoryTarget,
    pub input: TtlSweepPageInput,
    pub expected_policy_generation: TtlPolicyGeneration,
    pub captured_at: TimestampMillis,
}

impl<'de> Deserialize<'de> for TtlSweepPageCommand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            target: storage_types::TableHistoryTarget,
            input: TtlSweepPageInput,
            expected_policy_generation: TtlPolicyGeneration,
            captured_at: TimestampMillis,
        }

        let wire = Wire::deserialize(deserializer)?;
        Self::require_capture(wire.captured_at).map_err(D::Error::custom)?;
        Ok(Self {
            target: wire.target,
            input: wire.input,
            expected_policy_generation: wire.expected_policy_generation,
            captured_at: wire.captured_at,
        })
    }
}

impl TtlSweepPageCommand {
    pub fn try_new(
        policy: TtlSweepPolicy,
        input: TtlSweepPageInput,
        captured_at: TimestampMillis,
    ) -> StorageResult<Self> {
        Self::require_capture(captured_at)?;
        Ok(Self {
            target: policy.target,
            input,
            expected_policy_generation: policy.config.policy_generation,
            captured_at,
        })
    }

    fn require_capture(captured_at: TimestampMillis) -> StorageResult<()> {
        if captured_at.timestamp_millis() <= 0 {
            return Err(StorageError::validation(
                "ttl maintenance captured time must be positive",
            ));
        }
        Ok(())
    }
}

/// Whether a TTL config arms tracking writes. Only enabled or enabling
/// policies track; absent, disabled, or completed policies stay silent.
/// Shared by KV and SQL update paths so both arm fan-out identically.
#[must_use]
pub fn ttl_tracking_enabled(config: Option<&TtlConfigRecord>) -> bool {
    config.is_some_and(|config| {
        matches!(
            config.status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
        )
    })
}

/// Persisted TTL policy for a direct native table.
#[derive(Clone, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TtlConfigRecord {
    pub attribute_name: String,
    pub status: TimeToLiveStatus,
    pub gsi_name: IndexName,
    pub created_at: TimestampMillis,
    pub updated_at: TimestampMillis,
    pub policy_generation: TtlPolicyGeneration,
    pub checkpoint: TtlMaintenanceCheckpoint,
}

impl TtlConfigRecord {
    /// The writer-visible part of the policy.
    #[must_use]
    pub fn binding(&self) -> TtlBinding {
        TtlBinding {
            attribute: self.attribute_name.clone(),
            status: self.status.clone(),
        }
    }

    /// Equal policies that differ at most in the sweep checkpoint.
    #[must_use]
    pub fn same_policy(&self, other: &Self) -> bool {
        Self {
            checkpoint: other.checkpoint.clone(),
            ..self.clone()
        } == *other
    }

    pub fn try_new(
        attribute_name: String,
        table_name: &TableName,
        status: TimeToLiveStatus,
    ) -> StorageResult<Self> {
        Self::try_new_at(attribute_name, table_name, status, TimestampMillis::now())
    }

    pub fn try_new_at(
        attribute_name: String,
        table_name: &TableName,
        status: TimeToLiveStatus,
        now: TimestampMillis,
    ) -> StorageResult<Self> {
        let config = Self {
            attribute_name,
            status,
            gsi_name: IndexName::new(&format!(
                "{HIDDEN_TTL_INDEX_PREFIX}{}",
                table_name.sanitized_name()
            )),
            created_at: now,
            updated_at: now,
            policy_generation: TtlPolicyGeneration::one(),
            checkpoint: TtlMaintenanceCheckpoint::default(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> StorageResult<()> {
        if self.attribute_name.trim().is_empty() {
            return Err(StorageError::validation(
                "ttl policy attribute name must not be empty",
            ));
        }
        if self.gsi_name.as_ref().trim().is_empty() {
            return Err(StorageError::validation(
                "ttl policy gsi name must not be empty",
            ));
        }
        if self.created_at.timestamp_millis() <= 0
            || self.updated_at.timestamp_millis() <= 0
            || self.updated_at < self.created_at
        {
            return Err(StorageError::validation(
                "ttl policy timestamps are invalid",
            ));
        }
        Ok(())
    }

    pub fn touch(&mut self) {
        self.touch_at(TimestampMillis::now());
    }

    pub fn touch_at(&mut self, now: TimestampMillis) {
        self.updated_at = now;
    }

    pub fn activate_for_index(
        mut self,
        table_name: TableName,
        index_name: &IndexName,
        now: TimestampMillis,
    ) -> StorageResult<super::TtlConfigActivation> {
        if self.gsi_name != *index_name {
            return Err(StorageError::internal(
                "TTL policy does not belong to the activated index",
            ));
        }
        if self.status != TimeToLiveStatus::Enabling {
            return Err(StorageError::internal(
                "TTL index activation requires an enabling policy",
            ));
        }
        if now < self.updated_at {
            return Err(StorageError::internal(
                "TTL index activation time precedes the current policy",
            ));
        }
        let expected = self.clone();
        self.status = TimeToLiveStatus::Enabled;
        self.updated_at = now;
        Ok(super::TtlConfigActivation {
            table_name,
            expected,
            activated: self,
        })
    }
}

impl<'de> Deserialize<'de> for TtlConfigRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            attribute_name: String,
            status: TimeToLiveStatus,
            gsi_name: IndexName,
            created_at: TimestampMillis,
            updated_at: TimestampMillis,
            policy_generation: TtlPolicyGeneration,
            checkpoint: TtlMaintenanceCheckpoint,
        }

        let wire = Wire::deserialize(deserializer)?;
        let config = Self {
            attribute_name: wire.attribute_name,
            status: wire.status,
            gsi_name: wire.gsi_name,
            created_at: wire.created_at,
            updated_at: wire.updated_at,
            policy_generation: wire.policy_generation,
            checkpoint: wire.checkpoint,
        };
        config.validate().map_err(D::Error::custom)?;
        Ok(config)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TtlIndexKeyToken(String);

impl TtlIndexKeyToken {
    pub fn from_item(
        table_info: &StoredTableInfo,
        item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<Self> {
        let token =
            ItemKey::last_evaluated_key_from_last_item(item, table_info, &None).map_err(|err| {
                StorageError::internal(&format!("ttl index token build failed: {err}"))
            })?;
        token
            .map(Self)
            .ok_or_else(|| StorageError::internal("ttl index token missing key attributes"))
    }

    pub fn from_wire_item(table_info: &StoredTableInfo, item: &WireItem) -> StorageResult<Self> {
        let token = item.last_evaluated_key(table_info, &None).map_err(|err| {
            StorageError::internal(&format!("ttl index token build failed: {err}"))
        })?;
        token
            .map(Self)
            .ok_or_else(|| StorageError::internal("ttl index token missing key attributes"))
    }

    pub fn parse_key_map(
        &self,
        table_info: &StoredTableInfo,
    ) -> StorageResult<HashMap<String, AttributeValue>> {
        let item_key = ItemKey::item_key_from_next_page_token(&self.0, table_info, &None)
            .map_err(|err| {
                StorageError::internal(&format!("ttl index token decode failed: {err}"))
            })?
            .ok_or_else(|| StorageError::internal("ttl index token missing key data"))?;
        let mut key_map = HashMap::new();
        for element in &table_info.key_schema {
            match element.key_type {
                storage_types::KeyType::Hash => {
                    key_map.insert(element.attribute_name.clone(), item_key.hash_key().clone());
                }
                storage_types::KeyType::Range => {
                    let range = item_key.range_key().ok_or_else(|| {
                        StorageError::internal("ttl index token missing range key")
                    })?;
                    key_map.insert(element.attribute_name.clone(), range.clone());
                }
            }
        }
        Ok(key_map)
    }
}

impl From<String> for TtlIndexKeyToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<TtlIndexKeyToken> for String {
    fn from(token: TtlIndexKeyToken) -> Self {
        token.0
    }
}

impl AsRef<str> for TtlIndexKeyToken {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TtlIndexKey {
    ttl_seconds: i64,
    token: TtlIndexKeyToken,
}

impl TtlIndexKey {
    #[must_use]
    pub fn new(ttl_seconds: i64, token: TtlIndexKeyToken) -> Self {
        Self { ttl_seconds, token }
    }

    /// Signed seconds followed by the UTF-8 token preserve logical expiry/token
    /// order.
    pub fn append_ordered_bytes(&self, bytes: &mut Vec<u8>) -> StorageResult<()> {
        let count = 8usize
            .checked_add(self.token.as_ref().len())
            .ok_or(storage_types::AttemptFailure::CAPACITY)?;
        bytes
            .try_reserve_exact(count)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        bytes.extend_from_slice(&Self::encode_ordered_seconds(self.ttl_seconds));
        bytes.extend_from_slice(self.token.as_ref().as_bytes());
        Ok(())
    }

    pub fn encode_ordered_bytes(&self) -> StorageResult<Vec<u8>> {
        let mut bytes = Vec::new();
        self.append_ordered_bytes(&mut bytes)?;
        Ok(bytes)
    }

    #[must_use]
    pub fn encode_ordered_seconds(seconds: i64) -> [u8; 8] {
        let mut bytes = seconds.to_be_bytes();
        bytes[0] ^= 0x80;
        bytes
    }

    pub fn decode_ordered_bytes(mut bytes: Vec<u8>) -> StorageResult<Self> {
        let (seconds, token) = bytes
            .split_at_checked(8)
            .ok_or_else(|| StorageError::internal("TTL ordered key is too short"))?;
        let mut seconds: [u8; 8] = seconds
            .try_into()
            .map_err(|_| StorageError::internal("TTL ordered seconds have invalid width"))?;
        seconds[0] ^= 0x80;
        if token.is_empty() {
            return Err(StorageError::internal("TTL ordered token is empty"));
        }
        bytes.drain(..8);
        let token = String::from_utf8(bytes)
            .map_err(|_| StorageError::internal("TTL ordered token is invalid UTF-8"))?;
        Ok(Self::new(i64::from_be_bytes(seconds), token.into()))
    }

    #[must_use]
    pub fn token(&self) -> &str {
        self.token.as_ref()
    }

    pub fn ttl_seconds(&self) -> i64 {
        self.ttl_seconds
    }

    pub fn for_item(
        table_info: &StoredTableInfo,
        ttl_attribute: &str,
        item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<Option<Self>> {
        let Some(AttributeValue::N(ttl_value)) = item.get(ttl_attribute) else {
            return Ok(None);
        };
        let Ok(ttl_seconds) = ttl_value.parse::<i64>() else {
            return Ok(None);
        };
        Ok(Some(Self::new(
            ttl_seconds,
            TtlIndexKeyToken::from_item(table_info, item)?,
        )))
    }

    pub fn for_wire_item(
        table_info: &StoredTableInfo,
        ttl_attribute: &str,
        item: &WireItem,
    ) -> StorageResult<Option<Self>> {
        let value_and_token = item.ttl_value_and_table_key_token(table_info, ttl_attribute)?;
        let Some((ttl_seconds, token)) = value_and_token else {
            return Ok(None);
        };
        Ok(Some(Self::new(ttl_seconds, token.into())))
    }
}

impl From<TtlIndexKey> for (i64, String) {
    fn from(key: TtlIndexKey) -> Self {
        (key.ttl_seconds, key.token.0)
    }
}

impl<'key> From<&'key TtlIndexKey> for (i64, &'key str) {
    fn from(key: &'key TtlIndexKey) -> Self {
        (key.ttl_seconds, key.token.as_ref())
    }
}
