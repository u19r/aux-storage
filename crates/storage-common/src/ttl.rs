use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
};

use md5;
use serde::{Deserialize, Serialize};
#[cfg(feature = "rocksdb")]
use storage_types::SerializesToKey;
use storage_types::{
    AttributeValue, HIDDEN_TTL_INDEX_PREFIX, IndexName, ItemKey, StorageError, StorageResult,
    StoredTableInfo, TTL_PARTITION_ATTRIBUTE, TableName, TimeToLiveStatus, TimestampMillis,
    WireItem,
};

/// Distributed sweep lock metadata used by TTL jobs. Stored alongside the TTL
/// configuration so workers can coordinate ownership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlSweepLock {
    pub owner_id: String,
    pub expires_at: TimestampMillis,
}

impl TtlSweepLock {
    #[must_use]
    pub fn new(owner_id: String, now: TimestampMillis, ttl_ms: i64) -> Self {
        Self {
            owner_id,
            expires_at: now + ttl_ms,
        }
    }

    #[must_use]
    pub fn is_expired(&self, now: TimestampMillis) -> bool {
        *self.expires_at <= *now
    }

    pub fn renew(&mut self, now: TimestampMillis, ttl_ms: i64) {
        self.expires_at = now + ttl_ms;
    }
}

/// Persisted TTL configuration for a table. Mirrors DynamoDB metadata while
/// extending with sweep bookkeeping (locks, shard checkpoints, adaptive
/// batching).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlConfigRecord {
    pub attribute_name: String,
    pub status: TimeToLiveStatus,
    pub gsi_name: String,
    pub created_at: TimestampMillis,
    pub updated_at: TimestampMillis,
    #[serde(default)]
    pub next_shard: u8,
    #[serde(default)]
    pub last_sweep_started_at: Option<TimestampMillis>,
    #[serde(default)]
    pub last_sweep_runtime_ms: Option<u64>,
    #[serde(default)]
    pub skip_streak: u32,
    #[serde(default)]
    pub skip_runs_remaining: u32,
    #[serde(default)]
    pub adaptive_pk_batch_size: Option<u32>,
    #[serde(default)]
    pub sweep_lock: Option<TtlSweepLock>,
    #[serde(default)]
    pub adaptive_low_util_hits: u8,
    #[serde(default)]
    pub adaptive_high_util_hits: u8,
    #[serde(default)]
    pub throttled_runs: u32,
    #[serde(default)]
    pub last_processed_watermark: Option<i64>,
}

impl TtlConfigRecord {
    #[must_use]
    pub fn new(attribute_name: String, gsi_name: &IndexName, status: TimeToLiveStatus) -> Self {
        let now = TimestampMillis::now();
        Self {
            attribute_name,
            status,
            gsi_name: gsi_name.to_string(),
            created_at: now,
            updated_at: now,
            next_shard: 0,
            last_sweep_started_at: None,
            last_sweep_runtime_ms: None,
            skip_streak: 0,
            skip_runs_remaining: 0,
            adaptive_pk_batch_size: None,
            sweep_lock: None,
            adaptive_low_util_hits: 0,
            adaptive_high_util_hits: 0,
            throttled_runs: 0,
            last_processed_watermark: None,
        }
    }

    pub fn touch(&mut self) {
        self.updated_at = TimestampMillis::now();
    }

    #[must_use]
    pub fn gsi_name(&self) -> IndexName {
        IndexName::new(&self.gsi_name)
    }

    #[must_use]
    pub fn compute_shard_batch(&self, initial: usize, min: usize, max: usize) -> usize {
        self.adaptive_pk_batch_size
            .map(|v| v as usize)
            .unwrap_or(initial)
            .clamp(min, max)
    }

    #[must_use]
    pub fn should_skip(&self) -> bool {
        self.skip_runs_remaining > 0
    }

    pub fn consume_skip(&mut self) {
        if self.skip_runs_remaining > 0 {
            self.skip_runs_remaining -= 1;
        }
    }

    pub fn register_progress(&mut self) {
        self.skip_streak = 0;
        self.skip_runs_remaining = 0;
        self.throttled_runs = 0;
    }

    pub fn register_idle(&mut self, max_skip: u32) {
        let next_skip = (self.skip_streak.saturating_add(1)).min(max_skip);
        self.skip_streak = next_skip;
        if next_skip >= max_skip {
            self.skip_runs_remaining = max_skip.saturating_sub(1);
        } else {
            self.skip_runs_remaining = next_skip;
        }
    }

    pub fn register_throttle(&mut self) {
        self.throttled_runs = self.throttled_runs.saturating_add(1);
    }

    pub fn reset_throttle(&mut self) {
        self.throttled_runs = 0;
    }

    #[must_use]
    pub fn should_force_health_check(&self, now: TimestampMillis, interval_minutes: u64) -> bool {
        if interval_minutes == 0 {
            return false;
        }

        let Some(last) = self.last_sweep_started_at else {
            return true;
        };

        let interval_ms = interval_minutes.saturating_mul(60_000);
        let elapsed = (*now).saturating_sub(*last);
        elapsed >= i64::try_from(interval_ms).unwrap_or(i64::MAX)
    }

    /// Adjust the adaptive batch size using simple hysteresis. Returns the new
    /// batch size if it changed, otherwise `None`.
    pub fn update_adaptive_batch(
        &mut self,
        runtime_ms: u64,
        interval_ms: u64,
        min: usize,
        max: usize,
        initial: usize,
    ) -> Option<u32> {
        if runtime_ms == 0 || interval_ms == 0 {
            self.adaptive_low_util_hits = 0;
            self.adaptive_high_util_hits = 0;
            return None;
        }

        let current = self
            .adaptive_pk_batch_size
            .map(|v| v as usize)
            .unwrap_or(initial)
            .clamp(min, max);

        let utilization = runtime_ms as f64 / interval_ms as f64;
        const LOW_THRESHOLD: f64 = 0.30;
        const HIGH_THRESHOLD: f64 = 0.50;
        const REQUIRED_CONSECUTIVE: u8 = 2;

        if utilization < LOW_THRESHOLD && current < max {
            self.adaptive_low_util_hits = self.adaptive_low_util_hits.saturating_add(1);
            self.adaptive_high_util_hits = 0;
            if self.adaptive_low_util_hits < REQUIRED_CONSECUTIVE {
                return None;
            }
            let increased = (current + 1).min(max);
            if increased != current {
                self.adaptive_pk_batch_size = Some(increased as u32);
                self.adaptive_low_util_hits = 0;
                return Some(increased as u32);
            }
        } else if utilization > HIGH_THRESHOLD && current > min {
            self.adaptive_high_util_hits = self.adaptive_high_util_hits.saturating_add(1);
            self.adaptive_low_util_hits = 0;
            if self.adaptive_high_util_hits < REQUIRED_CONSECUTIVE {
                return None;
            }
            let decreased = current.saturating_sub(1).max(min);
            if decreased != current {
                self.adaptive_pk_batch_size = Some(decreased as u32);
                self.adaptive_high_util_hits = 0;
                return Some(decreased as u32);
            }
        } else {
            self.adaptive_low_util_hits = 0;
            self.adaptive_high_util_hits = 0;
        }

        None
    }
}

#[must_use]
pub fn ttl_gsi_name(table_name: &TableName) -> IndexName {
    IndexName::new(&format!(
        "{HIDDEN_TTL_INDEX_PREFIX}{}",
        table_name.sanitized_name()
    ))
}

#[must_use]
pub fn is_ttl_index(index_name: &IndexName) -> bool {
    index_name.as_ref().starts_with(HIDDEN_TTL_INDEX_PREFIX)
}

#[must_use]
pub fn compute_ttl_partition_value(bytes: &[u8]) -> String {
    let digest = md5::compute(bytes);
    let shard_bytes: [u8; 8] = digest[..8].try_into().unwrap_or([0; 8]);
    let shard = u64::from_be_bytes(shard_bytes) % 128;
    format!("{shard:03}")
}

#[must_use]
pub fn shard_to_string(shard: u8) -> String {
    format!("{shard:03}")
}

pub fn augment_item_with_ttl_partition(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
    ttl_attribute: &str,
) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
    let Some(AttributeValue::N(_)) = item.get(ttl_attribute) else {
        return Ok(None);
    };

    let shard = ttl_shard_for_item(table_info, item)?;

    let mut prepared = item.clone();
    prepared.insert(
        TTL_PARTITION_ATTRIBUTE.to_string(),
        AttributeValue::S(shard),
    );
    Ok(Some(prepared))
}

#[cfg(feature = "rocksdb")]
fn ttl_shard_for_item(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    let base_key =
        ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, item)?;
    let key_bytes = base_key.serialize_to_bytes()?;
    Ok(compute_ttl_partition_value(&key_bytes))
}

#[cfg(not(feature = "rocksdb"))]
fn ttl_shard_for_item(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    let mut repr = String::new();
    for key_element in &table_info.key_schema {
        if let Some(value) = item.get(&key_element.attribute_name) {
            if !repr.is_empty() {
                repr.push('|');
            }
            let value = value.inner_string().map_err(|err| {
                StorageError::internal(&format!("ttl shard key scalar conversion: {err}"))
            })?;
            repr.push_str(&value);
        }
    }
    Ok(compute_ttl_partition_value(repr.as_bytes()))
}

pub const TTL_INDEX_PREFIX: &str = "__ttl-index/";
pub const TTL_INDEX_WIDTH: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let item_key =
            ItemKey::item_key_from_next_page_token(&self.0, table_info, &None).map_err(|err| {
                StorageError::internal(&format!("ttl index token decode failed: {err}"))
            })?;
        let Some(item_key) = item_key else {
            return Err(StorageError::internal("ttl index token missing key data"));
        };

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

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for TtlIndexKeyToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for TtlIndexKeyToken {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl std::fmt::Display for TtlIndexKeyToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlIndexKey {
    ttl_seconds: i64,
    token: TtlIndexKeyToken,
}

impl TtlIndexKey {
    #[must_use]
    pub fn new(ttl_seconds: i64, token: TtlIndexKeyToken) -> Self {
        Self { ttl_seconds, token }
    }

    pub fn for_item(
        table_info: &StoredTableInfo,
        ttl_attribute: &str,
        item: &HashMap<String, AttributeValue>,
    ) -> StorageResult<Option<Self>> {
        let Some(ttl_seconds) = ttl_value_from_item(item, ttl_attribute) else {
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

    #[must_use]
    pub fn parse(key: &[u8], prefix: &[u8]) -> Option<Self> {
        if !key.starts_with(prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        if rest.len() <= TTL_INDEX_WIDTH {
            return None;
        }
        let (ttl_bytes, remainder) = rest.split_at(TTL_INDEX_WIDTH);
        if remainder.first().copied()? != b'/' {
            return None;
        }
        let token_bytes = &remainder[1..];
        if token_bytes.is_empty() {
            return None;
        }
        let ttl_str = std::str::from_utf8(ttl_bytes).ok()?;
        let ttl_u64 = ttl_str.parse::<u64>().ok()?;
        let ttl_seconds = i64::try_from(ttl_u64).unwrap_or(i64::MAX);
        let token = String::from_utf8(token_bytes.to_vec()).ok()?;
        Some(Self::new(ttl_seconds, token.into()))
    }

    #[must_use]
    pub fn encode(&self, table_name: &TableName) -> Vec<u8> {
        let mut key = ttl_index_prefix(table_name);
        let normalized = normalize_ttl_seconds(self.ttl_seconds);
        let ttl_str = format!("{normalized:0width$}", width = TTL_INDEX_WIDTH);
        key.extend_from_slice(ttl_str.as_bytes());
        key.push(b'/');
        key.extend_from_slice(self.token.as_str().as_bytes());
        key
    }

    #[must_use]
    pub fn ttl_seconds(&self) -> i64 {
        self.ttl_seconds
    }

    #[must_use]
    pub fn token(&self) -> &TtlIndexKeyToken {
        &self.token
    }

    #[must_use]
    pub fn into_parts(self) -> (i64, String) {
        (self.ttl_seconds, self.token.0)
    }
}

#[must_use]
pub fn ttl_index_prefix(table_name: &TableName) -> Vec<u8> {
    format!("{TTL_INDEX_PREFIX}{}/", table_name.sanitized_name()).into_bytes()
}

#[must_use]
pub fn ttl_index_key(table_name: &TableName, ttl_seconds: i64, key_token: &str) -> Vec<u8> {
    TtlIndexKey::new(ttl_seconds, key_token.into()).encode(table_name)
}

#[must_use]
pub fn ttl_index_range_start(table_name: &TableName) -> Vec<u8> {
    let mut key = ttl_index_prefix(table_name);
    let ttl_str = format!("{:0width$}", 0, width = TTL_INDEX_WIDTH);
    key.extend_from_slice(ttl_str.as_bytes());
    key
}

#[must_use]
pub fn ttl_index_range_end(table_name: &TableName, ttl_seconds: i64) -> Vec<u8> {
    let mut key = ttl_index_prefix(table_name);
    let normalized = normalize_ttl_seconds(ttl_seconds);
    let ttl_str = format!("{normalized:0width$}", width = TTL_INDEX_WIDTH);
    key.extend_from_slice(ttl_str.as_bytes());
    key
}

#[must_use]
pub fn parse_ttl_index_key(key: &[u8], prefix: &[u8]) -> Option<(i64, String)> {
    TtlIndexKey::parse(key, prefix).map(TtlIndexKey::into_parts)
}

pub fn ttl_index_key_token_for_item(
    table_info: &StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    TtlIndexKeyToken::from_item(table_info, item).map(|token| token.to_string())
}

pub fn ttl_index_key_token_for_wire_item(
    table_info: &StoredTableInfo,
    item: &WireItem,
) -> StorageResult<String> {
    TtlIndexKeyToken::from_wire_item(table_info, item).map(|token| token.to_string())
}

pub fn ttl_index_key_for_item(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    ttl_attribute: &str,
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<Option<Vec<u8>>> {
    Ok(TtlIndexKey::for_item(table_info, ttl_attribute, item)?
        .map(|ttl_index_key| ttl_index_key.encode(table_name)))
}

pub fn ttl_index_key_for_wire_item(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    ttl_attribute: &str,
    item: &WireItem,
) -> StorageResult<Option<Vec<u8>>> {
    Ok(TtlIndexKey::for_wire_item(table_info, ttl_attribute, item)?
        .map(|ttl_index_key| ttl_index_key.encode(table_name)))
}

pub fn ttl_index_key_map_from_token(
    token: &str,
    table_info: &StoredTableInfo,
) -> StorageResult<HashMap<String, AttributeValue>> {
    TtlIndexKeyToken::from(token).parse_key_map(table_info)
}

#[must_use]
pub fn ttl_value_from_item(
    item: &HashMap<String, AttributeValue>,
    ttl_attribute: &str,
) -> Option<i64> {
    match item.get(ttl_attribute) {
        Some(AttributeValue::N(value)) => value.parse::<i64>().ok(),
        _ => None,
    }
}

pub fn ttl_value_from_wire_item(
    item: &WireItem,
    ttl_attribute: &str,
) -> StorageResult<Option<i64>> {
    item.number_attribute_i64(ttl_attribute)
}

#[must_use]
pub fn normalize_ttl_seconds(ttl_seconds: i64) -> u64 {
    if ttl_seconds <= 0 {
        0
    } else {
        ttl_seconds as u64
    }
}
