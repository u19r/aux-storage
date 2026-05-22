use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hash::{Hash, Hasher},
    time::Instant,
};

use storage_types::{KeyAttributes, StorageResult, TableName, WireItem};

use crate::point_read_cache_types::{
    AuthoritativePointReadHit, DurableAbsenceProof, DurableItemRevision,
    InMemoryPointReadCacheConfig, PointReadCacheEvictionPolicy,
};

#[derive(Debug, Clone)]
pub(crate) enum PointReadCacheValue {
    Present {
        item: Box<WireItem>,
        revision: Option<DurableItemRevision>,
    },
    Absent {
        proof: Option<DurableAbsenceProof>,
    },
}

impl PointReadCacheValue {
    pub(crate) fn into_wire_item(self) -> Option<WireItem> {
        match self {
            Self::Present { item, .. } => Some(*item),
            Self::Absent { .. } => None,
        }
    }

    pub(crate) fn into_authoritative_hit(self) -> AuthoritativePointReadHit {
        match self {
            Self::Present { item, revision } => {
                AuthoritativePointReadHit::Present { item, revision }
            }
            Self::Absent { proof } => AuthoritativePointReadHit::Absent { proof },
        }
    }

    fn weight_bytes(&self) -> usize {
        match self {
            Self::Present { item, .. } => item.payload_len(),
            Self::Absent { .. } => 1,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PointReadCacheKey {
    table_name: TableName,
    key_json: String,
}

impl PointReadCacheKey {
    pub(crate) fn new(table_name: &TableName, key: &KeyAttributes) -> StorageResult<Self> {
        let key_json = canonical_key_json(key)?;
        Ok(Self {
            table_name: table_name.clone(),
            key_json,
        })
    }

    fn weight_bytes(&self) -> usize {
        self.table_name.to_string().len() + self.key_json.len()
    }
}

impl Hash for PointReadCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.key_json.hash(state);
    }
}

impl Ord for PointReadCacheKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.table_name
            .to_string()
            .cmp(&other.table_name.to_string())
            .then_with(|| self.key_json.cmp(&other.key_json))
    }
}

impl PartialOrd for PointReadCacheKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CacheQueue {
    Main,
    Recent,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct OrderKey {
    tick: u64,
    key: PointReadCacheKey,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    value: PointReadCacheValue,
    weight_bytes: usize,
    expires_at: Instant,
    queue: CacheQueue,
    order: OrderKey,
    write_version: u64,
}

pub(crate) struct InMemoryPointReadCacheState {
    config: InMemoryPointReadCacheConfig,
    entries: HashMap<PointReadCacheKey, CacheEntry>,
    main_order: BTreeSet<OrderKey>,
    recent_order: BTreeSet<OrderKey>,
    ghost_order: BTreeSet<OrderKey>,
    ghosts: HashMap<PointReadCacheKey, OrderKey>,
    current_bytes: usize,
    main_bytes: usize,
    recent_bytes: usize,
    next_tick: u64,
    epoch: u64,
    in_flight_writes: HashSet<PointReadCacheKey>,
    continuity_broken: bool,
}

impl InMemoryPointReadCacheState {
    pub(crate) fn new(config: InMemoryPointReadCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
            main_order: BTreeSet::new(),
            recent_order: BTreeSet::new(),
            ghost_order: BTreeSet::new(),
            ghosts: HashMap::new(),
            current_bytes: 0,
            main_bytes: 0,
            recent_bytes: 0,
            next_tick: 0,
            epoch: 1,
            in_flight_writes: HashSet::new(),
            continuity_broken: false,
        }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        !self.config.ttl.is_zero() && self.config.capacity > 0 && self.config.max_bytes > 0
    }

    pub(crate) fn get(
        &mut self,
        key: &PointReadCacheKey,
        now: Instant,
    ) -> Option<PointReadCacheValue> {
        if !self.is_enabled() {
            return None;
        }
        if self.continuity_broken {
            return None;
        }
        if self.in_flight_writes.contains(key) {
            return None;
        }
        if self.remove_if_expired(key, now) {
            return None;
        }

        let result = self.entries.get(key).map(|entry| entry.value.clone())?;
        match self.config.eviction_policy {
            PointReadCacheEvictionPolicy::Lru => self.touch_main(key),
            PointReadCacheEvictionPolicy::TwoQueue => {
                match self.entries.get(key).map(|e| e.queue) {
                    Some(CacheQueue::Main) => self.touch_main(key),
                    Some(CacheQueue::Recent) => self.promote_recent_to_main(key),
                    None => {}
                }
            }
        }
        Some(result)
    }

    pub(crate) fn insert(
        &mut self,
        key: PointReadCacheKey,
        value: PointReadCacheValue,
        now: Instant,
        write_version: u64,
    ) {
        if !self.is_enabled() {
            self.remove(&key);
            return;
        }

        self.remove_if_expired(&key, now);

        // Reject stale writes: if the existing entry has a higher or equal write
        // version, the incoming write is from an older operation and must be
        // discarded to prevent caching a stale value.
        if let Some(existing) = self.entries.get(&key)
            && existing.write_version >= write_version
        {
            return;
        }

        self.remove(&key);

        let weight_bytes = key.weight_bytes() + value.weight_bytes();
        if weight_bytes > self.config.max_bytes {
            self.remove_ghost(&key);
            return;
        }

        let queue = match self.config.eviction_policy {
            PointReadCacheEvictionPolicy::Lru => CacheQueue::Main,
            PointReadCacheEvictionPolicy::TwoQueue => {
                if self.remove_ghost(&key).is_some() {
                    CacheQueue::Main
                } else {
                    CacheQueue::Recent
                }
            }
        };
        let order = OrderKey {
            tick: self.next_tick(),
            key: key.clone(),
        };
        let entry = CacheEntry {
            value,
            weight_bytes,
            expires_at: now.checked_add(self.config.ttl).unwrap_or(now),
            queue,
            order: order.clone(),
            write_version,
        };

        self.entries.insert(key, entry);
        self.current_bytes += weight_bytes;
        self.insert_order(queue, order, weight_bytes);
        self.evict_over_budget(now);
    }

    pub(crate) fn remove(&mut self, key: &PointReadCacheKey) -> Option<()> {
        let entry = self.entries.remove(key)?;
        self.current_bytes = self.current_bytes.saturating_sub(entry.weight_bytes);
        self.remove_order(entry.queue, &entry.order, entry.weight_bytes);
        Some(())
    }

    pub(crate) fn prepare_write(&mut self, key: PointReadCacheKey) {
        self.in_flight_writes.insert(key);
    }

    pub(crate) fn complete_write(&mut self, key: &PointReadCacheKey) {
        self.in_flight_writes.remove(key);
    }

    pub(crate) fn mark_continuity_broken(&mut self) {
        self.continuity_broken = true;
    }

    pub(crate) fn rebuild(&mut self) {
        self.entries.clear();
        self.main_order.clear();
        self.recent_order.clear();
        self.ghost_order.clear();
        self.ghosts.clear();
        self.current_bytes = 0;
        self.main_bytes = 0;
        self.recent_bytes = 0;
        self.in_flight_writes.clear();
        self.continuity_broken = false;
        self.epoch = self.epoch.wrapping_add(1);
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    #[expect(dead_code)]
    pub(crate) fn is_continuity_intact(&self) -> bool {
        !self.continuity_broken
    }

    fn remove_if_expired(&mut self, key: &PointReadCacheKey, now: Instant) -> bool {
        let is_expired = self
            .entries
            .get(key)
            .is_some_and(|entry| now >= entry.expires_at);
        if is_expired {
            let _ = self.remove(key);
            return true;
        }
        false
    }

    fn touch_main(&mut self, key: &PointReadCacheKey) {
        let next_tick = self.next_tick();
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        if entry.queue != CacheQueue::Main {
            return;
        }
        let old_order = entry.order.clone();
        let _ = self.main_order.remove(&old_order);
        let new_order = OrderKey {
            tick: next_tick,
            key: key.clone(),
        };
        entry.order = new_order.clone();
        self.main_order.insert(new_order);
    }

    fn promote_recent_to_main(&mut self, key: &PointReadCacheKey) {
        let next_tick = self.next_tick();
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        if entry.queue != CacheQueue::Recent {
            return;
        }
        let old_order = entry.order.clone();
        let _ = self.recent_order.remove(&old_order);
        self.recent_bytes = self.recent_bytes.saturating_sub(entry.weight_bytes);
        let new_order = OrderKey {
            tick: next_tick,
            key: key.clone(),
        };
        entry.queue = CacheQueue::Main;
        entry.order = new_order.clone();
        self.main_order.insert(new_order);
        self.main_bytes += entry.weight_bytes;
    }

    fn evict_over_budget(&mut self, now: Instant) {
        self.remove_expired_fronts(now);

        while self.entries.len() > self.config.capacity
            || self.current_bytes > self.config.max_bytes
        {
            let evicted = match self.config.eviction_policy {
                PointReadCacheEvictionPolicy::Lru => self.evict_oldest_main(false),
                PointReadCacheEvictionPolicy::TwoQueue => {
                    let recent_limit_entries = self.recent_entry_limit();
                    let recent_limit_bytes = self.recent_byte_limit();
                    if self.recent_order.len() > recent_limit_entries
                        || self.recent_bytes > recent_limit_bytes
                    {
                        self.evict_oldest_recent(true)
                    } else if !self.main_order.is_empty() {
                        self.evict_oldest_main(false)
                    } else {
                        self.evict_oldest_recent(true)
                    }
                }
            };
            if !evicted {
                break;
            }
            self.remove_expired_fronts(now);
        }
    }

    fn evict_oldest_main(&mut self, remember_ghost: bool) -> bool {
        let Some(order) = self.main_order.iter().next().cloned() else {
            return false;
        };
        let key = order.key.clone();
        let removed = self.remove(&key);
        if removed.is_some() && remember_ghost {
            self.remember_ghost(key);
        }
        removed.is_some()
    }

    fn evict_oldest_recent(&mut self, remember_ghost: bool) -> bool {
        let Some(order) = self.recent_order.iter().next().cloned() else {
            return false;
        };
        let key = order.key.clone();
        let removed = self.remove(&key);
        if removed.is_some() && remember_ghost {
            self.remember_ghost(key);
        }
        removed.is_some()
    }

    fn remove_expired_fronts(&mut self, now: Instant) {
        loop {
            let mut removed_any = false;

            if let Some(order) = self.main_order.iter().next().cloned()
                && self
                    .entries
                    .get(&order.key)
                    .is_some_and(|entry| now >= entry.expires_at)
            {
                let _ = self.remove(&order.key);
                removed_any = true;
            }

            if let Some(order) = self.recent_order.iter().next().cloned()
                && self
                    .entries
                    .get(&order.key)
                    .is_some_and(|entry| now >= entry.expires_at)
            {
                let _ = self.remove(&order.key);
                removed_any = true;
            }

            if !removed_any {
                return;
            }
        }
    }

    fn remember_ghost(&mut self, key: PointReadCacheKey) {
        if self.config.eviction_policy != PointReadCacheEvictionPolicy::TwoQueue {
            return;
        }
        let _ = self.remove_ghost(&key);
        let order = OrderKey {
            tick: self.next_tick(),
            key: key.clone(),
        };
        self.ghost_order.insert(order.clone());
        self.ghosts.insert(key, order);

        while self.ghosts.len() > self.recent_entry_limit() {
            let Some(oldest) = self.ghost_order.iter().next().cloned() else {
                break;
            };
            self.ghost_order.remove(&oldest);
            self.ghosts.remove(&oldest.key);
        }
    }

    fn remove_ghost(&mut self, key: &PointReadCacheKey) -> Option<OrderKey> {
        let order = self.ghosts.remove(key)?;
        self.ghost_order.remove(&order);
        Some(order)
    }

    fn insert_order(&mut self, queue: CacheQueue, order: OrderKey, weight_bytes: usize) {
        match queue {
            CacheQueue::Main => {
                self.main_order.insert(order);
                self.main_bytes += weight_bytes;
            }
            CacheQueue::Recent => {
                self.recent_order.insert(order);
                self.recent_bytes += weight_bytes;
            }
        }
    }

    fn remove_order(&mut self, queue: CacheQueue, order: &OrderKey, weight_bytes: usize) {
        match queue {
            CacheQueue::Main => {
                let _ = self.main_order.remove(order);
                self.main_bytes = self.main_bytes.saturating_sub(weight_bytes);
            }
            CacheQueue::Recent => {
                let _ = self.recent_order.remove(order);
                self.recent_bytes = self.recent_bytes.saturating_sub(weight_bytes);
            }
        }
    }

    fn recent_entry_limit(&self) -> usize {
        self.config.capacity.div_ceil(4).max(1)
    }

    fn recent_byte_limit(&self) -> usize {
        self.config.max_bytes.div_ceil(4).max(1)
    }

    fn next_tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick = self.next_tick.saturating_add(1);
        tick
    }
}

fn canonical_key_json(key: &KeyAttributes) -> StorageResult<String> {
    key.canonical_dynamo_json()
        .map_err(|err| storage_types::StorageError::internal(&format!("encode cache key: {err}")))
}
