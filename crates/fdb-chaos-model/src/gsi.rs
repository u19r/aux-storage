use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GsiEntry {
    pub key: String,
    pub sort: String,
    pub value: String,
}

impl GsiEntry {
    #[must_use]
    pub fn new(key: String, sort: String, value: String) -> Self {
        Self { key, sort, value }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GsiIndexModel {
    by_partition: BTreeMap<String, BTreeSet<GsiEntry>>,
    by_key: BTreeMap<String, (String, GsiEntry)>,
}

impl GsiIndexModel {
    pub fn put(&mut self, partition: String, entry: GsiEntry) {
        self.delete(&entry.key);
        self.by_partition
            .entry(partition.clone())
            .or_default()
            .insert(entry.clone());
        self.by_key.insert(entry.key.clone(), (partition, entry));
    }

    pub fn delete(&mut self, key: &str) {
        let Some((partition, entry)) = self.by_key.remove(key) else {
            return;
        };
        if let Some(entries) = self.by_partition.get_mut(&partition) {
            entries.remove(&entry);
            if entries.is_empty() {
                self.by_partition.remove(&partition);
            }
        }
    }

    #[must_use]
    pub fn entries_for_partition(&self, partition: &str) -> BTreeSet<GsiEntry> {
        self.by_partition
            .get(partition)
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn partitions(&self) -> Vec<String> {
        self.by_partition.keys().cloned().collect()
    }
}
