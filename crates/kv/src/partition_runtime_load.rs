use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::partition_family::{
    PartitionFamilyKind, PartitionLoadSample, RuntimePartitionLoadSample, merge_partition_load,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RuntimePartitionLoadKey {
    family_kind: PartitionFamilyKind,
    family_component: String,
    partition_id: u16,
}

impl RuntimePartitionLoadKey {
    fn from_sample(sample: &RuntimePartitionLoadSample) -> Self {
        Self {
            family_kind: sample.family_kind,
            family_component: sample.family_component.clone(),
            partition_id: sample.partition_id,
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct RuntimePartitionLoadTracker {
    inner: Arc<Mutex<HashMap<RuntimePartitionLoadKey, PartitionLoadSample>>>,
}

impl RuntimePartitionLoadTracker {
    pub(crate) fn record(&self, sample: RuntimePartitionLoadSample) {
        let mut inner = self.lock_inner();
        let key = RuntimePartitionLoadKey::from_sample(&sample);
        let entry = inner.entry(key).or_default();
        merge_partition_load(entry, &sample.sample);
    }

    pub(crate) fn load_hint(
        &self,
        family_kind: PartitionFamilyKind,
        family_component: &str,
        partition_id: u16,
    ) -> u64 {
        let inner = self.lock_inner();
        inner
            .get(&RuntimePartitionLoadKey {
                family_kind,
                family_component: family_component.to_string(),
                partition_id,
            })
            .map_or(0, |sample| {
                sample
                    .writes
                    .saturating_add(sample.queue_claim_conflicts.saturating_mul(8))
                    .saturating_add(sample.conflicts.saturating_mul(8))
                    .saturating_add(sample.queue_scan_work / 4)
            })
    }

    pub(crate) fn drain(&self) -> Vec<RuntimePartitionLoadSample> {
        let mut inner = self.lock_inner();
        let drained = std::mem::take(&mut *inner);
        drained
            .into_iter()
            .map(|(key, sample)| RuntimePartitionLoadSample {
                family_kind: key.family_kind,
                family_component: key.family_component,
                partition_id: key.partition_id,
                sample,
            })
            .collect()
    }

    fn lock_inner(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<RuntimePartitionLoadKey, PartitionLoadSample>> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("recovering poisoned runtime partition load tracker mutex");
            poisoned.into_inner()
        })
    }
}
