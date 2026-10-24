//! Anchors belong to this process and never survive lost qualification or
//! restart.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use replication_provider::runtime_configuration::time_trust::TimeTrustConfiguration;
use replication_types::identity::resource::ControlOperationId;

use super::{age::QualifiedPrefixAge, source::QualifiedTimeSample};
use crate::journal::public_trim_scan::CompleteTrimPrefix;

#[derive(Default)]
pub(crate) struct RetentionTimeState {
    anchors: Mutex<BTreeMap<ControlOperationId, PrefixTimeAnchor>>,
    acquisition: tokio::sync::Mutex<()>,
}

struct PrefixTimeAnchor {
    prefix: CompleteTrimPrefix,
    first: QualifiedTimeSample,
    latest: QualifiedTimeSample,
    live: Arc<AtomicBool>,
}

struct QualificationAttempt<'state> {
    state: &'state RetentionTimeState,
    qualified: bool,
}

/// Covers native work, source admission and cancellation around time
/// acquisition.
pub(crate) struct TrimTimeAttempt<'state> {
    state: &'state RetentionTimeState,
    operation: ControlOperationId,
    retained: bool,
}

impl TrimTimeAttempt<'_> {
    pub(crate) fn retain_anchor(mut self) {
        self.retained = true;
    }
}

impl Drop for TrimTimeAttempt<'_> {
    fn drop(&mut self) {
        if !self.retained {
            self.state.invalidate_operation(self.operation);
        }
    }
}

impl RetentionTimeState {
    pub(crate) fn begin_attempt(&self, operation: ControlOperationId) -> TrimTimeAttempt<'_> {
        TrimTimeAttempt {
            state: self,
            operation,
            retained: false,
        }
    }

    pub(crate) async fn observe(
        &self,
        input: (ControlOperationId, tokio::time::Instant),
        prefix: Option<CompleteTrimPrefix>,
        trust: Option<&TimeTrustConfiguration>,
        policy: &crate::trim::proposal::TrimPolicy,
    ) -> Option<QualifiedPrefixAge> {
        let (operation, deadline) = input;
        let Some((prefix, trust)) = prefix.zip(trust) else {
            self.invalidate_operation(operation);
            return None;
        };
        if trust
            .require_retention_policy(policy.clock_policy, policy.minimum_retention_ms)
            .is_err()
        {
            self.invalidate_operation(operation);
            return None;
        }
        let mut attempt = QualificationAttempt {
            state: self,
            qualified: false,
        };
        let Ok(_acquisition) = tokio::time::timeout_at(deadline, self.acquisition.lock()).await
        else {
            return None;
        };
        let sample = QualifiedTimeSample::acquire(trust, deadline).await?;
        let age = self.retain_anchor(operation, prefix, sample, policy.minimum_retention_ms);
        attempt.qualified = true;
        age
    }

    fn invalidate_operation(&self, operation: ControlOperationId) {
        if let Ok(mut anchors) = self.anchors.lock() {
            anchors.remove(&operation);
        }
    }

    fn retain_anchor(
        &self,
        operation: ControlOperationId,
        prefix: CompleteTrimPrefix,
        sample: QualifiedTimeSample,
        minimum: std::num::NonZeroU64,
    ) -> Option<QualifiedPrefixAge> {
        let Ok(mut anchors) = self.anchors.lock() else {
            return None;
        };
        anchors.retain(|_, anchor| tokio::time::Instant::now() < anchor.latest.fresh_until);
        if anchors
            .values()
            .any(|anchor| !anchor.has_time_continuity(&sample))
        {
            anchors.clear();
        }
        let (_, publisher_ms) = prefix.time_binding();
        let publisher = i128::from(publisher_ms) * 1_000_000;
        if sample
            .clock
            .wall
            .checked_sub(sample.error)
            .is_none_or(|lower| lower < publisher)
        {
            anchors.remove(&operation);
            return None;
        }
        let retained = anchors
            .get(&operation)
            .is_some_and(|anchor| anchor.prefix == prefix);
        if !retained {
            anchors.insert(
                operation,
                PrefixTimeAnchor {
                    prefix: prefix.clone(),
                    first: sample.clone(),
                    latest: sample.clone(),
                    live: Arc::new(AtomicBool::new(true)),
                },
            );
        }
        let anchor = anchors.get_mut(&operation)?;
        anchor.latest = sample.clone();
        QualifiedPrefixAge::try_qualify(
            prefix,
            (anchor.first.clone(), sample),
            (minimum, Arc::clone(&anchor.live)),
        )
    }
}

impl Drop for QualificationAttempt<'_> {
    fn drop(&mut self) {
        if !self.qualified
            && let Ok(mut anchors) = self.state.anchors.lock()
        {
            anchors.clear();
        }
    }
}

impl PrefixTimeAnchor {
    fn has_time_continuity(&self, sample: &QualifiedTimeSample) -> bool {
        [&self.first, &self.latest].into_iter().all(|earlier| {
            if earlier.binding != sample.binding {
                return false;
            }
            let Some(errors) = earlier.error.checked_add(sample.error) else {
                return false;
            };
            sample
                .clock
                .require_continuity(
                    earlier.clock,
                    errors,
                    earlier.frequency.max(sample.frequency),
                )
                .is_some()
        })
    }
}

impl Drop for PrefixTimeAnchor {
    fn drop(&mut self) {
        self.live.store(false, Ordering::Release);
    }
}
