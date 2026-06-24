use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    anomaly::{Anomaly, AnomalyKind},
    constants::ARTIFACT_SCHEMA_VERSION,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackgroundLeaseEvent {
    pub lease_key: String,
    pub worker_id: String,
    pub at_ms: i64,
    pub kind: BackgroundLeaseEventKind,
}

impl BackgroundLeaseEvent {
    #[must_use]
    pub fn acquire(
        lease_key: impl Into<String>,
        worker_id: impl Into<String>,
        at_ms: i64,
        lease_until_ms: i64,
    ) -> Self {
        Self {
            lease_key: lease_key.into(),
            worker_id: worker_id.into(),
            at_ms,
            kind: BackgroundLeaseEventKind::Acquire { lease_until_ms },
        }
    }

    #[must_use]
    pub fn renew(
        lease_key: impl Into<String>,
        worker_id: impl Into<String>,
        at_ms: i64,
        lease_until_ms: i64,
    ) -> Self {
        Self {
            lease_key: lease_key.into(),
            worker_id: worker_id.into(),
            at_ms,
            kind: BackgroundLeaseEventKind::Renew { lease_until_ms },
        }
    }

    #[must_use]
    pub fn commit(
        lease_key: impl Into<String>,
        worker_id: impl Into<String>,
        at_ms: i64,
        effect: impl Into<String>,
    ) -> Self {
        Self {
            lease_key: lease_key.into(),
            worker_id: worker_id.into(),
            at_ms,
            kind: BackgroundLeaseEventKind::Commit {
                effect: effect.into(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackgroundLeaseEventKind {
    Acquire { lease_until_ms: i64 },
    Renew { lease_until_ms: i64 },
    Commit { effect: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BackgroundLeaseCheckReport {
    pub schema_version: u32,
    pub checked_event_count: usize,
    pub checked_commit_count: usize,
    pub anomaly_count: usize,
    pub anomalies: Vec<Anomaly>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BackgroundLeaseState {
    worker_id: String,
    lease_until_ms: i64,
}

#[must_use]
pub fn check_background_lease_events(
    events: &[BackgroundLeaseEvent],
) -> BackgroundLeaseCheckReport {
    let mut ordered_events = events.iter().enumerate().collect::<Vec<_>>();
    ordered_events.sort_by_key(|(index, event)| (event.at_ms, *index));

    let mut leases = BTreeMap::<String, BackgroundLeaseState>::new();
    let mut checked_commit_count = 0;
    let mut anomalies = Vec::new();
    for (_index, event) in ordered_events {
        match &event.kind {
            BackgroundLeaseEventKind::Acquire { lease_until_ms } => {
                apply_background_lease_acquire(&mut leases, &mut anomalies, event, *lease_until_ms);
            }
            BackgroundLeaseEventKind::Renew { lease_until_ms } => {
                apply_background_lease_renew(&mut leases, &mut anomalies, event, *lease_until_ms);
            }
            BackgroundLeaseEventKind::Commit { effect } => {
                checked_commit_count += 1;
                check_background_lease_commit(&leases, &mut anomalies, event, effect);
            }
        }
    }

    BackgroundLeaseCheckReport {
        schema_version: ARTIFACT_SCHEMA_VERSION,
        checked_event_count: events.len(),
        checked_commit_count,
        anomaly_count: anomalies.len(),
        anomalies,
    }
}

fn apply_background_lease_acquire(
    leases: &mut BTreeMap<String, BackgroundLeaseState>,
    anomalies: &mut Vec<Anomaly>,
    event: &BackgroundLeaseEvent,
    lease_until_ms: i64,
) {
    if lease_until_ms <= event.at_ms {
        anomalies.push(background_lease_anomaly(
            event,
            Some(format!("lease_until_ms>{}", event.at_ms)),
            Some(lease_until_ms.to_string()),
            "worker acquired a lease that was already expired at acquisition time",
        ));
        return;
    }
    if let Some(current) = leases.get(&event.lease_key)
        && current.worker_id != event.worker_id
        && current.lease_until_ms >= event.at_ms
    {
        anomalies.push(background_lease_anomaly(
            event,
            Some(format!(
                "owner={} lease_until_ms={}",
                current.worker_id, current.lease_until_ms
            )),
            Some(format!(
                "owner={} acquired_at_ms={}",
                event.worker_id, event.at_ms
            )),
            "worker acquired a lease while another worker still held an unexpired lease",
        ));
        return;
    }
    leases.insert(
        event.lease_key.clone(),
        BackgroundLeaseState {
            worker_id: event.worker_id.clone(),
            lease_until_ms,
        },
    );
}

fn apply_background_lease_renew(
    leases: &mut BTreeMap<String, BackgroundLeaseState>,
    anomalies: &mut Vec<Anomaly>,
    event: &BackgroundLeaseEvent,
    lease_until_ms: i64,
) {
    if lease_until_ms <= event.at_ms {
        anomalies.push(background_lease_anomaly(
            event,
            Some(format!("lease_until_ms>{}", event.at_ms)),
            Some(lease_until_ms.to_string()),
            "worker renewed a lease to a timestamp that was already expired",
        ));
        return;
    }
    match leases.get_mut(&event.lease_key) {
        Some(current)
            if current.worker_id == event.worker_id && current.lease_until_ms >= event.at_ms =>
        {
            current.lease_until_ms = lease_until_ms;
        }
        Some(current) => anomalies.push(background_lease_anomaly(
            event,
            Some(format!(
                "owner={} lease_until_ms={}",
                current.worker_id, current.lease_until_ms
            )),
            Some(format!(
                "owner={} renew_at_ms={}",
                event.worker_id, event.at_ms
            )),
            "worker renewed a lease it did not hold at renewal time",
        )),
        None => anomalies.push(background_lease_anomaly(
            event,
            Some("active lease".to_string()),
            None,
            "worker renewed a lease before any active acquisition",
        )),
    }
}

fn check_background_lease_commit(
    leases: &BTreeMap<String, BackgroundLeaseState>,
    anomalies: &mut Vec<Anomaly>,
    event: &BackgroundLeaseEvent,
    effect: &str,
) {
    match leases.get(&event.lease_key) {
        Some(current)
            if current.worker_id == event.worker_id && current.lease_until_ms >= event.at_ms => {}
        Some(current) => anomalies.push(background_lease_anomaly(
            event,
            Some(format!(
                "owner={} lease_until_ms={}",
                current.worker_id, current.lease_until_ms
            )),
            Some(format!(
                "owner={} commit_at_ms={} effect={}",
                event.worker_id, event.at_ms, effect
            )),
            "worker committed protected work without holding an active lease",
        )),
        None => anomalies.push(background_lease_anomaly(
            event,
            Some("active lease".to_string()),
            Some(format!(
                "owner={} commit_at_ms={} effect={}",
                event.worker_id, event.at_ms, effect
            )),
            "worker committed protected work before any active acquisition",
        )),
    }
}

fn background_lease_anomaly(
    event: &BackgroundLeaseEvent,
    expected: Option<String>,
    actual: Option<String>,
    detail: &str,
) -> Anomaly {
    Anomaly {
        kind: AnomalyKind::BackgroundLeaseViolation,
        client_id: -1,
        key: format!("background-lease/{}", event.lease_key),
        expected,
        actual,
        detail: detail.to_string(),
    }
}
