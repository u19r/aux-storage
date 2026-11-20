//! Pure audit of concurrent queue claims (W2). Times are simulated
//! milliseconds; a call committed inside `[called_at, returned_at]`.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClaimSeen {
    pub(crate) body: String,
    pub(crate) receive_count: u64,
    pub(crate) node: u16,
    pub(crate) called_at: i64,
    pub(crate) returned_at: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReleaseKind {
    Delete,
    Visibility,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReleaseOutcome {
    Acked,
    /// Unknown, aborted, or a stale-receipt answer: the effect is not known.
    Undetermined,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReleaseSeen {
    pub(crate) body: String,
    /// The receive count of the claim whose receipt the call used.
    pub(crate) receive_count: u64,
    pub(crate) kind: ReleaseKind,
    pub(crate) outcome: ReleaseOutcome,
    pub(crate) called_at: i64,
    pub(crate) returned_at: i64,
}

#[derive(Default)]
pub(crate) struct ClaimEvidence {
    pub(crate) acked_bodies: BTreeSet<String>,
    pub(crate) unknown_bodies: BTreeSet<String>,
    pub(crate) claims: Vec<ClaimSeen>,
    pub(crate) releases: Vec<ReleaseSeen>,
    /// Bodies returned by the final drain.
    pub(crate) drained: BTreeSet<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ClaimAuditCounts {
    pub(crate) effective_deletes: u64,
    pub(crate) undetermined_deletes: u64,
}

/// Returns the counts, or the first broken invariant.
pub(crate) fn audit_claims(
    evidence: &ClaimEvidence,
    visibility_millis: i64,
    skew_millis: i64,
) -> Result<ClaimAuditCounts, String> {
    let mut by_body: BTreeMap<&str, BTreeMap<u64, &ClaimSeen>> = BTreeMap::new();
    for claim in &evidence.claims {
        if claim.receive_count == 0 {
            return Err(format!("C6 receive count 0: body={}", claim.body));
        }
        if !evidence.acked_bodies.contains(&claim.body)
            && !evidence.unknown_bodies.contains(&claim.body)
        {
            return Err(format!("C5 foreign body claimed: {}", claim.body));
        }
        if let Some(previous) = by_body
            .entry(claim.body.as_str())
            .or_default()
            .insert(claim.receive_count, claim)
        {
            return Err(format!(
                "C1 two holders of one claim: body={} count={} nodes={} and {}",
                claim.body, claim.receive_count, previous.node, claim.node
            ));
        }
    }
    let mut first_release: BTreeMap<(&str, u64), i64> = BTreeMap::new();
    for release in &evidence.releases {
        let entry = first_release
            .entry((release.body.as_str(), release.receive_count))
            .or_insert(release.called_at);
        *entry = (*entry).min(release.called_at);
    }
    for (body, claims) in &by_body {
        let ordered: Vec<&ClaimSeen> = claims.values().copied().collect();
        for pair in ordered.windows(2) {
            let (held, next) = (pair[0], pair[1]);
            let lease_bound = held.called_at + visibility_millis + skew_millis;
            let bound = first_release
                .get(&(*body, held.receive_count))
                .map_or(lease_bound, |released| lease_bound.min(*released));
            if next.returned_at < bound {
                return Err(format!(
                    "C2 claim inside a lease: body={body} count {} (node {}, called {}) then \
                     count {} (node {}, returned {}) bound {bound}",
                    held.receive_count,
                    held.node,
                    held.called_at,
                    next.receive_count,
                    next.node,
                    next.returned_at
                ));
            }
        }
    }
    let mut counts = ClaimAuditCounts::default();
    let mut settled: BTreeSet<&str> = BTreeSet::new();
    for release in evidence
        .releases
        .iter()
        .filter(|release| release.kind == ReleaseKind::Delete)
    {
        let Some(held) = by_body
            .get(release.body.as_str())
            .and_then(|claims| claims.get(&release.receive_count))
        else {
            return Err(format!(
                "delete of an unobserved claim: body={} count={}",
                release.body, release.receive_count
            ));
        };
        let effective = release.outcome == ReleaseOutcome::Acked
            && release.returned_at < held.called_at + visibility_millis + skew_millis;
        if effective {
            counts.effective_deletes += 1;
            let later = by_body[release.body.as_str()]
                .range(release.receive_count + 1..)
                .next();
            if let Some((count, claim)) = later {
                return Err(format!(
                    "C3 claim after an effective delete: body={} deleted count={} then \
                     count={count} on node {}",
                    release.body, release.receive_count, claim.node
                ));
            }
            if evidence.drained.contains(&release.body) {
                return Err(format!(
                    "C3 effectively deleted body drained: {}",
                    release.body
                ));
            }
        } else {
            counts.undetermined_deletes += 1;
        }
        settled.insert(release.body.as_str());
    }
    for body in &evidence.acked_bodies {
        if !settled.contains(body.as_str()) && !evidence.drained.contains(body) {
            return Err(format!("C4 acknowledged message lost: {body}"));
        }
    }
    Ok(counts)
}
