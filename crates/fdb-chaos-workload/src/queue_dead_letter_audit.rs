use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub(crate) struct TargetSeen {
    pub(crate) body: String,
    pub(crate) message_id: String,
    /// The drain's delete of this receipt was acknowledged.
    pub(crate) delete_acked: bool,
}

/// D1–D3. D4 is `audit_claims` on the source evidence.
pub(crate) fn audit_dead_letter(
    source_claims: &[crate::queue_claim_audit::ClaimSeen],
    source_ids: &BTreeMap<String, String>,
    target: &[TargetSeen],
    max_receive_count: u64,
) -> Result<u64, String> {
    if let Some(claim) = source_claims
        .iter()
        .find(|claim| claim.receive_count > max_receive_count)
    {
        return Err(format!(
            "D1 source claim past maxReceiveCount: body={} count={} node={}",
            claim.body, claim.receive_count, claim.node
        ));
    }
    let mut id_of_body: BTreeMap<&str, &str> = BTreeMap::new();
    let mut acked_ids: BTreeSet<&str> = BTreeSet::new();
    for seen in target {
        if let Some(id) = id_of_body.insert(seen.body.as_str(), seen.message_id.as_str()) {
            if id != seen.message_id {
                return Err(format!(
                    "D2 two target copies: body={} ids {id} and {}",
                    seen.body, seen.message_id
                ));
            }
            if acked_ids.contains(seen.message_id.as_str()) {
                return Err(format!(
                    "D2 target message redelivered after an acknowledged delete: {}",
                    seen.message_id
                ));
            }
        }
        if seen.delete_acked {
            acked_ids.insert(seen.message_id.as_str());
        }
        if let Some(source_id) = source_ids.get(&seen.body)
            && source_id != &seen.message_id
        {
            return Err(format!(
                "D3 identity changed: body={} source={source_id} target={}",
                seen.body, seen.message_id
            ));
        }
    }
    Ok(id_of_body.len() as u64)
}
