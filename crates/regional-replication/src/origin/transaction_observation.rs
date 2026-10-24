use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::order::WinnerProvenance;
use storage_provider::AcceptedTransactWriteMember;
use storage_types::{ItemProvenance, KeyAttributes, StorageResult};

use crate::native_record::corrupt_record;

/// Preserve every winner actually observed by acceptance, including
/// checks and ordinary-table members. No extra item read or source publication.
pub(super) fn observe_transaction_dependencies(
    members: &[AcceptedTransactWriteMember],
    codec: &CanonicalJsonCodec,
) -> StorageResult<Vec<WinnerProvenance>> {
    let mut observations = Vec::new();
    for member in members {
        let (key, provenance) = observed_provenance(member);
        let Some(provenance) =
            provenance.filter(|value| value.as_bytes() != ItemProvenance::LOCAL_BYTES)
        else {
            continue;
        };
        use replication_types::event::content::CanonicalEventCodec;
        let key = codec.encode_key(key).map_err(|_| corrupt_record())?;
        let winner = codec
            .decode_winner_provenance(provenance.as_bytes(), &key)
            .map_err(|_| corrupt_record())?;
        observations.push(winner);
    }
    Ok(observations)
}

fn observed_provenance(
    member: &AcceptedTransactWriteMember,
) -> (&KeyAttributes, Option<&ItemProvenance>) {
    match member {
        AcceptedTransactWriteMember::Put(item) | AcceptedTransactWriteMember::Update(item) => {
            (&item.key, item.old_provenance.as_ref())
        }
        AcceptedTransactWriteMember::Delete(item) => (&item.key, item.old_provenance.as_ref()),
        AcceptedTransactWriteMember::ConditionCheck(item) => {
            (&item.key, item.old_provenance.as_ref())
        }
    }
}
