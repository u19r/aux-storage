//! Refuse the superseded unsigned TTL layout before publishing a KV store.

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

use crate::{
    KvAttempt, KvMutation, ReadProtection,
    constants::{TTL_INDEX_FORMAT_KEY, TTL_INDEX_FORMAT_VERSION},
};

pub(crate) async fn initialize_index_format<A: KvAttempt>(attempt: &mut A) -> StorageResult<()> {
    if let Some(bytes) = attempt
        .get(TTL_INDEX_FORMAT_KEY, ReadProtection::Conflict)
        .await?
    {
        return if bytes == [TTL_INDEX_FORMAT_VERSION] {
            Ok(())
        } else {
            Err(unsupported_format())
        };
    }
    // An absent format key is a new store: the table-first layout has no
    // reader of an earlier TTL index format.
    attempt.stage(vec![KvMutation::PutUnchunked {
        key: TTL_INDEX_FORMAT_KEY.to_vec(),
        value: vec![TTL_INDEX_FORMAT_VERSION],
    }])
}

fn unsupported_format() -> storage_types::StorageError {
    InfrastructureResultKind::UnsupportedVersion {
        context: InfrastructureContext::PhysicalLayout,
    }
    .into()
}
