//! Refuse the superseded unsigned TTL layout before publishing a KV store.

use std::num::{NonZeroU32, NonZeroUsize};

use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageResult};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection,
    constants::{TTL_INDEX_FAMILY, TTL_INDEX_FORMAT_KEY, TTL_INDEX_FORMAT_VERSION},
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
    let range =
        KvRange::try_for_prefix(vec![TTL_INDEX_FAMILY], NonZeroU32::MIN, NonZeroUsize::MIN)?;
    let page = attempt.scan(range, ReadProtection::Conflict).await?;
    if !page.entries.is_empty() || page.more {
        return Err(unsupported_format());
    }
    attempt.stage(vec![KvMutation::Put {
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
