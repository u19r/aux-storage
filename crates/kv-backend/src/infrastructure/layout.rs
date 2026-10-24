//! Ordinary retained images and explicit record attachment share one physical
//! pool.

use std::num::{NonZeroU32, NonZeroUsize};

use infrastructure_provider::limits::InfrastructureLimits;
use storage_types::{InfrastructureContext, InfrastructureResultKind, StorageError, StorageResult};

use crate::{
    KvAttempt, KvBackend, KvMutation, KvRange, ReadProtection,
    constants::{
        INFRASTRUCTURE_FORMAT_KEY, INFRASTRUCTURE_LAYOUT_VERSION, INFRASTRUCTURE_PREFIX,
        INFRASTRUCTURE_RECORD_HEADER_BYTES,
    },
};

pub(super) fn record_capacity<B: KvBackend>(
    backend: &B,
    limits: InfrastructureLimits,
) -> StorageResult<infrastructure_provider::physical_store::PhysicalRecordCapacity> {
    let key_bytes = super::key_layout::encoded_key_length_bound(limits.max_key_bytes().get())?;
    let value_bytes = limits
        .max_record_bytes()
        .get()
        .checked_add(INFRASTRUCTURE_RECORD_HEADER_BYTES)
        .ok_or(InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::Bounds,
        })?;
    let count = limits
        .max_records()
        .get()
        .checked_add(1)
        .and_then(|count| u32::try_from(count).ok())
        .and_then(NonZeroU32::new)
        .ok_or(InfrastructureResultKind::InvalidBound {
            context: InfrastructureContext::Bounds,
        })?;
    let layout = backend.native_layout();
    layout.require_record_layout(key_bytes, value_bytes)?;
    let (_, page_bytes) = super::key_layout::lower_page_bounds(limits.max_bytes(), limits, count)?;
    layout.record_capacity(key_bytes, limits, page_bytes)
}

pub(crate) async fn initialize_layout<A: KvAttempt>(attempt: &mut A) -> StorageResult<[u8; 16]> {
    if let Some(identity) = load_identity(attempt).await? {
        return Ok(identity);
    }
    let range = KvRange::try_for_prefix(
        INFRASTRUCTURE_PREFIX.to_vec(),
        NonZeroU32::MIN,
        NonZeroUsize::MIN,
    )?;
    if attempt
        .has_key_in_range(&range.begin, &range.end, ReadProtection::Conflict)
        .await?
    {
        return Err(invalid_layout());
    }
    let identity = uuid::Uuid::new_v4().into_bytes();
    let mut bytes = vec![INFRASTRUCTURE_LAYOUT_VERSION];
    bytes.extend_from_slice(&identity);
    attempt.stage(vec![KvMutation::Put {
        key: INFRASTRUCTURE_FORMAT_KEY.to_vec(),
        value: bytes,
    }])?;
    Ok(identity)
}

async fn load_identity<A: KvAttempt>(attempt: &A) -> StorageResult<Option<[u8; 16]>> {
    let budget =
        NonZeroUsize::new(1 + std::mem::size_of::<[u8; 16]>()).ok_or_else(invalid_layout)?;
    let mut values = attempt
        .get_batch_bounded(
            vec![INFRASTRUCTURE_FORMAT_KEY.to_vec()],
            ReadProtection::Conflict,
            budget,
        )
        .await?;
    if values.len() != 1 {
        return Err(invalid_layout());
    }
    values
        .pop()
        .ok_or_else(invalid_layout)?
        .map(|bytes| decode_identity(&bytes))
        .transpose()
}

fn decode_identity(bytes: &[u8]) -> StorageResult<[u8; 16]> {
    let Some((&version, identity)) = bytes.split_first() else {
        return Err(invalid_layout());
    };
    if version != INFRASTRUCTURE_LAYOUT_VERSION {
        return Err(InfrastructureResultKind::UnsupportedVersion {
            context: InfrastructureContext::PhysicalLayout,
        }
        .into());
    }
    let identity: [u8; 16] = identity.try_into().map_err(|_| invalid_layout())?;
    if identity == [0; 16] {
        return Err(invalid_layout());
    }
    Ok(identity)
}

fn invalid_layout() -> StorageError {
    InfrastructureResultKind::Encoding {
        context: InfrastructureContext::PhysicalLayout,
    }
    .into()
}
