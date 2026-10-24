//! Bounded GSI build discovery and private history-hold validation.

use std::num::NonZeroU32;

use storage_backfill::{GsiBuild, GsiBuildTarget};
use storage_types::{StorageError, StorageResult, storage_serde};

use super::key_layout::hold_key;
use crate::{
    KvAttempt, KvRange, KvRangePage, ReadProtection,
    constants::{GSI_RUNNABLE_FAMILY, PAGE_READ_BYTES},
};

pub(crate) async fn scan_build_candidates<A: KvAttempt>(
    attempt: &mut A,
    after: Option<&GsiBuildTarget>,
) -> StorageResult<[Option<GsiBuild<Vec<u8>>>; 2]> {
    let mut ranges = vec![candidate_range(after)?];
    if after.is_some() {
        ranges.push(candidate_range(None)?);
    }
    let pages = attempt.scan_batch(ranges, ReadProtection::Snapshot).await?;
    let expected_pages = if after.is_some() { 2 } else { 1 };
    if pages.len() != expected_pages {
        return Err(StorageError::internal(
            "KV GSI candidate batch returned the wrong page count",
        ));
    }
    let mut candidates = pages.into_iter().map(decode_candidate_page);
    let after_candidate = candidates
        .next()
        .ok_or_else(|| StorageError::internal("KV GSI candidate batch has no first page"))??;
    let first_candidate = candidates.next().transpose()?.flatten();
    Ok([after_candidate, first_candidate])
}

fn decode_candidate_page(page: KvRangePage) -> StorageResult<Option<GsiBuild<Vec<u8>>>> {
    if page.more && page.entries.is_empty() {
        return Err(StorageError::internal(
            "KV GSI candidate page cannot advance",
        ));
    }
    if page.entries.len() > 1 {
        return Err(StorageError::internal(
            "KV GSI candidate page has invalid cardinality",
        ));
    }
    let Some(entry) = page.entries.into_iter().next() else {
        return Ok(None);
    };
    decode_candidate(&entry.key, &entry.value).map(Some)
}

pub(crate) async fn read_candidate_holds<A: KvAttempt>(
    attempt: &mut A,
    build: &GsiBuild<Vec<u8>>,
) -> StorageResult<storage_provider::maintenance::GsiCandidateObservation> {
    let checkpoint = build.state.required_stream_history();
    let mut candidate_key = vec![GSI_RUNNABLE_FAMILY];
    candidate_key.extend_from_slice(build.target.incarnation.as_ref());
    let expected = checkpoint.frontier().len();
    let mut keys = Vec::with_capacity(expected + 1);
    keys.push(candidate_key);
    for position in checkpoint.frontier() {
        keys.push(hold_key(
            checkpoint,
            position,
            build.target.incarnation.as_ref(),
        )?);
    }
    let values = attempt.get_batch(keys, ReadProtection::Conflict).await?;
    if values.len() != expected + 1 {
        return Err(StorageError::internal(
            "KV GSI candidate validation returned the wrong value count",
        ));
    }
    let mut values = values.into_iter();
    let observed = values
        .next()
        .flatten()
        .map(|bytes| storage_serde::from_bytes(&bytes))
        .transpose()?;
    let mut holds = Vec::with_capacity(expected);
    for (value, position) in values.zip(checkpoint.frontier()) {
        if let Some(bytes) = value {
            if !bytes.is_empty() {
                return Err(StorageError::internal("KV GSI hold value is malformed"));
            }
            holds.push((checkpoint.scope().clone(), position.clone()));
        }
    }
    Ok(storage_provider::maintenance::GsiCandidateObservation {
        build: observed,
        holds,
    })
}

fn candidate_range(after: Option<&GsiBuildTarget>) -> StorageResult<KvRange> {
    let mut begin = vec![GSI_RUNNABLE_FAMILY];
    if let Some(after) = after {
        if after.incarnation.as_ref().len() != 6 {
            return Err(StorageError::internal(
                "KV GSI scheduler target is malformed",
            ));
        }
        begin.extend_from_slice(after.incarnation.as_ref());
        begin.push(0);
    }
    Ok(KvRange {
        entry_bounds: None,
        begin,
        end: vec![GSI_RUNNABLE_FAMILY + 1],
        reverse: false,
        limit: NonZeroU32::MIN,
        max_bytes: PAGE_READ_BYTES,
    })
}

fn decode_candidate(key: &[u8], bytes: &[u8]) -> StorageResult<GsiBuild<Vec<u8>>> {
    if key.len() != 7 || key[0] != GSI_RUNNABLE_FAMILY {
        return Err(StorageError::internal(
            "KV GSI discovery returned another key family",
        ));
    }
    let build: GsiBuild<Vec<u8>> = storage_serde::from_bytes(bytes)?;
    if build.target.incarnation.as_ref() != &key[1..] {
        return Err(StorageError::internal(
            "KV GSI build identity does not match its key",
        ));
    }
    if !build.state.permits_projection() {
        return Err(StorageError::internal(
            "KV GSI runnable index contains a blocked build",
        ));
    }
    Ok(build)
}
