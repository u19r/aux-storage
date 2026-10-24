use std::{num::NonZeroUsize, sync::Arc};

use replication_contract_tests::HarnessResult;
use replication_provider::{
    CANONICAL_EVENT_SEMANTIC_VERSION, canonical_event_codec::CanonicalJsonCodec,
    regional_application::import_admission::BoundedImport,
    runtime_configuration::limits::WorkLimits,
};
use replication_types::{
    configuration::membership::RegionalTableBinding,
    error::ReplicationProtocolError,
    event::{
        content::{CanonicalEvent, CanonicalEventCandidate, CanonicalEventCodec, CanonicalPayload},
        identity::OriginEventId,
        interval::{CanonicalEventLink, IntervalCodec, PositionCoverage, SourceIntervalManifest},
        order::HybridLogicalClock,
    },
    identity::{origin::OriginId, resource::TableIdentity},
    protocol::{CanonicalBytes, CanonicalDigest, PayloadBounds},
    regional_import::RegionalImport,
};

#[test]
fn given_multi_table_originals_when_selecting_table_and_fragment_then_full_manifest_and_exact_originals_remain_bound()
-> HarnessResult<()> {
    let import = import()?;
    let selected = import.select_table(import.bindings()[0].source_table)?;
    assert_eq!(2, import.events().len());
    assert_eq!(1, selected.events().len());
    assert_eq!(import.manifest(), selected.manifest());
    assert_eq!(
        import.events()[0].as_ref().bytes,
        selected.events()[0].as_ref().bytes
    );
    assert_eq!(
        import.table_page_id(&import.bindings()[0])?,
        selected.table_page_id(&selected.bindings()[0])?
    );
    let fragment = import.select_fragment((0, 1).try_into()?)?;
    assert_eq!(import.manifest(), fragment.manifest());
    assert_eq!(1, fragment.events().len());
    assert_eq!(
        import.events()[0].as_ref().digest,
        fragment.events()[0].as_ref().digest
    );
    assert_ne!(
        import.table_page_id(&import.bindings()[0])?,
        fragment.table_page_id(&fragment.bindings()[0])?
    );
    let declared: Vec<_> = fragment
        .fragment_selection()?
        .expect("complete table declaration")
        .into();
    assert_eq!(2, declared.len());
    BoundedImport::try_new(Arc::new(selected), &limits())?;
    BoundedImport::try_new(Arc::new(fragment), &limits())?;
    Ok(())
}

#[test]
fn given_authenticated_manifest_when_bindings_links_or_originals_change_then_import_is_rejected_before_admission()
-> HarnessResult<()> {
    let import = import()?;
    let bindings = import.bindings().to_vec();
    let manifest = import.manifest().clone();
    let events = import.events().to_vec();
    let mut foreign = bindings.clone();
    foreign[0].source_region = "us-east-2".parse()?;
    let mut lineage = bindings.clone();
    lineage[0].lineage = [9; 16].try_into()?;
    let mut reversed = bindings.clone();
    reversed.reverse();
    let mut duplicate = bindings.clone();
    duplicate.insert(0, duplicate[0].clone());
    for invalid in [vec![], foreign, lineage, reversed, duplicate] {
        assert!(
            RegionalImport::try_new((invalid, manifest.clone(), events.clone()), &codec()).is_err()
        );
    }
    assert!(
        RegionalImport::try_new((bindings.clone(), manifest.clone(), vec![]), &codec()).is_err()
    );
    let mut changed = manifest.clone();
    changed.links[0].digest = CanonicalDigest::of(b"changed original");
    assert!(matches!(
        RegionalImport::try_new((bindings.clone(), changed, events.clone()), &codec()),
        Err(ReplicationProtocolError::IdentityContentMismatch { .. })
    ));
    assert!(import.select_table([9; 16].try_into()?).is_err());
    assert!(import.select_fragment((0, 3).try_into()?).is_err());
    let fragment = import.select_fragment((0, 1).try_into()?)?;
    assert!(fragment.select_fragment((0, 1).try_into()?).is_err());
    assert!(fragment.select_table(bindings[0].source_table).is_err());
    Ok(())
}

#[test]
fn given_valid_originals_when_byte_event_or_declaration_budget_is_exhausted_then_admission_fails_and_exact_boundary_succeeds()
-> HarnessResult<()> {
    let import = Arc::new(import()?);
    let mut byte_limits = limits();
    byte_limits.import_bytes = NonZeroUsize::new(1).expect("one byte");
    assert!(BoundedImport::try_new(Arc::clone(&import), &byte_limits).is_err());
    let mut event_limits = limits();
    event_limits.import_events = NonZeroUsize::new(1).expect("one event");
    assert!(BoundedImport::try_new(Arc::clone(&import), &event_limits).is_err());
    let selected = Arc::new(import.select_table(import.bindings()[0].source_table)?);
    assert!(BoundedImport::try_new(Arc::clone(&selected), &event_limits).is_ok());
    event_limits.frame_events = NonZeroUsize::new(1).expect("one source link");
    assert!(BoundedImport::try_new(selected, &event_limits).is_err());
    let size = import
        .events()
        .iter()
        .map(|event| event.as_ref().bytes.as_ref().len())
        .sum();
    byte_limits.import_bytes = NonZeroUsize::new(size).expect("exact originals");
    let accepted = BoundedImport::try_new(Arc::clone(&import), &byte_limits)?;
    assert_eq!(import.manifest(), accepted.as_ref().manifest());
    byte_limits.import_bytes = NonZeroUsize::new(size - 1).expect("below boundary");
    assert!(BoundedImport::try_new(import, &byte_limits).is_err());
    Ok(())
}

fn import() -> HarnessResult<RegionalImport> {
    let origin = OriginId {
        region: "us-east-1".parse()?,
        database_group: [4; 16].try_into()?,
        journal_partition: 0.into(),
        journal_generation: 1.into(),
    };
    let first = event(origin.clone(), 1, [1; 16].try_into()?)?;
    let second = event(origin.clone(), 2, [2; 16].try_into()?)?;
    let mut manifest = SourceIntervalManifest {
        origin: origin.clone(),
        membership: 1.into(),
        range: (0.into(), 2.into()).try_into()?,
        links: vec![link(&first, 1), link(&second, 2)],
        coverage: (1..=2)
            .map(|position| PositionCoverage::Selected {
                position: position.into(),
                ordinals: vec![0.into()],
            })
            .collect(),
        digest: CanonicalDigest::of(b"unsealed"),
    };
    manifest.digest = CanonicalDigest::of(codec().encode_interval(&manifest)?.as_ref());
    let bindings = [&first, &second]
        .into_iter()
        .map(|event| RegionalTableBinding {
            source_region: origin.region.clone(),
            source_table: event.source_table(),
            local_table: event.source_table(),
            lineage: event.lineage(),
            membership: 1.into(),
            local_config: 1.into(),
        })
        .collect();
    Ok(RegionalImport::try_new(
        (bindings, manifest, vec![first, second]),
        &codec(),
    )?)
}

fn event(origin: OriginId, position: u64, table: TableIdentity) -> HarnessResult<CanonicalEvent> {
    let lineage = [3; 16].try_into()?;
    let id = OriginEventId {
        lineage,
        origin,
        position: position.into(),
        ordinal: 0.into(),
    }
    .into();
    let payload = CanonicalPayload::TableStream {
        table,
        lineage,
        bytes: CanonicalBytes::try_new(Arc::from(&b"original stream"[..]), bound())?,
    };
    let bytes = codec().encode_event(CANONICAL_EVENT_SEMANTIC_VERSION.into(), &id, &payload)?;
    Ok(CanonicalEventCandidate::try_decode(
        bytes.clone(),
        CanonicalDigest::of(bytes.as_ref()),
        &codec(),
        bounds(),
    )?)
}

fn link(event: &CanonicalEvent, position: u64) -> CanonicalEventLink {
    CanonicalEventLink {
        position: position.into(),
        ordinal: 0.into(),
        publication_clock: HybridLogicalClock {
            physical_ms: position,
            logical: 0,
        },
        table: event.source_table(),
        lineage: event.lineage(),
        event: event.as_ref().id.clone(),
        digest: event.as_ref().digest,
    }
}

fn codec() -> CanonicalJsonCodec {
    CanonicalJsonCodec::new(bounds())
}
fn bounds() -> PayloadBounds {
    PayloadBounds {
        event_bytes: bound(),
        manifest_bytes: bound(),
    }
}
fn bound() -> NonZeroUsize {
    NonZeroUsize::new(1024 * 1024).expect("bound")
}
fn limits() -> WorkLimits {
    WorkLimits {
        frame_bytes: bound(),
        frame_events: bound(),
        import_bytes: bound(),
        import_events: bound(),
        apply_window: bound(),
        apply_concurrency: bound(),
        queue_bytes: bound(),
    }
}
