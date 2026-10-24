use anyhow::{Context, Result};
use regional_replication::history::{
    discovery::{CanonicalHistoryDiscoveryRequest, DiscoveredHistorySource},
    read::CanonicalHistoryRequest,
    source_selection::CanonicalSourceSelection,
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::content::{CanonicalEventCodec, CanonicalPayload},
    protocol::PayloadBounds,
};
use storage_types::TableName;

use super::{TABLE, key};
use crate::DatabaseManager;

pub async fn read_originals(
    manager: &DatabaseManager,
    serving: usize,
    origin: usize,
    key_value: &str,
) -> Result<Vec<String>> {
    let codec = CanonicalJsonCodec::new(PayloadBounds {
        event_bytes: 65536.try_into()?,
        manifest_bytes: 1048576.try_into()?,
    });
    let mut selected = Vec::new();
    for original in read_partition_originals(manager, serving, origin, key_value)
        .await
        .with_context(|| {
            format!("read fixture history serving={serving} origin={origin} key={key_value}")
        })?
    {
        let (_, _, payload) = codec.decode_event_content(original.as_bytes())?;
        if matches!(payload, CanonicalPayload::Item(item) if item.key == key(key_value)) {
            selected.push(original);
        }
    }
    Ok(selected)
}

pub async fn read_partition_originals(
    manager: &DatabaseManager,
    serving: usize,
    origin: usize,
    key_value: &str,
) -> Result<Vec<String>> {
    let codec = CanonicalJsonCodec::new(PayloadBounds {
        event_bytes: 65536.try_into()?,
        manifest_bytes: 1048576.try_into()?,
    });
    let selected = select_source(
        manager,
        SourceKey {
            serving,
            origin,
            key: key_value,
            lineage: None,
        },
        &codec,
    )
    .await
    .context("discover canonical history source")?;
    read_selected_partition(manager, selected)
        .await
        .context("read discovered canonical partition")
}

pub async fn current_lineage(
    manager: &DatabaseManager,
    serving: usize,
) -> Result<replication_types::identity::resource::TableLineage> {
    let discovery = manager
        .discover_canonical_history(CanonicalHistoryDiscoveryRequest {
            serving_region: format!("region-{serving}").parse()?,
            serving_group: [serving as u8; 16].try_into()?,
            table: TableName::new(TABLE),
            after: None,
        })
        .await?;
    Ok(discovery.lineage)
}

pub async fn read_historical_originals(
    manager: &DatabaseManager,
    regions: (usize, usize),
    key_value: &str,
    lineage: replication_types::identity::resource::TableLineage,
) -> Result<Vec<String>> {
    let codec = CanonicalJsonCodec::new(PayloadBounds {
        event_bytes: 65536.try_into()?,
        manifest_bytes: 1048576.try_into()?,
    });
    let selected = select_source(
        manager,
        SourceKey {
            serving: regions.0,
            origin: regions.1,
            key: key_value,
            lineage: Some(lineage),
        },
        &codec,
    )
    .await?;
    let mut originals = Vec::new();
    for original in read_selected_partition(manager, selected).await? {
        let (_, _, payload) = codec.decode_event_content(original.as_bytes())?;
        if matches!(payload, CanonicalPayload::Item(item) if item.key == key(key_value)) {
            originals.push(original);
        }
    }
    Ok(originals)
}

async fn read_selected_partition(
    manager: &DatabaseManager,
    selected: (
        CanonicalSourceSelection,
        replication_types::identity::origin::OriginPosition,
    ),
) -> Result<Vec<String>> {
    let (selection, through) = selected;
    let mut after = 0.into();
    let mut originals = Vec::new();
    while after < through {
        let page = manager
            .read_canonical_history(CanonicalHistoryRequest {
                selection: selection.clone(),
                range: (after, through).try_into()?,
                cut: None,
            })
            .await?;
        anyhow::ensure!(
            page.manifest.range.through() > after,
            "canonical history page did not advance"
        );
        after = page.manifest.range.through();
        originals.extend(page.originals);
    }
    Ok(originals)
}

struct SourceKey<'a> {
    serving: usize,
    origin: usize,
    key: &'a str,
    lineage: Option<replication_types::identity::resource::TableLineage>,
}

async fn select_source(
    manager: &DatabaseManager,
    requested: SourceKey<'_>,
    codec: &CanonicalJsonCodec,
) -> Result<(
    CanonicalSourceSelection,
    replication_types::identity::origin::OriginPosition,
)> {
    let SourceKey {
        serving,
        origin,
        key: key_value,
        lineage: requested_lineage,
    } = requested;
    let region: replication_types::identity::replica::RegionId =
        format!("region-{serving}").parse()?;
    let group = [serving as u8; 16].try_into()?;
    let selected_region: replication_types::identity::replica::RegionId =
        format!("region-{origin}").parse()?;
    let mut after = None;
    for _ in 0..256 {
        let discovery = manager
            .discover_canonical_history(CanonicalHistoryDiscoveryRequest {
                serving_region: region.clone(),
                serving_group: group,
                table: TableName::new(TABLE),
                after,
            })
            .await?;
        let selected = match discovery.source {
            Some(
                DiscoveredHistorySource::Current(source) | DiscoveredHistorySource::Retired(source),
            ) if requested_lineage.is_none() => {
                Some((discovery.lineage, discovery.membership, source))
            }
            Some(DiscoveredHistorySource::Historical {
                lineage,
                membership,
                source,
            }) if requested_lineage == Some(lineage) => Some((lineage, membership, source)),
            _ => None,
        };
        if let Some((lineage, membership, source)) = selected
            && source.region == selected_region
        {
            let partition = source
                .partitions
                .select(lineage, &codec.encode_key(&key(key_value))?);
            if let Some(frontier) = discovery
                .available
                .into_iter()
                .find(|frontier| frontier.origin.journal_partition == partition)
            {
                return Ok((
                    CanonicalSourceSelection {
                        serving_region: region,
                        serving_group: group,
                        table: TableName::new(TABLE),
                        lineage,
                        membership,
                        source,
                        origin: frontier.origin,
                    },
                    frontier.through,
                ));
            }
        }
        after = Some(
            discovery
                .continuation
                .context("requested canonical source was not discovered")?,
        );
    }
    anyhow::bail!("canonical source discovery did not finish within the fixture inventory")
}
