use std::time::Duration;

use anyhow::{Context, Result};
use regional_replication::history::{
    read::CanonicalHistoryRequest, source_selection::CanonicalSourceSelection,
};
use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    configuration::installed::RegionalSourceBinding,
    event::{
        content::{CanonicalEventCodec, CanonicalPayload},
        interval::SourceIntervalManifest,
    },
    identity::origin::{OriginId, OriginPosition},
    protocol::PayloadBounds,
};
use serde_json::{Value, json};
use storage_types::{AttributeValue, KeyAttributes, TableName};

use super::{TABLE, request::HttpRegion};

impl HttpRegion {
    pub(super) async fn originals(&self, origin: usize, key: &str) -> Result<Vec<String>> {
        let codec = CanonicalJsonCodec::new(PayloadBounds {
            event_bytes: 65536.try_into()?,
            manifest_bytes: 1048576.try_into()?,
        });
        let (selection, through) = self.select_source(origin, key, &codec).await?;
        let mut after = 0.into();
        let mut originals = Vec::new();
        while after < through {
            let response = self
                .request(
                    "GetCanonicalHistory",
                    serde_json::to_value(CanonicalHistoryRequest {
                        selection: selection.clone(),
                        range: (after, through).try_into()?,
                        cut: None,
                    })?,
                )
                .await?;
            let manifest: SourceIntervalManifest =
                serde_json::from_value(response["manifest"].clone())?;
            anyhow::ensure!(
                manifest.range.through() > after,
                "public history page did not advance"
            );
            after = manifest.range.through();
            let page: Vec<String> = serde_json::from_value(response["originals"].clone())?;
            for original in page {
                let (_, _, payload) = codec.decode_event_content(original.as_bytes())?;
                if matches!(payload, CanonicalPayload::Item(item) if item.key == key_attributes(key))
                {
                    originals.push(original);
                }
            }
        }
        Ok(originals)
    }

    async fn select_source(
        &self,
        origin: usize,
        key: &str,
        codec: &CanonicalJsonCodec,
    ) -> Result<(CanonicalSourceSelection, OriginPosition)> {
        let mut after = Value::Null;
        for _ in 0..130 {
            let discovery = self.request("GetCanonicalHistory", json!({"serving_region": format!("region-{}", self.region), "serving_group": vec![self.region as u8; 16], "table": TABLE, "after": after})).await?;
            if discovery["source"]["state"] == "Current"
                && discovery["source"]["binding"]["region"] == format!("region-{origin}")
            {
                let source: RegionalSourceBinding =
                    serde_json::from_value(discovery["source"]["binding"].clone())?;
                let lineage = serde_json::from_value(discovery["lineage"].clone())?;
                let partition = source
                    .partitions
                    .select(lineage, &codec.encode_key(&key_attributes(key))?);
                for frontier in discovery["available"]
                    .as_array()
                    .context("history available frontiers")?
                {
                    let observed: OriginId = serde_json::from_value(frontier["origin"].clone())?;
                    if observed.journal_partition == partition {
                        return Ok((
                            CanonicalSourceSelection {
                                serving_region: format!("region-{}", self.region).parse()?,
                                serving_group: [self.region as u8; 16].try_into()?,
                                table: TableName::new(TABLE),
                                lineage,
                                membership: serde_json::from_value(
                                    discovery["membership"].clone(),
                                )?,
                                source,
                                origin: observed,
                            },
                            serde_json::from_value(frontier["through"].clone())?,
                        ));
                    }
                }
            }
            after = discovery["continuation"].clone();
            anyhow::ensure!(!after.is_null(), "public history source was not discovered");
        }
        anyhow::bail!("public history source exceeded its fixture inventory")
    }

    pub(super) async fn await_imported_item(
        &self,
        origin: usize,
        key: &str,
        value: &str,
        expected_originals: &[String],
    ) -> Result<()> {
        let expected = super::request::item(key, value);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut observation = None;
        let result = tokio::time::timeout_at(deadline, async {
            loop {
                let (primary, originals) = tokio::join!(
                    self.response("GetItem", json!({"TableName": TABLE, "Key": {"pk": {"S": key}}, "ConsistentRead": true})),
                    self.originals(origin, key),
                );
                // Read the index after the primary to preserve the atomic-image assertion.
                let index = self.query(value).await;
                observation = Some(format!("primary={primary:?}; originals={originals:?}; index={index:?}"));
                let (status, response) = primary?;
                anyhow::ensure!(status.is_success() || response["__type"].as_str().is_some_and(|kind| kind.ends_with("ResourceNotFoundException")),
                    "unexpected public read failure: {status}, {response}");
                if status.is_success() && response["Item"] == expected {
                    anyhow::ensure!(index? == vec![expected.clone()], "acknowledged primary was visible without its exact GSI image");
                    if originals.as_ref().is_ok_and(|observed| observed == expected_originals) {
                        return Ok::<(), anyhow::Error>(());
                    }
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }).await;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(error) => Err(error.into()),
        };
        outcome.with_context(|| {
            format!(
                "public HTTP region {} import/primary/index observation for {key}: {observation:?}",
                self.region
            )
        })
    }

    pub(super) async fn await_originals(
        &self,
        origin: usize,
        key: &str,
        expected: &[String],
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut observed = Vec::new();
        while tokio::time::Instant::now() < deadline {
            observed = self.originals(origin, key).await?;
            if observed == expected {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!(
            "public HTTP originals differed: serving={}, origin={origin}, expected={}, observed={}",
            self.region,
            expected.len(),
            observed.len()
        )
    }
}

fn key_attributes(value: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".into(), AttributeValue::S(value.into()))])
}
