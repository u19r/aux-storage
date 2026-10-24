pub mod archive;
pub mod history;
pub mod operator;
pub mod provider;
pub mod proxy;

use std::{net::TcpListener, time::Duration};

use anyhow::{Context, Result};
use operator::features;
use proxy::RegionalProxy;
use serde_json::json;
use storage_types::{
    AttributeMap, AttributeValue, CreateTableRequest, KeyAttributes, TableName, WireItem,
};

use crate::{DatabaseManager, DatabaseManagerRuntimeOptions, PutItemInput};

pub const TABLE: &str = "regional_startup_slice";

pub struct RegionalFixture {
    pub directory: tempfile::TempDir,
    pub features: [config::Features; 2],
    pub managers: Vec<DatabaseManager>,
    pub proxy: Option<RegionalProxy>,
    pub provider: provider::ProviderFixture,
    pub runtime_options: DatabaseManagerRuntimeOptions,
}

impl RegionalFixture {
    pub async fn start(declaration_bytes: usize) -> Result<Self> {
        Self::start_with_limits(declaration_bytes, 128, None).await
    }

    pub async fn start_with_limits(
        declaration_bytes: usize,
        frame_events: usize,
        latency: Option<u64>,
    ) -> Result<Self> {
        Self::start_with_options(
            (declaration_bytes, frame_events, latency),
            DatabaseManagerRuntimeOptions::default(),
            false,
        )
        .await
    }

    pub async fn start_with_options(
        limits: (usize, usize, Option<u64>),
        runtime_options: DatabaseManagerRuntimeOptions,
        archive: bool,
    ) -> Result<Self> {
        let (declaration_bytes, frame_events, latency) = limits;
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                "regional_replication=debug,storage=debug,scheduled_bg_jobs=debug,\
                 bg_job_store=debug",
            )
            .with_test_writer()
            .try_init();
        let directory = tempfile::tempdir()?;
        let listeners = [
            TcpListener::bind("127.0.0.1:0")?,
            TcpListener::bind("127.0.0.1:0")?,
        ];
        let endpoints = [listeners[0].local_addr()?, listeners[1].local_addr()?];
        let proxy = if let Some(latency) = latency {
            Some(RegionalProxy::start(endpoints[1], latency).await?)
        } else {
            None
        };
        let source_endpoints = [
            endpoints[0],
            proxy.as_ref().map_or(endpoints[1], |proxy| proxy.endpoint),
        ];
        let provider = provider::ProviderFixture::start(directory.path()).await?;
        let prepared = (|| {
            let features = [
                features(
                    directory.path(),
                    1,
                    source_endpoints,
                    declaration_bytes,
                    provider.backends[0].clone(),
                )?,
                features(
                    directory.path(),
                    2,
                    endpoints,
                    declaration_bytes,
                    provider.backends[1].clone(),
                )?,
            ];
            for (index, region) in features.iter().enumerate() {
                let path = &region.regional_connections["default"];
                let mut startup: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
                startup["configuration"]["limits"]["frame_events"] = json!(frame_events);
                startup["configuration"]["limits"]["import_events"] = json!(frame_events);
                if archive {
                    let root = directory.path().join(format!("archive-{}", index + 1));
                    std::fs::create_dir(&root)?;
                    startup["configuration"]["archive_root"] = json!(root);
                }
                std::fs::write(path, serde_json::to_vec(&startup)?)?;
            }
            Ok::<_, anyhow::Error>(features)
        })();
        let features = match prepared {
            Ok(features) => features,
            Err(error) => {
                provider.finish().await?;
                return Err(error);
            }
        };
        drop(listeners);
        let mut fixture = Self {
            directory,
            features,
            managers: Vec::new(),
            proxy,
            provider,
            runtime_options,
        };
        if let Err(error) = fixture.open_regions().await {
            let cleanup = fixture.stop().await;
            return Err(match cleanup {
                Ok(()) => error,
                Err(cleanup) => error.context(format!("fixture cleanup also failed: {cleanup}")),
            });
        }
        Ok(fixture)
    }

    pub async fn create_global_table(&self) -> Result<()> {
        let request: CreateTableRequest = serde_json::from_value(json!({
            "TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
        }))?;
        self.create_global_table_with_schema(request).await
    }

    pub async fn create_global_table_with_schema(&self, request: CreateTableRequest) -> Result<()> {
        self.managers[0].create_table(&request).await?;
        put(&self.managers[0], "baseline", "before-enable").await?;
        self.managers[0]
            .update_table(serde_json::from_value(json!({
                "TableName": TABLE, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]
            }))?)
            .await?;
        let result = await_item_for(
            &self.managers[1],
            "baseline",
            "before-enable",
            Duration::from_secs(120),
        )
        .await;
        if result.is_err() {
            dump_jobs(&self.managers[0]).await?;
        }
        result
    }

    async fn open_regions(&mut self) -> Result<()> {
        for features in &self.features {
            self.managers.push(
                crate::startup::database_manager_from_features(
                    features,
                    self.runtime_options.clone(),
                )
                .await
                .context("regional fixture startup")?,
            );
        }
        Ok(())
    }

    pub async fn restart(&mut self) -> Result<()> {
        for manager in &self.managers {
            manager.shutdown_background_services().await?;
        }
        drop(std::mem::take(&mut self.managers));
        self.open_regions().await
    }

    pub async fn stop(self) -> Result<()> {
        let mut result = Ok(());
        for manager in &self.managers {
            result = result.and(
                manager
                    .shutdown_background_services()
                    .await
                    .map_err(Into::into),
            );
        }
        drop(self.managers);
        result = result.and(self.provider.finish().await);
        drop(self.directory);
        result
    }
}

pub async fn open(features: &config::Features) -> Result<DatabaseManager> {
    Ok(crate::startup::database_manager_from_features(
        features,
        DatabaseManagerRuntimeOptions::default(),
    )
    .await?)
}

pub fn item(key: &str, value: &str) -> WireItem {
    WireItem::try_from(
        &[
            ("pk".into(), AttributeValue::S(key.into())),
            ("value".into(), AttributeValue::S(value.into())),
        ]
        .into_iter()
        .collect::<AttributeMap>(),
    )
    .unwrap()
}

pub fn key(value: &str) -> KeyAttributes {
    KeyAttributes::from([("pk".into(), AttributeValue::S(value.into()))])
}

pub async fn put(manager: &DatabaseManager, key: &str, value: &str) -> Result<()> {
    manager
        .put_item(
            PutItemInput::builder()
                .table_name(TableName::new(TABLE))
                .item(item(key, value))
                .build(),
        )
        .await
        .map_err(|error| anyhow::anyhow!("public put {key}: {value}: {error:?}"))?;
    Ok(())
}

pub async fn await_item(manager: &DatabaseManager, key_value: &str, value: &str) -> Result<()> {
    await_item_for(manager, key_value, value, Duration::from_secs(30)).await
}

pub async fn await_item_for(
    manager: &DatabaseManager,
    key_value: &str,
    value: &str,
    allowance: Duration,
) -> Result<()> {
    let expected = item(key_value, value).into_attribute_map()?;
    let deadline = tokio::time::Instant::now() + allowance;
    let mut last = String::new();
    while tokio::time::Instant::now() < deadline {
        match manager
            .get_item(TableName::new(TABLE), key(key_value))
            .await
        {
            Ok(Some(item)) => {
                let observed = item.into_attribute_map()?;
                if observed == expected {
                    return Ok(());
                }
                last = format!("{observed:?}");
            }
            observation => last = format!("{observation:?}"),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    dump_jobs(manager).await?;
    anyhow::bail!("remote public read did not converge: {last}")
}

pub async fn dump_jobs(manager: &DatabaseManager) -> Result<()> {
    if let Some(store) = manager.scheduled_job_store() {
        let capabilities = store.capabilities();
        let request = scheduled_bg_jobs::store::RangeRequest::try_from(
            scheduled_bg_jobs::store::RangeRequestInput {
                start: None,
                end: None,
                direction: scheduled_bg_jobs::store::RangeDirection::Forward,
                item_limit: capabilities.max_range_items(),
                byte_limit: capabilities.max_range_bytes(),
                continuation: None,
            },
        )?;
        for entry in store.read_range(request).await?.entries {
            if let Ok(record) =
                scheduled_bg_jobs::test_support::codec::decode_job_record(&entry.value)
            {
                eprintln!(
                    "job {}: {:?}, attempts {:?}",
                    record.kind.as_str(),
                    record.state,
                    record.attempts
                );
            }
        }
    }
    Ok(())
}

// A preceding control can expose installed metadata before its final
// acknowledgements settle. This exact busy rejection occurs before acceptance;
// unknown outcomes are returned unchanged.
pub async fn accept_control<T, F, Fut>(mut execute: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = storage_types::StorageResult<T>>,
{
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match execute().await {
            Err(error)
                if matches!(error.as_ref(), storage_types::StorageEnum::Validation { message, .. } if message == "table configuration control is in progress")
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await
            }
            result => return result.map_err(Into::into),
        }
    }
}
