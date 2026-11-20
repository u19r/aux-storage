//! One regional startup file per native connection: exactly two regions.

use std::{net::SocketAddr, num::NonZeroU16, path::Path, time::Duration};

use serde::Deserialize;
use storage_types::{RegionId, StorageError, StorageResult};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegionalFile {
    local_region: String,
    listen: SocketAddr,
    regions: Vec<RegionEntry>,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_frame_limit_bytes")]
    frame_limit_bytes: usize,
    #[serde(default = "default_page_byte_limit")]
    page_byte_limit: usize,
    #[serde(default = "default_page_entry_limit")]
    page_entry_limit: usize,
    #[serde(default = "default_max_concurrent_requests")]
    max_concurrent_requests: usize,
    #[serde(default = "default_send_interval_ms")]
    send_interval_ms: u64,
    #[serde(default = "default_lag_alert_ms")]
    lag_alert_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegionEntry {
    name: String,
    rank: u16,
    /// `host:port` of the region's regional listener (a load balancer over its
    /// nodes).
    address: String,
}

fn default_request_timeout_ms() -> u64 {
    30_000
}
fn default_frame_limit_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_page_byte_limit() -> usize {
    1024 * 1024
}
fn default_page_entry_limit() -> usize {
    1_000
}
fn default_max_concurrent_requests() -> usize {
    16
}
fn default_send_interval_ms() -> u64 {
    250
}
fn default_lag_alert_ms() -> u64 {
    300_000
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionName {
    pub name: String,
    pub id: RegionId,
    pub address: String,
}

#[derive(Clone, Debug)]
pub struct RegionalConfig {
    pub local: RegionName,
    pub peer: RegionName,
    pub listen: SocketAddr,
    pub request_timeout: Duration,
    pub frame_limit_bytes: usize,
    /// Raw JSON budget of one page or scan page (below `frame_limit_bytes`).
    pub page_byte_limit: usize,
    pub page_entry_limit: usize,
    pub max_concurrent_requests: usize,
    pub send_interval: Duration,
    pub lag_alert: Duration,
}

impl RegionalConfig {
    pub fn load(path: &Path) -> StorageResult<Self> {
        let bytes = std::fs::read(path).map_err(|error| {
            StorageError::validation(format!(
                "cannot read regional file {}: {error}",
                path.display()
            ))
        })?;
        let file: RegionalFile = serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::validation(format!("invalid regional file {}: {error}", path.display()))
        })?;
        Self::try_from_file(file)
    }

    pub fn from_json(json: &str) -> StorageResult<Self> {
        let file: RegionalFile = serde_json::from_str(json)
            .map_err(|error| StorageError::validation(format!("invalid regional file: {error}")))?;
        Self::try_from_file(file)
    }

    fn try_from_file(file: RegionalFile) -> StorageResult<Self> {
        let invalid = |message: &str| StorageError::validation(format!("regional file: {message}"));
        let [first, second]: [RegionEntry; 2] = file
            .regions
            .try_into()
            .map_err(|_| invalid("exactly two regions are required"))?;
        let entry = |entry: RegionEntry| -> StorageResult<RegionName> {
            let rank =
                NonZeroU16::new(entry.rank).ok_or_else(|| invalid("rank must be above zero"))?;
            if entry.name.trim().is_empty() || entry.address.trim().is_empty() {
                return Err(invalid("region name and address must not be empty"));
            }
            Ok(RegionName {
                name: entry.name,
                id: RegionId(rank),
                address: entry.address,
            })
        };
        let (first, second) = (entry(first)?, entry(second)?);
        if first.name == second.name || first.id == second.id {
            return Err(invalid("region names and ranks must differ"));
        }
        let (local, peer) = if first.name == file.local_region {
            (first, second)
        } else if second.name == file.local_region {
            (second, first)
        } else {
            return Err(invalid("local_region is not one of the regions"));
        };
        if file.page_byte_limit == 0
            || file.page_byte_limit >= file.frame_limit_bytes
            || file.page_entry_limit == 0
            || file.max_concurrent_requests == 0
            || file.request_timeout_ms == 0
            || file.send_interval_ms == 0
        {
            return Err(invalid(
                "limits must be above zero and the page below the frame limit",
            ));
        }
        Ok(Self {
            local,
            peer,
            listen: file.listen,
            request_timeout: Duration::from_millis(file.request_timeout_ms),
            frame_limit_bytes: file.frame_limit_bytes,
            page_byte_limit: file.page_byte_limit,
            page_entry_limit: file.page_entry_limit,
            max_concurrent_requests: file.max_concurrent_requests,
            send_interval: Duration::from_millis(file.send_interval_ms),
            lag_alert: Duration::from_millis(file.lag_alert_ms),
        })
    }

    /// The configured region with this DynamoDB `RegionName`.
    #[must_use]
    pub fn region_named(&self, name: &str) -> Option<&RegionName> {
        [&self.local, &self.peer]
            .into_iter()
            .find(|region| region.name == name)
    }

    #[must_use]
    pub fn region(&self, id: RegionId) -> Option<&RegionName> {
        [&self.local, &self.peer]
            .into_iter()
            .find(|region| region.id == id)
    }
}
