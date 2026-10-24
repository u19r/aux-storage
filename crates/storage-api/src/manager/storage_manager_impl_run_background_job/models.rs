use http_error::HttpApiError;
use serde::{Deserialize, Deserializer, de::Error as _};
use storage_common::{StorageJob, TtlMaintenanceCursor, TtlSweepPageInput, TtlSweepRequest};
use storage_types::{TableName, TimestampMillis};

pub(in crate::manager) struct TtlSweepPayload {
    pub(in crate::manager) request: TtlSweepRequest,
}

impl<'de> Deserialize<'de> for TtlSweepPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(rename = "JobName")]
            job_name: Option<String>,
            #[serde(rename = "TTLAttribute")]
            ttl_attribute: String,
            #[serde(rename = "ScanTime")]
            scan_time: i64,
            #[serde(rename = "Limit")]
            limit: usize,
            #[serde(rename = "Cursor")]
            cursor: Option<String>,
            #[serde(rename = "TableName")]
            table_name: Option<String>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let job_name = wire
            .job_name
            .as_deref()
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| D::Error::custom("JobName is required for TestUtil.RunBackgroundJob"))?
            .parse::<StorageJob>()
            .map_err(|error| D::Error::custom(format!("Unsupported background job: {error}")))?;
        if job_name != StorageJob::TtlSweep {
            return Err(D::Error::custom(
                "TestUtil.RunBackgroundJob requires a ttl-sweep request",
            ));
        }
        let table_name = wire
            .table_name
            .ok_or_else(|| D::Error::custom("ttl-sweep requires a non-empty TableName"))?;
        let table_name = TableName::try_from(table_name).map_err(D::Error::custom)?;
        let cursor = wire
            .cursor
            .map(TtlMaintenanceCursor::try_from)
            .transpose()
            .map_err(D::Error::custom)?;
        let input = TtlSweepPageInput::try_new(
            wire.ttl_attribute,
            TimestampMillis::from_timestamp(wire.scan_time),
            wire.limit,
            cursor,
        )
        .map_err(D::Error::custom)?;
        Ok(Self {
            request: TtlSweepRequest { table_name, input },
        })
    }
}

impl TryFrom<serde_json::Value> for TtlSweepPayload {
    type Error = HttpApiError;

    fn try_from(payload: serde_json::Value) -> Result<Self, Self::Error> {
        if !payload.is_object() {
            return Err(HttpApiError::validation_error(
                "background job payload must be a JSON object",
            ));
        }
        serde_json::from_value(payload).map_err(|error| {
            HttpApiError::validation_error(format!("invalid background job payload: {error}"))
        })
    }
}
