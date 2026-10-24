use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde::{Serialize, de::DeserializeOwned};
use storage_provider::RawOrderedJobStoreError;

use crate::provider::implementation::RemoteStorageProvider;

pub(crate) fn remaining(deadline: Instant) -> Result<Duration, RawOrderedJobStoreError> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or(RawOrderedJobStoreError::Timeout)
}

pub(crate) async fn invoke_with_deadline<Request, Response>(
    provider: Arc<RemoteStorageProvider>,
    target: &str,
    request: &Request,
    deadline: Instant,
) -> Result<Response, RawOrderedJobStoreError>
where
    Request: Serialize + ?Sized,
    Response: DeserializeOwned,
{
    let remaining = remaining(deadline)?;
    tokio::time::timeout(remaining, provider.invoke(target, request))
        .await
        .map_err(|_| RawOrderedJobStoreError::Timeout)?
        .map_err(RawOrderedJobStoreError::Backend)
}
