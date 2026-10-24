use std::{future::Future, pin::Pin, time::Duration};

use crate::{StorageResult, TimestampMillis};

/// Time source selected when a store is constructed.
pub trait Clock: Send + Sync {
    fn now(&self) -> TimestampMillis;

    fn sleep(
        &self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(duration).await;
            Ok(())
        })
    }
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::now()
    }
}
