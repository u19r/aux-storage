//! Initialized family stores already hold the pubsub schema or key format.

use async_trait::async_trait;

use crate::{PubsubPort, PubsubResult, PubsubRuntimeRepository};

#[async_trait]
impl<P: PubsubPort> PubsubRuntimeRepository for P {
    async fn initialize_pubsub(&self) -> PubsubResult<()> {
        Ok(())
    }
}
