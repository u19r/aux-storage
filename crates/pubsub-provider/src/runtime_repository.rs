use async_trait::async_trait;

use crate::PubsubResult;

#[async_trait]
pub trait PubsubRuntimeRepository: Send + Sync {
    fn take_admission_pressure_signal(&self) -> bool {
        false
    }

    async fn initialize_pubsub(&self) -> PubsubResult<()>;
}
