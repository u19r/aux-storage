use async_trait::async_trait;

use crate::{
    AcceptedPublication, ClaimedPublishChunk, PublishAcceptanceInput, PublishChunkClaimRequest,
    PublishChunkCommit, PubsubResult, SubscriptionSnapshot,
};

#[async_trait]
pub trait PubsubPublicationRepository: Send + Sync {
    async fn persist_publish(
        &self,
        snapshot: SubscriptionSnapshot,
        acceptance: PublishAcceptanceInput,
    ) -> PubsubResult<()> {
        self.persist_accepted_publication(AcceptedPublication::new(snapshot, acceptance))
            .await
    }

    async fn persist_accepted_publication(
        &self,
        publication: AcceptedPublication,
    ) -> PubsubResult<()>;

    async fn claim_publish_chunks(
        &self,
        request: PublishChunkClaimRequest,
    ) -> PubsubResult<Vec<ClaimedPublishChunk>>;

    async fn commit_publish_chunk(&self, commit: PublishChunkCommit) -> PubsubResult<()>;
}
