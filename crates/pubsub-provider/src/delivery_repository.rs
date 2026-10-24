use async_trait::async_trait;

use crate::{
    ClaimDeliveryRecordsRequest, ClaimDeliveryRecordsResponse, DeliveryRecord, DeliveryRecordId,
    DeliveryRecordUpdate, PubsubResult,
};

#[async_trait]
pub trait PubsubDeliveryRepository: Send + Sync {
    async fn put_delivery_record(&self, record: DeliveryRecord) -> PubsubResult<()>;

    async fn put_delivery_records(&self, records: Vec<DeliveryRecord>) -> PubsubResult<()> {
        for record in records {
            self.put_delivery_record(record).await?;
        }
        Ok(())
    }

    async fn claim_delivery_records(
        &self,
        request: ClaimDeliveryRecordsRequest,
    ) -> PubsubResult<ClaimDeliveryRecordsResponse>;

    async fn update_delivery_record(&self, update: DeliveryRecordUpdate) -> PubsubResult<()>;

    async fn get_delivery_record(
        &self,
        record_id: &DeliveryRecordId,
    ) -> PubsubResult<Option<DeliveryRecord>>;
}
