use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::sync::oneshot;

use super::{
    PointReadClient, PointReadPlan, PointReadReservation, constants, executor, generation, source,
};
use crate::types::{
    BatchGetItemRequest, BatchGetItemResponse, KeyAttributes, KeyOwned, KeysAndAttributes,
    StorageResult, TableName, VecMap, WireItem,
};

fn response(
    table: &TableName,
    hits: &[KeyAttributes],
    pending: &[KeyAttributes],
) -> BatchGetItemResponse {
    BatchGetItemResponse {
        responses: Some(VecMap::from_iter([(
            table.clone(),
            hits.iter()
                .map(|key| {
                    WireItem::try_from(
                        &key.iter()
                            .map(|(name, value)| (name.to_owned(), value.clone()))
                            .collect::<HashMap<_, _>>(),
                    )
                    .unwrap()
                })
                .collect(),
        )])),
        unprocessed_keys: Some(VecMap::from_iter([(
            table.clone(),
            KeysAndAttributes {
                keys: pending.to_vec(),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
                consistent_read: Some(true),
            },
        )])),
        consumed_capacity: None,
    }
}

struct LateUnprocessedDb(Arc<std::sync::atomic::AtomicUsize>);

impl PointReadClient for LateUnprocessedDb {
    fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
        _reservation: PointReadReservation,
    ) -> Pin<Box<dyn Future<Output = StorageResult<BatchGetItemResponse>> + Send + '_>> {
        let attempt = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async move {
            if attempt == 0 {
                std::thread::sleep(
                    constants::POINT_READ_SOURCE_TIMEOUT + Duration::from_millis(20),
                );
            }
            let (table, attributes) = request.request_items.into_iter().next().unwrap();
            Ok(response(&table, &[], &attributes.keys))
        })
    }
}

#[tokio::test]
async fn given_late_unprocessed_response_when_executor_resumes_then_no_retry_is_issued() {
    // Exercise the executor directly so the outer batch-cancellation select
    // cannot mask a post-deadline dispatch bug by winning its polling race.
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let db = LateUnprocessedDb(Arc::clone(&calls));
    let table = TableName::new("late_retry");
    let key = KeyOwned::pk_sk("late_retry", "META").into();
    let mut collector = PointReadPlan::default();
    collector.add_item(table, key).unwrap();
    let request = Arc::clone(&collector.into_parts().0[0]);
    let (sender, receiver) = oneshot::channel();
    let deadline = std::time::Instant::now() + constants::POINT_READ_SOURCE_TIMEOUT;
    let identities = generation::SourceGenerations::default();
    let validity = identities.capture(Arc::clone(&request), deadline).unwrap();
    let read = identities.start_read(validity).unwrap();
    let source = source::SourceKey::new(request, sender, read, None);
    let budget = Arc::new(tokio::sync::Semaphore::new(
        constants::POINT_READ_RETAINED_BYTES,
    ));
    let queue = lru_ttl_cache::RefreshQueue::new(Default::default());
    let work = lru_ttl_cache::RefreshWork::new(
        std::time::Instant::now() + constants::POINT_READ_SOURCE_TIMEOUT,
        source.request.charge,
        lru_ttl_cache::RefreshDemand::Cold,
    );
    assert_eq!(
        queue.try_schedule(lru_ttl_cache::RefreshTicket::new(
            source.clone(),
            source.read.id(),
            work,
        )),
        lru_ttl_cache::RefreshAdmission::Queued
    );
    let reservation =
        PointReadReservation::new(Arc::new(queue.next_batch(Duration::ZERO).await.unwrap()));
    executor::BatchExecutor::new(&db, &budget, reservation)
        .execute(&[source])
        .await;
    let error = receiver.await.unwrap().unwrap_err();
    assert!(matches!(
        error.into_enum(),
        crate::types::StorageEnum::ServiceUnavailable { .. }
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}
