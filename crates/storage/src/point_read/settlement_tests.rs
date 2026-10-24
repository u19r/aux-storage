use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use tokio::sync::{mpsc, oneshot};

use super::{PointReadClient, PointReadPlan, PointReadReservation, PointReadSource};
use crate::{
    admission::{AdmissionClass, AdmissionConfig, AdmissionController},
    types::{BatchGetItemRequest, BatchGetItemResponse, KeyOwned, StorageResult, TableName},
};

struct SettlingClient {
    admission: AdmissionController,
    started: mpsc::UnboundedSender<oneshot::Sender<()>>,
}

impl PointReadClient for SettlingClient {
    fn batch_get_item(
        &self,
        _request: BatchGetItemRequest,
        reservation: PointReadReservation,
    ) -> Pin<Box<dyn Future<Output = StorageResult<BatchGetItemResponse>> + Send + '_>> {
        Box::pin(async move {
            let started = self.started.clone();
            self.admission
                .settle_foreground(AdmissionClass::RangeRead, || async move {
                    let (release, wait) = oneshot::channel();
                    started.send(release).unwrap();
                    wait.await.unwrap();
                    Ok((
                        BatchGetItemResponse {
                            responses: None,
                            unprocessed_keys: None,
                            consumed_capacity: None,
                        },
                        reservation,
                    ))
                })
                .await
                .map(|(response, _reservation)| response)
        })
    }
}

fn fetch(source: &Arc<PointReadSource>, key: &str) -> tokio::task::JoinHandle<()> {
    let mut plan = PointReadPlan::default();
    plan.add_item(
        TableName::new("settlement"),
        KeyOwned::pk_sk(key, "META").into(),
    )
    .unwrap();
    let source = Arc::clone(source);
    tokio::spawn(async move {
        source.fetch_store(plan).await.unwrap();
    })
}

async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn given_cancelled_subscribers_when_native_work_is_detached_then_source_capacity_is_retained()
{
    let (started, mut calls) = mpsc::unbounded_channel();
    let source = Arc::new(PointReadSource::new(Arc::new(SettlingClient {
        admission: AdmissionController::new("point-settlement", AdmissionConfig::default())
            .unwrap(),
        started,
    })));
    let first = fetch(&source, "first");
    let release_first = calls.recv().await.unwrap();
    let second = fetch(&source, "second");
    let release_second = calls.recv().await.unwrap();
    let charged = source.stats().1.charged_bytes;
    first.abort();
    second.abort();
    let _ = first.await;
    let _ = second.await;
    wait_for(|| source.stats().0.active_keys == 0).await;
    // Let fixed workers observe cancellation and destroy their wrapper futures.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(source.stats().1.active_batches, 2);
    assert_eq!(source.stats().1.active_keys, 2);
    assert_eq!(source.stats().1.charged_bytes, charged);
    let third = fetch(&source, "third");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), calls.recv())
            .await
            .is_err()
    );
    release_first.send(()).unwrap();
    let release_third = tokio::time::timeout(Duration::from_secs(1), calls.recv())
        .await
        .unwrap()
        .unwrap();
    release_second.send(()).unwrap();
    release_third.send(()).unwrap();
    third.await.unwrap();
    wait_for(|| source.stats().1.active_batches == 0).await;
    assert_eq!(source.stats().1.charged_bytes, 0);
}
