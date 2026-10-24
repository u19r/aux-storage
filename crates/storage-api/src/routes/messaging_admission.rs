use std::{future::Future, time::Instant};

use axum::response::Response;
use http_error::HttpApiError;
use storage::{AdmissionOutcome, AdmissionPermit};
use storage_types::{StorageError, StorageResult};
use tracing::{Instrument as _, instrument::WithSubscriber as _};

#[derive(Debug, Clone, Copy)]
pub(super) struct ProviderPressureResponse;

pub(super) async fn settle_messaging_response<Fut>(
    permit: AdmissionPermit,
    started: Instant,
    operation: Fut,
) -> StorageResult<Response>
where
    Fut: Future<Output = Response> + Send + 'static,
{
    let settlement = async move {
        let response = operation.await;
        let elapsed = started.elapsed();
        let outcome = if response
            .extensions()
            .get::<ProviderPressureResponse>()
            .is_some()
        {
            AdmissionOutcome::RetryablePressure(elapsed)
        } else if response.status().is_server_error() {
            AdmissionOutcome::Failure(elapsed)
        } else {
            AdmissionOutcome::Success(elapsed)
        };
        permit.complete(outcome);
        response
    };
    match tokio::spawn(
        metrics_facade::inherit_request_cost(settlement)
            .instrument(tracing::Span::current())
            .with_current_subscriber(),
    )
    .await
    {
        Ok(response) => Ok(response),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(_) => Err(StorageError::internal("admitted messaging request stopped")),
    }
}

pub(super) fn is_provider_pressure_error(error: &HttpApiError) -> bool {
    let code = error
        .error_type
        .rsplit('#')
        .next()
        .unwrap_or(&error.error_type);
    matches!(
        code,
        "ServiceUnavailable"
            | "ThrottlingException"
            | "ProvisionedThroughputExceededException"
            | "LimitExceededException"
            | "RequestLimitExceeded"
            | "RequestTimeout"
            | "RequestTimeoutException"
    )
}
