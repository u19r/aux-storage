//! One-call request-cost and tracing capture for storage tests.
//!
//! Wrap the operation with `observe(async { operation().await }).await`, then
//! inspect `result` and assert exact database calls or one event with its level
//! and fields. Capture is local to the observed future. Call `observe` inside a
//! spawned task when that task needs its own tracing and metrics capture.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
};

use tracing::{Level, instrument::WithSubscriber as _};
use tracing_subscriber::{
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

pub struct Observation<T> {
    pub result: T,
    pub costs: metrics_facade::RequestCostSnapshot,
    pub events: Vec<TraceRecord>,
    pub spans: Vec<TraceRecord>,
}

impl<T> Observation<T> {
    pub fn assert_database_calls_exact(&self, expected: &[(&str, u64)]) {
        let actual: BTreeMap<_, _> = self
            .costs
            .db_call_breakdown
            .iter()
            .map(|entry| (entry.operation.as_str(), entry.calls))
            .collect();
        let expected_calls: BTreeMap<_, _> = expected.iter().copied().collect();
        assert_eq!(
            expected_calls.len(),
            expected.len(),
            "duplicate expected database operation"
        );
        assert_eq!(
            self.costs.db_calls,
            expected_calls.values().sum::<u64>(),
            "total database calls"
        );
        assert_eq!(actual, expected_calls, "database call breakdown");
    }

    pub fn assert_only_event_exact(&self, level: Level, expected: &[(&str, &str)]) {
        assert_eq!(self.events.len(), 1, "captured events: {:?}", self.events);
        Self::assert_event_exact(&self.events[0], level, expected);
    }

    pub fn assert_only_warning_or_error_exact(&self, level: Level, expected: &[(&str, &str)]) {
        assert!(matches!(level, Level::WARN | Level::ERROR));
        let failures: Vec<_> = self
            .events
            .iter()
            .filter(|event| matches!(event.level, Level::WARN | Level::ERROR))
            .collect();
        assert_eq!(failures.len(), 1, "captured events: {:?}", self.events);
        Self::assert_event_exact(failures[0], level, expected);
    }

    fn assert_event_exact(event: &TraceRecord, level: Level, expected: &[(&str, &str)]) {
        let actual_fields: BTreeMap<_, _> = event
            .fields
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let expected_fields: BTreeMap<_, _> = expected.iter().copied().collect();
        assert_eq!(
            expected_fields.len(),
            expected.len(),
            "duplicate expected trace field"
        );
        assert_eq!(event.level, level, "event level");
        assert_eq!(actual_fields, expected_fields, "event fields");
    }

    pub fn assert_no_warning_or_error(&self) {
        let unexpected: Vec<_> = self
            .events
            .iter()
            .filter(|event| matches!(event.level, Level::WARN | Level::ERROR))
            .collect();
        assert!(
            unexpected.is_empty(),
            "unexpected WARN or ERROR events: {unexpected:?}"
        );
    }

    pub fn one_event_with(&self, level: Level, expected: &[(&str, &str)]) -> &TraceRecord {
        let matching: Vec<_> = self
            .events
            .iter()
            .filter(|event| event.level == level && event.has_fields(expected))
            .collect();
        assert_eq!(matching.len(), 1, "captured events: {:?}", self.events);
        matching[0]
    }

    pub fn one_span_with(&self, name: &str, expected: &[(&str, &str)]) -> &TraceRecord {
        let matching: Vec<_> = self
            .spans
            .iter()
            .filter(|span| span.name == name && span.has_fields(expected))
            .collect();
        assert_eq!(matching.len(), 1, "captured spans: {:?}", self.spans);
        matching[0]
    }

    pub fn only_warning_or_error_in_span(
        &self,
        span_name: &str,
        span_fields: &[(&str, &str)],
    ) -> &TraceRecord {
        let matching_spans: Vec<_> = self
            .spans
            .iter()
            .enumerate()
            .filter(|(_, span)| span.name == span_name && span.has_fields(span_fields))
            .collect();
        assert_eq!(matching_spans.len(), 1, "captured spans: {:?}", self.spans);
        let failures: Vec<_> = self
            .events
            .iter()
            .filter(|event| matches!(event.level, Level::WARN | Level::ERROR))
            .collect();
        assert_eq!(failures.len(), 1, "captured events: {:?}", self.events);
        assert!(
            failures[0].span_indexes.contains(&matching_spans[0].0),
            "failure event is outside the expected span: {:?}",
            failures[0]
        );
        failures[0]
    }
}

#[derive(Debug)]
pub struct TraceRecord {
    pub name: String,
    pub level: Level,
    pub fields: BTreeMap<String, String>,
    span_indexes: Vec<usize>,
}

impl TraceRecord {
    fn has_fields(&self, expected: &[(&str, &str)]) -> bool {
        expected
            .iter()
            .all(|(key, value)| self.fields.get(*key).is_some_and(|actual| actual == value))
    }
}

#[derive(Default)]
struct TraceFields(BTreeMap<String, String>);

impl tracing::field::Visit for TraceFields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        self.record_str(field, value.trim_matches('"'));
    }
}

#[derive(Default)]
struct TraceCapture {
    events: Vec<TraceRecord>,
    spans: Vec<TraceRecord>,
}

struct SpanIndex(usize);

struct CaptureLayer {
    capture: Arc<Mutex<TraceCapture>>,
}

impl<S> tracing_subscriber::Layer<S> for CaptureLayer
where S: tracing::Subscriber + for<'span> LookupSpan<'span>
{
    fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
        let mut fields = TraceFields::default();
        event.record(&mut fields);
        let span_indexes = context
            .event_scope(event)
            .map(|scope| {
                scope
                    .from_root()
                    .filter_map(|span| span.extensions().get::<SpanIndex>().map(|index| index.0))
                    .collect()
            })
            .unwrap_or_default();
        self.capture.lock().unwrap().events.push(TraceRecord {
            name: event.metadata().name().to_owned(),
            level: *event.metadata().level(),
            fields: fields.0,
            span_indexes,
        });
    }

    fn on_new_span(
        &self,
        attributes: &tracing::span::Attributes<'_>,
        id: &tracing::Id,
        context: Context<'_, S>,
    ) {
        let mut fields = TraceFields::default();
        attributes.record(&mut fields);
        let index = {
            let mut capture = self.capture.lock().unwrap();
            let index = capture.spans.len();
            capture.spans.push(TraceRecord {
                name: attributes.metadata().name().to_owned(),
                level: *attributes.metadata().level(),
                fields: fields.0,
                span_indexes: Vec::new(),
            });
            index
        };
        context
            .span(id)
            .unwrap()
            .extensions_mut()
            .insert(SpanIndex(index));
    }

    fn on_record(
        &self,
        id: &tracing::Id,
        values: &tracing::span::Record<'_>,
        context: Context<'_, S>,
    ) {
        let mut fields = TraceFields::default();
        values.record(&mut fields);
        let span = context.span(id).unwrap();
        let index = span.extensions().get::<SpanIndex>().unwrap().0;
        self.capture.lock().unwrap().spans[index]
            .fields
            .extend(fields.0);
    }
}

pub async fn observe<T>(future: impl Future<Output = T>) -> Observation<T> {
    let capture = Arc::new(Mutex::new(TraceCapture::default()));
    let subscriber = tracing_subscriber::registry().with(CaptureLayer {
        capture: Arc::clone(&capture),
    });
    let dispatch = tracing::dispatcher::Dispatch::new(subscriber);
    let (result, costs) = metrics_facade::begin_request_cost_collection(None, || async {
        let result = future.await;
        let costs = metrics_facade::finish_request_cost_collection(None, 1.0, None).await;
        (result, costs)
    })
    .with_subscriber(dispatch)
    .await;
    let mut capture = capture.lock().unwrap();
    Observation {
        result,
        costs,
        events: std::mem::take(&mut capture.events),
        spans: std::mem::take(&mut capture.spans),
    }
}
