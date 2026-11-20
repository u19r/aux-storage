//! One assembled `storage-api` router with the `/queue` route over in-memory
//! SQLite.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode},
};
use storage::DatabaseManager;
use tower::ServiceExt;

use crate::{AppState, router::server_router};

pub(super) const QUEUE_BASE_URL: &str = "http://localhost/queue";

/// The router and its database, assembled as `storage-api` assembles them.
pub(super) struct QueueRoute {
    pub(super) app: Router,
}

/// One HTTP response with the body read to the end.
pub(super) struct RouteResponse {
    pub(super) status: StatusCode,
    pub(super) headers: axum::http::HeaderMap,
    pub(super) body: String,
}

impl RouteResponse {
    pub(super) fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }

    /// The text of every element at `path`, such as `ErrorResponse/Error/Code`.
    pub(super) fn xml_all(&self, path: &str) -> Vec<String> {
        xml_elements(&self.body)
            .into_iter()
            .filter(|(element_path, _)| element_path == path)
            .map(|(_, text)| text)
            .collect()
    }

    pub(super) fn xml(&self, path: &str) -> Option<String> {
        self.xml_all(path).into_iter().next()
    }

    pub(super) fn has_xml_element(&self, name: &str) -> bool {
        xml_elements(&self.body)
            .iter()
            .any(|(path, _)| path.rsplit('/').next() == Some(name))
    }
}

/// Every element of an XML document as (slash path, text). The text of an
/// element with children is empty. The parser rejects malformed XML.
fn xml_elements(body: &str) -> Vec<(String, String)> {
    let mut open = Vec::<(String, usize)>::new();
    let mut elements = Vec::<(String, String)>::new();
    for token in xmlparser::Tokenizer::from(body) {
        match token.expect("well-formed XML") {
            xmlparser::Token::ElementStart { local, .. } => {
                let path = open.last().map_or_else(
                    || local.as_str().to_owned(),
                    |(parent, _)| format!("{parent}/{}", local.as_str()),
                );
                open.push((path.clone(), elements.len()));
                elements.push((path, String::new()));
            }
            xmlparser::Token::Text { text } => {
                if let Some((_, index)) = open.last() {
                    elements[*index].1.push_str(&unescape(text.as_str()));
                }
            }
            xmlparser::Token::ElementEnd { end, .. }
                if !matches!(end, xmlparser::ElementEnd::Open) =>
            {
                open.pop();
            }
            _ => {}
        }
    }
    elements
}

fn unescape(text: &str) -> String {
    text.replace("&#xD;", "\r")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

impl QueueRoute {
    pub(super) async fn start() -> Self {
        Self::start_with_base(QUEUE_BASE_URL).await
    }

    pub(super) async fn start_with_base(base_url: &str) -> Self {
        let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
        let manager = Arc::new(
            queue::QueueManager::new(
                database
                    .messaging_store(storage::messaging::MessagingLane::Customer)
                    .map(|store| store.queue)
                    .unwrap(),
            )
            .with_endpoint(queue::QueueEndpoint {
                public_base_url: base_url.to_owned(),
            }),
        );
        let state = Arc::new(AppState::new(database).with_messaging(Some(manager), None));
        Self {
            app: server_router(state, false),
        }
    }

    /// A router whose ingress ceiling refuses every request.
    pub(super) async fn start_without_ingress() -> Self {
        let database = Arc::new(DatabaseManager::new_for_test().await.unwrap());
        let manager = Arc::new(
            queue::QueueManager::new(
                database
                    .messaging_store(storage::messaging::MessagingLane::Customer)
                    .map(|store| store.queue)
                    .unwrap(),
            )
            .with_endpoint(queue::QueueEndpoint {
                public_base_url: QUEUE_BASE_URL.to_owned(),
            }),
        );
        let state = AppState::new(database).with_messaging(Some(manager), None);
        state.ingress_semaphore.close();
        Self {
            app: server_router(Arc::new(state), false),
        }
    }

    /// Serves the router on a loopback port; returns the queue endpoint URL.
    pub(super) async fn serve(self) -> String {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, self.app).await });
        format!("http://{address}/queue")
    }

    pub(super) async fn post_json(&self, action: &str, body: &serde_json::Value) -> RouteResponse {
        self.post(
            "/queue",
            "application/x-amz-json-1.0",
            Some(&format!("AmazonSQS.{action}")),
            body.to_string(),
        )
        .await
    }

    pub(super) async fn post_form(&self, path: &str, fields: &[(&str, &str)]) -> RouteResponse {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(fields)
            .finish();
        self.post(path, "application/x-www-form-urlencoded", None, body)
            .await
    }

    pub(super) async fn post(
        &self,
        path: &str,
        content_type: &str,
        target: Option<&str>,
        body: String,
    ) -> RouteResponse {
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", content_type);
        if let Some(target) = target {
            request = request.header("x-amz-target", target);
        }
        let response = self
            .app
            .clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        read_response(response).await
    }
}

async fn read_response(response: Response<Body>) -> RouteResponse {
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    RouteResponse {
        status,
        headers,
        body: String::from_utf8(bytes.to_vec()).unwrap(),
    }
}
