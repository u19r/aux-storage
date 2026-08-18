#![allow(non_snake_case)]

use std::sync::Arc;

use axum::{
    body::{self, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
};
use quint_connect::{Driver, Result, State as QuintState, Step, quint_run, switch};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::runtime::{Builder, Runtime};

use crate::{
    AppState, StorageApiManagerOptions,
    routes::{
        dynamodb::dynamodb_endpoint,
        routes_test_support::{create_test_db, handle_create_table, handle_put_item},
    },
};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct ReadSequenceRuntimeState {
    phase: String,
    status: u16,
    #[serde(rename = "nodeCount")]
    node_count: usize,
    #[serde(rename = "tokenPresent")]
    token_present: bool,
}

impl QuintState<ReadSequenceRuntimeDriver> for ReadSequenceRuntimeState {
    fn from_driver(driver: &ReadSequenceRuntimeDriver) -> Result<Self> {
        Ok(driver.state.clone())
    }
}

/// A small real storage-api harness for the token boundary.
///
/// The model only describes the externally observable phases.  Every
/// transition below calls the endpoint against a fresh test database and
/// derives the resulting state from the response.  This keeps the adapter
/// useful as a conformance check without copying planner or token internals
/// into a second implementation.
struct ReadSequenceRuntimeDriver {
    runtime: Runtime,
    state: ReadSequenceRuntimeState,
    app_state: Option<Arc<AppState>>,
    request: Value,
    token: Option<String>,
}

impl std::fmt::Debug for ReadSequenceRuntimeDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReadSequenceRuntimeDriver")
            .field("state", &self.state)
            .field("app_state", &self.app_state.is_some())
            .field("request", &self.request)
            .field("token", &self.token.as_ref().map(|_| "<present>"))
            .finish()
    }
}

impl Default for ReadSequenceRuntimeDriver {
    fn default() -> Self {
        Self {
            runtime: Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build ReadSequence quint runtime"),
            state: initial_state(),
            app_state: None,
            request: read_sequence_request(),
            token: None,
        }
    }
}

impl Driver for ReadSequenceRuntimeDriver {
    type State = ReadSequenceRuntimeState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init(),
            FirstPage => self.apply_operation("FirstPage")?,
            Resume => self.apply_operation("Resume")?,
            Tamper => self.apply_operation("Tamper")?,
            step(operation: String) => self.apply_operation(&operation)?,
        })
    }
}

impl ReadSequenceRuntimeDriver {
    fn apply_operation(&mut self, operation: &str) -> Result {
        // `quint test` reuses one Driver for independent traces and does not
        // send the model's init action as a replay step. Reset this harness
        // before each one-operation trace.
        self.state = initial_state();
        self.app_state = None;
        self.request = read_sequence_request();
        self.token = None;

        match operation {
            "Init" => {
                self.init();
                Ok(())
            }
            "FirstPage" => self.first_page(),
            "Resume" => {
                self.first_page()?;
                self.resume()
            }
            "Tamper" => {
                self.first_page()?;
                self.tamper()
            }
            other => anyhow::bail!("unknown ReadSequence runtime operation {other}"),
        }
    }

    fn init(&mut self) {
        self.state = initial_state();
        self.app_state = Some(self.runtime.block_on(seed_database()));
        self.request = read_sequence_request();
        self.token = None;
    }

    fn ensure_initialized(&mut self) {
        if self.app_state.is_none() {
            self.app_state = Some(self.runtime.block_on(seed_database()));
            self.request = read_sequence_request();
            self.token = None;
        }
    }

    fn first_page(&mut self) -> Result {
        anyhow::ensure!(
            self.state.phase == "ready",
            "FirstPage is only enabled from ready state"
        );
        self.ensure_initialized();

        let app_state = self
            .app_state
            .clone()
            .ok_or_else(|| anyhow::anyhow!("ReadSequence driver was not initialized"))?;
        let (status, payload) = self
            .runtime
            .block_on(execute_read_sequence(app_state, self.request.clone()));
        anyhow::ensure!(
            status == StatusCode::OK,
            "first page returned {status}: {payload}"
        );
        let nodes = payload["Nodes"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("first page omitted Nodes: {payload}"))?;
        anyhow::ensure!(nodes.len() == 1, "first page returned {nodes:?}");
        anyhow::ensure!(nodes[0]["Name"] == "a", "first page returned {nodes:?}");
        let token = payload["NextSequenceToken"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("first page omitted continuation token: {payload}"))?
            .to_string();
        anyhow::ensure!(!token.is_empty(), "first page returned an empty token");
        self.token = Some(token);
        self.state = ReadSequenceRuntimeState {
            phase: "resumable".to_string(),
            status: StatusCode::OK.as_u16(),
            node_count: 1,
            token_present: true,
        };
        Ok(())
    }

    fn resume(&mut self) -> Result {
        anyhow::ensure!(
            self.state.phase == "resumable",
            "Resume is only enabled from resumable state"
        );
        self.ensure_initialized();

        let token = self
            .token
            .clone()
            .ok_or_else(|| anyhow::anyhow!("resumable state omitted its token"))?;
        let app_state = self
            .app_state
            .clone()
            .ok_or_else(|| anyhow::anyhow!("ReadSequence driver was not initialized"))?;
        let mut request = self.request.clone();
        request["NextSequenceToken"] = json!(token);
        let (status, payload) = self
            .runtime
            .block_on(execute_read_sequence(app_state, request));
        anyhow::ensure!(
            status == StatusCode::OK,
            "resume returned {status}: {payload}"
        );
        let nodes = payload["Nodes"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("resume omitted Nodes: {payload}"))?;
        anyhow::ensure!(nodes.len() == 1, "resume returned {nodes:?}");
        anyhow::ensure!(nodes[0]["Name"] == "b", "resume returned {nodes:?}");
        anyhow::ensure!(
            payload.get("NextSequenceToken").is_none(),
            "resume returned another token: {payload}"
        );
        self.token = None;
        self.state = ReadSequenceRuntimeState {
            phase: "complete".to_string(),
            status: StatusCode::OK.as_u16(),
            node_count: 1,
            token_present: false,
        };
        Ok(())
    }

    fn tamper(&mut self) -> Result {
        anyhow::ensure!(
            self.state.phase == "resumable",
            "Tamper is only enabled from resumable state"
        );
        self.ensure_initialized();

        let token = self
            .token
            .clone()
            .ok_or_else(|| anyhow::anyhow!("resumable state omitted its token"))?;
        let mut bytes = token.into_bytes();
        let last = bytes
            .len()
            .checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("continuation token was empty"))?;
        bytes[last] = if bytes[last] == b'0' { b'1' } else { b'0' };
        let app_state = self
            .app_state
            .clone()
            .ok_or_else(|| anyhow::anyhow!("ReadSequence driver was not initialized"))?;
        let mut request = self.request.clone();
        request["NextSequenceToken"] = json!(String::from_utf8(bytes)?);
        let (status, payload) = self
            .runtime
            .block_on(execute_read_sequence(app_state, request));
        anyhow::ensure!(
            status == StatusCode::BAD_REQUEST,
            "tampered token returned {status}: {payload}"
        );
        self.state = ReadSequenceRuntimeState {
            phase: "rejected".to_string(),
            status: StatusCode::BAD_REQUEST.as_u16(),
            node_count: 0,
            token_present: true,
        };
        Ok(())
    }
}

fn initial_state() -> ReadSequenceRuntimeState {
    ReadSequenceRuntimeState {
        phase: "ready".to_string(),
        status: 0,
        node_count: 0,
        token_present: false,
    }
}

fn read_sequence_request() -> Value {
    json!({
        "MaxTotalReadItems": 1,
        "Nodes": [
            {
                "Name": "a",
                "Operation": {
                    "Get": {
                        "TableName": "read-sequence-runtime",
                        "Key": {"id": {"S": "a"}}
                    }
                },
                "Inputs": {},
                "After": []
            },
            {
                "Name": "b",
                "Operation": {
                    "Get": {
                        "TableName": "read-sequence-runtime",
                        "Key": {"id": {"S": "b"}}
                    }
                },
                "Inputs": {},
                "After": []
            }
        ],
        "Outputs": ["a", "b"]
    })
}

async fn seed_database() -> Arc<AppState> {
    let db = create_test_db().await;
    handle_create_table(
        db.clone(),
        json!({
            "TableName": "read-sequence-runtime",
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}]
        })
        .try_into()
        .expect("create runtime table request"),
    )
    .await
    .expect("create runtime table");
    for (id, value) in [("a", "A"), ("b", "B")] {
        handle_put_item(
            db.clone(),
            json!({
                "TableName": "read-sequence-runtime",
                "Item": {"id": {"S": id}, "value": {"S": value}}
            })
            .try_into()
            .expect("put runtime item request"),
        )
        .await
        .expect("put runtime item");
    }
    Arc::new(AppState::new_with_manager_options(
        db,
        StorageApiManagerOptions::default(),
    ))
}

async fn execute_read_sequence(app_state: Arc<AppState>, payload: Value) -> (StatusCode, Value) {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        HeaderValue::from_static("DynamoDB_20120810.ReadSequence"),
    );
    let body = Bytes::from(serde_json::to_vec(&payload).expect("serialize runtime request"));
    let response = dynamodb_endpoint(State(app_state), headers, body)
        .await
        .unwrap_or_else(IntoResponse::into_response);
    let status = response.status();
    let bytes = body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read runtime response");
    let payload = serde_json::from_slice(&bytes).expect("decode runtime response");
    (status, payload)
}

#[quint_run(
    spec = "../../quint/read_sequence_runtime_mbt.qnt",
    init = "init",
    step = "step",
    max_samples = 96,
    max_steps = 1,
    seed = "0x715ec0de"
)]
fn read_sequence_runtime_mbt_replays_storage_api() -> impl Driver {
    ReadSequenceRuntimeDriver::default()
}
