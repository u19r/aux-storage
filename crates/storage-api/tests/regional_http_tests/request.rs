use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use super::TABLE;

pub(super) struct HttpRegion {
    pub(super) region: usize,
    client: reqwest::Client,
    endpoint: String,
}

impl HttpRegion {
    pub(super) fn new(address: SocketAddr, region: usize) -> Result<Self> {
        Ok(Self {
            region,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(15))
                .build()?,
            endpoint: format!("http://{address}/storage"),
        })
    }

    pub(super) async fn is_ready(&self) -> bool {
        self.client
            .get(format!("{}/ready", self.endpoint))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }

    pub(super) async fn response(
        &self,
        action: &str,
        body: Value,
    ) -> Result<(reqwest::StatusCode, Value)> {
        let response = self
            .client
            .post(&self.endpoint)
            .header("content-type", "application/x-amz-json-1.0")
            .header("x-amz-target", format!("DynamoDB_20120810.{action}"))
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        Ok((status, response.json().await?))
    }

    pub(super) async fn request(&self, action: &str, body: Value) -> Result<Value> {
        let (status, response) = self.response(action, body).await?;
        anyhow::ensure!(
            status.is_success(),
            "public HTTP {action} failed ({status}): {response}"
        );
        Ok(response)
    }

    pub(super) async fn put(&self, key: &str, value: &str) -> Result<()> {
        self.request(
            "PutItem",
            json!({"TableName": TABLE, "Item": item(key, value)}),
        )
        .await
        .with_context(|| format!("HTTP region {} put {key}: {value}", self.region))?;
        self.require_index(key, value).await
    }

    pub(super) async fn await_item(&self, key: &str, value: &str) -> Result<()> {
        let expected = item(key, value);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut last_observation = None;
        while tokio::time::Instant::now() < deadline {
            let (status, response) = self
                .response(
                    "GetItem",
                    json!({"TableName": TABLE, "Key": {"pk": {"S": key}}, "ConsistentRead": true}),
                )
                .await?;
            if status.is_success() && response["Item"] == expected {
                return self.require_index(key, value).await;
            }
            anyhow::ensure!(
                status.is_success()
                    || response["__type"]
                        .as_str()
                        .is_some_and(|kind| kind.ends_with("ResourceNotFoundException")),
                "unexpected public read failure: {status}, {response}"
            );
            last_observation = Some((status, response));
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        anyhow::bail!(
            "public HTTP region {} did not expose accepted fixture key {key} with value {value}; \
             last observation: {last_observation:?}",
            self.region
        )
    }

    pub(super) async fn query(&self, value: &str) -> Result<Vec<Value>> {
        let result = self.request("Query", json!({"TableName": TABLE, "IndexName": "value-index", "KeyConditionExpression": "#v = :v",
            "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": value}}})).await?;
        Ok(result["Items"]
            .as_array()
            .context("public Query Items")?
            .clone())
    }

    pub(super) async fn require_index(&self, key: &str, value: &str) -> Result<()> {
        anyhow::ensure!(
            self.query(value).await? == vec![item(key, value)],
            "acknowledged primary was visible without its exact GSI image"
        );
        Ok(())
    }

    pub(super) async fn require_hidden(&self) -> Result<()> {
        for (action, body) in [
            ("DescribeTable", json!({"TableName": TABLE})),
            (
                "GetItem",
                json!({"TableName": TABLE, "Key": {"pk": {"S": "baseline"}}}),
            ),
            (
                "Query",
                json!({"TableName": TABLE, "IndexName": "value-index", "KeyConditionExpression": "#v = :v",
                "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": "before-enable"}}}),
            ),
        ] {
            let (status, response) = self.response(action, body).await?;
            anyhow::ensure!(
                status == reqwest::StatusCode::BAD_REQUEST
                    && response["__type"]
                        .as_str()
                        .is_some_and(|kind| kind.ends_with("ResourceNotFoundException")),
                "hidden bootstrap admitted public {action}: {status}, {response}"
            );
        }
        Ok(())
    }

    pub(super) async fn enable(&self) -> Result<()> {
        self.request("CreateTable", json!({"TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "value", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], "GlobalSecondaryIndexes": [{"IndexName": "value-index", "KeySchema": [{"AttributeName": "value", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}]})).await?;
        self.put("baseline", "before-enable").await?;
        self.request(
            "UpdateTable",
            json!({"TableName": TABLE, "ReplicaUpdates": [{"Create": {"RegionName": "region-2"}}]}),
        )
        .await?;
        Ok(())
    }
}

pub(super) fn item(key: &str, value: &str) -> Value {
    json!({"pk": {"S": key}, "value": {"S": value}})
}
