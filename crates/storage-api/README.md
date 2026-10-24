# storage-api

`storage-api` exposes DynamoDB-compatible operations at `POST /storage`. The `queue` and
`pubsub` Cargo features also mount SQS-compatible `POST /queue` and SNS-compatible
`POST /pubsub` routes on the same configured backend.

## Request flow

The [router](src/router.rs) mounts the configured service paths. The
[DynamoDB route](src/routes/dynamodb/mod.rs) selects an operation from `x-amz-target`, parses
its request, and maps typed results and errors into protocol responses. The
[storage API manager](src/manager/storage_api_manager.rs) delegates storage work to
`DatabaseManager`; shared domain executors own business validation and error selection.

Keep request types, route behavior, OpenAPI annotations, and response tests consistent.
The generated OpenAPI contracts are [JSON](openapi-storage-api.json) and
[YAML](openapi-storage-api.yaml).

## Configuration and access

See the workspace [configuration guide](../../CONFIGURATION.md) for launch settings.
Inbound API and metrics requests have no application-layer authentication. Restrict access
with private ingress, security groups, or an authenticating proxy.

Route tests exercise operation dispatch and protocol errors. Integration tests exercise
complete HTTP flows against configured native and remote providers.
