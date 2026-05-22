# Storage Usage

## Diataxis Navigation

Use [TUTORIALS](./TUTORIALS.md) for guided walkthroughs that start with a simple table lifecycle and progress to operational hardening. This page focuses on repeatable how-to flows, endpoint and payload reference details, and conceptual guidance for teams integrating DynamoDB-compatible operations through a single API endpoint.

## Quick Overview (One Minute)

Storage exposes a DynamoDB-compatible API surface through one endpoint, `POST /storage`, with operation selection performed by the `x-amz-target` header. This lets teams reuse familiar request and response shapes while running on SQLite, Turso, Postgres, RocksDB, or FoundationDB.

This route is the standalone storage service gateway. Non-Dynamo helper traffic stays on explicit internal-only routes in the standalone `storage-api` harness.

Instead of implementing custom data operation RPCs for each feature, services can use standard create, put, get, query, scan, and table-management operations. The result is faster integration, easier migration of existing Dynamo workflows, and consistent observability around storage behavior.

## ST/LOC Routing Note

Shared-table routing (`st`) and location routing (`loc`) are not configured through `POST /storage`. Embedding applications own the control plane that supplies table placement, location dictionaries, and migration scheduling.

Use `storage` data-plane operations for reads/writes/queries, and have the embedding application call storage migration APIs for placement, location dictionaries, and migration scheduling/completion.

## Problems This Solves

Distributed systems often accumulate multiple storage interfaces over time, each with different pagination, error semantics, and naming behavior. That fragmentation slows delivery and increases incident complexity because each service must learn and maintain a unique persistence contract.

A DynamoDB-compatible unified endpoint reduces that burden. Teams get one predictable operation model and can evolve data workflows with less per-service storage glue. It also improves operator confidence because action names and payload structures remain consistent across use cases.

## Capabilities Enabled

Storage enables teams to provision tables, write and read items, run query and scan operations, and manage lifecycle controls such as TTL using stable operation headers and payloads. It supports both rapid feature development and robust production workflows by preserving typed operation boundaries.

For embedding applications, this means one reusable persistence foundation and less service-specific storage protocol translation.

## Who This Is For

Engineering teams use Storage when they need DynamoDB-style data operations without embedding backend-specific SDK logic in every service. Operators use it to standardize observability and operational controls for persistence workflows.

## Glossary

`x-amz-target` selects the DynamoDB-compatible operation name. A table operation manages table metadata and lifecycle. An item operation reads or writes row-level data. TTL controls whether expiration attributes are automatically interpreted for background expiry behavior.

## How-To Track (Solve a Specific Problem)

### How-to 1: Provision a table and verify first write path

Use this flow when you need to create a new table for a feature and prove that writes and reads are functioning before application rollout. The goal is to create a table, write one record, and fetch it back with deterministic item shape.

#### Inputs

Prepare table name, key schema, attribute definitions, and one representative item payload. Confirm naming conventions and key shape with your data model before table creation so you do not need immediate migration work.

#### Steps

Create the table first and wait for active status in response. Then write one test item and read it back using the same key. Treat read-back success as the release gate for first integration.

#### Developer API Flow

This transcript uses `POST /storage` with `x-amz-target` values for `CreateTable`, `PutItem`, and `GetItem`.

```bash
curl --json '{"TableName":"orders_prod","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]}' -H 'x-amz-target: DynamoDB_20120810.CreateTable' "$BASE_URL/storage"
  # {
  #   "TableDescription": {
  #     "TableName": "orders_prod",
  #     "TableStatus": "ACTIVE",
  #     "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/orders_prod"
  #   }
  # }

curl --json '{"TableName":"orders_prod","Item":{"pk":{"S":"order#1001"},"sk":{"S":"line#1"},"status":{"S":"pending"}}}' -H 'x-amz-target: DynamoDB_20120810.PutItem' "$BASE_URL/storage"
  # {}

curl --json '{"TableName":"orders_prod","Key":{"pk":{"S":"order#1001"},"sk":{"S":"line#1"}}}' -H 'x-amz-target: DynamoDB_20120810.GetItem' "$BASE_URL/storage"
  # {
  #   "Item": {
  #     "pk": {"S": "order#1001"},
  #     "sk": {"S": "line#1"},
  #     "status": {"S": "pending"}
  #   }
  # }
```

#### Output Shape

Create table success is indicated by a `TableDescription` with active status fields. Put item typically returns an empty object unless return-value options are requested. Get item should return an `Item` map using Dynamo attribute value envelopes.

#### Verify

Repeat get item with the same key and compare values against your source payload. Confirm table name and key shape match your application model before moving to write-heavy tests.

#### Rollback / Recovery

If table schema is wrong, delete the table and recreate it with corrected key definitions before feature traffic starts. Avoid patching schema assumptions in application code around an incorrect table model.

### How-to 2: Validate query and scan behavior for operational diagnostics

Use this workflow when your team needs to inspect live records and confirm index or key-condition behavior during incident response or rollout validation. The objective is to compare targeted query output with broader scan output.

#### Inputs

Prepare table name, key condition expression, expression attribute values, and optional scan limits. Ensure your query condition matches actual key schema; otherwise results can be misleading.

#### Steps

Run query for targeted key conditions first, then scan for broad inspection. Compare `Count` and `ScannedCount` to understand efficiency and coverage. Use this data to decide whether access patterns need index or key-model changes.

#### Developer API Flow

This transcript uses `POST /storage` with `x-amz-target` values for `Query` and `Scan`.

```bash
curl --json '{"TableName":"orders_prod","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"order#1001"}}}' -H 'x-amz-target: DynamoDB_20120810.Query' "$BASE_URL/storage"
  # {
  #   "Items": [
  #     {
  #       "pk": {"S": "order#1001"},
  #       "sk": {"S": "line#1"},
  #       "status": {"S": "pending"}
  #     }
  #   ],
  #   "Count": 1,
  #   "ScannedCount": 1
  # }

curl --json '{"TableName":"orders_prod","Limit":25}' -H 'x-amz-target: DynamoDB_20120810.Scan' "$BASE_URL/storage"
  # {
  #   "Items": [
  #     {
  #       "pk": {"S": "order#1001"},
  #       "sk": {"S": "line#1"},
  #       "status": {"S": "pending"}
  #     }
  #   ],
  #   "Count": 1,
  #   "ScannedCount": 1,
  #   "LastEvaluatedKey": null
  # }
```

#### Output Shape

Query and scan outputs include `Items`, `Count`, and `ScannedCount`. Query should return tightly scoped results when key conditions are correct, while scan returns broader traversal output and may include pagination state through `LastEvaluatedKey`.

#### Verify

Confirm expected records are present in query results and compare scan coverage to table size expectations. Large `ScannedCount` relative to result count indicates an access-pattern optimization opportunity.

#### Rollback / Recovery

If query patterns fail under load, revert to known-good key paths and schedule index or key-model changes. Avoid relying on high-volume scan behavior in production request paths.

### How-to 3: Enable and confirm TTL controls during data hygiene rollout

Use this workflow when your team needs automated expiration for stale data and wants to verify TTL activation with explicit API evidence. The goal is to enable TTL and confirm status before relying on background expiration behavior.

#### Inputs

Prepare target table name and TTL attribute name used in your item model. Confirm application writes that attribute consistently before enabling TTL.

#### Steps

Enable TTL for the table, then read TTL status back and verify that attribute and enabled state are correct. Treat status confirmation as required before deprecating manual cleanup workflows.

#### Developer API Flow

This transcript uses `POST /storage` with `x-amz-target` values for `UpdateTimeToLive` and `DescribeTimeToLive`.

```bash
curl --json '{"TableName":"orders_prod","TimeToLiveSpecification":{"AttributeName":"ttl","Enabled":true}}' -H 'x-amz-target: DynamoDB_20120810.UpdateTimeToLive' "$BASE_URL/storage"
  # {
  #   "TimeToLiveSpecification": {
  #     "AttributeName": "ttl",
  #     "Enabled": true
  #   }
  # }

curl --json '{"TableName":"orders_prod"}' -H 'x-amz-target: DynamoDB_20120810.DescribeTimeToLive' "$BASE_URL/storage"
  # {
  #   "TimeToLiveDescription": {
  #     "AttributeName": "ttl",
  #     "TimeToLiveStatus": "ENABLED"
  #   }
  # }
```

#### Output Shape

TTL update responses include the target attribute and enabled flag. TTL describe responses include current status and attribute metadata. Successful rollout requires both calls to agree on attribute name and enabled state.

#### Verify

Validate that newly written records include `ttl` and observe expiration behavior in non-critical data first. Keep manual cleanup fallback during early rollout until TTL behavior is confirmed.

#### Rollback / Recovery

If TTL was enabled for the wrong attribute, issue another update with corrected values and verify describe output again. If expiration behavior is unexpected, disable TTL and investigate data-writing paths.

### How-to 4: Trigger table storage migration workflow

Use this when moving a table namespace between storage locations.

1. Upsert the destination location dictionary entry in the embedding application's control plane.
2. Schedule migration for the target table namespace to run the caller-supplied migration index backfill and dual-write.
3. Complete migration at cutover.
4. Verify final placement through the embedding application's control plane.

## Reference Track (Authoritative Facts)

### Endpoint Map

All storage operations use `POST /storage` and select behavior via `x-amz-target`, including create and delete table, list and describe table, put and get item, delete item, query, scan, batch operations, transaction write, update table, and TTL operations.

### Request and Response Model Reference

Requests and responses follow DynamoDB-compatible JSON conventions with operation-specific payload shapes. Item values use typed attribute envelopes such as `{"S":"value"}` and `{"N":"123"}`.

### Error Catalog

Unknown or invalid `x-amz-target` values return validation-style errors. Malformed JSON and invalid request shapes return bad-request errors. Missing resources and conditional failures return operation-specific error envelopes.

### Limits and Defaults

List and scan operations support limits and pagination tokens. Route-level dispatch is deterministic based on `x-amz-target`, so operation naming accuracy is critical.

### Audit and Monitoring Reference

Track operation latency and volume by operation name, plus error-rate spikes by target operation. Alert when create or update table failures rise, since those often indicate broader service or backend issues.

## Explanation Track (Concepts and Reasoning)

### Concepts and Mental Model

Storage is a multiplexed operation gateway with one HTTP route and many operation types. Operation identity is explicit in the header, and payload semantics remain operation-specific.

### Why It Works This Way

One endpoint with explicit operation headers reduces surface-area complexity while preserving strong operation semantics. Teams can integrate using familiar Dynamo patterns without custom per-feature storage APIs.

### Problem-to-Solution Mapping

If teams need rapid data model iteration, use create, put, and get operations first. If teams need diagnostics or analytics-like retrieval, use query and scan with careful performance monitoring.

### Terminology and Domain Notes

`x-amz-target` names the action. Key schema defines hash and range access semantics. TTL manages expiration behavior through configured item attributes.

## Limitations and Known Boundaries

Compatibility is focused on supported operation set and current provider behavior. Some advanced Dynamo edge behavior may differ across backend providers and should be validated in staging.

## Security and Compliance Notes

Restrict table mutation operations to trusted automation paths. Validate table scoping in higher layers and avoid broad scan access in sensitive data contexts without explicit review.

## FAQ

Can existing Dynamo clients call this endpoint directly? Yes, if they can set compatible headers and payload shapes. Should every operation use scan first? No, prefer key-based query patterns for predictable performance and lower cost.
