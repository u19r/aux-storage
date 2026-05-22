# Storage Tutorials

## Tutorial Navigation

Start with Tutorial 1 if your team needs a quick, confidence-building table lifecycle walkthrough. Continue to Tutorial 2 when you are operationalizing query diagnostics and TTL controls for production data hygiene. These tutorials focus on practical execution using one endpoint, `POST /storage`, with explicit operation headers.

For shared-table placement, location dictionary updates, and online migration cutovers, use the storage migration notes in this repository.

## Sub-feature: DynamoDB-Compatible Operation Lifecycle

### Tutorial 1: First success with create, write, and read

#### Scenario

A feature team is beginning integration and needs to verify that table provisioning and item round-trips work before implementing business logic. They need a short path from zero to confirmed read-back.

This tutorial provides that path and gives a safe baseline for later query and mutation expansion.

#### Prerequisites

Prepare a table name, key schema, test item data, and credentials that permit create and item operations. Ensure your key schema matches planned access patterns so this first table can serve as a realistic integration target.

#### Walkthrough

Create a table, write one item, and read that item back by key. Confirm response fields and value envelopes exactly, because these forms are reused in every later operation.

#### Developer API Flow

This tutorial uses `POST /storage` with `x-amz-target` operations `CreateTable`, `PutItem`, and `GetItem`.

##### Control Plane Calls

```bash
curl --json '{"TableName":"inventory_demo","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]}' -H 'x-amz-target: DynamoDB_20120810.CreateTable' "$BASE_URL/storage"
  # {
  #   "TableDescription": {
  #     "TableName": "inventory_demo",
  #     "TableStatus": "ACTIVE",
  #     "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/inventory_demo"
  #   }
  # }

curl --json '{"TableName":"inventory_demo","Item":{"pk":{"S":"item#123"},"sk":{"S":"warehouse#east"},"quantity":{"N":"9"}}}' -H 'x-amz-target: DynamoDB_20120810.PutItem' "$BASE_URL/storage"
  # {}
```

##### Data Plane Calls

```bash
curl --json '{"TableName":"inventory_demo","Key":{"pk":{"S":"item#123"},"sk":{"S":"warehouse#east"}}}' -H 'x-amz-target: DynamoDB_20120810.GetItem' "$BASE_URL/storage"
  # {
  #   "Item": {
  #     "pk": {"S": "item#123"},
  #     "sk": {"S": "warehouse#east"},
  #     "quantity": {"N": "9"}
  #   }
  # }
```

#### Output Shape

Create table output must include `TableDescription`, write output is commonly an empty object, and read output must include `Item` with typed attribute values. This confirms operation dispatch and payload interpretation are working.

#### Monitoring

Track create-table latency and item operation error rates during first rollout. Early anomalies here usually indicate environment or configuration issues rather than domain logic bugs.

#### Troubleshooting

If get item returns empty, verify key names and types first. If create table fails, re-check key schema and attribute definitions for mismatched attribute references.

### Tutorial 2: Production-ready query diagnostics and TTL controls

#### Scenario

An operations team needs to verify query access behavior and turn on automatic expiration for stale data. They need a repeatable process that supports both diagnostics and operational hygiene controls.

This tutorial walks through query, scan, TTL update, and TTL status validation using the same endpoint and operation header model.

#### Prerequisites

You need a populated table, a key condition expression, and the TTL attribute name your writers will populate. Confirm data-writing paths already include TTL values before enabling the feature.

#### Walkthrough

Run query and scan to establish baseline data visibility and efficiency, then enable TTL and verify status. Keep this sequence in your runbook for new workloads and lifecycle policy updates.

#### Developer API Flow

This tutorial uses `POST /storage` with `x-amz-target` values `Query`, `Scan`, `UpdateTimeToLive`, and `DescribeTimeToLive`.

##### Control Plane Calls

```bash
curl --json '{"TableName":"inventory_demo","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"item#123"}}}' -H 'x-amz-target: DynamoDB_20120810.Query' "$BASE_URL/storage"
  # {
  #   "Items": [
  #     {
  #       "pk": {"S": "item#123"},
  #       "sk": {"S": "warehouse#east"},
  #       "quantity": {"N": "9"}
  #     }
  #   ],
  #   "Count": 1,
  #   "ScannedCount": 1
  # }

curl --json '{"TableName":"inventory_demo","Limit":10}' -H 'x-amz-target: DynamoDB_20120810.Scan' "$BASE_URL/storage"
  # {
  #   "Items": [
  #     {
  #       "pk": {"S": "item#123"},
  #       "sk": {"S": "warehouse#east"},
  #       "quantity": {"N": "9"}
  #     }
  #   ],
  #   "Count": 1,
  #   "ScannedCount": 1,
  #   "LastEvaluatedKey": null
  # }

curl --json '{"TableName":"inventory_demo","TimeToLiveSpecification":{"AttributeName":"ttl","Enabled":true}}' -H 'x-amz-target: DynamoDB_20120810.UpdateTimeToLive' "$BASE_URL/storage"
  # {
  #   "TimeToLiveSpecification": {
  #     "AttributeName": "ttl",
  #     "Enabled": true
  #   }
  # }
```

##### Data Plane Calls

```bash
curl --json '{"TableName":"inventory_demo"}' -H 'x-amz-target: DynamoDB_20120810.DescribeTimeToLive' "$BASE_URL/storage"
  # {
  #   "TimeToLiveDescription": {
  #     "AttributeName": "ttl",
  #     "TimeToLiveStatus": "ENABLED"
  #   }
  # }
```

#### Output Shape

Query and scan output includes item arrays plus count fields. TTL update and describe outputs provide explicit attribute and status fields that should agree. Use this agreement as your activation acceptance criterion.

#### Monitoring

Watch query and scan volume, scanned-to-returned ratios, and TTL status transitions. Elevated scan volume can indicate missing key access patterns and future performance risk.

#### Troubleshooting

If TTL remains disabled, verify attribute spelling and update payload structure. If query performance degrades, compare key model to access pattern and consider index additions before scaling traffic.

### Tutorial 3: Run ST/LOC online migration with dual-write cutover

#### Scenario

An operator needs to migrate one table namespace from `loc=1` to `loc=2` without downtime. They need backfill, dual-write, and controlled cutover sequencing.

#### Prerequisites

Use storage migration control-plane routes, ensure the caller-supplied migration index exists on the affected tables, and choose an `effective_at_ms` cutover timestamp.

#### Walkthrough

1. Upsert destination location dictionary entry.
2. Schedule migration (runs the caller-supplied migration index backfill and enables dual-write mode).
3. Complete migration at cutover timestamp.
4. Verify placement moved to destination `loc` and migration mode returned to `single`.

#### Verify

- No rows from other table namespaces were copied.
- Final placement reports destination `loc`.
- Migration mode is no longer `dual_write`.
