# DynamoDB API Extensions

Last updated: 2026-06-24

Aux-Storage preserves DynamoDB-compatible behavior by default while offering a
small set of opt-in extensions for teams that need stronger consistency or more
control over stream retention, dependent reads, and audit history. These
extensions are explicit in the API surface: standard DynamoDB clients can keep
using standard DynamoDB requests, and software that chooses an aux-storage
extension can do so deliberately.

## Immediate Consistency for Global Secondary Indexes

DynamoDB-compatible global secondary indexes are normally eventually consistent. That model is excellent for many high-throughput workloads. The immediate consistency GSI extension moves GSI maintenance into the committed write path. After the write succeeds, reads through the affected GSI can observe the new index entry without waiting for an asynchronous index worker to catch up.

It is intended for systems that want the DynamoDB data model and API shape, but need read-after-write behavior for secondary indexes.

### How to Enable It

Immediate consistency GSIs are enabled at the aux-storage service level:

```bash
storage-api --storage foundationdb --immediate-consistency-gsis
```

From a source checkout, the same launch shape is:

```bash
cargo run -p storage-api --bin storage-api -- --storage foundationdb --immediate-consistency-gsis
```

Use the equivalent deployment setting if your environment wraps `storage-api`
behind a service manager, container entrypoint, or platform-specific config.

After the service is running in immediate-consistency mode, application requests
use normal DynamoDB API calls. No per-request extension field is required.

### Example: Create a Table with a GSI

```json
{
  "TableName": "Orders",
  "AttributeDefinitions": [
    { "AttributeName": "pk", "AttributeType": "S" },
    { "AttributeName": "sk", "AttributeType": "S" },
    { "AttributeName": "gsi_pk", "AttributeType": "S" },
    { "AttributeName": "gsi_sk", "AttributeType": "S" }
  ],
  "KeySchema": [
    { "AttributeName": "pk", "KeyType": "HASH" },
    { "AttributeName": "sk", "KeyType": "RANGE" }
  ],
  "BillingMode": "PAY_PER_REQUEST",
  "GlobalSecondaryIndexes": [
    {
      "IndexName": "OrdersByAccountAndStatus",
      "KeySchema": [
        { "AttributeName": "gsi_pk", "KeyType": "HASH" },
        { "AttributeName": "gsi_sk", "KeyType": "RANGE" }
      ],
      "Projection": { "ProjectionType": "ALL" }
    }
  ]
}
```

### Example: Write and Immediately Query Through the GSI

`PutItem`:

```json
{
  "TableName": "Orders",
  "Item": {
    "pk": { "S": "ORDER#1001" },
    "sk": { "S": "ORDER#1001" },
    "gsi_pk": { "S": "ACCOUNT#acme" },
    "gsi_sk": { "S": "OPEN#2026-06-09T09:00:00Z" },
    "total_cents": { "N": "12900" }
  }
}
```

`Query`:

```json
{
  "TableName": "Orders",
  "IndexName": "OrdersByAccountAndStatus",
  "KeyConditionExpression": "gsi_pk = :account AND begins_with(gsi_sk, :status)",
  "ExpressionAttributeValues": {
    ":account": { "S": "ACCOUNT#acme" },
    ":status": { "S": "OPEN#" }
  }
}
```

With immediate consistency enabled, once `PutItem` returns successfully, the
following `Query` can rely on the committed GSI entry being available through
aux-storage.

### Performance and Storage Implications

Immediate consistency shifts GSI work from background maintenance into foreground
writes. This changes where the cost is paid:

- Write latency can increase because affected GSI entries are updated as part of
  the write path. For foundationdb, total transaction size is 10MB. Large items with many GSIs can hit this limit. Smaller items in a batch write or transact write items can also hit this limit.
- GSI read-after-write behavior becomes simpler for application code, often
  reducing retries, polling, defensive base-table reads, and reconciliation jobs.
- Background index lag is reduced as an operational concern for tables that use
  this mode.

For user-facing invariants, permissions, routing, and account-state lookups, the latency tradeoff is often worth it. For bulk ingest or write-heavy analytics tables where delayed index visibility is acceptable, eventual GSI maintenance may remain the better fit.

Operators should monitor write p95 and p99 latency, transaction retries, item size limits, and backend write pressure when enabling immediate consistency on tables with several GSIs and large item sizes.

## ReadSequence

`ReadSequence` is an aux-storage DynamoDB JSON protocol extension for bounded
N+1 read workflows. It runs an ordered sequence of `Get`, `BatchGet`, and
`Query` steps, lets later steps bind selected attributes from earlier results,
and returns a nested response that preserves the parent-child relationships.

Use it when a workflow cannot be expressed as independent point reads and would
otherwise require a client to issue one read, inspect the result, then issue a
bounded set of dependent reads. If the reads are independent, prefer standard
`BatchGetItem`. If the workflow is a small transactional point-read set, prefer
standard `TransactGetItems`.

`ReadSequence` is not a server-side scripting runtime. It does not run custom
code, loops, recursion, scans, or unbounded joins. Every request is bounded by
configured step, fanout, intermediate item, total read, child query, and response
byte limits.

### How to Call It

`ReadSequence` is exposed as a DynamoDB JSON protocol target:

```http
POST /storage
x-amz-target: DynamoDB_20120810.ReadSequence
content-type: application/x-amz-json-1.0
```

The request body contains a top-level `ReadConsistency` value and an ordered
`Sequence` of named read steps. `EVENTUAL` is the default consistency mode.

### Example: Read a User and Its Organization

```json
{
  "ReadConsistency": "EVENTUAL",
  "Sequence": [
    {
      "Name": "user",
      "Get": {
        "TableName": "Users",
        "Key": {
          "pk": { "S": "user#1" }
        }
      },
      "Select": {
        "org_id": "$.org_id"
      }
    },
    {
      "Name": "org",
      "ForEach": {
        "From": "user.Item.org_id",
        "As": "org_id",
        "OnMissing": "ERROR",
        "Get": {
          "TableName": "Organizations",
          "Key": {
            "pk": { "S": "${org_id}" }
          }
        },
        "Join": {
          "To": "user",
          "As": "org",
          "Type": "REQUIRED_ONE"
        }
      }
    }
  ]
}
```

The first step reads the user and selects `org_id`. The second step binds that
selected value into an organization key and attaches the organization result to
the user result as `org`.

### Example: Query Orders and Fetch Dependent Invoices

```json
{
  "ReadConsistency": "EVENTUAL",
  "MaxFanoutPerStep": 25,
  "MaxTotalReadItems": 100,
  "Sequence": [
    {
      "Name": "orders",
      "Query": {
        "TableName": "Orders",
        "IndexName": "by_customer",
        "KeyConditionExpression": "customer_id = :customer_id",
        "ExpressionAttributeValues": {
          ":customer_id": { "S": "cust#123" }
        },
        "ProjectionExpression": "pk, sk, invoice_id, status",
        "Limit": 25
      },
      "Select": {
        "invoice_id": "$.invoice_id"
      }
    },
    {
      "Name": "invoice",
      "ForEach": {
        "From": "orders.Items",
        "As": "order",
        "OnMissing": "NULL",
        "Get": {
          "TableName": "Invoices",
          "Key": {
            "pk": { "S": "invoice#${order.invoice_id.S}" },
            "sk": { "S": "meta" }
          },
          "ProjectionExpression": "pk, sk, total, status"
        },
        "Join": {
          "To": "orders",
          "As": "invoice",
          "Type": "LEFT_ONE"
        }
      }
    }
  ]
}
```

This collapses a common query-plus-child-get workflow into one bounded request.
aux-storage may return a top-level `Warning` when a query-plus-get sequence is
better modeled as a GSI. The warning names the suggested key shape but does not
include raw user data values.

### Consistency Modes

`ReadSequence` supports these consistency modes when the configured backend can
prove the requested behavior:

- `EVENTUAL`: allows table and GSI reads where the individual operation supports
  eventual consistency.
- `STRONG`: allows base-table `Get`, `BatchGet`, and table `Query` reads when
  the backend supports strong reads. GSI reads are rejected because DynamoDB
  GSIs do not support strong consistency.
- `TRANSACTIONAL`: executes the sequence page through one provider-owned
  backend snapshot, transaction, or read version when the backend supports it.
  GSI reads are rejected unless the backend is running with immediate GSI
  consistency.

Unsupported consistency modes fail before partial sequence execution. Remote
providers can run bounded non-transactional sequences by composing ordinary
DynamoDB-compatible operations, but reject transactional `ReadSequence` because
they cannot prove one backend snapshot across multiple remote calls.

### Pagination and Tokens

`ReadSequence` can stop before the logical sequence is exhausted when it reaches
a root page, child page, fanout boundary, response byte limit, total read limit,
or backend transaction budget. In those cases the response includes
`NextSequenceToken`.

Treat `NextSequenceToken` as opaque. Tokens are tied to the request shape and
service state needed for safe continuation. Stale, mismatched, or tampered
tokens fail validation instead of producing incomplete or duplicated joins.

Remote `BatchGetItem` partial responses with `UnprocessedKeys` are returned as
retryable throttling errors rather than incomplete joined data.

### Backend Availability

`ReadSequence` is capability-gated by backend configuration:

| Backend      | Eventual and strong base-table reads | Transactional snapshots | Transactional GSI reads |
| ------------ | ------------------------------------ | ----------------------- | ----------------------- |
| SQLite       | Supported.                           | Supported for file-backed SQLite. | Supported only with immediate GSI consistency. |
| Postgres     | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| Turso        | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| RocksDB / KV | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| FoundationDB | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| Remote       | Supported for non-transactional eligible reads. | Rejected.               | Rejected.               |

For production environments, verify the exact backend, service version, and GSI
consistency setting before accepting `ReadSequence` traffic from application
clients.

### Performance and Operational Implications

`ReadSequence` reduces application round trips and can reuse one backend read
context for a transactional sequence page, but it still performs real backend
reads. Operators should monitor total read items, response bytes, fanout
rejections, token counts, retryable throttling errors, snapshot expiration, and
backend read latency.

The extension is usually a good fit for bounded page assembly, authorization
lookups, account or organization hydration, and support tools that need a small
dependent graph. It is a poor fit for broad analytics, arbitrary graph traversal,
unbounded relationship expansion, or access patterns that should be represented
as a purpose-built GSI.

## Custom Stream Horizon and Item Duration

DynamoDB Streams expose recent item changes for consumers such as replication,
event processing, cache invalidation, and audit workflows. Standard DynamoDB
retention is fixed. aux-storage adds explicit retention controls so a table can
keep stream history for a shorter operational window, a longer window, or a
per-item duration.

The extension has two levels:

- A table stream horizon controls how long table-level stream records remain
  readable.
- An item stream duration controls how long the item history behind those stream
  records is physically retained.

When both table and item durations apply, aux-storage keeps the physical item
stream history for the longer of the two durations. This protects stream records
from pointing at item history that has already been trimmed.

### Duration Values

Duration values are expressed in hours:

- Omit the extension fields to use aux-storage's default 72-hour retention.
- Use a finite value from `1` through `61320` hours.
- `61320` hours is seven years: `24 * 365 * 7`.
- Use `-1` to retain the covered stream data indefinitely.

Retention cleanup is cooperative background work. It is allowed to lag when the
system is under load. Operators should treat retention values as minimum
availability windows, not as exact deletion deadlines.

### Table-Level API Fields

`CreateTable` and `UpdateTable` accept these aux-storage fields:

- `AuxStreamDurationHours`: table stream horizon.
- `AuxDefaultItemStreamDurationHours`: default item stream duration for writes
  that do not set a per-item duration.

### Item-Level API Field

`PutItem`, `UpdateItem`, `DeleteItem`, `BatchWriteItem` put/delete requests,
and `TransactWriteItems` put/update/delete requests accept:

- `AuxItemStreamTtlHours`: item-specific stream duration for that write.

The value is scoped to the item stream, not only to the individual version being
written. A later put, update, or delete with `AuxItemStreamTtlHours` changes the
retention policy for that item stream and can shorten or lengthen the horizon for
previous retained versions of that item. Transaction condition checks do not
accept an item stream duration because they do not write an item version.

### Example: Create a Table with Seven-Day Stream History

```json
{
  "TableName": "LedgerEntries",
  "AttributeDefinitions": [
    { "AttributeName": "account_id", "AttributeType": "S" },
    { "AttributeName": "entry_id", "AttributeType": "S" }
  ],
  "KeySchema": [
    { "AttributeName": "account_id", "KeyType": "HASH" },
    { "AttributeName": "entry_id", "KeyType": "RANGE" }
  ],
  "BillingMode": "PAY_PER_REQUEST",
  "StreamSpecification": {
    "StreamEnabled": true,
    "StreamViewType": "NEW_AND_OLD_IMAGES"
  },
  "AuxStreamDurationHours": 168,
  "AuxDefaultItemStreamDurationHours": 168
}
```

This keeps the table stream horizon and default item stream history for seven
days.

### Example: Update a Table to Keep Stream Records for 30 Days

```json
{
  "TableName": "LedgerEntries",
  "AuxStreamDurationHours": 720,
  "AuxDefaultItemStreamDurationHours": 720
}
```

### Example: Keep One Item History for Seven Years

```json
{
  "TableName": "LedgerEntries",
  "Item": {
    "account_id": { "S": "ACCOUNT#acme" },
    "entry_id": { "S": "ENTRY#2026-06-09#0001" },
    "amount_cents": { "N": "12900" },
    "currency": { "S": "USD" }
  },
  "AuxItemStreamTtlHours": 61320
}
```

This write keeps the item stream history for seven years, even if the table
default is shorter.

### Example: Retain a Critical Item Indefinitely

```json
{
  "TableName": "LedgerEntries",
  "Key": {
    "account_id": { "S": "ACCOUNT#acme" },
    "entry_id": { "S": "ENTRY#2026-06-09#0001" }
  },
  "UpdateExpression": "SET audit_status = :status",
  "ExpressionAttributeValues": {
    ":status": { "S": "locked" }
  },
  "AuxItemStreamTtlHours": -1
}
```

### Example: Delete an Item and Shorten Its Audit Horizon

```json
{
  "TableName": "LedgerEntries",
  "Key": {
    "account_id": { "S": "ACCOUNT#acme" },
    "entry_id": { "S": "ENTRY#2026-06-09#0001" }
  },
  "AuxItemStreamTtlHours": 24
}
```

This writes the delete marker and changes the item stream duration to 24 hours.
Previous retained versions for that item can be trimmed after the effective
retention window has passed. The table stream horizon still protects table-level
stream records, so the physical item history is retained for the longer of the
table horizon and the item-specific duration.

### Example: Batch Write with Item Durations

```json
{
  "RequestItems": {
    "LedgerEntries": [
      {
        "PutRequest": {
          "Item": {
            "account_id": { "S": "ACCOUNT#acme" },
            "entry_id": { "S": "ENTRY#2026-06-09#0002" },
            "amount_cents": { "N": "4500" }
          },
          "AuxItemStreamTtlHours": 24
        }
      },
      {
        "DeleteRequest": {
          "Key": {
            "account_id": { "S": "ACCOUNT#acme" },
            "entry_id": { "S": "ENTRY#2026-06-09#expired" }
          },
          "AuxItemStreamTtlHours": 24
        }
      }
    ]
  }
}
```

### Example: Transactional Write with Item Durations

```json
{
  "TransactItems": [
    {
      "Put": {
        "TableName": "LedgerEntries",
        "Item": {
          "account_id": { "S": "ACCOUNT#acme" },
          "entry_id": { "S": "ENTRY#2026-06-09#0003" },
          "amount_cents": { "N": "9800" }
        },
        "AuxItemStreamTtlHours": 168
      }
    },
    {
      "Delete": {
        "TableName": "LedgerEntries",
        "Key": {
          "account_id": { "S": "ACCOUNT#acme" },
          "entry_id": { "S": "ENTRY#2026-06-09#old" }
        },
        "AuxItemStreamTtlHours": 24
      }
    },
    {
      "Update": {
        "TableName": "LedgerEntries",
        "Key": {
          "account_id": { "S": "ACCOUNT#acme" },
          "entry_id": { "S": "ENTRY#2026-06-09#0001" }
        },
        "UpdateExpression": "SET reviewed = :reviewed",
        "ExpressionAttributeValues": {
          ":reviewed": { "BOOL": true }
        },
        "AuxItemStreamTtlHours": 61320
      }
    }
  ]
}
```

### Backend Availability

The extension is designed to be explicit about backend support:

| Backend          | Custom stream duration status                                                                         |
| ---------------- | ----------------------------------------------------------------------------------------------------- |
| SQLite           | Supported.                                                                                            |
| Postgres / Turso | Supported for SQL deployments; validate trim-job scheduling and backlog monitoring in your runtime.   |
| RocksDB / KV     | Supported.                                                                                            |
| FoundationDB     | Intended production target; validate against your live FoundationDB environment before broad rollout. |
| Remote           | Mirrors the configured aux-storage service.                                                           |

For production environments, operators should verify the exact backend and
service version before accepting aux-storage extension fields from application
clients.

### Performance and Storage Implications

Custom stream duration gives teams more control over retention, but longer
history is still durable data that must be written, indexed, scanned, and
eventually trimmed.

Important implications:

- Standard DynamoDB-compatible clients remain unchanged when aux-storage fields are omitted.
- Longer table horizons increase stream storage and the amount of history stream consumers can replay.
- Per-item durations add durable retention metadata so trim work can find the correct item history later. This creates additional small metadata writes for items that set an override.
- Reusing the same item duration for most writes is cheaper operationally than assigning highly varied durations to every item.
- `-1` should be reserved for records that genuinely need indefinite history, such as audit anchors, legal holds, or high-value reconciliation data.
- Cleanup is intentionally cooperative. During bursts, TTL cleanup and stream trimming may run late so foreground traffic and cluster health remain the priority.
- Capacity planning should include retained stream bytes, trim backlog, backend range-clear or delete throughput, and consumer replay windows.
- Deleting an item does not immediately delete the stream records. A delete with `AuxItemStreamTtlHours` changes the item stream policy, and cleanup removes old stream data later when the effective horizon allows it.

Finite item durations add a small amount of durable bookkeeping to each write. That is expected: the service is recording retention policy state in addition to the item write. The operational goal is to keep that extra work constant and bounded so large bursts do not create unbounded memory or CPU pressure.

### Choosing Values

Use the shortest duration that satisfies the workflow:

- Use the default horizon for ordinary event consumers.
- Use DynamoDB item TTL values to delete items themselves on a schedule; use `AuxItemStreamTtlHours` to control how long the stream history remains.
- Use weeks or months for operational audit and customer support workflows.
- Use seven years only when the data class needs that retention window.
- Use indefinite retention only for explicit legal, audit, or reconciliation
  requirements.

For most tables, set a table default that matches the common case and override
only the exceptional items. That keeps request payloads simpler and makes storage
growth easier to forecast.

## Audit Stream Reads

Custom stream retention is most useful when applications can read retained
history directly. aux-storage keeps two related streams for DynamoDB table
writes:

- The table stream records the ordered sequence of writes for the table.
- The table item stream records the ordered history for one item key.

The table stream is the right source for replication, event processing, and
"show me what changed in this table" workflows. The item stream is the right
source for an audit timeline for one record, including a record that has already
been deleted from the base table.

### Stream Names

For table `LedgerEntries`, the table stream name is:

```text
LedgerEntries/stream-table
```

For the item with key:

```json
{
  "account_id": { "S": "ACCOUNT#acme" },
  "entry_id": { "S": "ENTRY#2026-06-09#0001" }
}
```

the table item stream name is built from the table name and the DynamoDB key
attributes:

```text
LedgerEntries/stream-item/account_id=S:ACCOUNT#acme/entry_id=S:ENTRY#2026-06-09#0001
```

Very large keys may be represented by a stable hash suffix instead of the full
key payload. Client libraries should use aux-storage's item-stream-name helper
when available rather than reconstructing this string by hand.

### Read a Table Stream from the Beginning

Use `GetStreamRecords` to read a table stream in chronological order. Omit
`LastEvaluatedKey` to start at the oldest retained table stream record.

```http
POST /storage
x-amz-target: DynamoDB_20120810.GetStreamRecords
content-type: application/x-amz-json-1.0
```

```json
{
  "TableName": "LedgerEntries",
  "Limit": 100
}
```

Example response:

```json
{
  "TableName": "LedgerEntries",
  "Records": [
    {
      "Keys": {
        "account_id": { "S": "ACCOUNT#acme" },
        "entry_id": { "S": "ENTRY#2026-06-09#0001" }
      },
      "SequenceNumber": "0197657f8f0772c1917be0db",
      "NewImage": {
        "account_id": { "S": "ACCOUNT#acme" },
        "entry_id": { "S": "ENTRY#2026-06-09#0001" },
        "amount_cents": { "N": "12900" },
        "status": { "S": "posted" }
      }
    }
  ],
  "LastEvaluatedKey": "0197657f8f0772c1917be0db"
}
```

Pass `LastEvaluatedKey` from the previous response to continue the scan:

```json
{
  "TableName": "LedgerEntries",
  "LastEvaluatedKey": "0197657f8f0772c1917be0db",
  "Limit": 100
}
```

`GetStreamRecords` returns DynamoDB-style stream records. `NewImage` and
`OldImage` are controlled by the table's `StreamSpecification.StreamViewType`.

### Read a Table Stream with DynamoDB Streams Iterators

Applications that already use DynamoDB Streams can also use
`GetShardIterator` and `GetRecords`.

To replay from the oldest retained record:

```json
{
  "StreamArn": "arn:aws:dynamodb:local:000000000000:table/LedgerEntries/stream/aux",
  "ShardId": "shardId-000000000000",
  "ShardIteratorType": "TRIM_HORIZON"
}
```

To start at the current end of the table stream for tailing new changes:

```json
{
  "StreamArn": "arn:aws:dynamodb:local:000000000000:table/LedgerEntries/stream/aux",
  "ShardId": "shardId-000000000000",
  "ShardIteratorType": "LATEST"
}
```

Use the returned `ShardIterator` with `DynamoDBStreams_20120810.GetRecords`.
This path follows DynamoDB Streams semantics: it is for forward consumption from
the chosen position. Use the aux-storage stream read extension when a UI or
audit tool needs to page backward from the newest retained change.

### Read Backward from the Most Recent Change

Aux-storage stream clients can read table and item streams directly with an
explicit direction. Use `Direction: "backward"` and omit `PageToken` to start at
the newest retained record.

```json
{
  "StreamName": "LedgerEntries/stream-table",
  "Direction": "backward",
  "Limit": 50
}
```

Example response:

```json
{
  "Items": [
    {
      "ItemId": "0197658b4f587e4bbbcf3221",
      "Timestamp": "2026-06-09T10:31:12.408Z",
      "DataType": "stream_pointer",
      "Data": {
        "table_name": "LedgerEntries",
        "item_stream_name": "LedgerEntries/stream-item/account_id=S:ACCOUNT#acme/entry_id=S:ENTRY#2026-06-09#0001",
        "item_stream_version": 4
      }
    }
  ],
  "NextToken": "0197658b4f587e4bbbcf3221",
  "HasMore": true
}
```

Use `NextToken` as the next `PageToken` to continue moving backward:

```json
{
  "StreamName": "LedgerEntries/stream-table",
  "Direction": "backward",
  "PageToken": "0197658b4f587e4bbbcf3221",
  "Limit": 50
}
```

This pattern is useful for operator consoles and support tools that show the
most recent changes first.

### Read an Item Audit History

To show the full history for a single item, read its table item stream. Omit
`PageToken` and use `Direction: "forward"` to start at the oldest retained
version:

```json
{
  "StreamName": "LedgerEntries/stream-item/account_id=S:ACCOUNT#acme/entry_id=S:ENTRY#2026-06-09#0001",
  "Direction": "forward",
  "Limit": 100
}
```

Use `Direction: "backward"` to show the newest version first:

```json
{
  "StreamName": "LedgerEntries/stream-item/account_id=S:ACCOUNT#acme/entry_id=S:ENTRY#2026-06-09#0001",
  "Direction": "backward",
  "Limit": 25
}
```

Example response:

```json
{
  "Items": [
    {
      "ItemId": "000000000000000000000004",
      "Timestamp": "2026-06-09T10:31:12.408Z",
      "DataType": "dynamodb_json",
      "Data": {
        "account_id": { "S": "ACCOUNT#acme" },
        "entry_id": { "S": "ENTRY#2026-06-09#0001" },
        "amount_cents": { "N": "12900" },
        "status": { "S": "reviewed" }
      }
    },
    {
      "ItemId": "000000000000000000000003",
      "Timestamp": "2026-06-09T10:20:44.020Z",
      "DataType": "dynamodb_json",
      "Data": {
        "account_id": { "S": "ACCOUNT#acme" },
        "entry_id": { "S": "ENTRY#2026-06-09#0001" },
        "amount_cents": { "N": "12900" },
        "status": { "S": "posted" }
      }
    }
  ],
  "NextToken": "000000000000000000000003",
  "HasMore": true
}
```

Each item stream entry is one retained version of the item. For a human-facing
audit view, sort or page by the stream order and compare adjacent versions to
show field-level changes over time.

### Deleted Items

An item stream can still be read after the base item has been deleted, as long
as the stream history is still within the effective retention window. This is
the main difference between an item stream and `GetItem`: `GetItem` reads only
the current base-table image, while the item stream reads retained historical
versions.

A delete writes a delete marker into the item stream. In a table stream response
using `NEW_AND_OLD_IMAGES`, the delete appears with `OldImage` and no
`NewImage`:

```json
{
  "Keys": {
    "account_id": { "S": "ACCOUNT#acme" },
    "entry_id": { "S": "ENTRY#2026-06-09#0001" }
  },
  "SequenceNumber": "019765912a4e712fb2f04a99",
  "OldImage": {
    "account_id": { "S": "ACCOUNT#acme" },
    "entry_id": { "S": "ENTRY#2026-06-09#0001" },
    "amount_cents": { "N": "12900" },
    "status": { "S": "reviewed" }
  }
}
```

In a direct item stream read, the delete appears as a typed delete marker:

```json
{
  "ItemId": "000000000000000000000005",
  "Timestamp": "2026-06-09T10:40:02.100Z",
  "DataType": "delete_marker",
  "Data": null
}
```

Audit tools should treat this as the point where the item stopped existing in
the base table. Older retained versions can still be displayed before that
marker.

### Performance and Storage Implications

Audit stream reads are ordinary paged reads over retained stream data:

- Reading from the table stream is best for broad event timelines.
- Reading from an item stream is best for focused audit history for one key.
- Backward reads are useful for UI timelines because they avoid scanning from
  the beginning only to display the latest changes.
- Long retention windows increase the amount of history available to these
  reads and should be included in storage and replay planning.
- A deleted item's history remains available only until stream trimming removes
  it under the effective table and item retention policy.
