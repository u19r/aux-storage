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

## Ordered Item Indexers

`Indexers` is an aux-storage write extension that makes selected top-level
string attributes directly addressable by provider read plans. It is not a
DynamoDB secondary index: it does not create a new query surface or change the
logical item. `GetItem`, `Query`, streams, expressions, projections, billing,
replication, and backfill continue to see ordinary DynamoDB attributes.

Set the table's maximum declaration length when creating it:

```json
{
  "TableName": "ApplicationData",
  "AttributeDefinitions": [
    { "AttributeName": "pk", "AttributeType": "S" },
    { "AttributeName": "sk", "AttributeType": "S" }
  ],
  "KeySchema": [
    { "AttributeName": "pk", "KeyType": "HASH" },
    { "AttributeName": "sk", "KeyType": "RANGE" }
  ],
  "BillingMode": "PAY_PER_REQUEST",
  "MaxIndexers": 4
}
```

`MaxIndexers` defaults to `0` and may be increased, up to `32`, with
`UpdateTable`. It cannot be decreased. Increasing it updates the base table and
every GSI atomically from the API's perspective.

Each put may declare a different ordered list of attribute names:

```json
{
  "TableName": "ApplicationData",
  "Item": {
    "pk": { "S": "tenant#42" },
    "sk": { "S": "order#7" },
    "customer_id": { "S": "customer#9" },
    "related_pk": { "S": "entity#42#sub_model#7#v1" }
  },
  "Indexers": ["customer_id", "related_pk", "optional_id"]
}
```

The declaration belongs to this item, not the table. Ordinal `0` means
`customer_id` for this row, but another row may use a different name at ordinal
`0`. A declared value must be a non-empty top-level `S`; an absent attribute is
valid and occupies a null slot. Empty strings and every non-string DynamoDB
type are rejected.

For `PutItem`, batch puts, and transactional puts, omitting `Indexers` means an
empty declaration. For `UpdateItem` and transactional updates:

- omitting `Indexers` preserves the stored names and order and recomputes their
  values from the updated logical item;
- supplying a list replaces the complete declaration; and
- supplying `[]` clears the declaration while preserving the attributes in the
  logical item.

Batch puts store `Indexers` inside each `PutRequest`. Transactional puts and
updates store it inside their respective `Put` or `Update` action. Deletes and
condition checks do not accept it.

### Declare Indexers on Rust Entities

Rust entity code should declare indexers on fields instead of repeating wire
attribute names and ordinals at each write and read site:

```rust
use serde::Serialize;
use storage::{
    derive::{SingleTableKeys, WireItemEncode},
    types::TimestampMillis,
};

#[derive(Serialize, SingleTableKeys, WireItemEncode)]
#[single_table(
    entity_type = "ORDER",
    pk_lit = "ORDER",
    sk_expr = "format!(\"ORDER#{}\", self.order_id)"
)]
struct Order {
    order_id: String,

    #[single_table(indexer = 0)]
    customer_id: String,

    #[single_table(indexer = 1)]
    related_pk: Option<String>,

    updated_at: TimestampMillis,
}
```

Ordinals must be unique and contiguous from `0`. The derive rejects gaps,
duplicates, more than 32 fields, and duplicate effective wire names. If a field
uses `#[wire_item(rename = "...")]`, `#[serde(rename = "...")]`, or a supported
struct `rename_all`, the generated declaration uses that wire name.

`SingleTableKeys` generates the canonical declaration and a typed accessor for
each indexed field. In this example, `Order::customer_id_indexer()` returns
`customer_id` at ordinal `0`, while `Order::related_pk_indexer()` returns
`related_pk` at ordinal `1`. The accessor owns both facts, so application code
does not repeat either one.

Use the entity write helpers for individual puts and updates. They always apply
the generated declaration:

```rust
db.put_item_entity_encode(
    PutItemEntityEncodeInput::builder()
        .table_name(table_name.clone())
        .item(&order)
        .build(),
).await?;

db.update_item_entity::<Order>(update).await?;
```

For a batch or transaction, encode the same typed envelope and place it in the
put action. The envelope keeps the item and its declaration together through
retry and provider routing:

```rust
let item = storage::types::single_table_entity::to_wire_entity(&order)?;
let put = storage::types::EncodePutRequest::builder()
    .item(item)
    .build();
```

`TransactEncodePutRequest` accepts the same `WireEntity`. Raw encoded items must
opt out explicitly with `WireEntity::unindexed(wire_item)`; use that only when
the item intentionally has no declaration.

FoundationDB stores indexed strings in ordered Tuple value slots. SQL stores
them in nullable `__aux_indexer_n` columns, and RocksDB uses the same versioned
logical envelope. These representations are internal. Existing data written
with the superseded value layout is deliberately unsupported; reset and
recreate development tables when deploying this breaking format.

## ReadSequence

`ReadSequence` is an aux-storage DynamoDB JSON protocol extension for bounded
N+1 read workflows. It validates a directed acyclic graph of `Get`, `BatchGet`,
and `Query` nodes, lets nodes bind typed values selected from earlier results,
and returns flat node and invocation results with explicit input references.

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

The request body contains a top-level `ReadConsistency` value and a `Nodes`
array. Node array order is only a stable request ordinal; `After` and `Inputs`
define execution dependencies, so a node may refer to a node declared later in
the array. `EVENTUAL` is the default consistency mode.

### Example: Read a User and Its Organization

```json
{
  "ReadConsistency": "EVENTUAL",
  "Nodes": [
    {
      "Name": "user",
      "Operation": {
        "Get": {
          "TableName": "Users",
          "Key": {
            "pk": { "S": "user#1" }
          }
        }
      },
      "Inputs": {},
      "After": []
    },
    {
      "Name": "org",
      "Operation": {
        "Get": {
          "TableName": "Organizations",
          "Key": {
            "pk": { "FromInput": "org_id" }
          }
        }
      },
      "Inputs": {
        "org_id": {
          "From": {
            "Node": "user",
            "Select": "$.Get.Item.org_id"
          },
          "Cardinality": "ONE",
          "OnMissing": "ERROR"
        }
      },
      "After": []
    }
  ],
  "Outputs": ["user", "org"]
}
```

The second node depends on `user` through its typed `org_id` input. The
`FromInput` marker is replaced with the selected DynamoDB value before the
read. `After` may add an ordering dependency that does not bind data. The
response contains one flat result entry per output node; each invocation
records the input references that produced it.

#### Input Binding Forms

Use `FromInput` when the selected DynamoDB value should replace the complete
attribute value. It preserves the selected type, so an `N` remains an `N`, a
`B` remains a `B`, and an `S` remains an `S`:

```json
"pk": { "FromInput": "organization_id" }
```

Use `StringTemplate` when a string key or expression value combines fixed text
with one or more declared string inputs:

```json
"pk": {
  "StringTemplate": "entity#{id}#sub_model#{sub_id}#v1"
}
```

Each `{name}` placeholder refers to an entry in the node's `Inputs` object.
The example above resolves `id = "42"` and `sub_id = "7"` to the DynamoDB
string value `entity#42#sub_model#7#v1`. A placeholder may appear more than
once, and one template may combine inputs selected from different earlier
nodes. The node still has one `Iterate` input: that `MANY` value changes for
each invocation, while every other input uses its single selected value.

`StringTemplate` always produces an `S` attribute and accepts only selected
`S` values. It does not coerce `N`, `B`, Boolean, collection, or null values to
text. A template must contain at least one placeholder; placeholder names use
ASCII letters, numbers, and underscores, and literal braces are not supported.
Malformed templates, undeclared placeholders, missing `ERROR` inputs, and
non-string values fail rather than producing an ambiguous key.

Templates work anywhere `ReadSequence` can bind an attribute value: `Get` and
`BatchGet` keys, `Query.ExpressionAttributeValues`, a concrete
`ExclusiveStartKey`, and values nested inside `L` or `M`. Backend whole-plan
optimizations accept only shapes whose physical keys prove the same binding.

For example, a dependent query can compose an index partition key without
copying the complete value into its source item:

```json
"ExpressionAttributeValues": {
  ":entity": {
    "StringTemplate": "entity#{entity_id}"
  }
}
```

### Example: Compose and Fan Out Single-Table Keys

This example queries one entity's GSI rows, then exposes each matching item
through a dependent `Get` result. On FoundationDB, the dependent result comes
from the stored GSI projection; it does not fetch the base-table row. Configure
the index projection with every attribute the child returns.

```json
{
  "ReadConsistency": "EVENTUAL",
  "MaxFanoutPerStep": 25,
  "MaxTotalReadItems": 100,
  "Nodes": [
    {
      "Name": "sub_models",
      "Operation": {
        "Query": {
          "TableName": "ApplicationData",
          "IndexName": "by_entity",
          "KeyConditionExpression": "gsi1pk = :entity",
          "ExpressionAttributeValues": {
            ":entity": { "S": "entity#42" }
          }
        }
      },
      "Inputs": {},
      "After": []
    },
    {
      "Name": "versioned_sub_model",
      "Operation": {
        "Get": {
          "TableName": "ApplicationData",
          "Key": {
            "pk": {
              "StringTemplate": "entity#{entity_id}#sub_model#{sub_id}#v1"
            }
          }
        }
      },
      "Inputs": {
        "entity_id": {
          "From": {
            "Node": "sub_models",
            "Select": "$.Query.Items[0].entity_id"
          },
          "Cardinality": "ONE",
          "OnMissing": "ERROR"
        },
        "sub_id": {
          "From": {
            "Node": "sub_models",
            "Select": "$.Query.Items[*].sub_id"
          },
          "Cardinality": "MANY",
          "OnMissing": "SKIP"
        }
      },
      "Iterate": "sub_id",
      "After": []
    }
  ],
  "Outputs": ["sub_models", "versioned_sub_model"]
}
```

For each `sub_id`, `versioned_sub_model` receives the same scalar `entity_id`
and the current iterated `sub_id`. If the query returns `7` and `9`, the two
point reads use `entity#42#sub_model#7#v1` and
`entity#42#sub_model#9#v1`. Each result's `InputRefs` identifies both source
item ordinals: `entity_id` refers to item `0`, while `sub_id` refers to the
current iterated item.

For this example, each GSI row must describe the same base item that the
template names. A representative projected GSI item is:

```json
{
  "pk": { "S": "entity#42#sub_model#7#v1" },
  "gsi1pk": { "S": "entity#42" },
  "entity_id": { "S": "42" },
  "sub_id": { "S": "7" }
}
```

Configure the GSI projection to include `entity_id`, `sub_id`, and every child
result attribute. DynamoDB GSI projections already include the base-table key
`pk`. Use `ProjectionType: ALL` when the child omits `ProjectionExpression` and
therefore requests the complete item. With `KEYS_ONLY` or `INCLUDE`, the child
must explicitly project only attributes present in the index. aux-storage
returns a validation error instead of reading the base item to fill a gap.

#### FoundationDB Mapped Execution

FoundationDB's mapped-range language substitutes complete Tuple elements; it
does not concatenate fragments inside one element. aux-storage can source a
target key element from a physical parent key Tuple element or from one
declared item indexer. Public indexer ordinal `n` compiles to FoundationDB value
slot `{V[2+n]}`; Tuple value slots `0` and `1` hold the format header and
residual payload.

Name and ordinal are both required on the dependent input:

```json
"customer": {
  "From": {
    "Node": "orders",
    "Select": "$.Query.Items[*].customer_id"
  },
  "MappedKeySource": {
    "AttributeName": "customer_id",
    "Indexer": 0
  },
  "Cardinality": "MANY",
  "OnMissing": "SKIP"
}
```

The ordinal locates the physical slot. `AttributeName` verifies the row-owned
declaration before any child is published, preventing one entity type from
accidentally using another type's value at the same ordinal. SQLite, Turso, and
PostgreSQL apply the same check to `__aux_indexer_0`.

In Rust, use the generated accessor to construct both the selector and mapped
source consistently:

```rust
let input = Order::customer_id_indexer()
    .many_from_query("orders", ReadSequenceOnMissing::Skip);
```

`many_from_query` selects `$.Query.Items[*].customer_id`, sets cardinality to
`MANY`, and emits the matching `MappedKeySource`. Use `one_from_query` for
`$.Query.Items[0].customer_id` and `ONE` cardinality. This is the preferred
construction path for entity-owned indexers; the JSON form remains available
for non-Rust clients.

The Rust request types default every optional limit and omit absent `Inputs`
and `After` fields. Use `ReadSequenceNode::new` for an independent node and the
builder when a dependent node has inputs:

```rust
use std::collections::BTreeMap;
use storage::types::{
    GetItemRequest, KeyAttributes, ReadSequenceNode, ReadSequenceNodeOperation,
    ReadSequenceOnMissing, ReadSequenceRequest, TableName,
    read_sequence_input_marker,
};
use storage_api::StorageApiManager;

let related_pk_input = Order::related_pk_indexer()
    .many_from_query("orders", ReadSequenceOnMissing::Skip);

let invoices = ReadSequenceNode::builder()
    .name("invoices")
    .operation(ReadSequenceNodeOperation::Get(GetItemRequest::new(
        TableName::new("Invoices"),
        KeyAttributes::from([(
            "pk".to_string(),
            read_sequence_input_marker("related_pk"),
        )]),
    )))
    .inputs(BTreeMap::from([(
        "related_pk".to_string(),
        related_pk_input,
    )]))
    .iterate("related_pk")
    .build();

let request = ReadSequenceRequest {
    nodes: vec![
        ReadSequenceNode::new(
            "orders",
            ReadSequenceNodeOperation::Query(orders_query),
        ),
        invoices,
    ],
    outputs: Some(vec!["orders".to_string(), "invoices".to_string()]),
    max_fanout_per_step: Some(25),
    max_total_read_items: Some(100),
    ..Default::default()
};

let response = manager.read_sequence(request).await?;
```

`ReadSequenceNode` does not implement `Default`: a node without a name and
operation is not valid. Its constructor and builder default only the genuinely
optional fields while keeping those required values compile-time mandatory.

For a composite relationship key, write the complete key into the indexed
field when constructing the entity:

```rust
let order = Order {
    related_pk: Some(format!(
        "entity#{id}#sub_model#{sub_id}#v1"
    )),
    // remaining fields
};
```

Then bind the complete value directly rather than asking the FoundationDB
mapper to concatenate fragments:

```json
"Key": {
  "pk": { "FromInput": "related_pk" }
},
"Inputs": {
  "related_pk": {
    "From": {
      "Node": "orders",
      "Select": "$.Query.Items[*].related_pk"
    },
    "MappedKeySource": {
      "AttributeName": "related_pk",
      "Indexer": 1
    },
    "Cardinality": "MANY",
    "OnMissing": "SKIP"
  }
},
"Iterate": "related_pk"
```

In Rust, `Order::related_pk_indexer().many_from_query("orders", ... )`
constructs that input metadata without repeating `related_pk` or ordinal `1`.

For the same-item example above, the GSI Tuple already contains the complete
base key and the GSI value contains the configured projection. aux-storage
evaluates the public `StringTemplate`, requires it to equal that physical base
key, and materializes the child from the GSI value. The provider uses one
primary range read and performs no secondary base-table read.

For a child in another table, FoundationDB copies the selected source key's
complete type/value Tuple pairs into a mapper containing the resolved target
table ID. Both source and target may use hash-only or hash+range keys. The
target table name is constant metadata; inputs cannot choose a table.

The example above uses FoundationDB mapped execution when all of these
conditions hold:

- the parent is an eventual, unbounded base-table or GSI `Query`;
- the Query key condition resolves to a physical hash or hash+range prefix;
- direct inputs select physical source-key attributes, a `StringTemplate`
  reconstructs a complete physical source-key attribute, or one direct input
  names a verified `MappedKeySource` indexer;
- a `MANY` + `SKIP` iterate input uses
  `$.Query.Items[*].attribute`, or a non-iterated `ONE` input uses
  `$.Query.Items[0].attribute`;
- the child is a point `Get` whose complete hash or hash+range key is derived
  from those physical source-key attributes;
- `FilterExpression`, `ProjectionExpression`, `AttributesToGet`, reverse order,
  and a complete `ExclusiveStartKey` are evaluated with normal Query/Get
  semantics; and
- a same-item GSI child requests only attributes stored in the GSI projection.

Mapped execution still rejects a Query `Limit`, legacy `QueryFilter` or
`ConditionalOperator`, consumed-capacity results, strong/transactional reads,
dynamic tables, and target keys that require combining several item values
inside one Tuple element. Those valid graph shapes use ordinary DAG execution.
A native mapped page whose single FoundationDB `more` bit cannot prove complete
secondary results is also discarded and retried through the ordinary DAG.

`StringTemplate` does not make FoundationDB concatenate mapper fragments. In
`entity#{id}#sub_model#{sub_id}#v1`, the composed `pk` must already be one
complete physical source-key element. The projected `id` and `sub_id` values
prove that the public binding names the same key before results are published.
The public template always works through ordinary DAG execution. To make that
relationship eligible for a value-based mapped lookup, write the complete key
once as a non-empty string such as `related_pk =
entity#42#sub_model#7#v1`, include `related_pk` in `Indexers`, bind the child key
with `FromInput`, and declare its `MappedKeySource` name and ordinal. This avoids
parsing the residual payload and lets FoundationDB copy one complete Tuple
element.

An absent indexed attribute is stored as Tuple Nil and returns an empty child
association without a point read. FoundationDB 7.4.5 reports error 2030 when a
row's declaration is shorter than the requested `{V[n]}` slot. aux-storage
treats only that error as an optimization miss, discards the complete mapped
attempt, and reruns the validated graph through ordinary reads. A slot whose
declaration names another attribute or whose returned child key does not match
the compiled target follows the same whole-attempt fallback. Malformed stored
headers, payloads, or markers remain corruption errors.

Mapped ranges use serializable reads with FoundationDB read-your-writes support
enabled. The 7.4.5 client rejects mapped ranges in snapshot mode or when
read-your-writes is disabled. ReadSequence does not expose those invalid
transaction modes.

On the measured composite-key fixture, one public ReadSequence request replaced
an average 5.50 standard DynamoDB Query/Get/BatchGet HTTP calls. Native mapped
execution achieved 670.3 requests/s at 4.23 ms p95; client composition achieved
313.7 requests/s at 21.18 ms p95, with zero errors in both runs. These figures
describe the local 100-item benchmark, not a production latency guarantee. See
the [ordered indexer benchmark](../docs/benchmarks/indexers-20260810.md) for the
fixture, commands, provider counters, and interpretation.

For a GSI parent, aux-storage reads only the stored GSI projection and never
follows the entry to the base item. A child without `ProjectionExpression`
requires an `ALL` projection. With `KEYS_ONLY` or `INCLUDE`, every selected
input, filter attribute, and requested child attribute must be covered by the
GSI; otherwise the request fails rather than silently fetching the base row.

### Example: Map Composite Keys Across Tables

This FoundationDB-eligible example queries a base table in reverse, resumes
after a complete hash+range key, filters source rows client-side, and maps the
two physical source-key attributes directly to a differently named composite
key in another table:

```json
{
  "ReadConsistency": "EVENTUAL",
  "Nodes": [
    {
      "Name": "events",
      "Operation": {
        "Query": {
          "TableName": "Events",
          "KeyConditionExpression": "pk = :pk",
          "FilterExpression": "enabled = :enabled",
          "ProjectionExpression": "pk, sk",
          "ExpressionAttributeValues": {
            ":pk": { "S": "tenant#42" },
            ":enabled": { "BOOL": true }
          },
          "ScanIndexForward": false,
          "ExclusiveStartKey": {
            "pk": { "S": "tenant#42" },
            "sk": { "S": "event#900" }
          }
        }
      },
      "Inputs": {},
      "After": []
    },
    {
      "Name": "archive",
      "Operation": {
        "Get": {
          "TableName": "EventArchive",
          "Key": {
            "account": { "FromInput": "partition" },
            "event": { "FromInput": "sort" }
          },
          "ProjectionExpression": "account, event, payload"
        }
      },
      "Inputs": {
        "partition": {
          "From": {
            "Node": "events",
            "Select": "$.Query.Items[0].pk"
          },
          "Cardinality": "ONE",
          "OnMissing": "ERROR"
        },
        "sort": {
          "From": {
            "Node": "events",
            "Select": "$.Query.Items[*].sk"
          },
          "Cardinality": "MANY",
          "OnMissing": "SKIP"
        }
      },
      "Iterate": "sort",
      "After": []
    }
  ],
  "Outputs": ["events", "archive"]
}
```

`Events.pk` and `Events.sk` are physical source-key attributes. The mapper
copies their complete Tuple type/value pairs to `EventArchive.account` and
`EventArchive.event`; the different attribute names do not matter because the
declared inputs make the mapping explicit. The source filter runs before
fan-out, source and child projections run before publication, and input item
ordinals refer to the filtered source item order.

### Example: Query Orders and Fetch Dependent Invoices

```json
{
  "ReadConsistency": "EVENTUAL",
  "MaxFanoutPerStep": 25,
  "MaxTotalReadItems": 100,
  "Nodes": [
    {
      "Name": "orders",
      "Operation": {
        "Query": {
          "TableName": "Orders",
          "IndexName": "by_customer",
          "KeyConditionExpression": "customer_id = :customer_id",
          "ExpressionAttributeValues": {
            ":customer_id": { "S": "cust#123" }
          },
          "ProjectionExpression": "pk, sk, invoice_id, status",
          "Limit": 25
        }
      },
      "Inputs": {},
      "After": []
    },
    {
      "Name": "invoice",
      "Operation": {
        "Get": {
          "TableName": "Invoices",
          "Key": {
            "pk": { "FromInput": "invoice_id" },
            "sk": { "S": "meta" }
          },
          "ProjectionExpression": "pk, sk, total, status"
        }
      },
      "Inputs": {
        "invoice_id": {
          "From": {
            "Node": "orders",
            "Select": "$.Query.Items[*].invoice_id"
          },
          "Cardinality": "MANY",
          "OnMissing": "SKIP"
        }
      },
      "Iterate": "invoice_id",
      "After": []
    }
  ],
  "Outputs": ["orders", "invoice"]
}
```

`Cardinality: MANY` with `Iterate` creates one bounded child invocation per
selected order. `OnMissing: SKIP` omits missing child invocations; use `NULL`
for an explicit null binding or `ERROR` to reject the request. This collapses a
common query-plus-child-get workflow into one bounded graph request without an
implicit Cartesian product.

Here `invoice_id` is a projected non-key value and the child also adds a
literal sort key, so this relationship deliberately uses ordinary DAG
execution. Model both target key components in physical source-key attributes
when this relationship needs FoundationDB mapped execution.

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

#### Standard DynamoDB Remote Execution

Send `ReadSequence` to aux-storage even when its remote provider points at an
AWS DynamoDB table or another service that implements only the standard
DynamoDB API. aux-storage plans the graph locally; it does not forward the
custom `DynamoDB_20120810.ReadSequence` target to the remote endpoint.

The fallback uses the ordinary API operations represented by the graph:

- a single point read uses `GetItem`;
- multiple dependent `Get` invocations for one node are deduplicated into
  standard `BatchGetItem` requests of at most 100 keys each;
- an explicit `BatchGet` node remains a standard `BatchGetItem` request; and
- each dependent `Query` invocation becomes a standard `Query` request, so two
  selected partition keys produce two remote Query calls.

BatchGet responses are unordered. aux-storage matches each returned item by
the table's complete hash or hash+range key, restores invocation order, then
applies the child `Get` projection. Repeated dependent keys within each
100-key request chunk are sent once and may still produce repeated ordered
invocations. A malformed remote response
that omits key attributes or returns an unrequested key fails instead of being
joined to the wrong parent.

This fallback supports the same input selectors, `FromInput`,
`StringTemplate`, `ONE`, `MANY`, projections, filters, base tables, GSIs, and
multiple table names as ordinary local DAG execution. The remote tables do not
need aux-storage's `MaxIndexers` metadata or indexed value layout. Provider-only
optimizations such as FoundationDB mapped ranges are simply unavailable; they
do not change the public request syntax.

A remote GSI read uses only the item projected into that GSI. aux-storage does
not follow the GSI entry with a base-table read. Configure the GSI to project
every attribute used by selectors, filters, projections, and child operations;
an unprojected filter or projection fails before the Query, and a missing child
input follows its explicit `OnMissing` rule. Use `OnMissing: ERROR` when the
child cannot be correct without that projected value.

### Pagination and Tokens

`ReadSequence` can stop before the logical graph is exhausted when it reaches a
root page, child page, fanout boundary, response byte limit, total read limit,
or backend transaction budget. In those cases the response includes
`NextSequenceToken`.

Treat `NextSequenceToken` as opaque. Tokens are tied to the request shape and
service state needed for safe continuation. Stale, mismatched, or tampered
tokens fail validation instead of producing incomplete or duplicated results.

Remote `BatchGetItem` partial responses are never published as complete joined
data. aux-storage retries only `UnprocessedKeys` with bounded exponential
backoff and merges successful responses. If keys remain after four attempts,
the sequence fails with a retryable throttling error containing the remaining
key count.

### Backend Availability

`ReadSequence` is capability-gated by backend configuration:

| Backend      | Eventual and strong base-table reads | Transactional snapshots | Transactional GSI reads |
| ------------ | ------------------------------------ | ----------------------- | ----------------------- |
| SQLite       | Supported.                           | Supported for file-backed SQLite. | Supported only with immediate GSI consistency. |
| Postgres     | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| Turso        | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| RocksDB / KV | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| FoundationDB | Supported.                           | Supported.              | Supported only with immediate GSI consistency. |
| Remote       | Supported through standard DynamoDB calls; the custom target is not forwarded. | Rejected.               | Rejected.               |

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
