# DynamoDB compatibility matrix

Last updated: 2026-05-22

Legend: ✅ supported | 🟡 partial or different | ❌ unsupported

## Overall status

| Area                                       | Status | Unsupported / differences                                                                                                          |
| ------------------------------------------ | ------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| DynamoDB JSON HTTP endpoint                | ✅     |                                                                                                                                    |
| AWS SDK-style request bodies               | ✅     |                                                                                                                                    |
| SQLite backend                             | ✅     |                                                                                                                                    |
| Turso/libSQL backend                       | ✅     |                                                                                                                                    |
| Postgres backend                           | ✅     |                                                                                                                                    |
| RocksDB backend                            | ✅     |                                                                                                                                    |
| FoundationDB backend                       | ✅     |                                                                                                                                    |
| Remote provider mode                       | ✅     |                                                                                                                                    |
| `CreateTable`                              | 🟡     | LSI, capacity, resource policy, SSE/KMS, tags, table class, and deletion protection fields do not provide AWS-managed behavior.    |
| `ListTables`                               | ✅     |                                                                                                                                    |
| `DescribeTable`                            | 🟡     | AWS size/count, billing, control-plane, and some stream metadata are omitted or best effort.                                       |
| `DeleteTable`                              | ✅     |                                                                                                                                    |
| `UpdateTable`                              | 🟡     | Capacity, deletion protection, SSE/KMS, table class, and non-GSI attribute definition changes do not provide AWS-managed behavior. |
| `PutItem`                                  | 🟡     | Legacy condition fields, consumed capacity, and item collection metrics differ.                                                    |
| `GetItem`                                  | 🟡     | Consumed-capacity accounting differs.                                                                                              |
| `DeleteItem`                               | 🟡     | Legacy condition fields, consumed capacity, and item collection metrics differ.                                                    |
| `UpdateItem`                               | 🟡     | Legacy update/condition fields, consumed capacity, and item collection metrics differ.                                             |
| `Query`                                    | 🟡     | Legacy query filters, legacy projection fields, and consumed-capacity accounting differ.                                           |
| `Scan`                                     | 🟡     | Legacy scan filters and consumed-capacity accounting differ.                                                                       |
| `BatchWriteItem`                           | 🟡     | AWS throttling, consumed capacity, and item-collection metrics differ.                                                             |
| `BatchGetItem`                             | 🟡     | Consumed-capacity reporting is best effort.                                                                                        |
| `TransactWriteItems`                       | 🟡     | Consumed capacity and item-collection metrics differ.                                                                              |
| `TransactGetItems`                         | 🟡     | Consumed-capacity reporting is best effort.                                                                                        |
| `UpdateTimeToLive`                         | ✅     |                                                                                                                                    |
| `DescribeTimeToLive`                       | ✅     |                                                                                                                                    |
| `GetStreamRecords`                         | 🟡     | Simplified API. Not full DynamoDB Streams compatibility.                                                                           |
| Global secondary indexes                   | ✅     | Optional immediate consistency mode                                                                                                |
| Local secondary indexes                    | ❌     | Real LSI semantics are not implemented.                                                                                            |
| Condition expressions                      | ✅     |                                                                                                                                    |
| Update expressions                         | ✅     |                                                                                                                                    |
| Key-condition expressions                  | ✅     |                                                                                                                                    |
| Filter expressions                         | ✅     |                                                                                                                                    |
| Projection expressions                     | ✅     |                                                                                                                                    |
| Strongly consistent reads                  | ✅     | Possible consistent GSI reads as well                                                                                              |
| Pagination markers                         | ✅     |                                                                                                                                    |
| Table streams                              | 🟡     | Not the AWS DynamoDB Streams API.                                                                                                  |
| TTL expiry behavior                        | ✅     |                                                                                                                                    |
| Consumed capacity reporting                | 🟡     | Values are estimates, not AWS billing-equivalent accounting.                                                                       |
| Item collection metrics                    | 🟡     | Complete AWS item-collection metrics are not implemented.                                                                          |
| Provisioned/on-demand capacity enforcement | ❌     | Capacity fields are compatibility inputs, not billing or throttling controls.                                                      |
| PartiQL                                    | ❌     | `ExecuteStatement` and `BatchExecuteStatement` are not implemented.                                                                |
| Continuous backups / PITR                  | ❌     | `UpdateContinuousBackups` returns an unsupported validation error. Backups and PITR available via backend provider.                |
| Export/import APIs                         | ❌     | DynamoDB export/import control-plane APIs are not implemented.                                                                     |
| IAM / SigV4 / account isolation            | ❌     | Auth and account-management services are outside the compatibility surface.                                                        |
| KMS / SSE management                       | ❌     | AWS KMS/SSE behavior is not implemented.                                                                                           |
| Tags as AWS resource management            | ❌     | AWS resource tagging behavior is not implemented.                                                                                  |
| CloudWatch / AWS service events            | 🟡     | CloudWatch metrics and AWS service events are not implemented. Uses prometheus /metrics endpoint                                   |
| Global tables                              | 🟡     | Control-plane API is unique                                                                                                        |
