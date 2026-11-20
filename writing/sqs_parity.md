# SQS compatibility matrix

Last updated: 2026-11-13

Legend: ✅ supported | 🟡 partial or different | ❌ unsupported

Method: this matrix was written by reading the code and the tests named in it. No row claims live AWS
verification unless it says so. The recorded live AWS probes are the eight in
[docs/sqs-error-parity.md](../docs/sqs-error-parity.md) (`eu-central-1`, 2026-05-21; marked "verified against
AWS") and probes 1 to 13 plus the run 2 extras in [docs/sqs-sns-aws-probes.md](../docs/sqs-sns-aws-probes.md)
(`eu-central-1`, 2026-11-09 and 2026-11-10; cited as "probe N"). Everything else is marked "from AWS
documentation, not probed". Where a note says "not verified", the code was read but the AWS behaviour or the
Aux behaviour was not exercised.

Query SDK check (2026-11-10): botocore 1.31.80 (the last SQS model that speaks the Query protocol) ran
[scripts/sqs-query-sdk-check.py](../scripts/sqs-query-sdk-check.py) against the assembled `/queue` route
(ignored test `storage-api` `query_sdk_tests`). Its real XML parser read `CreateQueue` with attributes and
tags, `SendMessageBatch` with String and Binary message attributes (body and attribute MD5s), `ReceiveMessage`
with `All` system and message attributes (botocore checks `MD5OfBody` and `MD5OfMessageAttributes`; a body
with `\r\n` came back unchanged), `GetQueueAttributes` (`All` and no names), `ListQueues` (one URL and an
empty result), `ListQueueTags`, `DeleteMessage`, `DeleteQueue` and a `NonExistentQueue` error (code, status
400, `Type` `Sender`). Every check passed with no change to the code.

Source map: request types and validation in [queue-provider](../crates/queue-provider/src/),
domain logic in [queue-provider/src/queue_*.rs](../crates/queue-provider/src/), manager in
[queue/src/manager/mod.rs](../crates/queue/src/manager/mod.rs), HTTP route in
[storage-api/src/routes/queue/mod.rs](../crates/storage-api/src/routes/queue/mod.rs), wire protocols in
[queue-provider/src/wire](../crates/queue-provider/src/wire/). The standalone `queue` binary is deleted (W14).

## Overall status

| Area                                          | Status | Unsupported / differences                                                                                                                                                                                  |
| --------------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SQS JSON protocol endpoint (`x-amz-json-1.0`) | 🟡 | `POST /queue`, `POST /queue/` and `POST /queue/{account}/{queue_name}`. `X-Amz-Target: AmazonSQS.<Action>`. The body decodes once into the typed request (`queue_provider::wire`). Unknown members are ignored, as on AWS (probe 12); a misspelled member falls back to its default. Missing or unknown target: 400 `InvalidAction`. |
| SQS Query protocol requests | 🟡 | `application/x-www-form-urlencoded`, `Action=`. A static shape table maps `Attribute.N`, `Tag.N`, `TagKey.N`, `AttributeName.N`, `MessageAttributeName.N`, `MessageSystemAttributeName.N`, `MessageAttribute.N.*` and the batch `*RequestEntry.N.*` (with their attribute maps) to the JSON members, then the same typed decode and validation run. `Attribute.N.Name` without a value: 400 `MissingParameter`. A non-integer integer member: 400 `Value v for parameter P is invalid.`. Unknown parameters are ignored. Query by GET is not supported. |
| Query protocol responses | 🟡 | Typed XML per action: `<{Action}Response xmlns="http://queue.amazonaws.com/doc/2012-11-05/">`, `<{Action}Result>` only for actions with a result, `ResponseMetadata/RequestId`. Errors: `ErrorResponse/Error/{Type,Code,Message,Detail}`; `Code` keeps the `AWS.SimpleQueueService.` prefix (probe 6); `Type` is `Sender` for 4xx and `Receiver` for 5xx. The shapes of the actions in the Query SDK check (above) are verified with botocore's parser; the other shapes (the batch delete and visibility results, `ListDeadLetterSourceQueues`) are documentation-derived. |
| Queue URL addressing | 🟡 | A queue is found by its name, the last path segment, after the account check; the URL is never a storage key, so a URL that another node or an earlier base URL produced finds the queue. Responses build URLs as `{public_base_url}/{account_id}/{name}` from the answering node's setting. A bare name is also accepted. A segment before the name that is another account is 400 `NonExistentQueue` with no storage read. A body `QueueUrl` wins over the posted path (not probed). |
| SQLite, Turso, Postgres, RocksDB, FoundationDB | 🟡 | Queue, pubsub and transport conformance (`storage-rewrite-conformance`, filters `queue_`, `pubsub_`, `transport_`) pass on all five providers (W14, 2026-11-10). |
| Remote provider mode                          | 🟡     | Library mode only (used by aux-fn), not a service mode: the service rejects `queue.enabled` with a remote backend at startup. `queue-remote` forwards operations; the long poll is then performed by the remote server (`waits_with_local_probe` is false). Not verified beyond reading.                                                 |
| IAM / SigV4 / account isolation               | ❌     | No authentication in the queue route. Account id is a configured constant used only in the URL.                                                                                                            |
| `CreateQueue` | 🟡 | Attributes of the table below, including stored `Policy`, KMS and `SqsManagedSseEnabled`. `tags` are stored (shared tag rule); the existing queue keeps its tags on a repeated create; different requested tags are `QueueAlreadyExists`. Name clash: Query code `QueueAlreadyExists`, JSON `com.amazonaws.sqs#QueueNameExists`. No FIFO. |
| `DeleteQueue` | ✅ | A missing queue is 400 `AWS.SimpleQueueService.NonExistentQueue` (probe 6). |
| `ListQueues` | ✅ | `MaxResults` 1 to 1,000 (0 or absent: up to 1,000). `NextToken` only when `MaxResults` was given and queues remain. A bad token: 400 `Invalid or expired next token.` (probe 7). |
| `GetQueueUrl` | 🟡 | Missing-queue error verified against AWS. `QueueOwnerAWSAccountId` other than the configured account: 400 `NonExistentQueue` (not probed). |
| `GetQueueAttributes` | ✅ | No names or an empty list: `{}` after one catalog read (probe 7). Unknown name: 400 `InvalidAttributeName` `Unknown Attribute X.`. `All` adds `QueueArn`, the counts, `CreatedTimestamp`, `LastModifiedTimestamp`, the defaults and `SqsManagedSseEnabled` `true`. Stored values read back byte for byte. |
| `SetQueueAttributes` | 🟡 | Same attributes as `CreateQueue`. Read-only and unknown names alike: 400 `InvalidAttributeName` `Unknown Attribute X.` (suite scripts). A number out of range, or a `Policy` that is not a JSON object: 400 `InvalidAttributeValue` `Invalid value for the parameter X.`. An empty collection is `MissingParameter`. Empty `RedrivePolicy` or `Policy` removes it. Sets `LastModifiedTimestamp`. |
| `PurgeQueue`                                  | 🟡     | Implemented by retiring the queue incarnation. The AWS limit of one purge per 60 seconds and `PurgeQueueInProgress` are not implemented.                                                                   |
| `SendMessage` | 🟡 | `MessageSystemAttributes` with `AWSTraceHeader` (DataType `String`) is stored and answered with `MD5OfMessageSystemAttributes`; another name: 400 `Message system attribute name 'Foo' is invalid.` (probe 12). `MessageGroupId` is ignored. `MessageDeduplicationId`: 400 with the probe 12 text. |
| `SendMessageBatch` | 🟡 | Up to 10 entries. Total payload over 1,048,576 bytes gives `BatchRequestTooLong` with the AWS text. A bad body, delay, system or message attribute, an empty body (`EmptyValue`), or an entry over the queue `MaximumMessageSize` fails only that entry (`Failed`, `SenderFault` true). An entry id outside letters, digits, `-` and `_` (80 characters) is a whole-request `InvalidBatchEntryId`. A missing queue is a whole-request 400 `NonExistentQueue` (probe 6). Query entry message attributes are kept (C12). |
| `ReceiveMessage` | 🟡 | `AttributeNames` and `MessageSystemAttributeNames` select their union. System attributes: `SentTimestamp`, `ApproximateReceiveCount`, `DeadLetterQueueSourceArn`, `AWSTraceHeader`. `ReceiveRequestAttemptId` is ignored (probe 12). Long poll is process-local. One claim inserts at most 7 MiB (`QUEUE_CLAIM_WRITE_BYTES`) of stored dead-letter copies, because FoundationDB rejects a transaction above 10,000,000 bytes. Later due messages stay due for the next receive. The cap counts dead-letter copies, not the messages returned; a receive of ten 1 MiB messages without redrive is not capped by it. |
| `DeleteMessage`                               | 🟡     | A stale receipt handle succeeds and deletes nothing (AWS also deletes with the two previous handles, run 2 extras). Malformed handle: HTTP 404 `ReceiptHandleIsInvalid`, verified against AWS.                            |
| `DeleteMessageBatch` | 🟡 | Same stale-handle behaviour per entry. A missing queue is a whole-request 400 `NonExistentQueue` (probe 6). |
| `ChangeMessageVisibility`                     | 🟡     | Works with the current receipt handle, also after the visibility timeout ended and before the next receive. A stale handle gives HTTP 400 `InvalidParameterValue` with the AWS text. No 12 hour limit from first receive. |
| `ChangeMessageVisibilityBatch` | 🟡 | Same as the single call. A missing queue is a whole-request 400 `NonExistentQueue` (probe 6). |
| `TagQueue`, `UntagQueue`, `ListQueueTags` | ✅ | Stored in the catalog row. At most 50 tags after the merge, key 1 to 128 characters not starting with `aws:`, value at most 256 (one rule with SNS); a violation is 400 `InvalidParameterValue` (texts from the suite scripts: `Too many tags added for queue N.`, `Tag keys must be between 1 and 128 characters in length.`; the rest not probed). An unknown key in `UntagQueue` is not an error. Query `Tag/Key`, `Tag/Value` (documentation-derived). |
| `AddPermission`, `RemovePermission`           | ❌     | Not implemented.                                                                                                                                                                                           |
| `ListDeadLetterSourceQueues`                  | 🟡     | Returns `queueUrls` (lower-case `q`, as AWS; `QueueUrl` elements in the Query form) of the queues whose policy names the queue. `MaxResults` 1 to 1,000. One call reads at most 1,000 catalog rows, so a page can have fewer matches than `MaxResults` and still a `NextToken`. The token is the name of the last row read and is returned whenever rows remain, also without `MaxResults` (AWS documents it only with `MaxResults`). |
| `StartMessageMoveTask`                        | ❌     | Not implemented.                                                                                                                                                                                           |
| `CancelMessageMoveTask`                       | ❌     | Not implemented.                                                                                                                                                                                           |
| `ListMessageMoveTasks`                        | ❌     | Not implemented.                                                                                                                                                                                           |
| Standard queue semantics                      | 🟡     | At-least-once through visibility timeout. Messages are claimed in (visible time, message id) order; ordering is best effort, not a contract.                                                               |
| Repeated calls | 🟡 | Delete, visibility and checkpoint operations are idempotent in the provider contract (`queue-provider/src/queue_operations.rs`). Delivery is at least once. |
| FIFO queues                                   | ❌     | `.fifo` names and `FifoQueue` / `ContentBasedDeduplication` attributes are rejected.                                                                                                                       |
| Dead-letter queues, `RedrivePolicy`           | 🟡     | See item 14 under "Behavioural differences". `RedriveAllowPolicy` is rejected as an unsupported attribute. Message move tasks are not implemented. |
| Delay queues and per-message delay            | ✅     | `DelaySeconds` 0 to 900 on the queue and on each message. An explicit 0 overrides the queue delay.                                                                                                         |
| Message retention                             | ✅     | `MessageRetentionPeriod` 60 to 1,209,600 seconds, default 345,600. Expiry is stamped at send and enforced at receive and by cleanup.                                                                       |
| Long polling                                  | 🟡     | Up to 20 seconds, taken from the request or the queue attribute. Wake-up is immediate for sends through the same process and polled every 200 ms for other nodes.                                          |
| Message attributes                            | 🟡     | `String`, `Number`, `Binary` with `.custom` suffix, at most 10. Name rules differ from AWS.                                                                                                                |
| Message system attributes | 🟡 | `SentTimestamp`, `ApproximateReceiveCount`, `DeadLetterQueueSourceArn`, `AWSTraceHeader`. A dead-letter move keeps `AWSTraceHeader`; an SNS delivery to SQS sets none (Aux choice, not probed). |
| MD5 checksums                                 | ✅     | `MD5OfMessageBody`, `MD5OfMessageAttributes` and (with a trace header) `MD5OfMessageSystemAttributes` use the documented length-prefixed algorithm. botocore validated the body and attribute digests (Query SDK check). |
| Approximate message counts                    | ✅     | Visible, in flight and delayed counts are exact up to 100,000 index entries per region (behavioural difference 12), and cost a scan of the queue.                                                         |
| Server-side encryption / KMS | 🟡 | `SqsManagedSseEnabled` (`true`/`false`, default read `true`), `KmsMasterKeyId` and `KmsDataKeyReusePeriodSeconds` (60 to 86,400) are stored and returned as sent. Aux does not encrypt. |
| Queue policies | 🟡 | `Policy` (a JSON object) is stored verbatim and returned; it is not enforced. |
| CloudWatch metrics                            | ❌     | Not implemented. `storage-api` serves Prometheus metrics at `/metrics` when enabled.                                                                                                                   |
| Size limits of queue settings | 🟡 | Aux limits, not probed: the catalog row's attributes and tags at most 90 KiB in total, `KmsMasterKeyId` and the `AWSTraceHeader` value at most 2 KiB each. |

## Request field handling

A member that AWS documentation marks required and the request omits is 400 `MissingParameter`
`The request must contain the parameter X.`; an empty `QueueUrl` can come from the posted queue
URL path. AWS request types ignore unknown members (probe 12; D3 of the W14 plan); only the Aux
`PublishMessage` request rejects them. Integer members are JSON numbers or JSON strings that hold an
integer (Query text is converted by the shape table); a negative or out-of-range value gives the AWS
range text. Whether AWS accepts a numeric string in JSON is not probed. `null` is treated as absent, so
a required member sent as `null` is `MissingParameter`. A missing member of a batch entry is named with
its entry, such as `DeleteMessageBatchRequestEntry.2.Id` (AWS text not probed). Each request decodes
once and is validated once (`queue_provider::wire`).

### CreateQueue

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| ------------------------------------------ | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `QueueName`                                | Accepted     | 1 to 80 characters. Letters, digits, `-`, `_`. The letter test is Unicode aware (`char::is_alphanumeric`), so non-ASCII letters pass. `.fifo` suffix rejected ([request_validation.rs](../crates/queue-provider/src/request_validation.rs)). |
| `Attributes`                               | Accepted     | Map of strings, with the attributes of the table below. An empty value is accepted only for `RedrivePolicy` and `Policy` and stores nothing. |
| `tags` | Accepted | Stored in the catalog row under the shared tag rule (50 after the merge). A repeated create keeps the existing tags. A capital `Tags` member is ignored (probe 12). |
| Same name, same effective attributes       | Accepted     | Returns the existing queue's URL with HTTP 200. Verified against AWS (docs/sqs-error-parity.md). The attributes are compared with the defaults applied, so `VisibilityTimeout` `30` equals an omitted one; the URL is not compared ([queue_creation.rs](../crates/queue-provider/src/queue_creation.rs)). Whether AWS applies defaults in this comparison is not probed. |
| Same name, different attributes            | Rejected     | HTTP 400. JSON `__type` `com.amazonaws.sqs#QueueNameExists` verified against AWS; `x-amzn-query-error` `QueueAlreadyExists;Sender` as AWS. Only requested attributes and tags are compared with the current values; the message names the first differing attribute (`... a different value for attribute X`, or `... for tags ` with a trailing space). A `Policy` is compared without `Sid`. |
| Invalid name (for example contains `.`)    | Rejected     | HTTP 400 `InvalidParameterValue`, message `Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length`. Verified against AWS. An empty name gives a different message (`Queue name cannot be empty`), not probed. |

Queue attributes (create and set):

| Attribute                       | Handling     | Validation / default                                                                                                          |
| ------------------------------- | ------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `DelaySeconds`                  | Accepted     | Integer 0 to 900. Default 0.                                                                                                  |
| `VisibilityTimeout`             | Accepted     | Integer 0 to 43,200. Default 30. Used by `ReceiveMessage` when the request omits `VisibilityTimeout`.                         |
| `MaximumMessageSize`            | Accepted     | Integer 1,024 to 1,048,576. Default 1,048,576. Checked at send against body plus attribute bytes ([queue_send.rs](../crates/queue-provider/src/queue_send.rs)). |
| `MessageRetentionPeriod`        | Accepted     | Integer 60 to 1,209,600. Default 345,600.                                                                                     |
| `ReceiveMessageWaitTimeSeconds` | Accepted     | Integer 0 to 20. Default 0. Used by `ReceiveMessage` when the request omits `WaitTimeSeconds`.                                |
| `FifoQueue`                     | Rejected     | `FIFO queue attributes are not supported`, for any value.                                                                      |
| `ContentBasedDeduplication`     | Rejected     | Same message.                                                                                                                 |
| `DeduplicationScope`, `FifoThroughputLimit` | Rejected | `FIFO queue attributes are not supported`. |
| `RedrivePolicy` | Accepted | See "Dead-letter queues". `RedriveAllowPolicy` is rejected: `RedriveAllowPolicy is not supported` (an ignored allow policy would let any queue redrive here). |
| `Policy` | Accepted | A JSON object, stored verbatim and returned, not enforced. An empty value removes it. |
| `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`, `SqsManagedSseEnabled` | Accepted | Stored and returned as sent: a non-empty key id, 60 to 86,400 seconds, `true` or `false` (default read `true`, probe 7). Aux does not encrypt. |
| `QueueArn`, `CreatedTimestamp`, `LastModifiedTimestamp`, `Approximate*` | Rejected on write | 400 `InvalidAttributeName` `Unknown Attribute X.`, as for any unknown name (suite scripts). |

Out-of-range or non-numeric values give HTTP 400 `InvalidParameterValue` with `<Attribute> must be between <min> and <max>`
([request_validation.rs](../crates/queue-provider/src/request_validation.rs)). AWS message text is not probed.

### GetQueueAttributes

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| ---------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `QueueUrl`       | Accepted     | Non-empty. Unknown URL gives `AWS.SimpleQueueService.NonExistentQueue`, HTTP 400.                                                                                              |
| `AttributeNames` | Accepted | Omitted or empty: no attributes, after one catalog read (probe 7). An unknown name: 400 `InvalidAttributeName` `Unknown Attribute X.` (probe 7). `All` returns every name below. |

Returned for `All`: the stored attributes byte for byte, the defaults of `DelaySeconds`, `VisibilityTimeout`,
`MaximumMessageSize`, `MessageRetentionPeriod`, `ReceiveMessageWaitTimeSeconds` and `SqsManagedSseEnabled` (`true`),
the three `ApproximateNumberOf*` counts, `QueueArn`, `CreatedTimestamp` and `LastModifiedTimestamp` (seconds).
A known name with no value is omitted. The counts are computed only when a count name or `All` is requested.

### SetQueueAttributes

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| ------------ | -------- | ----------------------------------------------------------------------------------------------------------- |
| `QueueUrl`       | Accepted     | Non-empty. Unknown URL gives `AWS.SimpleQueueService.NonExistentQueue`, HTTP 400.                                                                                              |
| `Attributes` | Accepted | Required. Same attributes and limits as create. Supplied keys overwrite; others are kept ([queue_operation.rs](../crates/queue-provider/src/queue_operation.rs)). An empty `RedrivePolicy` or `Policy` removes it; an empty value of another attribute is rejected. |

A change takes effect for later receives and sends. Messages already stored keep the expiry stamped at send.

### ListQueues, GetQueueUrl, DeleteQueue, PurgeQueue

| Operation / parameter                  | Handling     | Validation / default                                                                                      |
| -------------------------------------- | ------------ | --------------------------------------------------------------------------------------------------------- |
| `ListQueues.QueueNamePrefix`           | Accepted     | Non-empty if present. Letters, digits, `-`, `_` only. Read as a name range of the catalog index on every provider. |
| `ListQueues.MaxResults`, `NextToken` | Accepted | `MaxResults` 1 to 1,000; 0 or absent lists up to 1,000. `NextToken` only when `MaxResults` was given and queues remain. A bad token: 400 `Invalid or expired next token.` (probe 7). |
| `GetQueueUrl.QueueName`                | Accepted     | Same name rules as create. Missing queue: HTTP 400, `AWS.SimpleQueueService.NonExistentQueue`, JSON `com.amazonaws.sqs#QueueDoesNotExist`, message `The specified queue does not exist.` Verified against AWS. |
| `GetQueueUrl.QueueOwnerAWSAccountId` | Accepted | The configured account: normal. Another account: 400 `NonExistentQueue` (not probed). |
| `DeleteQueue.QueueUrl`                 | Accepted     | Missing queue: 400 `AWS.SimpleQueueService.NonExistentQueue` (probe 6). AWS's Query text adds `for this wsdl version.`; Aux sends the JSON text in both protocols. |
| `PurgeQueue.QueueUrl`                  | Accepted     | Missing queue gives `NonExistentQueue`. Messages become unreachable at once (new incarnation); physical removal is by cleanup. |

### SendMessage

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| ------------------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueueUrl`       | Accepted     | Non-empty. Unknown URL gives `AWS.SimpleQueueService.NonExistentQueue`, HTTP 400.                                                                                              |
| `MessageBody`                   | Accepted     | 1 byte to 1,048,576 bytes; allowed Unicode ranges as in the AWS documentation. Empty body: HTTP 400 `MissingParameter`, JSON `com.amazon.coral.service#MissingRequiredParameterException`, message `The request must contain the parameter MessageBody.` Verified against AWS. A body over the queue `MaximumMessageSize` (body plus attribute name, type and value bytes) gives `InvalidParameterValue`. |
| `DelaySeconds`                  | Accepted     | 0 to 900. Omitted: queue `DelaySeconds`. Explicit 0 overrides the queue delay.                                                                                              |
| `MessageAttributes`             | Accepted     | At most 10. Name 1 to 256 characters of letters, digits, `_`, `-`, `.`. AWS rules not checked: no `AWS.` / `Amazon.` prefix ban, no leading, trailing or double period rule. Unicode letters pass. |
| `MessageAttributes.*.DataType`  | Accepted     | `String`, `Number`, `Binary`, each with an optional `.suffix`. Anything else is rejected.                                                                                   |
| `MessageAttributes.*.StringValue` / `BinaryValue` | Accepted | Exactly one. `Number` must parse within 10^-128 to 10^126 and is stored as sent. `Binary` must be valid base64. Empty string value is accepted (not probed against AWS). |
| `MessageSystemAttributes` | Accepted | Only `AWSTraceHeader` with DataType `String`; its value is not checked (probe 12). Another name: 400 `Message system attribute name 'Foo' is invalid.` Stored with the message; a dead-letter move keeps it. |
| `MessageGroupId`, `MessageDeduplicationId` | Ignored / Rejected | `MessageGroupId` is ignored (probe 12). `MessageDeduplicationId`: 400 `Value d for parameter MessageDeduplicationId is invalid. Reason: The request include parameter that is not valid for this queue type.` |

Response: `MessageId` (a hyphenated UUID, as AWS), `MD5OfMessageBody`, `MD5OfMessageAttributes` (only when attributes were sent).
`MD5OfMessageSystemAttributes` when a trace header was sent. No `SequenceNumber`.

### SendMessageBatch

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| -------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `Entries`                        | Accepted     | 1 to 10 entries. No entry: 400 `AWS.SimpleQueueService.EmptyBatchRequest`; more than 10: 400 `AWS.SimpleQueueService.TooManyEntriesInBatchRequest` (AWS documentation; texts not probed). |
| `Entries[].Id`                   | Accepted     | 1 to 80 characters of letters, digits, `-`, `_`. Duplicate id: HTTP 400 `AWS.SimpleQueueService.BatchEntryIdsNotDistinct`, message `Id <id> repeated.` Verified against AWS. |
| `Entries[].MessageBody`, `DelaySeconds`, `MessageAttributes` | Accepted | Same rules as `SendMessage`, checked per entry. An empty body fails only that entry (`Failed`, `EmptyValue`, `No Value Found for SendMessageBatchRequestEntry.<n>.MessageBody.`, sender fault) and the other entries are sent; a body over the queue `MaximumMessageSize` also fails alone. |
| Total payload | Accepted | Over 1,048,576 bytes: 400 `AWS.SimpleQueueService.BatchRequestTooLong` with the AWS text (probe 5). |
| `Entries[].MessageSystemAttributes`, `MessageGroupId`, `MessageDeduplicationId` | As `SendMessage` | Same rules per entry. |
| Per-entry size over `MaximumMessageSize` | Accepted | That entry is reported in `Failed` (`InvalidParameterValue`); the others are written. |

Response: `Successful` (`Id`, `MessageId`, `MD5OfMessageBody`, `MD5OfMessageAttributes`, and
`MD5OfMessageSystemAttributes` when the entry has a trace header) and `Failed` (`Id`, `SenderFault`, `Code`,
`Message`), HTTP 200.

### ReceiveMessage

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| ----------------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueueUrl`       | Accepted     | Non-empty. Unknown URL gives `AWS.SimpleQueueService.NonExistentQueue`, HTTP 400.                                                                                              |
| `MaxNumberOfMessages`         | Accepted     | 1 to 10, default 1. Out of range: `Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided.` Verified against AWS.             |
| `VisibilityTimeout`             | Accepted     | Integer 0 to 43,200. Omitted: the queue `VisibilityTimeout` attribute (default 30). |
| `WaitTimeSeconds`             | Accepted     | 0 to 20 (out of range is rejected). Omitted: the queue `ReceiveMessageWaitTimeSeconds` attribute. A value above 20 cannot reach the manager.                                 |
| `MessageSystemAttributeNames` | Accepted | Union with `AttributeNames`; both may be sent. |
| `AttributeNames` | Accepted | Deprecated by AWS; union with `MessageSystemAttributeNames`. Names: `All`, `SentTimestamp`, `ApproximateReceiveCount`, `DeadLetterQueueSourceArn`, `AWSTraceHeader`. |
| `MessageAttributeNames`       | Accepted     | `All`, `.*`, exact names, or `prefix.*`. Omitted: no `MessageAttributes` in the response. `MD5OfMessageAttributes` is recomputed over the returned subset.                    |
| `ReceiveRequestAttemptId` | Ignored | Accepted and ignored on a standard queue (probe 12). |

Response message: `MessageId`, `ReceiptHandle`, `MD5OfBody`, `MD5OfMessageAttributes`, `Body`, `Attributes`, `MessageAttributes`
([types.rs](../crates/queue-provider/src/types.rs)). An empty JSON result is `{"Messages": []}` and an empty
`ListQueues` is `{"QueueUrls": []}`; AWS may omit these members (not probed). In the Query form both are
absent elements, which botocore reads as no member (Query SDK check).

### DeleteMessage and DeleteMessageBatch

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| --------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `QueueUrl`       | Accepted     | Non-empty. Unknown URL gives `AWS.SimpleQueueService.NonExistentQueue`, HTTP 400.                                                                                              |
| `ReceiptHandle`             | Accepted | Non-empty. A malformed handle gives HTTP 404 `ReceiptHandleIsInvalid`, message `The input receipt handle "<handle>" is not a valid receipt handle.` Verified against AWS for the value `invalid`; a handle with a character outside the base64 alphabet (a hyphen) gets `The input receipt handle is invalid.` (suite scripts). A handle of another queue gets 404 `ReceiptHandleIsInvalid`, `The receipt handle "<handle>" is not valid for this queue.`; a handle of a deleted queue of the same name gets `NonExistentQueue` (suite scripts; a purge keeps the queue identity, so its old handles stay stale, not refused). A well-formed handle with no current row (already deleted, or superseded by a later receive) succeeds without an effect (behavioural difference 6, probe 1). |
| Batch `Entries`, `Id`       | Accepted | 1 to 10 entries, same id rules as `SendMessageBatch`.                                                                                                                                         |

### ChangeMessageVisibility and ChangeMessageVisibilityBatch

| Parameter        | Handling     | Validation / default                                                                                                                                                           |
| ------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ReceiptHandle`             | Accepted | Non-empty. A malformed handle gives HTTP 404 `ReceiptHandleIsInvalid` (verified against AWS for the value `invalid`). A well-formed handle with no current row gives HTTP 400 `InvalidParameterValue` `Value <handle> for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change.` (probe 3). |
| `VisibilityTimeout`             | Accepted     | Required. Integer 0 to 43,200. Aux also requires it in each batch entry (AWS documentation marks it optional there). |
| Batch `Entries`     | Accepted | 1 to 10, same id rules. Invalid timeout in any entry fails the request.                                                                                 |

## Protocols and errors


| Item                                  | Status | Notes                                                                                                                                                                             |
| ------------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| JSON request: content type | 🟡 | Must start with `application/x-amz-json-1.0`; another type is 400 `InvalidParameterValue`. A missing or unknown `X-Amz-Target` is 400 `InvalidAction` ([wire/mod.rs](../crates/queue-provider/src/wire/mod.rs)). |
| JSON response                         | 🟡     | `Content-Type: application/x-amz-json-1.0`, `x-amzn-requestid`. Field names follow the AWS JSON protocol. Request id is a UUID v7.                                                     |
| Query request: `Attribute.N.Name/Value`, `AttributeName.N`, `MessageAttributeName.N`, `MessageAttribute.N.Name/Value.*` | ✅ | Mapped to the JSON members by the shape table in [wire/query.rs](../crates/queue-provider/src/wire/query.rs); entries and lists follow `N`. |
| Query request: `MessageSystemAttributeName.N`, `MessageSystemAttribute.N` | ✅ | Mapped by the shape table to `MessageSystemAttributeNames` and `MessageSystemAttributes`. |
| Query request: batch entry message attributes | ✅ | `SendMessageBatchRequestEntry.N.MessageAttribute.M.*` becomes the entry `MessageAttributes` map (C12, W14-Q1). |
| Query response | 🟡 | Typed XML per response type ([wire/query_xml.rs](../crates/queue-provider/src/wire/query_xml.rs)). The actions of the Query SDK check are verified with botocore's parser; the others are documentation-derived. |
| Unknown action or bad content type | 🟡 | 400 `InvalidAction` (`The action X is not valid for this endpoint.`) or 400 `InvalidParameterValue` `The content type X is not supported.` JSON for an unknown target may be `UnknownOperationException` on AWS (not probed). Client texts never carry internal codes. |
| Route errors | 🟡 | A refused ingress slot (503 `ServiceUnavailable` with `Retry-After`) and a body over the 5 MiB route limit (413 `InvalidParameterValue`) are answered in the request's protocol. `POST {queue}/` (trailing `/`) is the queue's route. AWS codes for these are not probed. |
| `x-amzn-query-error` header | ✅ | `<full stored code>;Sender` for 4xx and `;Receiver` for 5xx, in both protocols. |
| Storage overload                      | 🟡     | HTTP 503 `ServiceUnavailable` with `Retry-After`, JSON `com.amazon.coral.service#ServiceUnavailableException`. Text `The request has failed due to a temporary failure of the server.` (AWS type not probed). |
| Retry exhaustion under contention     | 🟡     | Send, delete and visibility operations retry five times, then return the same HTTP 503 (the attempt proved that nothing committed). A receive that loses every retry returns an empty result. An unknown commit outcome stays HTTP 500 `InternalError`. |

Error rows. "Verified" means the AWS response is recorded in
[docs/sqs-error-parity.md](../docs/sqs-error-parity.md) (live probe, `eu-central-1`, 2026-05-21) and the Aux code
path was read and gives the same code, status and message. The Aux side was not re-run for this matrix.

| Case                                       | AWS evidence                         | Aux status, code (`x-amzn-query-error`), JSON `__type`, message                                                                                              | Match |
| ------------------------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----- |
| `GetQueueUrl` missing queue | Verified against AWS | 400, `AWS.SimpleQueueService.NonExistentQueue`, `com.amazonaws.sqs#QueueDoesNotExist`, `The specified queue does not exist.` | ✅ |
| `CreateQueue` invalid name | Verified against AWS | 400, `InvalidParameterValue`, `...InvalidParameterValueException`, AWS text | ✅ |
| `CreateQueue` same name, same attributes | Verified against AWS | 200 with the existing URL; the existing queue keeps its tags (tags not probed) | ✅ |
| `CreateQueue` same name, different attributes | Verified against AWS | 400, `QueueAlreadyExists`, `com.amazonaws.sqs#QueueNameExists` | ✅ |
| `SendMessage` empty or missing body | Verified against AWS (empty body) | 400, `MissingParameter`, `com.amazon.coral.service#MissingRequiredParameterException`, `The request must contain the parameter MessageBody.` | ✅ |
| `ReceiveMessage` `MaxNumberOfMessages` 11  | Verified against AWS                 | 400, `InvalidParameterValue`, `...InvalidParameterValueException`, AWS text                                                                                  | ✅    |
| `DeleteMessage` handle `invalid`           | Verified against AWS                 | 404, `ReceiptHandleIsInvalid`, `com.amazonaws.sqs#ReceiptHandleIsInvalid`, AWS text                                                                          | ✅    |
| `SendMessageBatch` duplicate ids           | Verified against AWS                 | 400, `AWS.SimpleQueueService.BatchEntryIdsNotDistinct`, `com.amazonaws.sqs#BatchEntryIdsNotDistinct`, `Id dup repeated.`                                      | ✅    |
| Send / receive / delete on a missing queue | From AWS documentation, not probed   | 400 `AWS.SimpleQueueService.NonExistentQueue` (single calls)                                                                                                 | 🟡    |
| Validation failures other than the above   | From AWS documentation, not probed   | 400 `InvalidParameterValue` with Aux-specific text (limits, attribute names); a value of the wrong JSON type gives `A parameter has a value of the wrong type or format.` | 🟡    |
| Internal failure                           | From AWS documentation, not probed   | 500 `InternalError` / `com.amazonaws.sqs#InternalError`                                                                                                      | 🟡    |
| Batch entry failure                        | From AWS documentation, not probed   | Entry in `Failed` with `SenderFault`, `Code` (same code as the single call), `Message`                                                                       | 🟡    |
| Unknown action                             | Not live-confirmed (docs/sqs-error-parity.md) | See protocol table                                                                                                                                  | 🟡    |

The repository also holds guarded tests that compare Aux with a real AWS account
([storage-api/tests/aws_sqs_parity_tests/mod.rs](../crates/storage-api/tests/aws_sqs_parity_tests/mod.rs), `#[ignore]`, need an
explicit run guard and AWS profile). No recorded results of those runs are in the repository, so they are not
counted as verification here.

## Behavioural differences

Each item names the code that shows it.

`MessageId` is a hyphenated UUID, as AWS returns; ordinary sends use UUIDv7, so ids sort by creation time ([newtypes.rs](../crates/queue-provider/src/newtypes.rs)).

1. **Receipt handles are short.** URL-safe base64 of `<timestamp>:<uuid>:<queue id>:<queue name>`, about 150 characters
   ([newtypes.rs](../crates/queue-provider/src/newtypes.rs)). The handle encodes the next-visible time; each
   receive creates a new one. The queue id is fixed at creation and survives a purge, so a handle is checked against its queue before any row is read.
2. **No FIFO.** Queue names ending in `.fifo` and the FIFO attributes are rejected
   ([request_validation.rs](../crates/queue-provider/src/request_validation.rs)).
   There is no group or deduplication model.
3. **Delivery and ordering.** `ReceiveMessage` selects due messages ordered by (visible time, message id) and
   verifies that order ([queue_receive.rs](../crates/queue-provider/src/queue_receive.rs)). Concurrent receives
   claim each message once (conformance tests in
   [queue_claim_tests.rs](../crates/storage-rewrite-conformance/src/queue_claim_tests.rs)). Order is exact for a
   shallow queue; when more than 1,024 messages are due, an ordered-key store (RocksDB, FoundationDB) claims a random
   aligned slot of the first 1,024, so order is best effort. Postgres claims with `FOR UPDATE SKIP LOCKED`.
   Duplicate delivery still follows from visibility timeout expiry, and from a crash: receive, delete and visibility
   changes commit without waiting for the disk, so a crash can undo the latest of them and a message is delivered
   again (a standard queue permits this; sends wait for the disk). A receive that loses every conflict retry returns
   an empty result, not an error.
   A message that was received becomes due for another receive 500 ms after its visibility timeout ends (a margin for
   clock differences between nodes); `ChangeMessageVisibility` to 0 makes it due at once.
   Known bound: the due-head probe reads `QUEUE_DUE_HEAD` = 8 head marks
   ([queue-provider/src/constants.rs](../crates/queue-provider/src/constants.rs)). On SQL providers, when 8 or more received
   messages have a visibility end inside that 500 ms margin (`DEFAULT_MAX_CLOCK_SKEW_MILLIS`), a due message behind
   those 8 guarded leases can be seen up to 500 ms late: a long poll learns the next due time and claims it that
   much later. KV providers read the marks up to the claim time in the 1,024 entry window, so the code reports
   the exact time there (read, not measured). The wake probe interval of 200 ms (note 5) is a separate bound.
4. **Expired messages disappear at receive.** A message past its stamped expiry is deleted when a receive selects it
   and is not returned ([queue_receive.rs](../crates/queue-provider/src/queue_receive.rs)). Expiry is fixed at
   send, so a later `SetQueueAttributes` retention change does not affect stored messages.
5. **Long polling.** `WaitTimeSeconds` or the queue default, capped at 20. Storage never blocks; the manager waits
   ([manager/mod.rs](../crates/queue/src/manager/mod.rs)). A send, visibility change or attribute change
   through the same process wakes waiters at once. Messages committed by another node are found by a read-only probe
   at most every 200 ms ([constants.rs](../crates/queue/src/constants.rs)). The doc comment states that a short poll
   can return empty for a message another node committed within the last probe interval. At most 10,000 receives
   wait per process; a receive over the bound returns empty at once.
   A long poll holds no ingress slot ([routes/queue/mod.rs](../crates/storage-api/src/routes/queue/mod.rs)).
6. **Stale receipt handle on `DeleteMessage`.** The row is found by exact handle equality only. Once a later receive
   issued a new handle, the old handle matches nothing and the delete succeeds without an effect
   ([queue_receipt.rs](../crates/queue-provider/src/queue_receipt.rs)). AWS deletes the message with the current
   handle or one of the two before it, and is a no-op only for older handles (probe of 2026-11-10). Here a stale
   handle never deletes a message that another consumer holds. A crash that undoes a Relaxed receive can make an older
   handle current again.
7. **Handle stays valid after the timeout ends.** Nothing checks that the message is still in flight, so `DeleteMessage`
   and `ChangeMessageVisibility` succeed with the last handle until the next receive replaces it. `CannotOperateVisibleMessage`
   exists in [errors.rs](../crates/queue-provider/src/errors.rs) but nothing raises it.
8. **Batch limits.** 1 to 10 entries and distinct ids ([request_validation.rs](../crates/queue-provider/src/request_validation.rs)),
   and at most 1,048,576 bytes of bodies and attributes in total (`BatchRequestTooLong`).
9. (Removed: batches against a missing queue now fail the whole request, as AWS, probe 6.)
10. (Removed: `DeleteQueue` of a missing queue is now 400 `NonExistentQueue`, probe 6.)
11. (Removed: `ListQueues` is paged with `MaxResults` and `NextToken`, probe 7.)
12. **Counts stop at 100,000.** Each of the regions "due now" and "due later" is counted up to 100,000 index entries,
    so `ApproximateNumberOfMessages` and the sum of `...NotVisible` and `...Delayed` stop at that bound
    ([queue_message.rs](../crates/sql-backend/src/queue_message.rs), [kv-backend queue_message.rs](../crates/kv-backend/src/queue_message.rs)).
    Expired messages are excluded. A message that was received and whose timeout ended, but was not yet received again,
    counts as visible. The counts can include receives and deletes that a crash later undoes.
13. **System attributes are four.** `SentTimestamp` (milliseconds), `ApproximateReceiveCount`,
    `DeadLetterQueueSourceArn` on a moved message, and `AWSTraceHeader` when one was sent
    ([types.rs](../crates/queue-provider/src/types.rs), [queue_receive.rs](../crates/queue-provider/src/queue_receive.rs)).
    `ApproximateFirstReceiveTimestamp`, `SenderId`, `MessageGroupId`, `SequenceNumber` are never produced; no
    first-receive time is stored.
14. **Dead-letter queues** ([queue_redrive.rs](../crates/queue-provider/src/queue_redrive.rs)). A receive that finds a
    message with `ApproximateReceiveCount >= maxReceiveCount` moves it to the target in the same native transaction as
    the claim: the source row is deleted and a target row with the same `MessageId`, body, attributes, send time and
    receive count is inserted, visible at once, with retention counted from the first send. The moved message uses its
    slot, so the receive can return fewer messages. A missing target, or the queue as its own target, delivers as usual
    and the count keeps increasing (as AWS). A message past the target's retention is deleted. The first receive from
    the target shows the kept count plus one and `DeadLetterQueueSourceArn` (the queue it left most recently). A move
    wakes long polls on the target in the same process. Each hop delivers at least once: a moved message that the
    target has not delivered yet is delivered there even when its kept count is at the target's own limit, so a
    cycle of queues or a chain with a lower limit delivers the message on each queue before it moves on. An expired
    target copy with the same `MessageId` is replaced; a live copy (from `PublishMessage`) keeps the message in the
    source, where it is delivered as usual. `CreateQueue` with identical attributes returns the existing queue even
    when its target was deleted. Not applied to a moved message: the target's `DelaySeconds` and `MaximumMessageSize`.
    Not probed against AWS: the exact error texts, the hop rule, and the target's delay and size limit on a move.
15. **Queue URL, account and ARN.** A response URL embeds the answering node's `queue.public_base_url` and the
    configured account id; a queue is stored and found by its name, so every node and every earlier base URL
    address the same queue ([manager/dispatch.rs](../crates/queue/src/manager/dispatch.rs)). `QueueArn` is
    `arn:{partition}:sqs:{region}:{account}:{name}` from `queue.partition`, `queue.region` and
    `queue.account_id` (the identity that SNS ARNs also use).
16. **Queue names accept non-ASCII letters** ([request_validation.rs](../crates/queue-provider/src/request_validation.rs)).
17. **Delete and recreate.** A new incarnation makes messages of the old queue unreachable
    ([queue_catalog.rs](../crates/queue-provider/src/queue_catalog.rs)).

## Planned

Not yet true. These come from [fable-improvements.md](../docs/plans/incomplete/fable-improvements.md) (sections 6 and 9) and
[fable-w2-sqs-claim-design.md](../docs/plans/incomplete/fable-w2-sqs-claim-design.md) (D10). Do not rely on them.

- `RedriveAllowPolicy` (design D10 left it optional; it is rejected today). Message move tasks and FIFO stay out of scope.

## Extensions that AWS does not have

| Extension                              | Where                                                                                                   | Notes                                                                                                                                                                                  |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PublishMessage` action                | JSON `X-Amz-Target: AmazonSQS.PublishMessage`. [queue_publication.rs](../crates/queue-provider/src/queue_publication.rs) | Request `{MessageId, Message}` where `Message` is a `SendMessage` body and `MessageId` is a caller-chosen hyphenated UUID (the nil UUID is reserved). Repeating the id with the same content keeps the pending delivery, receipt, visibility, expiry and checkpoint; different content gives `InvalidParameterValue`. Query protocol: `PublishMessage` is 400 `InvalidAction`, so use JSON. Posted to a queue URL path, the path fills `Message.QueueUrl`. |
| Message checkpoint data                | Rust API `QueueManager::update_message_snapshot` ([manager/mod.rs](../crates/queue/src/manager/mod.rs)). | Stores `checkpoint_data` on a message by receipt handle. It is not an HTTP action in `QueueAction`. Not returned by `ReceiveMessage`. |
| Stable message identity                | Same as `PublishMessage`.                                                                               | Makes producer retries and fan-out idempotent. Used by the SNS bridge only through `SendMessage` today (SNS delivery uses ordinary random ids).                                       |
