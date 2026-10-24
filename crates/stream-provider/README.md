# stream-provider

`stream-provider` defines typed stream, cursor, retained-image, and logical SystemStream
contracts. Concrete storage providers implement the
[StreamProvider trait](src/stream_provider/provider_trait.rs).

## User streams

Call `initialize_stream` before using a provider. `create_stream` takes a `UserStreamName`,
an optional `DurationSeconds` retention value, and a `StreamPartitioningMode`; it returns
the physical `StreamName` used by append and read operations. `get_stream` and
`delete_stream` take the logical `UserStreamName`.

`append_item` takes a `StreamName`, borrowed item bytes, and an optional partition key,
and returns a `StreamItemId`. `read_forward` and `read_backward` take an optional exclusive
`StreamItemId` boundary and a limit from 1 through 1,000. Forward reads return oldest items
first; backward reads return newest items first. Treat returned continuation identifiers
as provider-owned boundaries.

## Cursors

Cursor operations use typed `CursorName` and `StreamName` values. `create_cursor` starts a
cursor at a `CursorPosition`. `read_from_cursor` reads a page without advancing it;
call `advance_cursor` only after the consumer has durably handled the page.

Protected bootstrap cursors use separate typed operations. Generic cursor reads, advances,
and deletion do not authorize changes to protected bootstrap state.

## Logical history and retained images

SystemStream pages carry logical scope, lineage, and checkpoint identities. Providers
without the required logical-history capability return an unsupported error. Splits must
atomically seal the parent, publish child lineages, and record a durable operation receipt;
replaying the same operation returns its recorded result.

Pointer reads resolve the exact retained item version named by a stored pointer. Missing or
corrupt history is an error, even when a newer version is available. Background consumers
may omit deleted source incarnations only when their provider contract permits it.

Operations return typed `StreamResult` errors. Backend capabilities and retention policy
remain explicit; callers must not infer support from another provider's behavior.
