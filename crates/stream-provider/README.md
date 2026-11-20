# stream-provider

`stream-provider` defines typed stream, cursor, retained-image, and logical SystemStream
contracts. Concrete storage providers implement the
[StreamProvider trait](src/stream_provider/provider_trait.rs).

## User streams

`create_stream` takes a `UserStreamName`, an optional `DurationSeconds` retention and a
`StreamPartitioningMode`, validates the name (1–255 bytes, alphanumeric first and last character,
`_ - .` between) and the TTL (1 second to 1 year) before any I/O, and returns the internal `StreamName`.
`get_stream`, `delete_stream` and `append_item` take the `UserStreamName`. Payloads are 1 byte to 1 MiB.

Cursor and page operations take a `StreamAddress`: `User(UserStreamName)` resolves the catalog row by name
on the operation's own native attempt; `Internal(StreamName)` addresses a user stream's internal id or a
table-history stream. `read_forward` and `read_backward` take an optional exclusive `StreamItemId` and a
limit from 1 through 1,000.

## Cursors

`create_cursor` validates the `CursorName` (1–64 bytes, alphanumeric first and last character, `_ -`
between) and starts the cursor at a `CursorPosition`. `read_from_cursor` reads a page and advances the cursor
in the same attempt; `advance_cursor` moves it forward to an existing item.

The library entry for user streams is `storage::DatabaseManager` (`create_user_stream` and the other
`*_user_stream*` methods), one admitted operation per call over the default connection.

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
