# Stream Provider

The Stream Provider crate provides a trait-based abstraction for implementing time-ordered streaming data storage with cursor-based navigation. This crate enables storing sequences of items with timestamps and tracking multiple named cursors to maintain reading positions within streams.

## Overview

Streams are time-ordered collections of items that can be read forwards or backwards. They support:

- **Time-ordered storage**: Items are stored with timestamps for chronological ordering
- **Named cursors**: Create and manage multiple reading positions within a stream
- **TTL support**: Optional time-to-live for automatic cleanup of old items
- **Pagination**: Iterator-based reading with page tokens
- **Background cleanup**: Automatic deletion of expired items with configurable parallelism

## Core Concepts

### Streams

A stream is identified by a **stream prefix** and contains time-ordered items. The stream prefix acts as the namespace for all items in the stream and must be provided when creating a stream.

**Stream Name Restrictions:**

- Length: 1-255 characters
- Allowed characters: alphanumeric, hyphens, underscores, periods
- Must not start or end with special characters
- Case-sensitive

### Cursors

Cursors are named bookmarks that track reading positions within a stream. Multiple cursors can exist per stream, allowing different consumers to track their own reading positions.

**Cursor Name Restrictions:**

- Length: 1-64 characters
- Allowed characters: alphanumeric, hyphens, underscores
- Must not start or end with special characters
- Case-sensitive
- Must be unique within a stream

### TTL and Background Cleanup

Streams can optionally configure a TTL (time-to-live) value in seconds. A background task runs every minute to clean up expired items:

- **Processing schedule**: Maximum once per minute
- **Parallelism**: Configurable number of streams processed concurrently
- **Resource management**: Automatically adjusts timing to avoid overloading
- **Scale**: Designed to handle 100,000+ streams efficiently
- **Timing behavior**: If processing takes >1 minute, continues immediately; otherwise sleeps for remaining time

## StreamProvider Trait Methods

### Stream Management

#### `initialize() -> StreamResult<()>`

Initialize the stream storage backend. Must be called before using other methods.

#### `create_stream(stream_name: &str, stream_prefix: &str, ttl_seconds: Option<u64>) -> StreamResult<()>`

Create a new stream with the specified name and storage prefix. Optionally configure TTL for automatic cleanup.

#### `delete_stream(stream_name: &str) -> StreamResult<()>`

Delete a stream and all its items and cursors.

### Item Operations

#### `append_item(stream_name: &str, item_data: &[u8]) -> StreamResult<ItemId>`

Add an item to the end of the stream with the current timestamp. Returns the item ID.

#### `read_forward(stream_name: &str, page_token: Option<&str>, limit: u32) -> StreamResult<StreamPage>`

Read items in chronological order (oldest first). Use page token for pagination.

#### `read_backward(stream_name: &str, page_token: Option<&str>, limit: u32) -> StreamResult<StreamPage>`

Read items in reverse chronological order (newest first). Use page token for pagination.

### Cursor Management

#### `create_cursor(stream_name: &str, cursor_name: &str, position: CursorPosition) -> StreamResult<()>`

Create a named cursor at the specified position:

- `CursorPosition::Head`: Start at the beginning (oldest items)
- `CursorPosition::Tail`: Start at the end (newest items)

Returns error if cursor name already exists.

#### `read_from_cursor(stream_name: &str, cursor_name: &str, limit: u32) -> StreamResult<CursorPage>`

Read items starting from cursor position, advancing the cursor automatically.

#### `delete_cursor(stream_name: &str, cursor_name: &str) -> StreamResult<()>`

Delete a named cursor.

### Background Cleanup

#### `start_cleanup_task(parallelism: usize) -> StreamResult<()>`

Start the background TTL cleanup task with specified parallelism factor.

#### `stop_cleanup_task() -> StreamResult<()>`

Stop the background cleanup task gracefully.

## Usage Examples

### Basic Stream Operations

```rust
use stream_provider::{StreamProvider, CursorPosition};

// Initialize provider
let provider = MyStreamProvider::new("./data").await?;
provider.initialize().await?;

// Create a stream with 1 hour TTL
provider.create_stream("events", "app/events/", Some(3600)).await?;

// Add items
let item1_id = provider.append_item("events", b"first event").await?;
let item2_id = provider.append_item("events", b"second event").await?;

// Read forward (chronological order)
let page = provider.read_forward("events", None, 10).await?;
for item in page.items {
    println!("Item: {:?}", item.data);
}
```

### Cursor-based Reading

```rust
// Create cursor at stream head
provider.create_cursor("events", "consumer1", CursorPosition::Head).await?;

// Read from cursor (auto-advances)
let cursor_page = provider.read_from_cursor("events", "consumer1", 5).await?;
println!("Read {} items", cursor_page.items.len());

// Create another cursor at tail
provider.create_cursor("events", "consumer2", CursorPosition::Tail).await?;
```

### Background Cleanup

```rust
// Start cleanup with 4 parallel workers
provider.start_cleanup_task(4).await?;

// Cleanup runs automatically every minute
// Process up to 4 streams concurrently
// Handles timing to avoid overload

// Stop when shutting down
provider.stop_cleanup_task().await?;
```

### Pagination

```rust
// Read with pagination
let mut page_token = None;
loop {
    let page = provider.read_forward("events", page_token.as_deref(), 100).await?;

    for item in page.items {
        // Process item
    }

    if page.next_token.is_none() {
        break;
    }
    page_token = page.next_token;
}
```

## Error Handling

The trait uses `StreamResult<T>` for all operations, which maps to common error scenarios:

- **StreamNotFound**: Stream does not exist
- **CursorNotFound**: Cursor does not exist
- **CursorAlreadyExists**: Attempted to create duplicate cursor
- **InvalidStreamName**: Stream name violates restrictions
- **InvalidCursorName**: Cursor name violates restrictions
- **StorageError**: Backend storage operation failed
- **ValidationError**: Invalid parameters or state

## Implementation Notes

- All operations are async and thread-safe
- Implementations should handle concurrent access gracefully
- TTL cleanup should not interfere with active read/write operations
- Page tokens should be opaque and implementation-specific
- Timestamps should use UTC and be monotonic when possible
