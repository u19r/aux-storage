use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use storage_types::{
    AttributeValue, DurationSeconds, ItemStreamVersion, KeySchemaElement, KeyType, StreamItemId,
    StreamName, TableName, TimestampMillis, UserStreamName,
};

use crate::{
    CursorName, StreamDataType, StreamItem, StreamPage, StreamProvider,
    errors::StreamResult,
    types::{
        CursorPage, CursorPosition, EmbeddedStreamItem, StoredStreamPointer, Stream, StreamCursor,
        StreamPartitioningMode,
    },
};

struct InMemoryStreamProvider {
    pointer_streams: HashMap<StreamName, Vec<StreamItem>>,
    item_streams: HashMap<StreamName, Vec<StreamItem>>,
    read_backward_calls: Arc<AtomicUsize>,
}

impl InMemoryStreamProvider {
    fn new(
        pointer_streams: HashMap<StreamName, Vec<StreamItem>>,
        item_streams: HashMap<StreamName, Vec<StreamItem>>,
    ) -> Self {
        Self {
            pointer_streams,
            item_streams,
            read_backward_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn read_backward_calls(&self) -> usize {
        self.read_backward_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StreamProvider for InMemoryStreamProvider {
    async fn initialize_stream(&self) -> StreamResult<()> {
        Ok(())
    }

    async fn create_stream(
        &self,
        _stream_name: UserStreamName,
        _ttl_seconds: Option<DurationSeconds>,
        _partitioning_mode: StreamPartitioningMode,
    ) -> StreamResult<StreamName> {
        unimplemented!("create_stream not needed for pointer stream tests")
    }

    async fn delete_stream(&self, _stream_name: UserStreamName) -> StreamResult<()> {
        unimplemented!("delete_stream not needed for pointer stream tests")
    }

    async fn get_stream(&self, _stream_name: UserStreamName) -> StreamResult<Option<Stream>> {
        unimplemented!("get_stream not needed for pointer stream tests")
    }

    async fn append_item(
        &self,
        _stream_name: StreamName,
        _item_data: &[u8],
        _partition_key: Option<&str>,
    ) -> StreamResult<StreamItemId> {
        unimplemented!("append_item not needed for pointer stream tests")
    }

    async fn read_forward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        let mut items = self
            .pointer_streams
            .get(&stream_name)
            .cloned()
            .unwrap_or_default();

        items.sort_by(|a, b| a.id.cmp(&b.id));
        let filtered = match exclusive_start_key {
            Some(start) => items.into_iter().filter(|item| item.id > start).collect(),
            None => items,
        };

        let mut filtered: Vec<StreamItem> = filtered;
        let has_more = filtered.len() > limit as usize;
        filtered.truncate(limit as usize);
        let last_evaluated_key = filtered.last().map(|item| item.id);

        Ok(StreamPage {
            items: filtered,
            last_evaluated_key,
            has_more,
        })
    }

    async fn read_backward(
        &self,
        stream_name: StreamName,
        exclusive_start_key: Option<StreamItemId>,
        limit: u32,
    ) -> StreamResult<StreamPage> {
        self.read_backward_calls.fetch_add(1, Ordering::SeqCst);
        let mut items = self
            .item_streams
            .get(&stream_name)
            .cloned()
            .unwrap_or_default();

        items.sort_by(|a, b| a.id.cmp(&b.id));
        let filtered = match exclusive_start_key {
            Some(start) => items.into_iter().filter(|item| item.id < start).collect(),
            None => items,
        };

        let mut filtered: Vec<StreamItem> = filtered;
        filtered.sort_by(|a, b| b.id.cmp(&a.id));
        let has_more = filtered.len() > limit as usize;
        filtered.truncate(limit as usize);
        let last_evaluated_key = filtered.last().map(|item| item.id);

        Ok(StreamPage {
            items: filtered,
            last_evaluated_key,
            has_more,
        })
    }

    async fn create_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _position: CursorPosition,
    ) -> StreamResult<()> {
        unimplemented!("create_cursor not needed for pointer stream tests")
    }

    async fn delete_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
    ) -> StreamResult<()> {
        unimplemented!("delete_cursor not needed for pointer stream tests")
    }

    async fn read_from_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _limit: u32,
    ) -> StreamResult<CursorPage> {
        unimplemented!("read_from_cursor not needed for pointer stream tests")
    }

    async fn advance_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
        _to_item_id: StreamItemId,
    ) -> StreamResult<()> {
        unimplemented!("advance_cursor not needed for pointer stream tests")
    }

    async fn get_cursor(
        &self,
        _stream_name: StreamName,
        _cursor_name: CursorName,
    ) -> StreamResult<Option<StreamCursor>> {
        unimplemented!("get_cursor not needed for pointer stream tests")
    }

    async fn start_cleanup_task(&self, _parallelism: usize) -> StreamResult<()> {
        Ok(())
    }

    async fn stop_cleanup_task(&self) -> StreamResult<()> {
        Ok(())
    }

    async fn cleanup_expired_items(&self) -> StreamResult<u64> {
        Ok(0)
    }
}

fn stream_item_id(value: u8) -> StreamItemId {
    let mut bytes = [0u8; 12];
    bytes[11] = value;
    StreamItemId::from(bytes)
}

fn item_stream_version(value: u64) -> ItemStreamVersion {
    ItemStreamVersion::new(value)
}

fn build_stream_item(
    id: StreamItemId,
    stream_name: Option<StreamName>,
    data: Vec<u8>,
    data_type: StreamDataType,
) -> StreamItem {
    StreamItem {
        id,
        stream_name,
        data,
        data_type,
        created_at: TimestampMillis::from_timestamp(0),
    }
}

#[test]
fn pointer_stream_reads_item_stream_images() {
    futures::executor::block_on(async {
        let pointer_stream = StreamName::new(b"pointer-stream");
        let item_stream = StreamName::new(b"item-stream");
        let table_name = TableName::new("pointer_table");

        let new_item = HashMap::from([("pk".to_string(), AttributeValue::S("new".to_string()))]);
        let old_item = HashMap::from([("pk".to_string(), AttributeValue::S("old".to_string()))]);

        let old_id = StreamItemId::from(item_stream_version(1));
        let new_id = StreamItemId::from(item_stream_version(2));
        let pointer_id = stream_item_id(200);

        let item_stream_items = vec![
            build_stream_item(
                old_id,
                Some(item_stream.clone()),
                storage_types::storage_serde::to_bytes(&old_item).expect("old bytes"),
                StreamDataType::DynamoDbJson,
            ),
            build_stream_item(
                new_id,
                Some(item_stream.clone()),
                storage_types::storage_serde::to_bytes(&new_item).expect("new bytes"),
                StreamDataType::DynamoDbJson,
            ),
        ];

        let stored_pointer = StoredStreamPointer::pointer(
            item_stream.clone(),
            table_name.clone(),
            item_stream_version(2),
        );
        let pointer_stream_item = build_stream_item(
            pointer_id,
            None,
            storage_types::storage_serde::to_bytes(&stored_pointer).expect("pointer bytes"),
            StreamDataType::StreamPointer,
        );

        let provider = InMemoryStreamProvider::new(
            HashMap::from([(pointer_stream.clone(), vec![pointer_stream_item])]),
            HashMap::from([(item_stream.clone(), item_stream_items)]),
        );

        let result = provider
            .get_items_from_pointer_stream(pointer_stream, None, Some(10))
            .await
            .expect("pointer stream read");

        assert_eq!(result.records.len(), 1);
        let (pointer, items) = &result.records[0];
        assert_eq!(pointer.stream_name, item_stream);
        assert_eq!(pointer.table_name, table_name);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].data_type, StreamDataType::DynamoDbJson);
        assert_eq!(items[1].data_type, StreamDataType::DynamoDbJson);

        let decoded_new: HashMap<String, AttributeValue> =
            storage_types::storage_serde::from_bytes(&items[0].data).expect("decode new");
        let decoded_old: HashMap<String, AttributeValue> =
            storage_types::storage_serde::from_bytes(&items[1].data).expect("decode old");
        assert_eq!(decoded_new.get("pk"), new_item.get("pk"));
        assert_eq!(decoded_old.get("pk"), old_item.get("pk"));
    });
}

#[test]
fn embedded_pointer_stream_skips_item_stream_reads() {
    futures::executor::block_on(async {
        let pointer_stream = StreamName::new(b"pointer-stream");
        let item_stream = StreamName::new(b"item-stream");
        let table_name = TableName::new("embedded_table");

        let new_item = HashMap::from([("pk".to_string(), AttributeValue::S("new".to_string()))]);
        let old_item = HashMap::from([("pk".to_string(), AttributeValue::S("old".to_string()))]);

        let pointer_id = stream_item_id(2);
        let embedded = StoredStreamPointer::embedded(
            item_stream.clone(),
            table_name.clone(),
            item_stream_version(2),
            vec![
                EmbeddedStreamItem {
                    data: storage_types::storage_serde::to_bytes(&new_item).expect("new bytes"),
                    data_type: StreamDataType::DynamoDbJson,
                },
                EmbeddedStreamItem {
                    data: storage_types::storage_serde::to_bytes(&old_item).expect("old bytes"),
                    data_type: StreamDataType::DynamoDbJson,
                },
            ],
        );
        let pointer_stream_item = build_stream_item(
            pointer_id,
            None,
            storage_types::storage_serde::to_bytes(&embedded).expect("embedded bytes"),
            StreamDataType::StreamPointer,
        );

        let provider = InMemoryStreamProvider::new(
            HashMap::from([(pointer_stream.clone(), vec![pointer_stream_item])]),
            HashMap::new(),
        );

        let result = provider
            .get_items_from_pointer_stream(pointer_stream, None, Some(10))
            .await
            .expect("embedded pointer stream read");

        assert_eq!(result.records.len(), 1);
        let (pointer, items) = &result.records[0];
        assert_eq!(pointer.stream_name, item_stream);
        assert_eq!(pointer.table_name, table_name);
        assert_eq!(items.len(), 2);
        assert_eq!(provider.read_backward_calls(), 0);

        let decoded_new: HashMap<String, AttributeValue> =
            storage_types::storage_serde::from_bytes(&items[0].data).expect("decode new");
        let decoded_old: HashMap<String, AttributeValue> =
            storage_types::storage_serde::from_bytes(&items[1].data).expect("decode old");
        assert_eq!(decoded_new.get("pk"), new_item.get("pk"));
        assert_eq!(decoded_old.get("pk"), old_item.get("pk"));
    });
}

#[test]
fn embedded_pointer_stream_record_sequence_uses_target_item_version() {
    futures::executor::block_on(async {
        let pointer_stream = StreamName::new(b"pointer-stream");
        let item_stream = StreamName::new(b"item-stream");
        let table_name = TableName::new("embedded_sequence_table");

        let new_item = HashMap::from([("pk".to_string(), AttributeValue::S("new".to_string()))]);
        let old_item = HashMap::from([("pk".to_string(), AttributeValue::S("old".to_string()))]);

        let pointer_id = stream_item_id(9);
        let embedded = StoredStreamPointer::embedded(
            item_stream,
            table_name,
            item_stream_version(2),
            vec![
                EmbeddedStreamItem {
                    data: storage_types::storage_serde::to_bytes(&new_item).expect("new bytes"),
                    data_type: StreamDataType::DynamoDbJson,
                },
                EmbeddedStreamItem {
                    data: storage_types::storage_serde::to_bytes(&old_item).expect("old bytes"),
                    data_type: StreamDataType::DynamoDbJson,
                },
            ],
        );
        let pointer_stream_item = build_stream_item(
            pointer_id,
            None,
            storage_types::storage_serde::to_bytes(&embedded).expect("embedded bytes"),
            StreamDataType::StreamPointer,
        );

        let provider = InMemoryStreamProvider::new(
            HashMap::from([(pointer_stream.clone(), vec![pointer_stream_item])]),
            HashMap::new(),
        );

        let (records, last_pointer) = provider
            .get_stream_records_from_pointer_stream(
                pointer_stream,
                &[KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                None,
                Some(10),
            )
            .await
            .expect("embedded pointer stream records");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sequence_number, "2");
        assert_eq!(last_pointer, None);
        assert_eq!(provider.read_backward_calls(), 0);
    });
}

#[test]
fn pointer_stream_omits_last_evaluated_key_on_terminal_page() {
    futures::executor::block_on(async {
        let pointer_stream = StreamName::new(b"pointer-stream");
        let item_stream = StreamName::new(b"item-stream");
        let table_name = TableName::new("terminal_page_table");

        let first_pointer = build_stream_item(
            stream_item_id(1),
            None,
            storage_types::storage_serde::to_bytes(&StoredStreamPointer::embedded(
                item_stream.clone(),
                table_name.clone(),
                item_stream_version(1),
                vec![EmbeddedStreamItem {
                    data: b"first".to_vec(),
                    data_type: StreamDataType::Binary,
                }],
            ))
            .expect("first pointer bytes"),
            StreamDataType::StreamPointer,
        );
        let second_pointer = build_stream_item(
            stream_item_id(2),
            None,
            storage_types::storage_serde::to_bytes(&StoredStreamPointer::embedded(
                item_stream,
                table_name,
                item_stream_version(2),
                vec![EmbeddedStreamItem {
                    data: b"second".to_vec(),
                    data_type: StreamDataType::Binary,
                }],
            ))
            .expect("second pointer bytes"),
            StreamDataType::StreamPointer,
        );

        let provider = InMemoryStreamProvider::new(
            HashMap::from([(pointer_stream.clone(), vec![first_pointer, second_pointer])]),
            HashMap::new(),
        );

        let result = provider
            .get_items_from_pointer_stream(pointer_stream, Some(stream_item_id(1)), Some(10))
            .await
            .expect("pointer stream read");

        assert_eq!(result.records.len(), 1);
        assert!(
            result.last_evaluated_key.is_none(),
            "terminal pointer page should not emit a follow-up token"
        );
    });
}
