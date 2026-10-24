use storage_types::StreamItemId;

use crate::StreamRetentionHold;

fn item(value: u8) -> StreamItemId {
    StreamItemId::from([value; 12])
}

#[test]
fn stream_head_uses_the_earliest_retained_row() {
    let earliest = item(2);
    let current = item(3);
    let floor = StreamRetentionHold::from(StreamItemId::default())
        .merge_trim_floor(Some(earliest), Some(current))
        .expect("stream head observation should resolve");
    assert!(floor == Some(earliest));
}

#[test]
fn stream_head_allows_an_empty_stream() {
    let current = item(3);
    let floor = StreamRetentionHold::from(StreamItemId::default())
        .merge_trim_floor(None, Some(current))
        .expect("empty stream should not add a retention floor");
    assert!(floor == Some(current));
}

#[test]
fn exact_boundary_must_still_be_retained() {
    let boundary = item(2);
    assert!(
        StreamRetentionHold::from(boundary)
            .merge_trim_floor(None, None)
            .is_err()
    );
    assert!(
        StreamRetentionHold::from(boundary)
            .merge_trim_floor(Some(item(3)), None)
            .is_err()
    );
    assert!(
        StreamRetentionHold::from(boundary)
            .merge_trim_floor(Some(boundary), None)
            .is_ok()
    );
}
