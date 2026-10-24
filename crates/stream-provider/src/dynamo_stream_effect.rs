//! Stable effect identities and original receipts. Native commit positions
//! are deliberately excluded from the identity, including FoundationDB stamps.

use std::future::Future;

use storage_types::{
    StorageResult, StreamItemId, StreamName, SystemStreamEffectDigest, SystemStreamPosition,
};

use crate::{DynamoStreamEffectRow, StreamItem, StreamPointerReference};

/// Physical point reads joined to the caller's native snapshot. Results retain
/// input order and absence. The higher owner binds receipt identity,
/// incarnation and revision to accepted content before using a position or
/// importing rows.
pub trait DynamoStreamEffectAttempt: Send {
    fn load_stream_effects(
        &mut self,
        effects: &[SystemStreamEffectDigest],
    ) -> impl Future<Output = StorageResult<Vec<Option<DynamoStreamEffectRow>>>> + Send;

    /// Verified retained content at source-assigned positions. Exact effect
    /// identity accompanies the logical record. Absence is meaningful for
    /// released history; the caller qualifies retention.
    fn load_stream_records(
        &mut self,
        positions: &[SystemStreamPosition],
    ) -> impl Future<
        Output = StorageResult<
            Vec<Option<crate::retained_image::verified_system_stream::VerifiedSystemStreamRecord>>,
        >,
    > + Send;
    /// Original Dynamo item/table/system rows, excluding user-stream layouts.
    fn load_stream_items(
        &mut self,
        addresses: &[(&StreamName, StreamItemId)],
    ) -> impl Future<Output = StorageResult<Vec<Option<StreamItem>>>> + Send;

    /// Independently retained table/system pointer ownership at the same cut.
    fn load_stream_references(
        &mut self,
        addresses: &[(&StreamName, StreamItemId)],
    ) -> impl Future<Output = StorageResult<Vec<Option<StreamPointerReference>>>> + Send;
}
