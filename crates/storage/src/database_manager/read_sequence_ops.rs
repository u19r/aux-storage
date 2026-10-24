use storage_types::{ReadSequenceRequest, ReadSequenceResponse, StorageResult};

use super::{DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, record_storage_operation};
use crate::admission::AdmissionClass;

impl DatabaseManager {
    /// Execute a bounded read graph in one provider-owned read attempt.
    /// Admission and native settlement cover the complete graph, including
    /// retries.
    pub async fn read_sequence(
        &self,
        request: ReadSequenceRequest,
    ) -> StorageResult<ReadSequenceResponse> {
        self.run_dynamo_admitted(
            ROUTED_DEFAULT_CONNECTION_ID,
            AdmissionClass::RangeRead,
            |dynamo| async move {
                record_storage_operation("read_sequence", dynamo.read_sequence(request)).await
            },
        )
        .await
    }
}
