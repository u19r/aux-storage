use http_error::HttpApiError;
use storage_types::DescribeTimeToLiveRequest;

use crate::{manager::StorageApiManagerImpl, types::Response};

impl StorageApiManagerImpl {
    pub(super) async fn describe_time_to_live_internal(
        &self,
        request: DescribeTimeToLiveRequest,
    ) -> Result<Response, HttpApiError> {
        let response = self.db().describe_time_to_live(&request.table_name).await?;
        Ok(Response::DescribeTimeToLive(response))
    }
}
