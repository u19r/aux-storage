//! The existing Dynamo physical capabilities lent by one native attempt.
//! This names the contract once for ordinary and generation-scoped views.

use crate::{
    CreateTableAttempt, DeleteTableAttempt, DynamoRangeReadAttempt, DynamoTransactWriteAttempt,
    GetItemAttempt, ListStreamsAttempt, ListTablesAttempt, PreparedWriteAttempt,
    UpdateTimeToLiveAttempt, dynamo_table_stream::DynamoTableStreamAttempt,
};

pub trait DynamoAttempt:
    CreateTableAttempt
    + DeleteTableAttempt
    + DynamoRangeReadAttempt
    + DynamoTransactWriteAttempt
    + GetItemAttempt
    + ListTablesAttempt
    + ListStreamsAttempt
    + UpdateTimeToLiveAttempt
    + DynamoTableStreamAttempt
    + PreparedWriteAttempt
    + crate::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>
{
}

impl<A> DynamoAttempt for A where A: CreateTableAttempt
        + DeleteTableAttempt
        + DynamoRangeReadAttempt
        + DynamoTransactWriteAttempt
        + GetItemAttempt
        + ListTablesAttempt
        + ListStreamsAttempt
        + UpdateTimeToLiveAttempt
        + DynamoTableStreamAttempt
        + PreparedWriteAttempt
        + crate::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>
{
}
