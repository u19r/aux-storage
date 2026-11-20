//! The delivery payload and state rows for SQLite, Turso and Postgres (W3S
//! plan 5.4). The payload row is written once and deleted with its state.
//! The state row holds the schedule; `due_at` is the only stored due time
//! (R2) and the index `(destination, due_at, record_id)` is the only due
//! source. Writes are conditional on `(record_id, generation, lease_nonce)`.

use std::num::NonZeroU32;

use pubsub_provider::{
    AskMode, ClaimRows, ClaimScan, DEADLINE_HEAD_WINDOW, DeliveryFence, DeliveryLease,
    DeliveryLocator, DeliveryPayload, DeliveryRecordId, DeliveryState, DestinationAsk,
    DestinationHead, DestinationId, PayloadRow, RecordObservations, RecordReads, StateChange,
    StateRow, SweepCursor, SweepLane, SweepPage,
};
use storage_types::{
    MissExpectation, ReadProtection, StorageError, StorageResult, TimestampMillis,
};

use crate::{
    SqlAttempt, SqlEngine, SqlLayout, SqlRow, SqlStatement, SqlValue,
    constants::{PUBSUB_CATALOG_READ_BYTES, PUBSUB_DELIVERY_READ_BYTES},
};

const STATE_COLUMNS: &str = "s.record_id, s.destination, s.destination_key, s.due_at, \
                             s.subscription_arn, s.retry_deadline, s.attempts, s.generation, \
                             s.lease_owner, s.lease_nonce, s.lease_until, s.last_failure";

pub(crate) async fn initialize_pubsub_schedule<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    let (text, integer, blob) = (
        engine.engine().text_type(),
        engine.engine().integer_type(),
        engine.engine().blob_type(),
    );
    let suffix = engine.engine().table_suffix();
    let statements = [
        format!(
            "CREATE TABLE IF NOT EXISTS {} (record_id {text} PRIMARY KEY, row_blob {blob} NOT \
             NULL){suffix}",
            engine.relation_name("sys_pubsub_delivery_payload"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (record_id {text} PRIMARY KEY, destination {text} NOT \
             NULL, destination_key {text} NOT NULL, due_at {integer} NOT NULL, subscription_arn \
             {text} NOT NULL, retry_deadline {integer} NOT NULL, attempts {integer} NOT NULL, \
             generation {integer} NOT NULL, lease_owner {text}, lease_nonce {blob}, lease_until \
             {integer}, last_failure {text}){suffix}",
            engine.relation_name("sys_pubsub_delivery_state"),
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}(destination, due_at, record_id)",
            engine.relation_name("sys_pubsub_delivery_partition"),
            engine.relation_name("sys_pubsub_delivery_state"),
        ),
    ];
    for sql in statements {
        native.execute(SqlStatement::new(sql, vec![])).await?;
    }
    Ok(())
}

/// Insert only: an existing id is a conflict (a chunk materialized twice).
pub(crate) async fn store_new_deliveries<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    rows: Vec<(DeliveryPayload, DeliveryState)>,
) -> StorageResult<()> {
    let payload_sql = format!(
        "INSERT INTO {} (record_id, row_blob) VALUES ({}) ON CONFLICT DO NOTHING",
        engine.relation_name("sys_pubsub_delivery_payload"),
        engine.engine().placeholders(2),
    );
    let state_sql = format!(
        "INSERT INTO {} (record_id, destination, destination_key, due_at, subscription_arn, \
         retry_deadline, attempts, generation, lease_owner, lease_nonce, lease_until, \
         last_failure) VALUES ({}) ON CONFLICT DO NOTHING",
        engine.relation_name("sys_pubsub_delivery_state"),
        engine.engine().placeholders(12),
    );
    let mut statements = Vec::with_capacity(rows.len() * 2);
    for (payload, state) in rows {
        if payload.id != state.id {
            return Err(StorageError::internal("delivery payload and state differ"));
        }
        statements.push(SqlStatement::new(
            payload_sql.clone(),
            vec![
                SqlValue::Text(payload.id.0.clone()),
                SqlValue::Blob(payload.encode()?),
            ],
        ));
        let mut binds = vec![
            SqlValue::Text(state.id.0.clone()),
            SqlValue::Text(state.destination.id().to_string()),
            SqlValue::Text(state.destination.as_str().to_owned()),
            SqlValue::Integer(state.due_at.timestamp_millis()),
            SqlValue::Text(state.subscription_arn.to_string()),
            SqlValue::Integer(state.retry_deadline.timestamp_millis()),
        ];
        binds.extend(schedule_binds(&state)?);
        statements.push(SqlStatement::new(state_sql.clone(), binds));
    }
    if native
        .execute_batch_exact(statements)
        .await?
        .iter()
        .any(|changed| *changed != 1)
    {
        return Err(StorageError::write_miss(
            "pubsub_delivery",
            MissExpectation::Absent,
        ));
    }
    Ok(())
}

/// One bounded statement per ask and one `next_due` read per ask, in one
/// batch. Postgres locks only the state rows (`FOR UPDATE OF s SKIP
/// LOCKED`); the payload is immutable. SQLite and Turso serialize writers.
pub(crate) async fn scan_claimable<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    scan: &ClaimScan,
) -> StorageResult<storage_types::Exact<ClaimRows>> {
    let mut statements = Vec::with_capacity(scan.asks.len() * 2);
    for ask in &scan.asks {
        statements.push(claim_statement(engine, ask, scan.now)?);
        statements.push(next_due_statement(engine, ask)?);
    }
    let pages = native
        .query_batch_protected_exact(statements, PUBSUB_DELIVERY_READ_BYTES)
        .await?;
    // Pages alternate claim, next-due, one pair per ask.
    let (claim_pages, next_pages): (Vec<_>, Vec<_>) = pages
        .into_iter()
        .enumerate()
        .partition(|(index, _)| index % 2 == 0);
    let mut claims = Vec::with_capacity(scan.asks.len());
    for ((ask, (_, claimed)), (_, next)) in scan.asks.iter().zip(claim_pages).zip(next_pages) {
        let rows = claimed
            .rows
            .into_iter()
            .map(decode_claim_row)
            .collect::<StorageResult<Vec<_>>>()?;
        // The first of the `take + 1` head records that this claim did not
        // read (W3S review finding 5): a due record that a lock or the
        // deadline filter skipped, or the record after the claimed ones.
        let mut next_due = None;
        for head in next.rows {
            let (due, id) = decode_due(head)?;
            let is_read = rows.iter().any(|(state, _)| match state {
                StateRow::Decoded(state) => state.id.0 == id,
                StateRow::Undecodable(locator) => locator.id.0 == id,
            });
            if !is_read {
                next_due = Some(due);
                break;
            }
        }
        claims.push(ClaimRows {
            destination: ask.destination.id(),
            rows,
            next_due,
        });
    }
    storage_types::Exact::try_new(scan.asks.len(), claims)
}

fn claim_statement(
    engine: impl SqlLayout,
    ask: &DestinationAsk,
    now: TimestampMillis,
) -> StorageResult<(SqlStatement, ReadProtection)> {
    let state = engine.relation_name("sys_pubsub_delivery_state");
    let payload = engine.relation_name("sys_pubsub_delivery_payload");
    let p = |index| engine.engine().placeholder(index);
    let filter = match ask.mode {
        AskMode::Due | AskMode::Probe => {
            format!("s.destination = {} AND s.due_at <= {}", p(1), p(2))
        }
        AskMode::DeadlineOnly => format!(
            "s.record_id IN (SELECT w.record_id FROM {state} w WHERE w.destination = {} AND \
             w.due_at <= {} ORDER BY w.due_at, w.record_id LIMIT {}) AND s.retry_deadline <= {}",
            p(1),
            p(2),
            DEADLINE_HEAD_WINDOW,
            p(2),
        ),
    };
    let (lock, protection) = match engine.engine() {
        SqlEngine::Postgres => (" FOR UPDATE OF s SKIP LOCKED", ReadProtection::Snapshot),
        SqlEngine::Sqlite | SqlEngine::Turso => ("", ReadProtection::Claim),
    };
    let sql = format!(
        "SELECT {STATE_COLUMNS}, p.row_blob FROM {state} s LEFT JOIN {payload} p ON p.record_id = \
         s.record_id WHERE {filter} ORDER BY s.due_at, s.record_id LIMIT {}{lock}",
        p(3),
    );
    let binds = vec![
        SqlValue::Text(ask.destination.id().to_string()),
        SqlValue::Integer(now.timestamp_millis()),
        SqlValue::Integer(i64::from(ask.take.get())),
    ];
    Ok((SqlStatement::new(sql, binds), protection))
}

/// The first `take + 1` records of the destination in due order, read
/// without a lock: the first one that the claim did not read gives the
/// timer hint.
fn next_due_statement(
    engine: impl SqlLayout,
    ask: &DestinationAsk,
) -> StorageResult<(SqlStatement, ReadProtection)> {
    let sql = format!(
        "SELECT due_at, record_id FROM {} WHERE destination = {} ORDER BY due_at, record_id LIMIT \
         {}",
        engine.relation_name("sys_pubsub_delivery_state"),
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
    );
    let binds = vec![
        SqlValue::Text(ask.destination.id().to_string()),
        SqlValue::Integer(i64::from(ask.take.get()) + 1),
    ];
    Ok((SqlStatement::new(sql, binds), ReadProtection::Snapshot))
}

/// Rows that this attempt read with claim protection: the generation guard
/// is a physical check; a count other than one is a conflict.
pub(crate) async fn store_claimed_states<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    claims: Vec<(DeliveryState, DeliveryState)>,
) -> StorageResult<()> {
    if claims.is_empty() {
        return Ok(());
    }
    let statements = claims
        .iter()
        .map(|(previous, next)| update_statement(engine, previous.generation, None, next))
        .collect::<StorageResult<Vec<_>>>()?;
    if native
        .execute_batch(statements)
        .await?
        .iter()
        .any(|changed| *changed != 1)
    {
        return Err(StorageError::write_miss(
            "pubsub_delivery_state",
            MissExpectation::Revision,
        ));
    }
    Ok(())
}

/// Fenced updates; `false` where the fence no longer holds.
pub(crate) async fn store_delivery_states<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    changes: Vec<StateChange>,
) -> StorageResult<Vec<bool>> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }
    let statements = changes
        .iter()
        .map(|change| {
            let expected = &change.expected;
            update_statement(engine, expected.generation, Some(expected), &change.next)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(native
        .execute_batch(statements)
        .await?
        .into_iter()
        .map(|changed| changed == 1)
        .collect())
}

fn update_statement(
    engine: impl SqlLayout,
    generation: u64,
    fence: Option<&DeliveryFence>,
    next: &DeliveryState,
) -> StorageResult<SqlStatement> {
    let p = |index| engine.engine().placeholder(index);
    let nonce = if fence.is_some() {
        format!(" AND lease_nonce = {}", p(10))
    } else {
        String::new()
    };
    let sql = format!(
        "UPDATE {} SET due_at = {}, attempts = {}, generation = {}, lease_owner = {}, lease_nonce \
         = {}, lease_until = {}, last_failure = {} WHERE record_id = {} AND generation = {}{nonce}",
        engine.relation_name("sys_pubsub_delivery_state"),
        p(1),
        p(2),
        p(3),
        p(4),
        p(5),
        p(6),
        p(7),
        p(8),
        p(9),
    );
    let mut binds = vec![SqlValue::Integer(next.due_at.timestamp_millis())];
    binds.extend(schedule_binds(next)?);
    binds.push(SqlValue::Text(next.id.0.clone()));
    binds.push(SqlValue::Integer(stored_generation(generation)?));
    if let Some(fence) = fence {
        binds.push(SqlValue::Blob(fence.nonce.as_bytes().to_vec()));
    }
    Ok(SqlStatement::new(sql, binds))
}

/// Fenced terminal deletes in the caller's (record id) order: the state
/// row, then its payload when no state row is left. `false`: fence lost.
/// The fenced state delete and the payload delete of one delivery: the
/// payload goes only when no state row of the record is left.
pub(crate) fn delivery_delete_statements(
    engine: impl SqlLayout,
    fence: &DeliveryFence,
) -> StorageResult<[SqlStatement; 2]> {
    let state = engine.relation_name("sys_pubsub_delivery_state");
    let p = |index| engine.engine().placeholder(index);
    let state_sql = format!(
        "DELETE FROM {state} WHERE record_id = {} AND generation = {} AND lease_nonce = {}",
        p(1),
        p(2),
        p(3),
    );
    let payload_sql = format!(
        "DELETE FROM {} WHERE record_id = {} AND NOT EXISTS (SELECT 1 FROM {state} WHERE \
         record_id = {})",
        engine.relation_name("sys_pubsub_delivery_payload"),
        p(1),
        p(2),
    );
    let id = || SqlValue::Text(fence.locator.id.0.clone());
    Ok([
        SqlStatement::new(
            state_sql,
            vec![
                id(),
                SqlValue::Integer(stored_generation(fence.generation)?),
                SqlValue::Blob(fence.nonce.as_bytes().to_vec()),
            ],
        ),
        SqlStatement::new(payload_sql, vec![id(), id()]),
    ])
}

pub(crate) async fn delete_deliveries<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    fences: Vec<DeliveryFence>,
) -> StorageResult<Vec<bool>> {
    if fences.is_empty() {
        return Ok(Vec::new());
    }
    let mut statements = Vec::with_capacity(fences.len() * 2);
    for fence in &fences {
        statements.extend(delivery_delete_statements(engine, fence)?);
    }
    let changed = native.execute_batch_exact(statements).await?.into_vec();
    Ok(changed.chunks(2).map(|pair| pair[0] == 1).collect())
}

/// The fence states (`Conflict`) and the queue catalog row (`Snapshot`) in
/// one `query_batch_protected` (W3 C8; one pipeline on Postgres). A fence
/// holds when its exact `(record_id, generation, lease_nonce)` row exists.
pub(crate) async fn load_records<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    reads: RecordReads<'_>,
) -> StorageResult<RecordObservations> {
    let count = reads.fences.len() + usize::from(reads.queue_name.is_some());
    if count == 0 {
        return Ok(RecordObservations {
            held: storage_types::Exact::empty(),
            queue: None,
        });
    }
    let p = |index| engine.engine().placeholder(index);
    let held_sql = format!(
        "SELECT record_id FROM {} WHERE record_id = {} AND generation = {} AND lease_nonce = {}",
        engine.relation_name("sys_pubsub_delivery_state"),
        p(1),
        p(2),
        p(3),
    );
    let mut statements = Vec::with_capacity(count);
    for fence in reads.fences {
        statements.push((
            SqlStatement::new(
                held_sql.clone(),
                vec![
                    SqlValue::Text(fence.locator.id.0.clone()),
                    SqlValue::Integer(stored_generation(fence.generation)?),
                    SqlValue::Blob(fence.nonce.as_bytes().to_vec()),
                ],
            ),
            ReadProtection::Conflict,
        ));
    }
    if let Some(name) = reads.queue_name {
        statements.push((
            crate::queue_catalog::queue_by_name_statement(engine, name),
            ReadProtection::Snapshot,
        ));
    }
    let mut pages = native
        .query_batch_protected_exact(statements, crate::constants::QUEUE_CATALOG_READ_BYTES)
        .await?
        .into_iter();
    // Fence pages come first; the queue catalog page, when asked, is last.
    let held_pages = pages.by_ref().take(reads.fences.len()).collect::<Vec<_>>();
    let queue = match reads.queue_name.zip(pages.next()) {
        Some((name, page)) => crate::queue_catalog::decode_named_page(page, name)?,
        None => None,
    };
    let held = held_pages
        .into_iter()
        .map(|page| {
            if page.more || page.rows.len() > 1 {
                return Err(StorageError::internal(
                    "SQL delivery fence row is not unique",
                ));
            }
            Ok(page.rows.len() == 1)
        })
        .collect::<StorageResult<Vec<bool>>>()?;
    Ok(RecordObservations {
        held: storage_types::Exact::try_new(reads.fences.len(), held)?,
        queue,
    })
}

/// Move one state row that does not decode, or whose payload does not, one
/// hour ahead; the row must still be at the observed due time.
pub(crate) async fn move_undecodable_state<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    locator: &DeliveryLocator,
    until: TimestampMillis,
) -> StorageResult<()> {
    let sql = format!(
        "UPDATE {} SET due_at = {} WHERE record_id = {} AND due_at = {}",
        engine.relation_name("sys_pubsub_delivery_state"),
        engine.engine().placeholder(1),
        engine.engine().placeholder(2),
        engine.engine().placeholder(3),
    );
    let binds = vec![
        SqlValue::Integer(until.timestamp_millis()),
        SqlValue::Text(locator.id.0.clone()),
        SqlValue::Integer(locator.due_at.timestamp_millis()),
    ];
    if native.execute(SqlStatement::new(sql, binds)).await? != 1 {
        return Err(StorageError::write_miss(
            "pubsub_delivery_state",
            MissExpectation::Present,
        ));
    }
    Ok(())
}

/// Delete one state row at its observed due time, then its payload when no
/// state row of the record is left.
pub(crate) async fn delete_state_at<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    locator: &DeliveryLocator,
) -> StorageResult<()> {
    let state = engine.relation_name("sys_pubsub_delivery_state");
    let p = |index| engine.engine().placeholder(index);
    let id = || SqlValue::Text(locator.id.0.clone());
    let changed = native
        .execute_batch(vec![
            SqlStatement::new(
                format!(
                    "DELETE FROM {state} WHERE record_id = {} AND due_at = {}",
                    p(1),
                    p(2)
                ),
                vec![id(), SqlValue::Integer(locator.due_at.timestamp_millis())],
            ),
            SqlStatement::new(
                format!(
                    "DELETE FROM {} WHERE record_id = {} AND NOT EXISTS (SELECT 1 FROM {state} \
                     WHERE record_id = {})",
                    engine.relation_name("sys_pubsub_delivery_payload"),
                    p(1),
                    p(2),
                ),
                vec![id(), id()],
            ),
        ])
        .await?;
    if changed.first() != Some(&1) {
        return Err(StorageError::write_miss(
            "pubsub_delivery_state",
            MissExpectation::Present,
        ));
    }
    Ok(())
}

/// The heads of up to `limit` destinations after the cursor, in destination
/// order: one recursive statement; each step and each head is one index seek.
pub(crate) async fn scan_destination_heads<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    after: &SweepCursor,
    limit: NonZeroU32,
) -> StorageResult<SweepPage> {
    let start = match after.0.first() {
        Some(SweepLane::Done) => {
            return Ok(SweepPage {
                heads: Vec::new(),
                next: None,
            });
        }
        Some(SweepLane::After(Some(id))) => id.to_string(),
        Some(SweepLane::After(None)) | None => String::new(),
    };
    let sql = sweep_sql(engine);
    let binds = vec![
        SqlValue::Text(start),
        SqlValue::Integer(i64::from(limit.get())),
    ];
    let page = native
        .query(
            SqlStatement::new(sql, binds),
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    let visited = page.rows.len();
    let mut heads = Vec::with_capacity(visited);
    let mut undecodable = Vec::new();
    let mut last = None;
    for row in page.rows {
        let (id, head) = decode_head(row)?;
        last = Some(id);
        match head {
            Ok(head) => heads.push(head),
            Err(times) => undecodable.push((id, times)),
        }
    }
    heads.extend(undecodable_heads(native, engine, undecodable).await?);
    let next = (visited >= limit.get() as usize).then(|| SweepCursor(vec![SweepLane::After(last)]));
    Ok(SweepPage { heads, next })
}

/// Each recursive step finds the next destination with one seek on the
/// partition index; each head is one seek and one key lookup. Idle
/// destinations cost nothing; a dead destination costs one seek per visit.
pub(crate) fn sweep_sql(engine: impl SqlLayout) -> String {
    let state = engine.relation_name("sys_pubsub_delivery_state");
    let p = |index| engine.engine().placeholder(index);
    format!(
        "WITH RECURSIVE d(destination, n) AS (SELECT (SELECT destination FROM {state} WHERE \
         destination > {} ORDER BY destination LIMIT 1), CAST(1 AS BIGINT) UNION ALL SELECT \
         (SELECT destination FROM {state} WHERE destination > d.destination ORDER BY destination \
         LIMIT 1), d.n + 1 FROM d WHERE d.destination IS NOT NULL AND d.n < {}) SELECT \
         h.destination, h.destination_key, h.due_at, h.retry_deadline FROM d JOIN {state} h ON \
         h.record_id = (SELECT s.record_id FROM {state} s WHERE s.destination = d.destination \
         ORDER BY s.due_at, s.record_id LIMIT 1) ORDER BY d.n",
        p(1),
        p(2),
    )
}

/// The read of one delivery (audits, and the existence check of a
/// subscription confirmation).
pub(crate) async fn load_delivery<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    id: &DeliveryRecordId,
) -> StorageResult<Option<(StateRow, PayloadRow)>> {
    let sql = format!(
        "SELECT {STATE_COLUMNS}, p.row_blob FROM {} s LEFT JOIN {} p ON p.record_id = s.record_id \
         WHERE s.record_id = {}",
        engine.relation_name("sys_pubsub_delivery_state"),
        engine.relation_name("sys_pubsub_delivery_payload"),
        engine.engine().placeholder(1),
    );
    let page = native
        .query(
            SqlStatement::new(sql, vec![SqlValue::Text(id.0.clone())]),
            // Postgres cannot lock the nullable side of the payload join; an
            // insert of the same id conflicts, so no lock is needed.
            ReadProtection::Snapshot,
            PUBSUB_DELIVERY_READ_BYTES,
        )
        .await?;
    page.rows
        .into_iter()
        .next()
        .map(decode_claim_row)
        .transpose()
}

/// `attempts`, `generation`, lease and failure binds, in column order.
fn schedule_binds(state: &DeliveryState) -> StorageResult<Vec<SqlValue>> {
    let lease = state.lease.as_ref();
    Ok(vec![
        SqlValue::Integer(i64::from(state.attempts)),
        SqlValue::Integer(stored_generation(state.generation)?),
        lease.map_or(SqlValue::Null, |lease| SqlValue::Text(lease.owner.clone())),
        lease.map_or(SqlValue::Null, |lease| {
            SqlValue::Blob(lease.nonce.as_bytes().to_vec())
        }),
        lease.map_or(SqlValue::Null, |lease| {
            SqlValue::Integer(lease.until.timestamp_millis())
        }),
        state
            .last_failure
            .map_or(SqlValue::Null, |failure| SqlValue::Text(failure.stored())),
    ])
}

fn stored_generation(generation: u64) -> StorageResult<i64> {
    i64::try_from(generation)
        .map_err(|_| StorageError::internal("delivery generation exceeds storage range"))
}

/// A state and its payload. A row whose typed columns are malformed is a
/// schema defect (an error); a row whose values do not decode is a typed
/// observation that the provider sets aside.
fn decode_claim_row(row: SqlRow) -> StorageResult<(StateRow, PayloadRow)> {
    let mut values = row.into_values();
    let blob = values
        .pop()
        .ok_or_else(|| StorageError::internal("SQL delivery row has invalid width"))?;
    let state = decode_state(values)?;
    let id = match &state {
        StateRow::Decoded(state) => state.id.clone(),
        StateRow::Undecodable(locator) => locator.id.clone(),
    };
    let payload = match blob {
        SqlValue::Blob(bytes) => DeliveryPayload::decode(&bytes, &id)
            .map_or(PayloadRow::Undecodable, |payload| {
                PayloadRow::Decoded(Box::new(payload))
            }),
        SqlValue::Null => PayloadRow::Missing,
        _ => PayloadRow::Undecodable,
    };
    Ok((state, payload))
}

fn decode_state(values: Vec<SqlValue>) -> StorageResult<StateRow> {
    let [
        id,
        destination,
        key,
        due,
        subscription,
        deadline,
        attempts,
        generation,
        owner,
        nonce,
        until,
        failure,
    ]: [SqlValue; 12] = values
        .try_into()
        .map_err(|_| StorageError::internal("SQL delivery state has invalid width"))?;
    let (SqlValue::Text(id), SqlValue::Text(destination), SqlValue::Integer(due)) =
        (id, destination, due)
    else {
        return Err(StorageError::internal(
            "SQL delivery state has invalid types",
        ));
    };
    let locator = DeliveryLocator {
        destination: destination.parse::<DestinationId>()?,
        due_at: TimestampMillis::from_timestamp(due),
        id: DeliveryRecordId(id),
    };
    let decoded = (|| -> Option<DeliveryState> {
        let SqlValue::Text(key) = key else {
            return None;
        };
        let destination = key.parse::<pubsub_provider::DestinationKey>().ok()?;
        let lease = match (owner, nonce, until) {
            (SqlValue::Text(owner), SqlValue::Blob(nonce), SqlValue::Integer(until)) => {
                Some(DeliveryLease {
                    owner,
                    nonce: uuid::Uuid::from_slice(&nonce).ok()?,
                    until: TimestampMillis::from_timestamp(until),
                })
            }
            (SqlValue::Null, SqlValue::Null, SqlValue::Null) => None,
            _ => return None,
        };
        let last_failure = match failure {
            SqlValue::Text(failure) => Some(failure.parse().ok()?),
            SqlValue::Null => None,
            _ => return None,
        };
        let (
            SqlValue::Text(subscription),
            SqlValue::Integer(deadline),
            SqlValue::Integer(attempts),
            SqlValue::Integer(generation),
        ) = (subscription, deadline, attempts, generation)
        else {
            return None;
        };
        (destination.id() == locator.destination).then_some(DeliveryState {
            id: locator.id.clone(),
            destination,
            due_at: locator.due_at,
            subscription_arn: pubsub_provider::SubscriptionArn::new(subscription).ok()?,
            retry_deadline: TimestampMillis::from_timestamp(deadline),
            attempts: u32::try_from(attempts).ok()?,
            generation: u64::try_from(generation).ok()?,
            lease,
            last_failure,
        })
    })();
    Ok(decoded.map_or(StateRow::Undecodable(locator), StateRow::Decoded))
}

/// A head whose `destination_key` does not parse still names its
/// destination, so that a claim is asked and sets the head aside (W3S review
/// finding 2): the key text comes from a later row of the same partition.
/// One batch of bounded reads; a partition with no parseable key is skipped.
async fn undecodable_heads<A: SqlAttempt>(
    native: &mut A,
    engine: impl SqlLayout,
    undecodable: Vec<(DestinationId, (TimestampMillis, TimestampMillis))>,
) -> StorageResult<Vec<DestinationHead>> {
    if undecodable.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT destination_key FROM {} WHERE destination = {} ORDER BY due_at, record_id LIMIT 16",
        engine.relation_name("sys_pubsub_delivery_state"),
        engine.engine().placeholder(1),
    );
    let statements = undecodable
        .iter()
        .map(|(id, _)| SqlStatement::new(sql.clone(), vec![SqlValue::Text(id.to_string())]))
        .collect();
    let pages = native
        .query_batch(
            statements,
            ReadProtection::Snapshot,
            PUBSUB_CATALOG_READ_BYTES,
        )
        .await?;
    Ok(undecodable
        .into_iter()
        .zip(pages)
        .filter_map(|((id, (head_due, head_retry_deadline)), page)| {
            let destination =
                page.rows
                    .into_iter()
                    .find_map(|row| match row.into_values().as_slice() {
                        [SqlValue::Text(key)] => key
                            .parse::<pubsub_provider::DestinationKey>()
                            .ok()
                            .filter(|key| key.id() == id),
                        _ => None,
                    })?;
            Some(DestinationHead {
                destination,
                head_due,
                head_retry_deadline,
            })
        })
        .collect())
}

/// The head of one destination, or its times when its key does not parse.
type DecodedHead = Result<DestinationHead, (TimestampMillis, TimestampMillis)>;

fn decode_head(row: SqlRow) -> StorageResult<(DestinationId, DecodedHead)> {
    let [destination, key, due, deadline]: [SqlValue; 4] = row
        .into_values()
        .try_into()
        .map_err(|_| StorageError::internal("SQL destination head has invalid width"))?;
    let (
        SqlValue::Text(destination),
        SqlValue::Text(key),
        SqlValue::Integer(due),
        SqlValue::Integer(deadline),
    ) = (destination, key, due, deadline)
    else {
        return Err(StorageError::internal(
            "SQL destination head has invalid types",
        ));
    };
    let id = destination.parse::<DestinationId>()?;
    let (head_due, head_retry_deadline) = (
        TimestampMillis::from_timestamp(due),
        TimestampMillis::from_timestamp(deadline),
    );
    let head = key
        .parse::<pubsub_provider::DestinationKey>()
        .ok()
        .filter(|destination| destination.id() == id)
        .map(|destination| DestinationHead {
            destination,
            head_due,
            head_retry_deadline,
        })
        .ok_or((head_due, head_retry_deadline));
    Ok((id, head))
}

fn decode_due(row: SqlRow) -> StorageResult<(TimestampMillis, String)> {
    match <[SqlValue; 2]>::try_from(row.into_values()) {
        Ok([SqlValue::Integer(due), SqlValue::Text(id)]) => {
            Ok((TimestampMillis::from_timestamp(due), id))
        }
        _ => Err(StorageError::internal("SQL delivery due has invalid type")),
    }
}
