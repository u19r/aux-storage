//! Postgres catalog lock: one transaction-level advisory lock for each table
//! name. SQLite and Turso have one writer and send nothing.
//!
//! Rules (W6F section 4.2):
//! - The lock key is the schema and the table name, so two schemas of one
//!   database never share a lock.
//! - An attempt takes the lock in the first window that reads the catalog of
//!   the table, before it reads the stamp. Item writers (`ReadWriteItems`, READ
//!   COMMITTED) take it shared and then read the stamp without a row lock: the
//!   new statement snapshot sees every change that committed before the lock
//!   was granted, and no change can commit until the attempt ends.
//! - Background pages (`ReadWrite` with `DeadlineClass::BackgroundPage`,
//!   SERIALIZABLE) take it shared. Their transaction snapshot can be older than
//!   the lock, so they keep the row lock on the stamp; a concurrent change then
//!   fails the attempt with a serialization error. That row lock assigns a
//!   transaction ID. Ordinary expiration pages use ReadWriteItems instead; the
//!   protected TTL fill that can activate policy takes the lock exclusive
//!   before its first read (`lock_catalog_for_change`).
//! - A regional pair end (ReadWriteItems, READ COMMITTED) takes the lock
//!   exclusive before its first read, so its cleanup deletes see every marker
//!   and progress row that a shared holder committed before the grant. A
//!   SERIALIZABLE snapshot is taken before the lock wait and would miss them.
//! - Other `ReadWrite` attempts (schema changes, SERIALIZABLE) take it
//!   exclusive and keep the row lock on the stamp for the same reason.
//! - Every path that takes item locks holds the catalog lock of each target
//!   table first (`item_lock_prefix` rejects anything else).
//! - Every statement that changes a catalog stamp or binding runs through
//!   `execute_catalog_change`, after the exclusive lock.
//! - No exclusive lock after an item lock: a writer that holds the shared lock
//!   can wait for that item lock.
//! - The first ordering lock of an attempt (the exclusive catalog lock or the
//!   first item or token lock) saves the transaction ID state before waiting.
//!   The stream position comes from the transaction ID, so a write attempt that
//!   has an ID before its ordering lock must not reserve a stream position.
//! - A later item-lock window that adds an item checks the state again: an ID
//!   assigned since the first ordering lock (a write) would have waited with a
//!   stream position already fixed, so that window is rejected. A window that
//!   locks only items this attempt holds cannot wait and is not checked.

use std::num::NonZeroUsize;

use storage_types::{
    AttemptMode, AttemptRequest, DeadlineClass, StorageError, StorageResult, TableName,
};

use crate::{
    ColumnIndex, ReadProtection, SqlAttempt, SqlEngine, SqlLayout, SqlQueryPage, SqlStatement,
    SqlValue, catalog::SqlCatalogAttempt, constants::POSTGRES_CATALOG_LOCK_CLASS,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogLockMode {
    Shared,
    Exclusive,
}

/// The catalog locks of one native attempt.
pub(crate) struct CatalogLockState {
    /// `None`: read-only attempt or a single-writer engine.
    mode: Option<CatalogLockMode>,
    /// READ COMMITTED: a statement after the lock sees every earlier change.
    snapshot_after_lock: bool,
    postgres: bool,
    held: Vec<(TableName, CatalogLockMode)>,
    ordering_locked: bool,
    item_locked: bool,
    /// Item lock identities this attempt holds.
    held_items: Vec<String>,
    xid_before_ordering_lock: bool,
}

/// Lock statements at the head of one native window. Pass their pages to
/// `finish_lock_prefix` before the caller's pages.
#[must_use]
pub(crate) struct LockPrefix {
    xid_check: XidCheck,
    statements: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum XidCheck {
    None,
    /// The first ordering lock records whether an ID was assigned.
    First,
    /// A later item-lock window rejects an ID assigned since the first.
    Later,
}

impl CatalogLockState {
    pub(crate) fn new(engine: SqlEngine, request: AttemptRequest) -> Self {
        let postgres = engine == SqlEngine::Postgres;
        Self {
            mode: match (request.mode, request.class()) {
                _ if !postgres => None,
                (AttemptMode::ReadOnly, _) => None,
                (AttemptMode::ReadWriteItems, _)
                | (AttemptMode::ReadWrite, DeadlineClass::BackgroundPage) => {
                    Some(CatalogLockMode::Shared)
                }
                (AttemptMode::ReadWrite, _) => Some(CatalogLockMode::Exclusive),
            },
            snapshot_after_lock: request.mode == AttemptMode::ReadWriteItems,
            postgres,
            held: Vec::new(),
            ordering_locked: false,
            item_locked: false,
            held_items: Vec::new(),
            xid_before_ordering_lock: false,
        }
    }

    /// The protection of a catalog row read after the catalog lock.
    pub(crate) fn row_protection(&self, attempt: ReadProtection) -> ReadProtection {
        match self.mode {
            Some(CatalogLockMode::Shared) if self.snapshot_after_lock => ReadProtection::Snapshot,
            Some(_) | None => attempt,
        }
    }

    /// The protection of a catalog listing. A listing only selects names;
    /// each later stamp read takes the catalog lock.
    pub(crate) fn listing_protection(&self, attempt: ReadProtection) -> ReadProtection {
        if self.mode.is_some() {
            ReadProtection::Snapshot
        } else {
            attempt
        }
    }

    pub(crate) fn xid_before_ordering_lock(&self) -> bool {
        self.xid_before_ordering_lock
    }

    fn holds(&self, name: &TableName, mode: CatalogLockMode) -> bool {
        self.held.iter().any(|(held, strength)| {
            held == name && (*strength == CatalogLockMode::Exclusive || *strength == mode)
        })
    }

    pub(crate) fn catalog_prefix(
        &mut self,
        names: &[&TableName],
        requested: Option<CatalogLockMode>,
        window: &mut Vec<(SqlStatement, ReadProtection)>,
    ) -> StorageResult<LockPrefix> {
        let empty = LockPrefix {
            xid_check: XidCheck::None,
            statements: 0,
        };
        let Some(mode) = requested.or(self.mode) else {
            return Ok(empty);
        };
        if !self.postgres {
            return Ok(empty);
        }
        let mut needed: Vec<&TableName> = Vec::new();
        for name in names {
            if !self.holds(name, mode) && !needed.contains(name) {
                needed.push(name);
            }
        }
        if needed.is_empty() {
            return Ok(empty);
        }
        if mode == CatalogLockMode::Exclusive && self.item_locked {
            return Err(StorageError::internal(
                "SQL catalog change was requested after an item lock",
            ));
        }
        let xid_check = mode == CatalogLockMode::Exclusive && !self.ordering_locked;
        if xid_check {
            self.ordering_locked = true;
        }
        window.push((
            save_xid_before_lock(catalog_lock_statement(&needed, mode), xid_check),
            ReadProtection::Snapshot,
        ));
        for name in needed {
            self.held.retain(|(held, _)| held != name);
            self.held.push((name.clone(), mode));
        }
        Ok(LockPrefix {
            xid_check: if xid_check {
                XidCheck::First
            } else {
                XidCheck::None
            },
            statements: 1,
        })
    }

    /// Put the item lock (and the transaction ID check before the first
    /// ordering lock) at the head of `window`. Every target table must hold
    /// its catalog lock in this attempt.
    pub(crate) fn item_lock_prefix<'a>(
        &mut self,
        targets: impl Iterator<
            Item = (
                &'a storage_types::BoundTableInfo,
                &'a storage_types::KeyAttributes,
            ),
        > + Clone,
        window: &mut Vec<(SqlStatement, ReadProtection)>,
    ) -> StorageResult<LockPrefix> {
        let empty = LockPrefix {
            xid_check: XidCheck::None,
            statements: 0,
        };
        if !self.postgres {
            return Ok(empty);
        }
        let mut targets = targets.peekable();
        if targets.peek().is_none() {
            return Ok(empty);
        }
        for (table, _) in targets.clone() {
            if !self.holds(&table.info.table_name, CatalogLockMode::Shared) {
                return Err(StorageError::internal(
                    "SQL item lock was requested before the catalog lock",
                ));
            }
        }
        let identities = crate::item_lock::item_lock_identities(targets)?;
        let adds_item = identities
            .iter()
            .any(|identity| !self.held_items.contains(identity));
        let xid_check = if !self.ordering_locked {
            XidCheck::First
        } else if self.item_locked && adds_item && !self.xid_before_ordering_lock {
            XidCheck::Later
        } else {
            XidCheck::None
        };
        window.push((
            save_xid_before_lock(
                crate::item_lock::lock_statement(&identities),
                xid_check != XidCheck::None,
            ),
            ReadProtection::Snapshot,
        ));
        self.held_items
            .try_reserve(identities.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for identity in identities {
            if !self.held_items.contains(&identity) {
                self.held_items.push(identity);
            }
        }
        self.ordering_locked = true;
        self.item_locked = true;
        Ok(LockPrefix {
            xid_check,
            statements: 1,
        })
    }

    pub(crate) fn token_lock_prefix(
        &mut self,
        slot: storage_provider::ReceiptSlot<'_>,
        window: &mut Vec<(SqlStatement, ReadProtection)>,
    ) -> LockPrefix {
        // A read-only attempt only reads an immutable receipt; it takes no
        // lock.
        if !self.postgres || self.mode.is_none() {
            return LockPrefix {
                xid_check: XidCheck::None,
                statements: 0,
            };
        }
        let xid_check = !self.ordering_locked;
        window.push((
            save_xid_before_lock(crate::item_lock::receipt_lock_statement(slot), xid_check),
            ReadProtection::Snapshot,
        ));
        self.ordering_locked = true;
        LockPrefix {
            xid_check: if xid_check {
                XidCheck::First
            } else {
                XidCheck::None
            },
            statements: 1,
        }
    }

    /// Advisory locks of regional progress records, after any item locks of
    /// this attempt (R5). Postgres only; no transaction-ID check (no write
    /// precedes it in a receive attempt).
    pub(crate) fn progress_lock_prefix(
        &mut self,
        identities: &[String],
        window: &mut Vec<(SqlStatement, ReadProtection)>,
    ) -> LockPrefix {
        if !self.postgres || identities.is_empty() {
            return LockPrefix {
                xid_check: XidCheck::None,
                statements: 0,
            };
        }
        window.push((
            crate::item_lock::lock_statement(identities),
            ReadProtection::Snapshot,
        ));
        LockPrefix {
            xid_check: XidCheck::None,
            statements: 1,
        }
    }

    /// Consume the prefix pages of one window.
    pub(crate) fn finish_lock_prefix(
        &mut self,
        prefix: LockPrefix,
        pages: &mut impl Iterator<Item = SqlQueryPage>,
    ) -> StorageResult<()> {
        let mut remaining = prefix.statements;
        if prefix.xid_check != XidCheck::None {
            let page = pages
                .next()
                .ok_or_else(|| StorageError::internal("SQL transaction ID page is missing"))?;
            if count_column(&page)? != 0 {
                if prefix.xid_check == XidCheck::Later {
                    return Err(StorageError::internal(
                        "SQL item lock window was requested after a write",
                    ));
                }
                self.xid_before_ordering_lock = true;
            }
            remaining -= 1;
        }
        for _ in 0..remaining {
            pages
                .next()
                .ok_or_else(|| StorageError::internal("SQL catalog lock page is missing"))?;
        }
        Ok(())
    }
}

impl<A: SqlAttempt, L: SqlLayout> SqlCatalogAttempt<'_, A, L> {
    /// Run `statements` in one window after the catalog lock of `names` in
    /// this attempt's lock mode.
    pub(crate) async fn query_with_catalog_lock(
        &mut self,
        names: &[&TableName],
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<storage_types::Exact<SqlQueryPage>> {
        self.query_locked(names, None, statements, max_bytes).await
    }

    /// Take the exclusive catalog lock of `name` unless this attempt holds it.
    pub(crate) async fn take_exclusive_catalog_lock(
        &mut self,
        name: &TableName,
    ) -> StorageResult<()> {
        let pages = self
            .query_locked(
                &[name],
                Some(CatalogLockMode::Exclusive),
                Vec::new(),
                storage_provider::CATALOG_READ_BYTES,
            )
            .await?;
        if !pages.is_empty() {
            return Err(StorageError::internal("SQL catalog lock returned rows"));
        }
        Ok(())
    }

    /// The one owner of statements that change the catalog stamp of `name`
    /// (revision, incarnation, or status): it runs them after the exclusive
    /// catalog lock and returns one changed-row count for each statement.
    pub(crate) async fn execute_catalog_change(
        &mut self,
        name: &TableName,
        statements: Vec<SqlStatement>,
    ) -> StorageResult<storage_types::Exact<u64>> {
        self.take_exclusive_catalog_lock(name).await?;
        self.attempt.execute_batch_exact(statements).await
    }

    async fn query_locked(
        &mut self,
        names: &[&TableName],
        requested: Option<CatalogLockMode>,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<storage_types::Exact<SqlQueryPage>> {
        let mut window = Vec::new();
        let prefix = self
            .catalog_lock
            .catalog_prefix(names, requested, &mut window)?;
        if window.is_empty() && statements.is_empty() {
            return Ok(storage_types::Exact::empty());
        }
        let expected = statements.len();
        window
            .try_reserve_exact(expected)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        window.extend(statements);
        let mut pages = self
            .attempt
            .query_batch_protected(window, max_bytes)
            .await?
            .into_iter();
        self.catalog_lock.finish_lock_prefix(prefix, &mut pages)?;
        // The lock prefix is consumed above; the rest answers `statements`.
        storage_types::Exact::try_new(expected, pages.collect())
    }
}

/// Advisory locks in the two-`int4` form have a different lock tag from the
/// `bigint` item and token locks, so the two never collide. The key hashes
/// the current schema with the name.
fn catalog_lock_statement(names: &[&TableName], mode: CatalogLockMode) -> SqlStatement {
    let function = match mode {
        CatalogLockMode::Shared => "pg_advisory_xact_lock_shared",
        CatalogLockMode::Exclusive => "pg_advisory_xact_lock",
    };
    let names = names
        .iter()
        .map(|name| name.as_ref())
        .collect::<Vec<&str>>();
    SqlStatement::new(
        format!(
            "SELECT count(*) FROM (SELECT {function}({POSTGRES_CATALOG_LOCK_CLASS}, lock_id) FROM \
             (SELECT DISTINCT hashtext(current_schema() || '.' || name) AS lock_id FROM \
             unnest($1::text::text[]) AS names(name) ORDER BY lock_id) AS ids) AS locked"
        ),
        vec![crate::item_lock::postgres_text_array(&names)],
    )
}

/// Save the transaction ID state before the first ordering lock can wait.
/// The correlated OFFSET forces the lock subquery to depend on the materialized
/// value. The next statement retains its own READ COMMITTED snapshot after
/// waiting.
fn save_xid_before_lock(statement: SqlStatement, save: bool) -> SqlStatement {
    if !save {
        return statement;
    }
    let (sql, binds) = statement.into_parts();
    SqlStatement::new(
        format!(
            "WITH xid_before_lock AS MATERIALIZED (SELECT CASE WHEN \
             pg_current_xact_id_if_assigned() IS NULL THEN 0 ELSE 1 END AS assigned) SELECT \
             xid_before_lock.assigned::bigint FROM xid_before_lock CROSS JOIN LATERAL ({sql} \
             OFFSET xid_before_lock.assigned * 0) AS locked"
        ),
        binds,
    )
}

fn count_column(page: &SqlQueryPage) -> StorageResult<i64> {
    let [row] = page.rows.as_slice() else {
        return Err(StorageError::internal("SQL count page has no single row"));
    };
    match row.value(ColumnIndex::new(0))? {
        SqlValue::Integer(count) => Ok(*count),
        _ => Err(StorageError::internal("SQL count column has invalid type")),
    }
}
