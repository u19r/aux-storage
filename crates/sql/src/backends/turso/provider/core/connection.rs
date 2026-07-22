use crate::backends::turso::provider::core::*;

impl TursoStorageProvider {
    #[instrument(level = "info", fields(feature = "storage", backend = "turso", database_path = tracing::field::Empty, use_memory = tracing::field::Empty))]
    pub async fn new(database_path: &str) -> StorageResult<Self> {
        let final_path = database_path
            .strip_prefix("sqlite:")
            .unwrap_or(database_path)
            .split('?')
            .next()
            .unwrap_or(database_path)
            .to_string();
        let use_memory_db = final_path == ":memory:";

        tracing::Span::current().record("database_path", &final_path);
        tracing::Span::current().record("use_memory", use_memory_db);

        let database = Builder::new_local(&final_path)
            .build()
            .await
            .map_err(|error| {
                tracing::error!(error = %error, "failed to build turso database");
                StorageError::internal(&format!(
                    "failed to open turso database at '{final_path}': {error}"
                ))
            })?;
        let connection_pool_size = std::env::var("AUXFN_TURSO_CONNECTION_POOL_SIZE")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|size| *size > 0)
            .unwrap_or(TURSO_CONNECTION_POOL_SIZE);
        let connection_pool = Self::build_connection_pool(&database, connection_pool_size)?;

        let provider = Self {
            connection_pool: Arc::new(connection_pool),
            next_connection: Arc::new(AtomicUsize::new(0)),
            job_manager: JobManager::new_for_test(),
            table_info_cache: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            immediate_gsi_consistency: false,
            gsi_propagation_governor: Arc::new(GsiPropagationGovernor::default()),
            ddl_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        provider.configure_connection_pragmas(use_memory_db).await?;

        Ok(provider)
    }

    #[must_use]
    pub fn with_job_manager(mut self, job_manager: JobManager) -> Self {
        self.job_manager = job_manager;
        self
    }

    #[must_use]
    pub fn with_immediate_gsi_consistency(mut self, immediate_gsi_consistency: bool) -> Self {
        self.immediate_gsi_consistency = immediate_gsi_consistency;
        self
    }

    fn build_connection_pool(
        database: &turso::Database,
        pool_size: usize,
    ) -> StorageResult<Vec<Arc<tokio::sync::Mutex<TursoConnection>>>> {
        let mut connections = Vec::with_capacity(pool_size.max(1));
        for _ in 0..pool_size.max(1) {
            let connection = database.connect().map_err(|error| {
                StorageError::internal(&format!("failed to open turso connection: {error}"))
            })?;
            connection
                .busy_timeout(Duration::from_millis(5_000))
                .map_err(map_turso_error)
                .context("set turso busy_timeout on pooled connection")?;
            connections.push(Arc::new(tokio::sync::Mutex::new(connection)));
        }
        Ok(connections)
    }

    async fn configure_connection_pragmas(&self, use_memory_db: bool) -> StorageResult<()> {
        let conn = self.primary_connection().await?;

        conn.pragma_update("journal_mode", "'mvcc'")
            .await
            .map_err(map_turso_error)
            .context("set turso journal_mode")?;

        let journal_mode = read_pragma_text(&conn, "journal_mode").await?;
        if !journal_mode.eq_ignore_ascii_case("mvcc") {
            return Err(StorageError::internal(&format!(
                "turso journal_mode did not apply mvcc (actual='{journal_mode}')"
            )));
        }
        drop(conn);

        let page_cache_size_kb = sqlite_page_cache_size_kb();
        for pooled_conn in self.connection_pool.iter() {
            let pooled_conn = pooled_conn.clone().lock_owned().await;
            pooled_conn
                .pragma_update("synchronous", "'FULL'")
                .await
                .map_err(map_turso_error)
                .context("set turso synchronous")?;
            pooled_conn
                .pragma_update("cache_size", page_cache_size_kb)
                .await
                .map_err(map_turso_error)
                .context("set turso cache_size")?;
            if use_memory_db {
                pooled_conn
                    .pragma_update("temp_store", "'MEMORY'")
                    .await
                    .map_err(map_turso_error)
                    .context("set turso temp_store")?;
            }
        }

        Ok(())
    }

    pub(crate) async fn primary_connection(
        &self,
    ) -> StorageResult<tokio::sync::OwnedMutexGuard<TursoConnection>> {
        let conn = self.connection_pool.first().cloned().ok_or_else(|| {
            StorageError::internal(
                "turso connection pool is empty; at least one connection is required",
            )
        })?;
        Ok(conn.lock_owned().await)
    }

    pub(crate) async fn connect(
        &self,
    ) -> StorageResult<tokio::sync::OwnedMutexGuard<TursoConnection>> {
        let pool_len = self.connection_pool.len();
        if pool_len == 0 {
            return Err(StorageError::internal(
                "turso connection pool is empty; at least one connection is required",
            ));
        }
        let index = self.next_connection.fetch_add(1, Ordering::Relaxed) % pool_len;
        let conn = self.connection_pool.get(index).cloned().ok_or_else(|| {
            StorageError::internal("failed to acquire turso connection from pool")
        })?;
        Ok(conn.lock_owned().await)
    }

    pub(crate) async fn with_transaction<T>(
        &self,
        retry_conflicts: bool,
        operation: impl for<'a> Fn(&'a TursoTransactionConnection<'a>) -> TxFuture<'a, T>,
    ) -> StorageResult<T> {
        self.with_transaction_mode(sql_statements::BEGIN_CONCURRENT, retry_conflicts, operation)
            .await
    }

    pub(crate) async fn with_exclusive_transaction<T>(
        &self,
        retry_conflicts: bool,
        operation: impl for<'a> Fn(&'a TursoTransactionConnection<'a>) -> TxFuture<'a, T>,
    ) -> StorageResult<T> {
        self.with_transaction_mode(sql_statements::BEGIN_EXCLUSIVE, retry_conflicts, operation)
            .await
    }

    async fn with_transaction_mode<T>(
        &self,
        begin_sql: &'static str,
        retry_conflicts: bool,
        operation: impl for<'a> Fn(&'a TursoTransactionConnection<'a>) -> TxFuture<'a, T>,
    ) -> StorageResult<T> {
        let max_attempts = if retry_conflicts {
            MAX_PUT_ITEM_ATTEMPTS
        } else {
            1
        };

        for attempt in 0..max_attempts {
            #[cfg(test)]
            provider_perf::record_amount("turso", "tx_attempt", 1);
            let conn = self.connect().await?;

            #[cfg(test)]
            let begin_started = Instant::now();
            let begin_result = conn.execute(begin_sql, ()).await.map_err(map_turso_error);
            #[cfg(test)]
            provider_perf::record("turso", "tx_begin", begin_started.elapsed());
            if let Err(begin_err) = begin_result {
                if is_conflict_storage_error(&begin_err) {
                    #[cfg(test)]
                    provider_perf::record_amount("turso", "tx_begin_conflict", 1);
                    if retry_conflicts && attempt + 1 < max_attempts {
                        sleep_backoff(attempt).await;
                        continue;
                    }
                    return Err(begin_err);
                }

                return Err(StorageError::internal(&format!(
                    "transaction begin failed: {begin_err}"
                )));
            }

            let tx_conn = TursoTransactionConnection::new(&conn);
            match operation(&tx_conn).await {
                Ok(value) => {
                    #[cfg(test)]
                    let commit_started = Instant::now();
                    let commit_result = conn
                        .execute(sql_statements::commit(), ())
                        .await
                        .map_err(map_turso_error);
                    #[cfg(test)]
                    provider_perf::record("turso", "tx_commit", commit_started.elapsed());
                    match commit_result {
                        Ok(_) => return Ok(value),
                        Err(commit_err) => {
                            let _ = conn.execute(sql_statements::rollback(), ()).await;
                            if is_conflict_storage_error(&commit_err) {
                                #[cfg(test)]
                                provider_perf::record_amount("turso", "tx_commit_conflict", 1);
                                if retry_conflicts && attempt + 1 < max_attempts {
                                    sleep_backoff(attempt).await;
                                    continue;
                                }
                                return Err(commit_err);
                            }

                            return Err(StorageError::internal(&format!(
                                "transaction commit failed: {commit_err}"
                            )));
                        }
                    }
                }
                Err(op_err) => {
                    let _ = conn.execute(sql_statements::rollback(), ()).await;
                    if retry_conflicts
                        && is_conflict_storage_error(&op_err)
                        && attempt + 1 < max_attempts
                    {
                        #[cfg(test)]
                        provider_perf::record_amount("turso", "tx_operation_conflict", 1);
                        sleep_backoff(attempt).await;
                        continue;
                    }
                    return Err(op_err);
                }
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: "Turso concurrent transaction retry budget exhausted".to_string(),
        }
        .into())
    }

    pub(crate) async fn query_rows<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
    ) -> StorageResult<Vec<HashMap<String, TursoValue>>>
    where
        C: TursoSqlConnection + ?Sized,
    {
        match self
            .query_with_retry(conn, sql, params, TursoQueryShape::MappedRows)
            .await?
        {
            TursoQueryOutput::MappedRows(rows) => Ok(rows),
            TursoQueryOutput::RowSet(_) => Err(StorageError::internal(
                "turso query_rows received row-set output",
            )),
        }
    }

    pub(crate) async fn query_row_set<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
    ) -> StorageResult<TursoRowSet>
    where
        C: TursoSqlConnection + ?Sized,
    {
        match self
            .query_with_retry(conn, sql, params, TursoQueryShape::RowSet)
            .await?
        {
            TursoQueryOutput::RowSet(rows) => Ok(rows),
            TursoQueryOutput::MappedRows(_) => Err(StorageError::internal(
                "turso query_row_set received mapped-row output",
            )),
        }
    }

    async fn query_with_retry<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
        shape: TursoQueryShape,
    ) -> StorageResult<TursoQueryOutput>
    where
        C: TursoSqlConnection + ?Sized,
    {
        #[cfg(test)]
        TURSO_QUERY_CALLS.fetch_add(1, Ordering::Relaxed);

        let max_attempts = if conn.retry_conflicts() {
            MAX_PUT_ITEM_ATTEMPTS
        } else {
            1
        };
        let raw_conn = conn.as_turso_connection();

        for attempt in 0..max_attempts {
            #[cfg(test)]
            let query_started = Instant::now();
            let result = query_once(raw_conn, sql, params.clone(), shape).await;
            #[cfg(test)]
            provider_perf::record("turso", classify_query_sql(sql), query_started.elapsed());

            match result {
                Ok(rows) => return Ok(rows),
                Err(error) if is_conflict_storage_error(&error) && attempt + 1 < max_attempts => {
                    #[cfg(test)]
                    provider_perf::record_amount("turso", "sql_query_conflict", 1);
                    sleep_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: "Turso query retry budget exhausted".to_string(),
        }
        .into())
    }

    pub(crate) async fn execute<C>(
        &self,
        conn: &C,
        sql: &str,
        params: Vec<TursoValue>,
    ) -> StorageResult<u64>
    where
        C: TursoSqlConnection + ?Sized,
    {
        #[cfg(test)]
        TURSO_EXECUTE_CALLS.fetch_add(1, Ordering::Relaxed);

        let max_attempts = if conn.retry_conflicts() {
            MAX_PUT_ITEM_ATTEMPTS
        } else {
            1
        };
        let raw_conn = conn.as_turso_connection();

        for attempt in 0..max_attempts {
            #[cfg(test)]
            let execute_started = Instant::now();
            let result = raw_conn
                .execute(sql, params.clone())
                .await
                .map_err(map_turso_error)
                .with_context(|| format!("execute sql {}", classify_execute_sql(sql)));
            #[cfg(test)]
            provider_perf::record(
                "turso",
                classify_execute_sql(sql),
                execute_started.elapsed(),
            );

            match result {
                Ok(affected) => return Ok(affected),
                Err(error) if is_conflict_storage_error(&error) && attempt + 1 < max_attempts => {
                    #[cfg(test)]
                    provider_perf::record_amount("turso", "sql_execute_conflict", 1);
                    sleep_backoff(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(StorageEnum::TransactionConflict {
            message: "Turso execute retry budget exhausted".to_string(),
        }
        .into())
    }
}

async fn query_once(
    raw_conn: &TursoConnection,
    sql: &str,
    params: Vec<TursoValue>,
    shape: TursoQueryShape,
) -> StorageResult<TursoQueryOutput> {
    let mut rows = raw_conn
        .query(sql, params)
        .await
        .map_err(map_turso_error)
        .context("query rows")?;
    let columns = rows.column_names();

    match shape {
        TursoQueryShape::MappedRows => {
            let mut out = Vec::new();
            while let Some(row) = rows.next().await.map_err(map_turso_error)? {
                let mut mapped = HashMap::with_capacity(columns.len());
                for (index, name) in columns.iter().enumerate() {
                    let value = row.get_value(index).map_err(map_turso_error)?;
                    mapped.insert(name.clone(), value);
                }
                out.push(mapped);
            }
            Ok(TursoQueryOutput::MappedRows(out))
        }
        TursoQueryShape::RowSet => {
            let mut out = Vec::new();
            while let Some(row) = rows.next().await.map_err(map_turso_error)? {
                let mut values = Vec::with_capacity(columns.len());
                for index in 0..columns.len() {
                    values.push(row.get_value(index).map_err(map_turso_error)?);
                }
                out.push(values);
            }
            Ok(TursoQueryOutput::RowSet(TursoRowSet::from_parts(
                columns, out,
            )))
        }
    }
}
