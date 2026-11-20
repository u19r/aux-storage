use futures::future::LocalBoxFuture;

use crate::{
    common::*,
    constants::*,
    imports::*,
    table_recreate_audit::{PutOutcome, PutSeen, audit_recreate, is_vacuous},
};

type Store = kv_backend::KvStore<kv_backend::foundationdb::FoundationDbBackend>;
type SimClock = crate::simulation_clock::SimulationClock;

const TABLE_RECREATE_PREFIX: &[u8] = b"aux-storage/fdb-chaos/table-recreate/";
const TABLE_RECREATE_NAME: &str = "FdbChaosRecreate";
const TABLE_RECREATE_SCAN_PAGES: u32 = 100;
const TABLE_RECREATE_RECREATOR_DELAY_MILLIS: u64 = 100;
const TABLE_RECREATE_ROUND_PAUSE_MILLIS: u64 = 150;

/// A recreate round whose delete and create both settled as done. Each pair is
/// `(called_at, returned_at)` in simulated milliseconds.
#[derive(Clone, Copy, Debug)]
struct RecreateSeen {
    #[allow(dead_code)]
    delete: (i64, i64),
    create: (i64, i64),
}

enum NodeOutput {
    Writer(Vec<PutSeen>, Vec<String>),
    Recreator(Vec<RecreateSeen>, Vec<String>, bool),
}

fn table_name() -> TableName {
    TableName::new(TABLE_RECREATE_NAME)
}

fn create_request() -> CreateTableRequest {
    CreateTableRequest::new(
        table_name(),
        vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: KeyAttributeType::S,
        }],
        vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        BillingMode::PayPerRequest,
    )
}

async fn store(db: &SimDatabase, clock: &Arc<SimClock>) -> Result<Store, String> {
    // Format initialization reads its key and stages only what is absent, so
    // calling it again after an unknown commit is a fresh observation.
    let mut unknown_detail = String::new();
    for _ in 0..QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
        let driver = kv_backend::foundationdb::FoundationDbBackend::from_simulation(
            Arc::clone(db),
            Some(TABLE_RECREATE_PREFIX),
            None,
            storage_types::ReadVersionPolicy::Fresh,
            Arc::clone(clock) as Arc<dyn storage_types::Clock>,
        );
        match kv_backend::KvStore::initialize_format_with_clock(
            driver,
            storage_provider::StorageBackend::FoundationDb.default_initial_stream_lineages(),
            Arc::clone(clock) as Arc<dyn storage_types::Clock>,
        )
        .await
        {
            Ok(store) => return Ok(store.with_metadata_cache_ttl(SIMULATION_METADATA_CACHE_TTL)),
            Err(error) if error.is_commit_unknown() => {
                unknown_detail = storage_error_detail(&error);
            }
            Err(error) => return Err(storage_error_detail(&error)),
        }
    }
    Err(format!(
        "format initialization outcome unknown after {QUEUE_IDEMPOTENT_CALL_ATTEMPTS} attempts: \
         {unknown_detail}"
    ))
}

fn now(clock: &SimClock) -> i64 {
    storage_types::Clock::now(clock).timestamp_millis()
}

async fn sleep(clock: &SimClock, millis: u64) -> Result<(), String> {
    storage_types::Clock::sleep(clock, std::time::Duration::from_millis(millis))
        .await
        .map_err(|error| storage_error_detail(&error))
}

fn put_outcome(error: &StorageError) -> Option<PutOutcome> {
    if error.is_commit_unknown() {
        return Some(PutOutcome::Unknown);
    }
    if error.class().is_retryable() {
        return Some(PutOutcome::Aborted);
    }
    match error.base_error() {
        Some(StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. }) => {
            Some(PutOutcome::Missing)
        }
        _ => None,
    }
}

fn is_table_missing(error: &StorageError) -> bool {
    matches!(
        error.base_error(),
        Some(StorageEnum::TableNotFound { .. } | StorageEnum::ResourceNotFound { .. })
    )
}

fn is_table_exists(error: &StorageError) -> bool {
    matches!(
        error.base_error(),
        Some(StorageEnum::TableAlreadyExists { .. })
    )
}

/// One put of `key`, recorded with its call interval.
async fn put_one(
    store: &Store,
    clock: &SimClock,
    writer: usize,
    key: &str,
) -> Result<PutSeen, String> {
    let item: storage_types::AttributeMap = [
        ("pk".to_owned(), AttributeValue::S(key.to_owned())),
        ("node".to_owned(), AttributeValue::S(writer.to_string())),
    ]
    .into_iter()
    .collect();
    let called_at = now(clock);
    let result = store
        .put_item(
            storage_types::PutItemRequest::builder()
                .table_name(table_name())
                .item(WireItem::try_from(item).expect("valid workload item"))
                .condition_expression(None)
                .build(),
        )
        .await;
    let returned_at = now(clock);
    let outcome = match result {
        Ok(_) => PutOutcome::Done,
        Err(error) => put_outcome(&error)
            .ok_or_else(|| format!("put {key}: {}", storage_error_detail(&error)))?,
    };
    Ok(PutSeen {
        key: key.to_owned(),
        called_at,
        returned_at,
        outcome,
    })
}

async fn run_writer(writer: usize, store: &Store, clock: &SimClock, operations: u64) -> NodeOutput {
    let mut puts = Vec::new();
    let mut failures = Vec::new();
    for sequence in 0..operations {
        let key = format!("w{writer}-{sequence}");
        match put_one(store, clock, writer, &key).await {
            Ok(seen) => puts.push(seen),
            Err(error) => {
                failures.push(format!("writer {writer}: {error}"));
                break;
            }
        }
        if let Err(error) = sleep(clock, TABLE_RECREATE_PAUSE_MILLIS).await {
            failures.push(format!("writer {writer}: {error}"));
            break;
        }
    }
    NodeOutput::Writer(puts, failures)
}

/// Delete or create the table, calling again after a known abort or an
/// unknown outcome. Returns the call interval, or `None` when the state could
/// not be settled within `QUEUE_IDEMPOTENT_CALL_ATTEMPTS` calls.
async fn change_table(
    store: &Store,
    clock: &SimClock,
    create: bool,
) -> Result<Option<(i64, i64)>, String> {
    let called_at = now(clock);
    for _ in 0..QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
        let error = if create {
            match store.create_table(create_request()).await {
                Ok(_) => return Ok(Some((called_at, now(clock)))),
                Err(error) => error,
            }
        } else {
            match store
                .delete_table(storage_types::DeleteTableRequest {
                    table_name: table_name(),
                })
                .await
            {
                Ok(_) => return Ok(Some((called_at, now(clock)))),
                Err(error) => error,
            }
        };
        // After an unknown or aborted call the change may have happened.
        if (create && is_table_exists(&error)) || (!create && is_table_missing(&error)) {
            return Ok(Some((called_at, now(clock))));
        }
        if error.is_commit_unknown() || error.class().is_retryable() {
            sleep(clock, TABLE_RECREATE_PAUSE_MILLIS).await?;
            continue;
        }
        return Err(format!(
            "{}_table: {}",
            if create { "create" } else { "delete" },
            storage_error_detail(&error)
        ));
    }
    Ok(None)
}

async fn run_recreator(store: &Store, clock: &SimClock, recreates: u64) -> NodeOutput {
    let mut seen = Vec::new();
    let mut failures = Vec::new();
    if let Err(error) = sleep(clock, TABLE_RECREATE_RECREATOR_DELAY_MILLIS).await {
        failures.push(error);
        return NodeOutput::Recreator(seen, failures, false);
    }
    for _ in 0..recreates {
        let delete = match change_table(store, clock, false).await {
            Ok(Some(interval)) => interval,
            Ok(None) => return NodeOutput::Recreator(seen, failures, true),
            Err(error) => {
                failures.push(error);
                return NodeOutput::Recreator(seen, failures, false);
            }
        };
        let create = match change_table(store, clock, true).await {
            Ok(Some(interval)) => interval,
            Ok(None) => return NodeOutput::Recreator(seen, failures, true),
            Err(error) => {
                failures.push(error);
                return NodeOutput::Recreator(seen, failures, false);
            }
        };
        seen.push(RecreateSeen { delete, create });
        if let Err(error) = sleep(clock, TABLE_RECREATE_ROUND_PAUSE_MILLIS).await {
            failures.push(error);
            return NodeOutput::Recreator(seen, failures, false);
        }
    }
    NodeOutput::Recreator(seen, failures, false)
}

/// Every `pk` of the table, paged consistent scan.
async fn scan_keys(store: &Store) -> Result<BTreeSet<String>, String> {
    let mut request: storage_types::ScanRequest = serde_json::from_value(serde_json::json!({
        "TableName": TABLE_RECREATE_NAME,
        "Limit": 100,
        "ConsistentRead": true,
    }))
    .expect("valid recreate scan");
    let mut keys = BTreeSet::new();
    for _ in 0..TABLE_RECREATE_SCAN_PAGES {
        let page = store
            .scan(request.clone())
            .await
            .map_err(|error| storage_error_detail(&error))?;
        for item in page.items.unwrap_or_default() {
            let attributes = item
                .to_attribute_map()
                .map_err(|error| storage_error_detail(&error))?;
            keys.insert(string_attr(&attributes, "pk")?);
        }
        request.exclusive_start_key = page
            .last_evaluated_key
            .map(storage_types::ExclusiveStartKey::Key);
        if request.exclusive_start_key.is_none() {
            return Ok(keys);
        }
    }
    Err(format!("scan exceeded {TABLE_RECREATE_SCAN_PAGES} pages"))
}

pub(crate) struct TableRecreateWorkload {
    name: String,
    profile: String,
    operation_count: u64,
    writer_count: usize,
    recreate_count: u64,
    artifact_root: String,
    client_id: i32,
    puts: Vec<PutSeen>,
    recreates: Vec<RecreateSeen>,
    present: BTreeSet<String>,
    fresh_present: BTreeSet<String>,
    scanned: bool,
    incomplete: bool,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    error_count: u64,
    incomplete_count: u64,
    context: std::rc::Rc<WorkloadContext>,
    clock: Option<Arc<SimClock>>,
}

impl TableRecreateWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let operation_count = option_or_default(&context, OPTION_OPERATION_COUNT, 24_u64).max(1);
        let writer_count =
            option_or_default(&context, OPTION_WRITER_COUNT, 4_i64).clamp(2, 11) as usize;
        let recreate_count = option_or_default(&context, OPTION_RECREATE_COUNT, 3_u64).max(1);
        let _: u64 = option_or_default(&context, OPTION_HISTORY_SAMPLE_LIMIT, 0_u64);
        let _: i32 = option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32);
        let artifact_root =
            option_or_default_string(&context, OPTION_ARTIFACT_ROOT, "run-artifacts/fdb-chaos");
        let client_id = context.client_id();
        Self {
            name,
            profile,
            operation_count,
            writer_count,
            recreate_count,
            artifact_root,
            client_id,
            puts: Vec::new(),
            recreates: Vec::new(),
            present: BTreeSet::new(),
            fresh_present: BTreeSet::new(),
            scanned: false,
            incomplete: false,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            error_count: 0,
            incomplete_count: 0,
            context: std::rc::Rc::new(context),
            clock: None,
        }
    }

    fn is_active_client(&self) -> bool {
        self.client_id == 0
    }

    fn count_puts(&self, outcome: PutOutcome) -> u64 {
        self.puts
            .iter()
            .filter(|put| put.outcome == outcome)
            .count() as u64
    }

    fn trace_phase(&self, phase: &'static str) {
        self.context.trace(
            Severity::Info,
            "AuxStorageFdbChaosTableRecreatePhase",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "WriterCount" => self.writer_count as u64,
                "OperationCount" => self.operation_count,
                "RecreateCount" => self.recreate_count,
                "ArtifactRoot" => &self.artifact_root,
            ],
        );
    }

    fn trace_error(&mut self, phase: &'static str, error: String) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "AuxStorageFdbChaosTableRecreateError",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Error" => error,
            ],
        );
    }

    /// Unknown outcomes left the run unprovable. Not an error: the store
    /// answered correctly.
    fn trace_incomplete(&mut self, phase: &'static str, reason: String) {
        self.incomplete_count += 1;
        self.incomplete = true;
        self.context.trace(
            Severity::Warn,
            "AuxStorageFdbChaosTableRecreateIncomplete",
            details![
                "Layer" => "aux-storage",
                "Workload" => &self.name,
                "Phase" => phase,
                "ClientId" => self.client_id,
                "Reason" => reason,
            ],
        );
    }

    async fn setup_phase(&mut self, db: SimDatabase) {
        self.setup_count += 1;
        self.trace_phase("setup");
        if !self.is_active_client() {
            return;
        }
        let Some(clock) = self.clock.as_ref().map(Arc::clone) else {
            self.trace_error("setup", "simulation phase clock is absent".to_owned());
            return;
        };
        let store = match store(&db, &clock).await {
            Ok(store) => store,
            Err(error) => {
                self.trace_error("setup", error);
                return;
            }
        };
        match change_table(&store, clock.as_ref(), true).await {
            Ok(Some(_)) => {}
            Ok(None) => self.trace_incomplete(
                "create_table",
                "create outcome unknown after retries".to_owned(),
            ),
            Err(error) => self.trace_error("create_table", error),
        }
    }

    async fn start_phase(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_phase("start");
        if !self.is_active_client() || self.incomplete {
            return;
        }
        let Some(clock) = self.clock.as_ref().map(Arc::clone) else {
            self.trace_error("start", "simulation phase clock is absent".to_owned());
            return;
        };
        let mut writers = Vec::with_capacity(self.writer_count);
        for _ in 0..self.writer_count {
            match store(&db, &clock).await {
                Ok(store) => writers.push(store),
                Err(error) => {
                    self.trace_error("start", error);
                    return;
                }
            }
        }
        let recreator = match store(&db, &clock).await {
            Ok(store) => store,
            Err(error) => {
                self.trace_error("start", error);
                return;
            }
        };
        // Warm each writer's binding cache; the first recreate deletes these.
        for (writer, store) in writers.iter().enumerate() {
            let key = format!("warm-{writer}");
            match put_one(store, clock.as_ref(), writer, &key).await {
                Ok(seen) => self.puts.push(seen),
                Err(error) => {
                    self.trace_error("warm_put", error);
                    return;
                }
            }
            let request = GetItemRequest::new(
                table_name(),
                KeyAttributes::from([("pk".to_owned(), AttributeValue::S(key))]),
            );
            if let Err(error) = store.get_item(request).await
                && !error.is_commit_unknown()
                && !error.class().is_retryable()
            {
                self.trace_error("warm_get", storage_error_detail(&error));
                return;
            }
        }
        let operations = self.operation_count;
        let recreates = self.recreate_count;
        let mut futures: Vec<LocalBoxFuture<'_, NodeOutput>> = Vec::new();
        for (writer, store) in writers.iter().enumerate() {
            let clock = clock.as_ref();
            futures.push(Box::pin(async move {
                run_writer(writer, store, clock, operations).await
            }));
        }
        {
            let recreator = &recreator;
            let clock = clock.as_ref();
            futures.push(Box::pin(async move {
                run_recreator(recreator, clock, recreates).await
            }));
        }
        let outputs = futures::future::join_all(futures).await;
        for output in outputs {
            match output {
                NodeOutput::Writer(puts, failures) => {
                    self.puts.extend(puts);
                    for failure in failures {
                        self.trace_error("writer", failure);
                    }
                }
                NodeOutput::Recreator(seen, failures, incomplete) => {
                    self.recreates.extend(seen);
                    for failure in failures {
                        self.trace_error("recreator", failure);
                    }
                    if incomplete {
                        self.trace_incomplete(
                            "recreate",
                            "table state not settled after retries".to_owned(),
                        );
                    }
                }
            }
        }
        if self.error_count > 0 || self.incomplete {
            return;
        }
        // The writers may have finished before the last create was called, so
        // each puts once more through its warm store after it returned.
        let mut tail_done = false;
        let mut tail_unknown = false;
        for (writer, store) in writers.iter().enumerate() {
            for attempt in 0..QUEUE_IDEMPOTENT_CALL_ATTEMPTS {
                let key = if attempt == 0 {
                    format!("tail-{writer}")
                } else {
                    format!("tail-{writer}-{attempt}")
                };
                let seen = match put_one(store, clock.as_ref(), writer, &key).await {
                    Ok(seen) => seen,
                    Err(error) => {
                        self.trace_error("tail_put", error);
                        return;
                    }
                };
                let outcome = seen.outcome;
                self.puts.push(seen);
                match outcome {
                    PutOutcome::Done => tail_done = true,
                    PutOutcome::Unknown => tail_unknown = true,
                    PutOutcome::Aborted => {
                        if let Err(error) = sleep(clock.as_ref(), TABLE_RECREATE_PAUSE_MILLIS).await
                        {
                            self.trace_error("tail_put", error);
                            return;
                        }
                        continue;
                    }
                    PutOutcome::Missing => {}
                }
                break;
            }
        }
        if !tail_done && tail_unknown {
            self.trace_incomplete(
                "tail_put",
                "every put after the last create had an unknown outcome".to_owned(),
            );
            return;
        }
        // One view through a warm cache that may be stale, one through a fresh
        // store.
        match scan_keys(&writers[0]).await {
            Ok(keys) => self.present = keys,
            Err(error) => {
                self.trace_error("scan", error);
                return;
            }
        }
        let fresh = match store(&db, &clock).await {
            Ok(store) => store,
            Err(error) => {
                self.trace_error("scan", error);
                return;
            }
        };
        match scan_keys(&fresh).await {
            Ok(keys) => self.fresh_present = keys,
            Err(error) => {
                self.trace_error("scan", error);
                return;
            }
        }
        self.scanned = true;
    }

    async fn check_phase(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_phase("check");
        if !self.is_active_client() || self.incomplete || !self.scanned {
            return;
        }
        let Some(last) = self.recreates.last() else {
            self.trace_error("audit", "vacuous run: no recreate settled".to_owned());
            return;
        };
        let last_create = last.create;
        if self.recreates.len() as u64 != self.recreate_count {
            self.trace_error(
                "audit",
                format!(
                    "vacuous run: {} of {} recreates settled",
                    self.recreates.len(),
                    self.recreate_count
                ),
            );
            return;
        }
        if let Err(error) =
            audit_recreate(&self.puts, last_create, &self.present, &self.fresh_present)
        {
            self.trace_error("audit", error);
            return;
        }
        if is_vacuous(&self.puts, last_create) {
            self.trace_error(
                "audit",
                format!("vacuous run: puts do not span the last create {last_create:?}"),
            );
        }
    }

    async fn run_simulation_phase(
        &mut self,
        db: SimDatabase,
        phase: crate::simulation_clock::SimulationPhase,
    ) {
        let context = std::rc::Rc::clone(&self.context);
        let (clock, requests) = SimClock::new();
        self.clock = Some(clock);
        let operation = async {
            match phase {
                crate::simulation_clock::SimulationPhase::Setup => self.setup_phase(db).await,
                crate::simulation_clock::SimulationPhase::Start => self.start_phase(db).await,
                crate::simulation_clock::SimulationPhase::Check => self.check_phase(db).await,
            }
        };
        crate::simulation_clock::drive_operation(context, requests, operation).await;
        self.clock = None;
    }
}

impl RustWorkload for TableRecreateWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        self.run_simulation_phase(db, crate::simulation_clock::SimulationPhase::Setup)
            .await;
    }

    async fn start(&mut self, db: SimDatabase) {
        self.run_simulation_phase(db, crate::simulation_clock::SimulationPhase::Start)
            .await;
    }

    async fn check(&mut self, db: SimDatabase) {
        self.run_simulation_phase(db, crate::simulation_clock::SimulationPhase::Check)
            .await;
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric_val_u64(
                "aux_storage_table_recreate_writer_count",
                self.writer_count as u64,
            ),
            metric_val_u64(
                "aux_storage_table_recreate_put_done",
                self.count_puts(PutOutcome::Done),
            ),
            metric_val_u64(
                "aux_storage_table_recreate_put_aborted",
                self.count_puts(PutOutcome::Aborted),
            ),
            metric_val_u64(
                "aux_storage_table_recreate_put_unknown",
                self.count_puts(PutOutcome::Unknown),
            ),
            metric_val_u64(
                "aux_storage_table_recreate_put_missing",
                self.count_puts(PutOutcome::Missing),
            ),
            metric_val_u64(
                "aux_storage_table_recreate_recreate_done",
                self.recreates.len() as u64,
            ),
            metric_val_u64(
                "aux_storage_table_recreate_present",
                self.present.len() as u64,
            ),
            metric_val_u64("aux_storage_table_recreate_error", self.error_count),
            metric_val_u64(
                "aux_storage_table_recreate_incomplete",
                self.incomplete_count,
            ),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        120.0
    }
}
