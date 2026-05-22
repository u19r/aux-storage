/// MBT surface for sync Raft startup admission.
///
/// The storage backend does not have to be SQLite. The Raft log currently does:
/// production startup may use a durable SQLite Raft log for a local non-SQL
/// storage backend when an explicit sync data directory is configured.
module sync_raft_startup_policy_mbt {
  type StartupInput = {
    sqliteFeatureEnabled: bool,
    storageBackend: str,
    dataDirConfigured: bool,
    storageConnectionConfigured: bool,
  }

  var lastInput: StartupInput
  var lastDecision: str

  pure def expectedDecision(input) =
    if (not(input.sqliteFeatureEnabled)) {
      "sqlite_feature_required"
    } else if (input.storageBackend == "remote" or input.storageBackend == "") {
      "local_backend_required"
    } else if (input.storageBackend == "sqlite") {
      if (input.dataDirConfigured or input.storageConnectionConfigured) {
        "allow"
      } else {
        "sqlite_raft_path_required"
      }
    } else if (Set("postgres", "turso", "rocksdb", "foundationdb").contains(input.storageBackend)) {
      if (input.dataDirConfigured) {
        "allow"
      } else {
        "sync_data_dir_required"
      }
    } else {
      "local_backend_required"
    }

  action init = all {
    lastInput' = {
      sqliteFeatureEnabled: false,
      storageBackend: "",
      dataDirConfigured: false,
      storageConnectionConfigured: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet sqliteFeatureEnabled = Set(false, true).oneOf()
    nondet storageBackend = Set("", "sqlite", "postgres", "turso", "rocksdb", "foundationdb", "remote").oneOf()
    nondet dataDirConfigured = Set(false, true).oneOf()
    nondet storageConnectionConfigured = Set(false, true).oneOf()
    val input = {
      sqliteFeatureEnabled: sqliteFeatureEnabled,
      storageBackend: storageBackend,
      dataDirConfigured: dataDirConfigured,
      storageConnectionConfigured: storageConnectionConfigured,
    }
    all {
      lastInput' = input,
      lastDecision' = expectedDecision(input),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "allow"
        or lastInput.sqliteFeatureEnabled,
      lastDecision != "allow"
        or lastInput.storageBackend != "remote",
      lastDecision != "allow"
        or lastInput.storageBackend != "",
      lastDecision != "allow"
        or (
          lastInput.storageBackend == "sqlite"
            or lastInput.dataDirConfigured
        ),
    }

  run sqliteStorageMayUseStorageConnection =
    true.expect(expectedDecision({
      sqliteFeatureEnabled: true,
      storageBackend: "sqlite",
      dataDirConfigured: false,
      storageConnectionConfigured: true,
    }) == "allow")

  run rocksdbStorageRequiresSyncDataDir =
    true.expect(expectedDecision({
      sqliteFeatureEnabled: true,
      storageBackend: "rocksdb",
      dataDirConfigured: false,
      storageConnectionConfigured: true,
    }) == "sync_data_dir_required")

  run rocksdbStorageCanUseSeparateDurableRaftLog =
    true.expect(expectedDecision({
      sqliteFeatureEnabled: true,
      storageBackend: "rocksdb",
      dataDirConfigured: true,
      storageConnectionConfigured: false,
    }) == "allow")

  run remoteStorageRejected =
    true.expect(expectedDecision({
      sqliteFeatureEnabled: true,
      storageBackend: "remote",
      dataDirConfigured: true,
      storageConnectionConfigured: true,
    }) == "local_backend_required")
}
