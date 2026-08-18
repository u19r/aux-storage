/// MBT surface for promoted mixed-backend learners serving storage operations.
///
/// The first production cluster is homogeneous, but validation-only mixed
/// backend learners must not be treated as ready after catchup/promotion unless
/// every storage domain needed by DynamoDB-compatible operations was imported
/// and the resolved apply surface is covered.
module sync_promoted_learner_surface_mbt {
  type SurfaceGate = {
    backendDecision: str,
    learnerPromoted: bool,
    tableMetadataImported: bool,
    itemRecordsImported: bool,
    indexerMetadataImported: bool,
    durableRevisionsImported: bool,
    streamRecordsImported: bool,
    ttlRecordsImported: bool,
    gsiRecordsImported: bool,
    storageControlPlaneImported: bool,
    syncControlPlaneImported: bool,
    tableLifecycleApplyConformance: bool,
    itemPutDeleteApplyConformance: bool,
    durableRevisionApplyConformance: bool,
    streamApplyConformance: bool,
    ttlApplyConformance: bool,
    gsiApplyConformance: bool,
    syncControlPlaneApplyConformance: bool,
    replayIdempotencyConformance: bool,
  }

  var lastGate: SurfaceGate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (gate.backendDecision == "rejected") {
      "backend_pair_rejected"
    } else if (not(gate.learnerPromoted)) {
      "learner_not_promoted"
    } else if (not(gate.tableMetadataImported)) {
      "table_metadata_missing"
    } else if (not(gate.itemRecordsImported)) {
      "item_records_missing"
    } else if (not(gate.indexerMetadataImported)) {
      "indexer_metadata_missing"
    } else if (not(gate.durableRevisionsImported)) {
      "durable_revisions_missing"
    } else if (not(gate.streamRecordsImported)) {
      "stream_records_missing"
    } else if (not(gate.ttlRecordsImported)) {
      "ttl_records_missing"
    } else if (not(gate.gsiRecordsImported)) {
      "gsi_records_missing"
    } else if (not(gate.storageControlPlaneImported)) {
      "storage_control_plane_missing"
    } else if (not(gate.syncControlPlaneImported)) {
      "sync_control_plane_missing"
    } else if (not(gate.tableLifecycleApplyConformance)) {
      "table_lifecycle_apply_missing"
    } else if (not(gate.itemPutDeleteApplyConformance)) {
      "item_put_delete_apply_missing"
    } else if (not(gate.durableRevisionApplyConformance)) {
      "durable_revision_apply_missing"
    } else if (not(gate.streamApplyConformance)) {
      "stream_apply_missing"
    } else if (not(gate.ttlApplyConformance)) {
      "ttl_apply_missing"
    } else if (not(gate.gsiApplyConformance)) {
      "gsi_apply_missing"
    } else if (not(gate.syncControlPlaneApplyConformance)) {
      "sync_control_plane_apply_missing"
    } else if (not(gate.replayIdempotencyConformance)) {
      "replay_idempotency_missing"
    } else {
      "allow"
    }

  action init = all {
    lastGate' = {
      backendDecision: "rejected",
      learnerPromoted: false,
      tableMetadataImported: false,
      itemRecordsImported: false,
      indexerMetadataImported: false,
      durableRevisionsImported: false,
      streamRecordsImported: false,
      ttlRecordsImported: false,
      gsiRecordsImported: false,
      storageControlPlaneImported: false,
      syncControlPlaneImported: false,
      tableLifecycleApplyConformance: false,
      itemPutDeleteApplyConformance: false,
      durableRevisionApplyConformance: false,
      streamApplyConformance: false,
      ttlApplyConformance: false,
      gsiApplyConformance: false,
      syncControlPlaneApplyConformance: false,
      replayIdempotencyConformance: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet backendDecision = Set("production_supported", "validation_only", "rejected").oneOf()
    nondet learnerPromoted = Set(false, true).oneOf()
    nondet tableMetadataImported = Set(false, true).oneOf()
    nondet itemRecordsImported = Set(false, true).oneOf()
    nondet indexerMetadataImported = Set(false, true).oneOf()
    nondet durableRevisionsImported = Set(false, true).oneOf()
    nondet streamRecordsImported = Set(false, true).oneOf()
    nondet ttlRecordsImported = Set(false, true).oneOf()
    nondet gsiRecordsImported = Set(false, true).oneOf()
    nondet storageControlPlaneImported = Set(false, true).oneOf()
    nondet syncControlPlaneImported = Set(false, true).oneOf()
    nondet tableLifecycleApplyConformance = Set(false, true).oneOf()
    nondet itemPutDeleteApplyConformance = Set(false, true).oneOf()
    nondet durableRevisionApplyConformance = Set(false, true).oneOf()
    nondet streamApplyConformance = Set(false, true).oneOf()
    nondet ttlApplyConformance = Set(false, true).oneOf()
    nondet gsiApplyConformance = Set(false, true).oneOf()
    nondet syncControlPlaneApplyConformance = Set(false, true).oneOf()
    nondet replayIdempotencyConformance = Set(false, true).oneOf()
    val gate = {
      backendDecision: backendDecision,
      learnerPromoted: learnerPromoted,
      tableMetadataImported: tableMetadataImported,
      itemRecordsImported: itemRecordsImported,
      indexerMetadataImported: indexerMetadataImported,
      durableRevisionsImported: durableRevisionsImported,
      streamRecordsImported: streamRecordsImported,
      ttlRecordsImported: ttlRecordsImported,
      gsiRecordsImported: gsiRecordsImported,
      storageControlPlaneImported: storageControlPlaneImported,
      syncControlPlaneImported: syncControlPlaneImported,
      tableLifecycleApplyConformance: tableLifecycleApplyConformance,
      itemPutDeleteApplyConformance: itemPutDeleteApplyConformance,
      durableRevisionApplyConformance: durableRevisionApplyConformance,
      streamApplyConformance: streamApplyConformance,
      ttlApplyConformance: ttlApplyConformance,
      gsiApplyConformance: gsiApplyConformance,
      syncControlPlaneApplyConformance: syncControlPlaneApplyConformance,
      replayIdempotencyConformance: replayIdempotencyConformance,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "allow"
      or all {
        lastGate.backendDecision != "rejected",
        lastGate.learnerPromoted,
        lastGate.tableMetadataImported,
        lastGate.itemRecordsImported,
        lastGate.indexerMetadataImported,
        lastGate.durableRevisionsImported,
        lastGate.streamRecordsImported,
        lastGate.ttlRecordsImported,
        lastGate.gsiRecordsImported,
        lastGate.storageControlPlaneImported,
        lastGate.syncControlPlaneImported,
        lastGate.tableLifecycleApplyConformance,
        lastGate.itemPutDeleteApplyConformance,
        lastGate.durableRevisionApplyConformance,
        lastGate.streamApplyConformance,
        lastGate.ttlApplyConformance,
        lastGate.gsiApplyConformance,
        lastGate.syncControlPlaneApplyConformance,
        lastGate.replayIdempotencyConformance,
      }

  run validationPairCanServeAfterFullSurface =
    true.expect(expectedDecision({
      backendDecision: "validation_only",
      learnerPromoted: true,
      tableMetadataImported: true,
      itemRecordsImported: true,
      indexerMetadataImported: true,
      durableRevisionsImported: true,
      streamRecordsImported: true,
      ttlRecordsImported: true,
      gsiRecordsImported: true,
      storageControlPlaneImported: true,
      syncControlPlaneImported: true,
      tableLifecycleApplyConformance: true,
      itemPutDeleteApplyConformance: true,
      durableRevisionApplyConformance: true,
      streamApplyConformance: true,
      ttlApplyConformance: true,
      gsiApplyConformance: true,
      syncControlPlaneApplyConformance: true,
      replayIdempotencyConformance: true,
    }) == "allow")

  run gsiIsRequiredForPostPromotionServing =
    true.expect(expectedDecision({
      backendDecision: "validation_only",
      learnerPromoted: true,
      tableMetadataImported: true,
      itemRecordsImported: true,
      indexerMetadataImported: true,
      durableRevisionsImported: true,
      streamRecordsImported: true,
      ttlRecordsImported: true,
      gsiRecordsImported: false,
      storageControlPlaneImported: true,
      syncControlPlaneImported: true,
      tableLifecycleApplyConformance: true,
      itemPutDeleteApplyConformance: true,
      durableRevisionApplyConformance: true,
      streamApplyConformance: true,
      ttlApplyConformance: true,
      gsiApplyConformance: true,
      syncControlPlaneApplyConformance: true,
      replayIdempotencyConformance: true,
    }) == "gsi_records_missing")

  run resolvedGsiApplyIsRequiredForPostPromotionServing =
    true.expect(expectedDecision({
      backendDecision: "validation_only",
      learnerPromoted: true,
      tableMetadataImported: true,
      itemRecordsImported: true,
      indexerMetadataImported: true,
      durableRevisionsImported: true,
      streamRecordsImported: true,
      ttlRecordsImported: true,
      gsiRecordsImported: true,
      storageControlPlaneImported: true,
      syncControlPlaneImported: true,
      tableLifecycleApplyConformance: true,
      itemPutDeleteApplyConformance: true,
      durableRevisionApplyConformance: true,
      streamApplyConformance: true,
      ttlApplyConformance: true,
      gsiApplyConformance: false,
      syncControlPlaneApplyConformance: true,
      replayIdempotencyConformance: true,
    }) == "gsi_apply_missing")
}
