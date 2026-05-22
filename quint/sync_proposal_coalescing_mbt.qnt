/// Compact MBT surface for sync proposal coalescing.
///
/// Proposal coalescing may reduce Raft/fsync round trips, including on
/// FoundationDB-backed nodes, but only when the grouped proposal is equivalent
/// to serial proposal order. The conservative boundary rejects lifecycle
/// mutations, overlapping writes, and stale right-side reads.
module sync_proposal_coalescing_mbt {
  type Gate = {
    leftLifecycle: bool,
    rightLifecycle: bool,
    writeWriteConflict: bool,
    rightReadDependsOnLeftWrite: bool,
  }

  var lastGate: Gate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (gate.leftLifecycle or gate.rightLifecycle) {
      "reject_lifecycle_boundary"
    } else if (gate.writeWriteConflict) {
      "reject_write_write_conflict"
    } else if (gate.rightReadDependsOnLeftWrite) {
      "reject_stale_read_dependency"
    } else {
      "coalesce"
    }

  action init = all {
    lastGate' = {
      leftLifecycle: false,
      rightLifecycle: false,
      writeWriteConflict: false,
      rightReadDependsOnLeftWrite: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet leftLifecycle = Set(false, true).oneOf()
    nondet rightLifecycle = Set(false, true).oneOf()
    nondet writeWriteConflict = Set(false, true).oneOf()
    nondet rightReadDependsOnLeftWrite = Set(false, true).oneOf()
    val gate = {
      leftLifecycle: leftLifecycle,
      rightLifecycle: rightLifecycle,
      writeWriteConflict: writeWriteConflict,
      rightReadDependsOnLeftWrite: rightReadDependsOnLeftWrite,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    lastDecision != "coalesce"
      or all {
        lastGate.leftLifecycle == false,
        lastGate.rightLifecycle == false,
        lastGate.writeWriteConflict == false,
        lastGate.rightReadDependsOnLeftWrite == false,
      }

  run lifecycleRejectsBeforeWriteConflict =
    true.expect(expectedDecision({
      leftLifecycle: true,
      rightLifecycle: false,
      writeWriteConflict: true,
      rightReadDependsOnLeftWrite: true,
    }) == "reject_lifecycle_boundary")

  run writeConflictRejectsBeforeStaleRead =
    true.expect(expectedDecision({
      leftLifecycle: false,
      rightLifecycle: false,
      writeWriteConflict: true,
      rightReadDependsOnLeftWrite: true,
    }) == "reject_write_write_conflict")

  run staleReadRejectsOtherwiseSafeBatch =
    true.expect(expectedDecision({
      leftLifecycle: false,
      rightLifecycle: false,
      writeWriteConflict: false,
      rightReadDependsOnLeftWrite: true,
    }) == "reject_stale_read_dependency")

  run disjointItemBatchesCoalesce =
    true.expect(expectedDecision({
      leftLifecycle: false,
      rightLifecycle: false,
      writeWriteConflict: false,
      rightReadDependsOnLeftWrite: false,
    }) == "coalesce")
}
