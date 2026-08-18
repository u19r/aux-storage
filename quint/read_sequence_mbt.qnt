/// Compact exploratory contract for ReadSequence validation and execution
/// boundaries. This model deliberately combines planner limits, execution
/// fanout, and continuation-token state so Quint can search their dangerous
/// combinations. The Rust conformance adapters live at the owning boundaries
/// (`read_sequence_dag.qnt` for the planner and storage-api tests for token
/// execution); this cross-boundary model is not replayed through one synthetic
/// driver.
module read_sequence_mbt {
  type ReadSequenceCase = {
    stepCount: int,
    dependencyOrderValid: bool,
    selectorValid: bool,
    selectorProjected: bool,
    childFanout: int,
    setFanout: int,
    duplicateChildKeys: bool,
    hasMissingChild: bool,
    joinMode: str,
    consistency: str,
    readsGsi: bool,
    immediateGsi: bool,
    backendSnapshot: bool,
    continuationKind: str,
    tokenCarriesCursor: bool,
  }

  var lastCase: ReadSequenceCase
  var lastResult: str
  var plannedReads: int
  var tokenRequired: bool

  pure val MAX_SEQUENCE_STEPS = 3
  pure val MAX_FANOUT = 3
  pure val MAX_TOTAL_READS = 5

  pure def initialCase = {
    stepCount: 1,
    dependencyOrderValid: true,
    selectorValid: true,
    selectorProjected: true,
    childFanout: 0,
    setFanout: 1,
    duplicateChildKeys: false,
    hasMissingChild: false,
    joinMode: "LEFT_ONE",
    consistency: "EVENTUAL",
    readsGsi: false,
    immediateGsi: false,
    backendSnapshot: false,
    continuationKind: "none",
    tokenCarriesCursor: false,
  }

  pure def childReadCount(readCase) =
    if (readCase.childFanout == 0)
      0
    else if (readCase.duplicateChildKeys)
      1
    else
      readCase.childFanout * readCase.setFanout

  pure def totalReadsFor(readCase) =
    1 + childReadCount(readCase)

  pure def expandedChildFanout(readCase) =
    if (readCase.childFanout == 0)
      0
    else
      readCase.childFanout * readCase.setFanout

  pure def needsToken(readCase) =
    readCase.continuationKind != "none" or totalReadsFor(readCase) > MAX_TOTAL_READS

  pure def expectedResult(readCase) =
    if (not(readCase.dependencyOrderValid))
      "RejectedDependencyOrder"
    else if (readCase.stepCount > MAX_SEQUENCE_STEPS)
      "RejectedStepCap"
    else if (not(readCase.selectorValid))
      "RejectedSelector"
    else if (not(readCase.selectorProjected))
      "RejectedUnprojectedSelector"
    else if (expandedChildFanout(readCase) > MAX_FANOUT)
      "RejectedFanoutCap"
    else if (readCase.consistency == "STRONG" and readCase.readsGsi)
      "RejectedStrongGsi"
    else if (
      readCase.consistency == "TRANSACTIONAL"
        and readCase.readsGsi
        and not(readCase.immediateGsi)
    )
      "RejectedTransactionalDelayedGsi"
    else if (readCase.consistency == "TRANSACTIONAL" and not(readCase.backendSnapshot))
      "RejectedTransactionalUnsupported"
    else if (readCase.hasMissingChild and readCase.joinMode == "REQUIRED_ONE")
      "RejectedRequiredChildMissing"
    else if (needsToken(readCase) and not(readCase.tokenCarriesCursor))
      "RejectedInvalidTokenState"
    else
      "Accepted"

  action init = all {
    lastCase' = initialCase,
    lastResult' = "NotChecked",
    plannedReads' = 0,
    tokenRequired' = false,
  }

  action Check = {
    nondet stepCount = Set(1, 2, 3, 4).oneOf()
    nondet dependencyOrderValid = Set(true, false).oneOf()
    nondet selectorValid = Set(true, false).oneOf()
    nondet selectorProjected = Set(true, false).oneOf()
    nondet childFanout = Set(0, 1, 2, 3, 4).oneOf()
    nondet setFanout = Set(1, 2, 3, 4).oneOf()
    nondet duplicateChildKeys = Set(true, false).oneOf()
    nondet hasMissingChild = Set(true, false).oneOf()
    nondet joinMode = Set("LEFT_ONE", "REQUIRED_ONE", "ARRAY", "INNER_ONE").oneOf()
    nondet consistency = Set("EVENTUAL", "STRONG", "TRANSACTIONAL").oneOf()
    nondet readsGsi = Set(true, false).oneOf()
    nondet immediateGsi = Set(true, false).oneOf()
    nondet backendSnapshot = Set(true, false).oneOf()
    nondet continuationKind = Set("none", "root_page", "child_page", "fanout").oneOf()
    nondet tokenCarriesCursor = Set(true, false).oneOf()
    val readCase = {
      stepCount: stepCount,
      dependencyOrderValid: dependencyOrderValid,
      selectorValid: selectorValid,
      selectorProjected: selectorProjected,
      childFanout: childFanout,
      setFanout: setFanout,
      duplicateChildKeys: duplicateChildKeys,
      hasMissingChild: hasMissingChild,
      joinMode: joinMode,
      consistency: consistency,
      readsGsi: readsGsi,
      immediateGsi: immediateGsi,
      backendSnapshot: backendSnapshot,
      continuationKind: continuationKind,
      tokenCarriesCursor: tokenCarriesCursor,
    }
    all {
      lastCase' = readCase,
      lastResult' = expectedResult(readCase),
      plannedReads' = totalReadsFor(readCase),
      tokenRequired' = needsToken(readCase),
    }
  }

  action step = Check

  val inv =
    (lastResult != "Accepted" or expandedChildFanout(lastCase) <= MAX_FANOUT)
      and (lastResult != "Accepted" or plannedReads <= MAX_TOTAL_READS or tokenRequired)
      and (lastResult != "Accepted" or lastCase.consistency != "STRONG" or not(lastCase.readsGsi))
      and (
        lastResult != "Accepted"
          or lastCase.consistency != "TRANSACTIONAL"
          or lastCase.backendSnapshot
      )
      and (
        lastResult != "Accepted"
          or lastCase.consistency != "TRANSACTIONAL"
          or not(lastCase.readsGsi)
          or lastCase.immediateGsi
      )

  run transactionalDelayedGsiRejected =
    true.expect(expectedResult(
      initialCase
        .with("consistency", "TRANSACTIONAL")
        .with("readsGsi", true)
        .with("backendSnapshot", true)
    ) == "RejectedTransactionalDelayedGsi")

  run duplicateChildKeysDedupeReads =
    true.expect(totalReadsFor(initialCase.with("childFanout", 3).with("setFanout", 2).with("duplicateChildKeys", true)) == 2)

  run setFanoutCountsTowardFanoutCap =
    true.expect(expectedResult(initialCase.with("childFanout", 2).with("setFanout", 2)) == "RejectedFanoutCap")

  run continuationRequiresCursor =
    true.expect(expectedResult(initialCase.with("continuationKind", "child_page")) == "RejectedInvalidTokenState")
}
