/// ReadSequence contract model.
///
/// This model defines the bounded dataflow, join, continuation, and
/// consistency gates that Rust must preserve. It is intentionally abstract:
/// steps are represented by counts and flags rather than DynamoDB wire shapes
/// so Quint can search the dangerous combinations before implementation.
module read_sequence {
  pure val MAX_SEQUENCE_STEPS = 3
  pure val MAX_FANOUT = 3
  pure val MAX_TOTAL_READS = 5
  pure val MAX_RESPONSE_BYTES = 100
  pure val ITEM_BYTES = 30

  type SequenceCase = {
    stepCount: int,
    dependencyOrderValid: bool,
    selectorValid: bool,
    selectorProjected: bool,
    rootItems: int,
    childFanout: int,
    setFanout: int,
    childItemsPerParent: int,
    duplicateChildKeys: bool,
    hasMissingChild: bool,
    joinMode: str,
    consistency: str,
    readsGsi: bool,
    immediateGsi: bool,
    backendSnapshot: bool,
    continuationKind: str,
    tokenCarriesCursor: bool,
    queryPlusGet: bool,
    warningSanitized: bool,
  }

  var lastCase: SequenceCase
  var lastOutcome: str
  var plannedReads: int
  var responseBytes: int
  var attachedParents: int
  var tokenRequired: bool
  var partialChildMarked: bool
  var oneSnapshot: bool
  var emittedWarning: bool

  pure def initialCase = {
    stepCount: 1,
    dependencyOrderValid: true,
    selectorValid: true,
    selectorProjected: true,
    rootItems: 1,
    childFanout: 0,
    setFanout: 1,
    childItemsPerParent: 0,
    duplicateChildKeys: false,
    hasMissingChild: false,
    joinMode: "LEFT_ONE",
    consistency: "EVENTUAL",
    readsGsi: false,
    immediateGsi: false,
    backendSnapshot: false,
    continuationKind: "none",
    tokenCarriesCursor: false,
    queryPlusGet: false,
    warningSanitized: true,
  }

  pure def maxInt(left, right) =
    if (left > right) left else right

  pure def minInt(left, right) =
    if (left < right) left else right

  pure def expandedChildFanout(readCase) =
    if (readCase.childFanout == 0)
      0
    else
      readCase.childFanout * maxInt(1, readCase.setFanout)

  pure def childReadCount(readCase) =
    if (readCase.childFanout == 0)
      0
    else if (readCase.duplicateChildKeys)
      minInt(1, expandedChildFanout(readCase) * readCase.childItemsPerParent)
    else
      expandedChildFanout(readCase) * readCase.childItemsPerParent

  pure def totalReadsFor(readCase) =
    readCase.rootItems + childReadCount(readCase)

  pure def childResponseCount(readCase) =
    if (readCase.hasMissingChild and readCase.joinMode == "INNER_ONE")
      maxInt(0, expandedChildFanout(readCase) - 1)
    else
      expandedChildFanout(readCase)

  pure def bytesFor(readCase) =
    (readCase.rootItems + childResponseCount(readCase)) * ITEM_BYTES

  pure def needsToken(readCase) =
    readCase.continuationKind != "none"
      or bytesFor(readCase) > MAX_RESPONSE_BYTES
      or totalReadsFor(readCase) > MAX_TOTAL_READS

  pure def consistencyAllowed(readCase) =
    if (readCase.consistency == "EVENTUAL")
      true
    else if (readCase.consistency == "STRONG")
      not(readCase.readsGsi)
    else if (readCase.consistency == "TRANSACTIONAL")
      readCase.backendSnapshot and (not(readCase.readsGsi) or readCase.immediateGsi)
    else
      false

  pure def expectedOutcome(readCase) =
    if (not(readCase.dependencyOrderValid))
      "rejected_dependency_order"
    else if (readCase.stepCount > MAX_SEQUENCE_STEPS)
      "rejected_step_cap"
    else if (not(readCase.selectorValid))
      "rejected_selector"
    else if (not(readCase.selectorProjected))
      "rejected_unprojected_selector"
    else if (expandedChildFanout(readCase) > MAX_FANOUT)
      "rejected_fanout_cap"
    else if (readCase.consistency == "STRONG" and readCase.readsGsi)
      "rejected_strong_gsi"
    else if (
      readCase.consistency == "TRANSACTIONAL"
        and readCase.readsGsi
        and not(readCase.immediateGsi)
    )
      "rejected_transactional_delayed_gsi"
    else if (readCase.consistency == "TRANSACTIONAL" and not(readCase.backendSnapshot))
      "rejected_transactional_unsupported"
    else if (readCase.hasMissingChild and readCase.joinMode == "REQUIRED_ONE")
      "rejected_required_child_missing"
    else if (
      needsToken(readCase)
        and (not(readCase.tokenCarriesCursor) or readCase.continuationKind == "partial_child_unmarked")
    )
      "rejected_invalid_token_state"
    else
      "accepted"

  pure def accepted(readCase) =
    expectedOutcome(readCase) == "accepted"

  pure val queryChildGetCase =
    initialCase
      .with("childFanout", 1)
      .with("childItemsPerParent", 1)

  pure val getChildBatchCase =
    initialCase
      .with("childFanout", 1)
      .with("setFanout", 3)
      .with("childItemsPerParent", 1)
      .with("joinMode", "ARRAY")
      .with("tokenCarriesCursor", true)

  pure val childQueryCase =
    initialCase
      .with("childFanout", 1)
      .with("childItemsPerParent", 2)
      .with("joinMode", "ARRAY")

  pure val missingChildCase =
    initialCase
      .with("childFanout", 1)
      .with("childItemsPerParent", 1)
      .with("hasMissingChild", true)

  pure val duplicateChildKeyCase =
    initialCase
      .with("childFanout", 3)
      .with("childItemsPerParent", 1)
      .with("duplicateChildKeys", true)
      .with("tokenCarriesCursor", true)

  pure val requiredMissingChildCase =
    missingChildCase.with("joinMode", "REQUIRED_ONE")

  pure val innerMissingChildCase =
    missingChildCase.with("joinMode", "INNER_ONE")

  pure val delayedGsiTransactionalCase =
    initialCase
      .with("consistency", "TRANSACTIONAL")
      .with("readsGsi", true)
      .with("backendSnapshot", true)

  pure val immediateGsiTransactionalCase =
    delayedGsiTransactionalCase.with("immediateGsi", true)

  pure val strongGsiCase =
    initialCase
      .with("consistency", "STRONG")
      .with("readsGsi", true)

  pure val tokenResumeCase =
    initialCase
      .with("continuationKind", "child_page")
      .with("tokenCarriesCursor", true)

  pure val rootPaginationCase =
    initialCase
      .with("continuationKind", "root_page")
      .with("tokenCarriesCursor", true)

  pure val invalidTokenCase =
    initialCase.with("continuationKind", "child_page")

  pure val dependencyOrderCase =
    initialCase.with("dependencyOrderValid", false)

  pure val selectorProjectionCase =
    initialCase.with("selectorProjected", false)

  pure val fanoutOverflowCase =
    initialCase.with("childFanout", 2).with("setFanout", 2)

  pure val modelingWarningCase =
    queryChildGetCase
      .with("queryPlusGet", true)
      .with("warningSanitized", true)

  action init = all {
    lastCase' = initialCase,
    lastOutcome' = "not_checked",
    plannedReads' = 0,
    responseBytes' = 0,
    attachedParents' = 0,
    tokenRequired' = false,
    partialChildMarked' = false,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action QueryChildGet = all {
    lastCase' = queryChildGetCase,
    lastOutcome' = expectedOutcome(queryChildGetCase),
    plannedReads' = totalReadsFor(queryChildGetCase),
    responseBytes' = bytesFor(queryChildGetCase),
    attachedParents' = childResponseCount(queryChildGetCase),
    tokenRequired' = needsToken(queryChildGetCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action GetChildBatch = all {
    lastCase' = getChildBatchCase,
    lastOutcome' = expectedOutcome(getChildBatchCase),
    plannedReads' = totalReadsFor(getChildBatchCase),
    responseBytes' = bytesFor(getChildBatchCase),
    attachedParents' = childResponseCount(getChildBatchCase),
    tokenRequired' = needsToken(getChildBatchCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action ChildQuery = all {
    lastCase' = childQueryCase,
    lastOutcome' = expectedOutcome(childQueryCase),
    plannedReads' = totalReadsFor(childQueryCase),
    responseBytes' = bytesFor(childQueryCase),
    attachedParents' = childResponseCount(childQueryCase),
    tokenRequired' = needsToken(childQueryCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action MissingChild = all {
    lastCase' = missingChildCase,
    lastOutcome' = expectedOutcome(missingChildCase),
    plannedReads' = totalReadsFor(missingChildCase),
    responseBytes' = bytesFor(missingChildCase),
    attachedParents' = childResponseCount(missingChildCase),
    tokenRequired' = needsToken(missingChildCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action DuplicateChildKey = all {
    lastCase' = duplicateChildKeyCase,
    lastOutcome' = expectedOutcome(duplicateChildKeyCase),
    plannedReads' = totalReadsFor(duplicateChildKeyCase),
    responseBytes' = bytesFor(duplicateChildKeyCase),
    attachedParents' = childResponseCount(duplicateChildKeyCase),
    tokenRequired' = needsToken(duplicateChildKeyCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action RequiredMissingChild = all {
    lastCase' = requiredMissingChildCase,
    lastOutcome' = expectedOutcome(requiredMissingChildCase),
    plannedReads' = totalReadsFor(requiredMissingChildCase),
    responseBytes' = bytesFor(requiredMissingChildCase),
    attachedParents' = childResponseCount(requiredMissingChildCase),
    tokenRequired' = needsToken(requiredMissingChildCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action InnerMissingChild = all {
    lastCase' = innerMissingChildCase,
    lastOutcome' = expectedOutcome(innerMissingChildCase),
    plannedReads' = totalReadsFor(innerMissingChildCase),
    responseBytes' = bytesFor(innerMissingChildCase),
    attachedParents' = childResponseCount(innerMissingChildCase),
    tokenRequired' = needsToken(innerMissingChildCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action DelayedGsiTransactional = all {
    lastCase' = delayedGsiTransactionalCase,
    lastOutcome' = expectedOutcome(delayedGsiTransactionalCase),
    plannedReads' = totalReadsFor(delayedGsiTransactionalCase),
    responseBytes' = bytesFor(delayedGsiTransactionalCase),
    attachedParents' = childResponseCount(delayedGsiTransactionalCase),
    tokenRequired' = needsToken(delayedGsiTransactionalCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action ImmediateGsiTransactional = all {
    lastCase' = immediateGsiTransactionalCase,
    lastOutcome' = expectedOutcome(immediateGsiTransactionalCase),
    plannedReads' = totalReadsFor(immediateGsiTransactionalCase),
    responseBytes' = bytesFor(immediateGsiTransactionalCase),
    attachedParents' = childResponseCount(immediateGsiTransactionalCase),
    tokenRequired' = needsToken(immediateGsiTransactionalCase),
    partialChildMarked' = true,
    oneSnapshot' = true,
    emittedWarning' = false,
  }

  action StrongGsi = all {
    lastCase' = strongGsiCase,
    lastOutcome' = expectedOutcome(strongGsiCase),
    plannedReads' = totalReadsFor(strongGsiCase),
    responseBytes' = bytesFor(strongGsiCase),
    attachedParents' = childResponseCount(strongGsiCase),
    tokenRequired' = needsToken(strongGsiCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action TokenResume = all {
    lastCase' = tokenResumeCase,
    lastOutcome' = expectedOutcome(tokenResumeCase),
    plannedReads' = totalReadsFor(tokenResumeCase),
    responseBytes' = bytesFor(tokenResumeCase),
    attachedParents' = childResponseCount(tokenResumeCase),
    tokenRequired' = needsToken(tokenResumeCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action RootPagination = all {
    lastCase' = rootPaginationCase,
    lastOutcome' = expectedOutcome(rootPaginationCase),
    plannedReads' = totalReadsFor(rootPaginationCase),
    responseBytes' = bytesFor(rootPaginationCase),
    attachedParents' = childResponseCount(rootPaginationCase),
    tokenRequired' = needsToken(rootPaginationCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action InvalidToken = all {
    lastCase' = invalidTokenCase,
    lastOutcome' = expectedOutcome(invalidTokenCase),
    plannedReads' = totalReadsFor(invalidTokenCase),
    responseBytes' = bytesFor(invalidTokenCase),
    attachedParents' = childResponseCount(invalidTokenCase),
    tokenRequired' = needsToken(invalidTokenCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action DependencyOrder = all {
    lastCase' = dependencyOrderCase,
    lastOutcome' = expectedOutcome(dependencyOrderCase),
    plannedReads' = totalReadsFor(dependencyOrderCase),
    responseBytes' = bytesFor(dependencyOrderCase),
    attachedParents' = childResponseCount(dependencyOrderCase),
    tokenRequired' = needsToken(dependencyOrderCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action SelectorProjection = all {
    lastCase' = selectorProjectionCase,
    lastOutcome' = expectedOutcome(selectorProjectionCase),
    plannedReads' = totalReadsFor(selectorProjectionCase),
    responseBytes' = bytesFor(selectorProjectionCase),
    attachedParents' = childResponseCount(selectorProjectionCase),
    tokenRequired' = needsToken(selectorProjectionCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action FanoutOverflow = all {
    lastCase' = fanoutOverflowCase,
    lastOutcome' = expectedOutcome(fanoutOverflowCase),
    plannedReads' = totalReadsFor(fanoutOverflowCase),
    responseBytes' = bytesFor(fanoutOverflowCase),
    attachedParents' = childResponseCount(fanoutOverflowCase),
    tokenRequired' = needsToken(fanoutOverflowCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = false,
  }

  action ModelingWarning = all {
    lastCase' = modelingWarningCase,
    lastOutcome' = expectedOutcome(modelingWarningCase),
    plannedReads' = totalReadsFor(modelingWarningCase),
    responseBytes' = bytesFor(modelingWarningCase),
    attachedParents' = childResponseCount(modelingWarningCase),
    tokenRequired' = needsToken(modelingWarningCase),
    partialChildMarked' = true,
    oneSnapshot' = false,
    emittedWarning' = true,
  }

  action step =
    any {
      QueryChildGet,
      GetChildBatch,
      ChildQuery,
      MissingChild,
      DuplicateChildKey,
      RequiredMissingChild,
      InnerMissingChild,
      DelayedGsiTransactional,
      ImmediateGsiTransactional,
      StrongGsi,
      TokenResume,
      RootPagination,
      InvalidToken,
      DependencyOrder,
      SelectorProjection,
      FanoutOverflow,
      ModelingWarning,
    }

  def dependencyOrderGate =
    lastCase.dependencyOrderValid or lastOutcome == "rejected_dependency_order"

  def selectorProjectionGate =
    lastCase.selectorProjected or lastOutcome == "rejected_unprojected_selector"

  def fanoutBound =
    not(accepted(lastCase)) or expandedChildFanout(lastCase) <= MAX_FANOUT

  def totalReadBound =
    not(accepted(lastCase)) or plannedReads <= MAX_TOTAL_READS or tokenRequired

  def responseByteBound =
    not(accepted(lastCase)) or responseBytes <= MAX_RESPONSE_BYTES or tokenRequired

  def transactionalUsesSnapshot =
    not(accepted(lastCase) and lastCase.consistency == "TRANSACTIONAL") or oneSnapshot

  def gsiConsistencyGate =
    consistencyAllowed(lastCase) or lastOutcome != "accepted"

  def continuationCarriesPosition =
    not(accepted(lastCase) and tokenRequired) or (lastCase.tokenCarriesCursor and partialChildMarked)

  def requiredChildMissingFails =
    not(lastCase.hasMissingChild and lastCase.joinMode == "REQUIRED_ONE") or lastOutcome == "rejected_required_child_missing"

  def innerJoinDropsMissingParent =
    not(accepted(lastCase) and lastCase.hasMissingChild and lastCase.joinMode == "INNER_ONE")
      or attachedParents < lastCase.childFanout

  def warningDoesNotLeakRawValues =
    not(emittedWarning) or lastCase.warningSanitized

  def duplicateKeysDoNotAmplifyReads =
    not(accepted(lastCase) and lastCase.duplicateChildKeys and lastCase.childFanout > 0)
      or plannedReads < lastCase.rootItems + (expandedChildFanout(lastCase) * lastCase.childItemsPerParent)

  def inv =
    dependencyOrderGate
      and selectorProjectionGate
      and fanoutBound
      and totalReadBound
      and responseByteBound
      and transactionalUsesSnapshot
      and gsiConsistencyGate
      and continuationCarriesPosition
      and requiredChildMissingFails
      and innerJoinDropsMissingParent
      and warningDoesNotLeakRawValues
      and duplicateKeysDoNotAmplifyReads

  def query_child_get_witness =
    accepted(lastCase)
      and lastCase.rootItems > 0
      and lastCase.childFanout > 0
      and lastCase.childItemsPerParent == 1
      and lastCase.joinMode == "LEFT_ONE"

  def get_child_batch_witness =
    accepted(lastCase)
      and lastCase.rootItems == 1
      and lastCase.childFanout > 1
      and lastCase.joinMode == "ARRAY"

  def child_query_witness =
    accepted(lastCase)
      and lastCase.childItemsPerParent == 2
      and lastCase.joinMode == "ARRAY"

  def missing_child_witness =
    accepted(lastCase)
      and lastCase.hasMissingChild
      and lastCase.joinMode == "LEFT_ONE"

  def duplicate_child_key_witness =
    accepted(lastCase)
      and lastCase.duplicateChildKeys
      and lastCase.childFanout > 1

  def transactional_gsi_rejected_witness =
    lastOutcome == "rejected_transactional_delayed_gsi"

  def immediate_gsi_transactional_witness =
    accepted(lastCase)
      and lastCase.consistency == "TRANSACTIONAL"
      and lastCase.readsGsi
      and lastCase.immediateGsi

  def token_resume_witness =
    accepted(lastCase)
      and tokenRequired
      and lastCase.tokenCarriesCursor

  def root_pagination_witness =
    token_resume_witness
      and lastCase.continuationKind == "root_page"

  def child_pagination_witness =
    token_resume_witness
      and lastCase.continuationKind == "child_page"

  run queryChildGet =
    true.expect(expectedOutcome(initialCase.with("childFanout", 1).with("childItemsPerParent", 1)) == "accepted")

  run getChildBatch =
    true.expect(expectedOutcome(initialCase.with("childFanout", 1).with("setFanout", 3).with("childItemsPerParent", 1).with("joinMode", "ARRAY").with("tokenCarriesCursor", true)) == "accepted")

  run childQuery =
    true.expect(expectedOutcome(initialCase.with("childFanout", 1).with("childItemsPerParent", 2).with("joinMode", "ARRAY")) == "accepted")

  run missingChildLeftOne =
    true.expect(expectedOutcome(initialCase.with("childFanout", 1).with("childItemsPerParent", 1).with("hasMissingChild", true)) == "accepted")

  run missingChildRequiredOneFails =
    true.expect(expectedOutcome(initialCase.with("childFanout", 1).with("childItemsPerParent", 1).with("hasMissingChild", true).with("joinMode", "REQUIRED_ONE")) == "rejected_required_child_missing")

  run transactionalDelayedGsiRejected =
    true.expect(expectedOutcome(initialCase.with("consistency", "TRANSACTIONAL").with("readsGsi", true).with("backendSnapshot", true)) == "rejected_transactional_delayed_gsi")

  run immediateGsiTransactionalAccepted =
    true.expect(expectedOutcome(initialCase.with("consistency", "TRANSACTIONAL").with("readsGsi", true).with("immediateGsi", true).with("backendSnapshot", true)) == "accepted")
}
