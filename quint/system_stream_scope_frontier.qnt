/// Design-level protocol model. This checks abstract transitions and assumptions,
/// not production implementation, provider atomicity, or crash durability.
/// Canonical SystemStream scope/frontier contract.
///
/// Setup follows normal append, issue, split, page, trim, and delivery paths.
/// The ten unsafe actions are reachable only after that setup trace.
module system_stream_scope_frontier {
  pure val SCOPE_VERSION = 1
  pure val SCOPE_ID = "system"
  pure val GENERATION = 7
  pure val MAX_FRONTIER = 4
  /// Finite refinement: four model entries/ranked offsets stand for the Rust
  /// bound of 128 entries and opaque offsets of at most 256 bytes. These are
  /// abstract shape/atomicity checks, not implementation or exhaustive size proof.
  pure val RUST_MAX_FRONTIER_ENTRIES = 128
  pure val RUST_MAX_OPAQUE_OFFSET_BYTES = 256
  pure val SHARDS = Set("parent", "left", "right", "other")
  pure val LINEAGES = Set("lineage-parent", "lineage-left", "lineage-right", "lineage-other")
  pure val TOKEN_INPUTS = Set("", "v1.token-a", "v1.token-b", "v1.unknown", "v1.forged", "raw-cursor")
  pure val SCOPE_INPUTS = Set(SCOPE_ID, "wrong-scope", "")
  pure val GENERATION_INPUTS = Set(GENERATION, GENERATION - 1, 0)
  pure val LINEAGE_INPUTS = Set("lineage-parent", "lineage-left", "lineage-right", "lineage-other", "unknown", "")
  pure val ROUTE_INPUTS = Set("logical", "physical-stale", "")

  type Scope = { version: int, id: str, generation: int }
  type Position = { shard: str, lineage: str, offset: int }
  type Token = { scope: Scope, frontier: Set[Position], issued: bool }
  type Issue = { id: str, token: str, frontier: Set[Position], phase: str, applied: bool, acknowledged: bool, retries: int }
  type Request = {
    token: str, scopeVersion: int, scopeId: str, generation: int, lineage: str,
    hasLineage: bool, route: str, hasRoute: bool, legacyCursor: str, hasLegacyCursor: bool,
  }
  type Durable = {
    scope: Scope, knownLineages: Set[str], tokens: str -> Token, issuedTokens: Set[str],
    records: str -> Position, pointers: str -> Position, recordIds: Set[str], pointerIds: Set[str],
    retainedRecords: Set[str], trimmedRecords: Set[str], outstandingRecords: Set[str],
    recordSequence: str -> int, nextSequence: int, readSequence: str -> int, nextReadSequence: int,
    persistedScope: Scope, persistedToken: str, persistedFrontier: Set[Position], persistedLineages: Set[str],
    activeShards: Set[str], sealedShards: Set[str], parentSealed: bool,
    consumerOffsets: str -> int, protectedOffsets: str -> int, trimOffsets: str -> int, readOffsets: str -> int,
    delivered: Set[str], redelivered: Set[str], reclaimed: Set[str], occurrences: str -> int, redeliveryCounts: str -> int,
    currentToken: str, issue: Issue, completedOperations: Set[str], continuationPage: int,
    restartCount: int, replayCount: int, stage: int,
  }
  type AppState = {
    durable: Durable, previousDurable: Durable, reads: Set[str], previousReads: Set[str],
    lastRequest: Request, lastOutcome: str,
  }

  var state: AppState

  pure val initialScope = { version: SCOPE_VERSION, id: SCOPE_ID, generation: GENERATION }
  pure def p(id) =
    if (id == "parent@1") { shard: "parent", lineage: "lineage-parent", offset: 1 }
    else if (id == "parent@2") { shard: "parent", lineage: "lineage-parent", offset: 2 }
    else if (id == "parent@3") { shard: "parent", lineage: "lineage-parent", offset: 3 }
    else if (id == "left@1") { shard: "left", lineage: "lineage-left", offset: 1 }
    else if (id == "left@2") { shard: "left", lineage: "lineage-left", offset: 2 }
    else if (id == "right@1") { shard: "right", lineage: "lineage-right", offset: 1 }
    else if (id == "right@2") { shard: "right", lineage: "lineage-right", offset: 2 }
    else { shard: "other", lineage: "lineage-other", offset: 1 }
  pure def lineage(shard) =
    if (shard == "parent") "lineage-parent" else if (shard == "left") "lineage-left" else if (shard == "right") "lineage-right" else "lineage-other"
  pure def opaque(token) = token != "" and token != "raw-cursor"
  pure def emptyToken = { scope: initialScope, frontier: Set(), issued: false }
  pure def emptyPosition = { shard: "", lineage: "", offset: 0 }
  pure def preSplit = Set(p("parent@2"), p("other@1"))
  pure def children = Set(p("left@2"), p("right@2"), p("other@1"))
  pure def tokenFrontier(token) = if (token == "v1.token-a") preSplit else if (token == "v1.token-b") children else Set()
  /// Quint maps have a finite declared key domain; empty slots are not records.
  /// `recordIds` and the retention sets are the durable presence authority.
  pure def initialRecords = Map(
    "parent@1" -> emptyPosition, "parent@2" -> emptyPosition, "parent@3" -> emptyPosition,
    "left@1" -> emptyPosition, "left@2" -> emptyPosition, "right@1" -> emptyPosition,
    "right@2" -> emptyPosition, "other@1" -> p("other@1"),
  )
  pure def offsets = Map("parent" -> 0, "left" -> 0, "right" -> 0, "other" -> 0)
  pure def counts = Map("parent@1" -> 0, "parent@2" -> 0, "parent@3" -> 0, "left@1" -> 0, "left@2" -> 0, "right@1" -> 0, "right@2" -> 0, "other@1" -> 0)
  pure def sequences = Map("parent@1" -> 0, "parent@2" -> 0, "parent@3" -> 0, "left@1" -> 0, "left@2" -> 0, "right@1" -> 0, "right@2" -> 0, "other@1" -> 0)
  pure def emptyIssue = { id: "idle", token: "", frontier: Set(), phase: "idle", applied: false, acknowledged: false, retries: 0 }
  pure def emptyRequest = { token: "", scopeVersion: 0, scopeId: "", generation: 0, lineage: "", hasLineage: false, route: "", hasRoute: false, legacyCursor: "", hasLegacyCursor: false }
  pure def initialTokens = Map(
    "v1.token-a" -> emptyToken, "v1.token-b" -> emptyToken, "v1.unknown" -> emptyToken,
    "v1.forged" -> emptyToken, "raw-cursor" -> emptyToken,
  )
  pure val initialDurable = {
    scope: initialScope, knownLineages: Set("lineage-other"), tokens: initialTokens, issuedTokens: Set(),
    records: initialRecords, pointers: initialRecords, recordIds: Set("other@1"), pointerIds: Set("other@1"),
    retainedRecords: Set("other@1"), trimmedRecords: Set(), outstandingRecords: Set("other@1"),
    recordSequence: sequences, nextSequence: 1, readSequence: sequences, nextReadSequence: 1,
    persistedScope: initialScope, persistedToken: "", persistedFrontier: Set(), persistedLineages: Set(),
    activeShards: Set("other"), sealedShards: Set(), parentSealed: false,
    consumerOffsets: offsets, protectedOffsets: offsets, trimOffsets: offsets, readOffsets: offsets,
    delivered: Set(), redelivered: Set(), reclaimed: Set(), occurrences: counts, redeliveryCounts: counts,
    currentToken: "", issue: emptyIssue, completedOperations: Set(), continuationPage: 0,
    restartCount: 0, replayCount: 0, stage: 0,
  }
  pure val initialState = { durable: initialDurable, previousDurable: initialDurable, reads: Set(), previousReads: Set(), lastRequest: emptyRequest, lastOutcome: "initialized" }

  pure def frontierRecordIds(d, frontier) = d.recordIds.filter(id => frontier.contains(d.records.get(id)))
  pure def lineagesOf(frontier) = frontier.fold(Set(), (s, position) => s.union(Set(position.lineage)))
  pure def add(d, id) = {
    val position = p(id)
    d.with("records", d.records.set(id, position)).with("pointers", d.pointers.set(id, position))
      .with("recordIds", d.recordIds.union(Set(id))).with("pointerIds", d.pointerIds.union(Set(id)))
      .with("retainedRecords", d.retainedRecords.union(Set(id)))
      .with("recordSequence", d.recordSequence.set(id, d.nextSequence)).with("nextSequence", d.nextSequence + 1)
      .with("occurrences", d.occurrences.set(id, 0)).with("redeliveryCounts", d.redeliveryCounts.set(id, 0))
      .with("knownLineages", d.knownLineages.union(Set(position.lineage)))
  }
  pure def deliver(d, id) = {
    val first = not(d.delivered.contains(id))
    d.with("delivered", d.delivered.union(Set(id))).with("occurrences", d.occurrences.set(id, d.occurrences.get(id) + 1))
      .with("readSequence", if (first) d.readSequence.set(id, d.nextReadSequence) else d.readSequence)
      .with("nextReadSequence", if (first) d.nextReadSequence + 1 else d.nextReadSequence)
  }
  pure def protect(d, frontier) = frontier.fold(d.protectedOffsets, (m, position) => m.set(position.shard, position.offset))
  pure def issueId(d) = if (d.parentSealed) "op-child" else "op-parent"
  pure def issueToken(d) = if (d.parentSealed) "v1.token-b" else "v1.token-a"
  pure def tokenForIssue(id) = if (id == "op-parent") "v1.token-a" else "v1.token-b"
  pure def issueFrontier(d) = tokenFrontier(issueToken(d))
  pure def withToken(d, token, frontier) = d.with("tokens", d.tokens.set(token, { scope: d.scope, frontier: frontier, issued: true })).with("issuedTokens", d.issuedTokens.union(Set(token))).with("protectedOffsets", protect(d, frontier)).with("outstandingRecords", d.outstandingRecords.union(frontierRecordIds(d, frontier))).with("knownLineages", d.knownLineages.union(lineagesOf(frontier)))
  pure def persistPreSplit(d, issue) = d.with("persistedScope", d.scope).with("persistedToken", issue.token).with("persistedFrontier", issue.frontier).with("persistedLineages", lineagesOf(issue.frontier))
  pure def readRecords(d, l) = d.recordIds.filter(id => d.records.get(id).lineage == l)
  pure def accepts(d, request) = {
    if (d.issuedTokens.contains(request.token)) {
      val token = d.tokens.get(request.token)
      token.frontier.exists(position => position.lineage == request.lineage)
        and opaque(request.token) and request.scopeVersion == d.scope.version and request.scopeId == d.scope.id
        and request.generation == d.scope.generation and request.hasLineage and request.hasRoute and request.route == "logical"
        and not(request.hasLegacyCursor) and request.legacyCursor == "" and d.issuedTokens.contains(request.token)
        and token.issued and token.scope == d.scope
    } else false
  }
  pure def requestReads(d, reads, request) = if (accepts(d, request)) reads.union(readRecords(d, request.lineage)) else reads
  pure def transition(s, d, reads, request, outcome) = s.with("previousDurable", s.durable).with("previousReads", s.reads).with("durable", d).with("reads", reads).with("lastRequest", request).with("lastOutcome", outcome)

  action modelInit = all { state' = initialState }

  action AppendParentRecordOne = all {
    state.durable.stage == 0,
    state' = transition(state, add(state.durable, "parent@1").with("activeShards", state.durable.activeShards.union(Set("parent"))).with("stage", 1), state.reads, emptyRequest, "parent_record_appended"),
  }
  action AppendParentRecordTwo = all {
    state.durable.stage == 1, state.durable.recordIds.contains("parent@1"),
    state' = transition(state, add(state.durable, "parent@2").with("stage", 2), state.reads, emptyRequest, "parent_record_appended"),
  }
  action ReadParentPage = all {
    state.durable.stage == 2, state.durable.recordIds.contains("parent@2"),
    state' = transition(state, deliver(deliver(state.durable, "parent@1"), "parent@2").with("readOffsets", state.durable.readOffsets.set("parent", 2)).with("consumerOffsets", state.durable.consumerOffsets.set("parent", 2)).with("protectedOffsets", state.durable.protectedOffsets.set("parent", 2)).with("stage", 3), state.reads.union(Set("parent@1", "parent@2")), emptyRequest, "parent_page_read"),
  }
  pure def prepared(d) = { id: issueId(d), token: issueToken(d), frontier: issueFrontier(d), phase: "prepared", applied: false, acknowledged: false, retries: 0 }
  action PrepareCheckpoint = all {
    (state.durable.stage == 3 or state.durable.stage == 7), (state.durable.issue.phase == "idle" or (state.durable.stage == 7 and state.durable.issue.phase == "acknowledged" and state.durable.issue.id == "op-parent")), not(state.durable.completedOperations.contains(issueId(state.durable))),
    state' = transition(state, state.durable.with("issue", prepared(state.durable)), state.reads, emptyRequest, "checkpoint_prepared"),
  }
  action DispatchCheckpoint = all {
    state.durable.issue.phase == "prepared",
    state' = transition(state, state.durable.with("issue", state.durable.issue.with("phase", "dispatched")), state.reads, emptyRequest, "checkpoint_dispatched"),
  }
  action ApplyCheckpointUnknown = all {
    state.durable.stage == 3, state.durable.issue.token == "v1.token-a", state.durable.issue.phase == "dispatched", state.durable.issue.retries == 0,
    state' = transition(state, state.durable.with("issue", state.durable.issue.with("phase", "unknown").with("applied", false).with("acknowledged", false)), state.reads, emptyRequest, "checkpoint_unknown"),
  }
  action AcknowledgeCheckpointUnknown = all {
    state.durable.issue.phase == "unknown", not(state.durable.issue.applied),
    state' = transition(state, state.durable.with("issue", state.durable.issue.with("acknowledged", false)), state.reads, emptyRequest, "checkpoint_unknown_ack"),
  }
  action RetryCheckpoint = all {
    (state.durable.stage == 3 or state.durable.stage == 11), state.durable.issue.phase == "unknown", not(state.durable.issue.applied),
    state' = transition(state, state.durable.with("issue", state.durable.issue.with("phase", "dispatched").with("retries", state.durable.issue.retries + 1)), state.reads, emptyRequest, "checkpoint_retried"),
  }
  action ApplyCheckpointCommitted = all {
    state.durable.issue.phase == "dispatched", state.durable.issue.token == issueToken(state.durable),
    (state.durable.stage == 3 and state.durable.issue.retries > 0) or state.durable.stage == 7 or (state.durable.stage == 11 and state.durable.issue.retries > 0),
    state' = transition(state, withToken(state.durable, state.durable.issue.token, state.durable.issue.frontier).with("issue", state.durable.issue.with("phase", "maybe_applied").with("applied", true)), state.reads, emptyRequest, "checkpoint_maybe_applied"),
  }
  action AcknowledgeCheckpointAccepted = all {
    state.durable.issue.phase == "maybe_applied", state.durable.issue.applied,
    val acknowledged = state.durable.with("issue", state.durable.issue.with("phase", "acknowledged").with("acknowledged", true))
      .with("currentToken", state.durable.issue.token)
      .with("completedOperations", state.durable.completedOperations.union(Set(state.durable.issue.id)))
    val durableNext = if (state.durable.stage == 3) persistPreSplit(acknowledged, state.durable.issue)
      else acknowledged.with("outstandingRecords", acknowledged.outstandingRecords.union(Set("left@1")))
    state' = transition(state, durableNext.with("stage", if (state.durable.stage == 3) 4 else 8), state.reads, emptyRequest, "checkpoint_acknowledged"),
  }
  action AcknowledgeCheckpointUnknownAfterApply = all {
    (state.durable.stage == 7 or state.durable.stage == 11), state.durable.issue.phase == "maybe_applied", state.durable.issue.applied,
    state' = transition(state, state.durable.with("issue", state.durable.issue.with("phase", "unknown").with("acknowledged", false)), state.reads, emptyRequest, "checkpoint_unknown_ack"),
  }
  action ReconcileCheckpoint = all {
    (state.durable.stage == 7 or state.durable.stage == 11), state.durable.issue.phase == "unknown", state.durable.issue.applied, state.durable.issuedTokens.contains(state.durable.issue.token),
    val reconciled = state.durable.with("issue", state.durable.issue.with("phase", "acknowledged").with("acknowledged", true)).with("currentToken", state.durable.issue.token).with("completedOperations", state.durable.completedOperations.union(Set(state.durable.issue.id))).with("outstandingRecords", if (state.durable.stage == 7) state.durable.outstandingRecords.union(Set("left@1")) else state.durable.outstandingRecords)
    state' = transition(state, reconciled.with("replayCount", if (state.durable.stage == 11) state.durable.replayCount + 1 else state.durable.replayCount).with("stage", if (state.durable.stage == 11) 12 else 8), state.reads, emptyRequest, if (state.durable.stage == 11) "replay_reconciled" else "checkpoint_reconciled"),
  }
  action SplitParent = all {
    state.durable.stage == 4, not(state.durable.parentSealed), state.durable.issue.phase == "acknowledged", state.durable.currentToken == "v1.token-a",
    state' = transition(state, state.durable.with("parentSealed", true).with("sealedShards", state.durable.sealedShards.union(Set("parent"))).with("activeShards", state.durable.activeShards.exclude(Set("parent")).union(Set("left", "right", "other"))).with("knownLineages", state.durable.knownLineages.union(Set("lineage-left", "lineage-right"))).with("continuationPage", 0).with("stage", 5), state.reads, emptyRequest, "parent_sealed"),
  }
  action RestartFromPreSplitCheckpoint = all {
    state.durable.stage == 5, state.durable.parentSealed, state.durable.currentToken == "v1.token-a", state.durable.issue.phase == "acknowledged",
    state.durable.persistedScope == state.durable.scope, state.durable.persistedToken == "v1.token-a",
    state.durable.persistedFrontier == preSplit, state.durable.persistedLineages == lineagesOf(preSplit),
    state.durable.issuedTokens.contains(state.durable.persistedToken),
    state.durable.tokens.get(state.durable.persistedToken).issued,
    state.durable.tokens.get(state.durable.persistedToken).scope == state.durable.persistedScope,
    state.durable.tokens.get(state.durable.persistedToken).frontier == state.durable.persistedFrontier,
    state.durable.persistedFrontier.forall(position => state.durable.knownLineages.contains(position.lineage)),
    state' = transition(state, state.durable.with("currentToken", state.durable.persistedToken).with("continuationPage", 0).with("restartCount", state.durable.restartCount + 1), state.reads, emptyRequest, "continuation_restarted"),
  }
  action ResumeChildrenPageOne = all {
    state.durable.stage == 5, state.durable.parentSealed, state.durable.continuationPage == 0, state.durable.activeShards.contains("left") and state.durable.activeShards.contains("right"),
    state' = transition(state, deliver(deliver(add(add(state.durable, "left@1"), "right@1"), "left@1"), "right@1").with("readOffsets", state.durable.readOffsets.set("left", 1).set("right", 1)).with("consumerOffsets", state.durable.consumerOffsets.set("left", 1).set("right", 1)).with("continuationPage", 1).with("stage", 6), state.reads.union(Set("left@1", "right@1")), emptyRequest, "child_page_one_read"),
  }
  action ResumeChildrenPageTwo = all {
    state.durable.stage == 6, state.durable.continuationPage == 1,
    state' = transition(state, deliver(deliver(add(add(state.durable, "left@2"), "right@2"), "left@2"), "right@2").with("readOffsets", state.durable.readOffsets.set("left", 2).set("right", 2)).with("consumerOffsets", state.durable.consumerOffsets.set("left", 2).set("right", 2)).with("continuationPage", 2).with("stage", 7), state.reads.union(Set("left@2", "right@2")), emptyRequest, "child_page_two_read"),
  }

  pure def trimCandidates(d) = d.recordIds.filter(id => {
    val position = d.records.get(id)
    position.offset < d.protectedOffsets.get(position.shard) and not(d.outstandingRecords.contains(id))
  })
  pure def trimAtFrontier(d) = {
    val trimmed = trimCandidates(d)
    d.with("trimOffsets", d.protectedOffsets).with("trimmedRecords", d.trimmedRecords.union(trimmed))
      .with("retainedRecords", d.retainedRecords.union(d.outstandingRecords).exclude(trimmed))
  }
  action TrimAtProtectedFrontier = all {
    state.durable.stage == 8, state.durable.currentToken == "v1.token-b", state.durable.issue.phase == "acknowledged", state.durable.issuedTokens.contains("v1.token-b"),
    state' = transition(state, trimAtFrontier(state.durable).with("stage", 9), state.reads, emptyRequest, "trim_at_protected_frontier"),
  }
  action RedeliverChild = all {
    state.durable.stage == 9, state.durable.delivered.contains("left@1"), state.durable.retainedRecords.contains("left@1"), not(state.durable.trimmedRecords.contains("left@1")),
    state' = transition(state, deliver(state.durable, "left@1").with("redelivered", state.durable.redelivered.union(Set("left@1"))).with("redeliveryCounts", state.durable.redeliveryCounts.set("left@1", state.durable.redeliveryCounts.get("left@1") + 1)).with("stage", 10), state.reads, emptyRequest, "child_redelivered"),
  }
  action ReclaimChild = all {
    state.durable.stage == 10, state.durable.redelivered.contains("left@1"),
    state' = transition(state, state.durable.with("reclaimed", state.durable.reclaimed.union(Set("left@1"))).with("outstandingRecords", state.durable.outstandingRecords.exclude(Set("left@1"))).with("stage", 11), state.reads, emptyRequest, "child_reclaimed"),
  }
  action ExactReplayCheckpoint = all {
    state.durable.stage == 11, state.durable.issue.phase == "acknowledged", state.durable.completedOperations.contains("op-child"),
    state' = transition(state, state.durable.with("issue", { id: "op-child", token: "v1.token-b", frontier: children, phase: "prepared", applied: false, acknowledged: false, retries: 0 }), state.reads, emptyRequest, "replay_prepared"),
  }
  action ApplyReplayUnknown = all {
    state.durable.stage == 11, state.durable.issue.id == "op-child", state.durable.issue.token == "v1.token-b", state.durable.issue.phase == "dispatched", state.durable.issue.retries == 0,
    state' = transition(state, state.durable.with("issue", state.durable.issue.with("phase", "unknown").with("applied", false).with("acknowledged", false)), state.reads, emptyRequest, "replay_unknown"),
  }
  action ReadRequest = {
    nondet token = TOKEN_INPUTS.oneOf()
    nondet scopeVersion = Set(SCOPE_VERSION, SCOPE_VERSION - 1, 0).oneOf()
    nondet scopeId = SCOPE_INPUTS.oneOf()
    nondet generation = GENERATION_INPUTS.oneOf()
    nondet lineage = LINEAGE_INPUTS.oneOf()
    nondet hasLineage = Set(false, true).oneOf()
    nondet route = ROUTE_INPUTS.oneOf()
    nondet hasRoute = Set(false, true).oneOf()
    nondet legacyCursor = Set("", "legacy-cursor", "physical-parent/1").oneOf()
    nondet hasLegacyCursor = Set(false, true).oneOf()
    val request = { token: token, scopeVersion: scopeVersion, scopeId: scopeId, generation: generation, lineage: lineage, hasLineage: hasLineage, route: route, hasRoute: hasRoute, legacyCursor: legacyCursor, hasLegacyCursor: hasLegacyCursor }
    val accepted = accepts(state.durable, request)
    all {
      state.durable.issuedTokens.size() > 0,
      state' = transition(state, state.durable, requestReads(state.durable, state.reads, request), request, if (accepted) "read" else if (route == "physical-stale") "stale_route_read" else "rejected"),
    }
  }

  action mutationSetupStep = any {
    AppendParentRecordOne, AppendParentRecordTwo, ReadParentPage, PrepareCheckpoint, DispatchCheckpoint,
    ApplyCheckpointUnknown, AcknowledgeCheckpointUnknown, RetryCheckpoint, ApplyCheckpointCommitted,
    AcknowledgeCheckpointAccepted, AcknowledgeCheckpointUnknownAfterApply, ReconcileCheckpoint, SplitParent,
    RestartFromPreSplitCheckpoint, ResumeChildrenPageOne, ResumeChildrenPageTwo, TrimAtProtectedFrontier,
    RedeliverChild, ReclaimChild, ExactReplayCheckpoint, ApplyReplayUnknown,
  }
  action modelStep = any { mutationSetupStep, ReadRequest }

  pure def validPosition(x) = SHARDS.contains(x.shard) and LINEAGES.contains(x.lineage) and x.lineage == lineage(x.shard) and x.offset > 0
  pure def frontierOK(f) = f.size() <= MAX_FRONTIER and f.forall(x => validPosition(x)) and SHARDS.forall(s => f.filter(x => x.shard == s).size() <= 1)
  pure def pointersOK(d) = d.recordIds == d.pointerIds and d.recordIds.forall(id => d.records.get(id) == d.pointers.get(id) and validPosition(d.records.get(id)))
  pure def catalogOK(d) = d.issuedTokens.forall(t => opaque(t) and d.tokens.get(t).issued and d.tokens.get(t).scope == d.scope and d.tokens.get(t).frontier == tokenFrontier(t) and frontierOK(d.tokens.get(t).frontier) and d.tokens.get(t).frontier.forall(x => d.knownLineages.contains(x.lineage)))
  pure def protectedOK(d) = d.issuedTokens.forall(t => d.tokens.get(t).frontier.forall(x => d.trimOffsets.get(x.shard) <= x.offset and d.protectedOffsets.get(x.shard) >= x.offset))
  pure def retainedOK(d) = d.retainedRecords.union(d.trimmedRecords) == d.recordIds and d.retainedRecords.intersect(d.trimmedRecords).size() == 0 and d.outstandingRecords.subseteq(d.retainedRecords) and d.outstandingRecords.intersect(d.trimmedRecords).size() == 0
  pure def issuedFrontierRetained(d) = d.issuedTokens.forall(t => frontierRecordIds(d, d.tokens.get(t).frontier).subseteq(d.retainedRecords))
  pure def trimOK(d) = SHARDS.forall(s => d.trimOffsets.get(s) >= 0 and d.trimOffsets.get(s) <= d.protectedOffsets.get(s)) and protectedOK(d) and retainedOK(d) and issuedFrontierRetained(d)
  pure def continuationOK(d) = d.continuationPage < 2 or (d.parentSealed and d.activeShards.contains("left") and d.activeShards.contains("right") and d.recordIds.contains("left@1") and d.recordIds.contains("left@2") and d.recordIds.contains("right@1") and d.recordIds.contains("right@2") and d.retainedRecords.contains("left@2") and d.retainedRecords.contains("right@2") and d.readOffsets.get("left") == 2 and d.readOffsets.get("right") == 2 and d.delivered.contains("left@2") and d.delivered.contains("right@2"))
  pure def deliveriesOK(d) = d.recordIds.forall(id => (not(d.delivered.contains(id)) and d.occurrences.get(id) == 0) or (d.delivered.contains(id) and d.occurrences.get(id) == 1 + d.redeliveryCounts.get(id))) and d.delivered.subseteq(d.recordIds) and d.redelivered.subseteq(d.delivered) and d.reclaimed.subseteq(d.delivered)
  pure def redeliveryOK(d) = d.redelivered.forall(id => d.retainedRecords.contains(id) and not(d.trimmedRecords.contains(id)) and d.delivered.contains(id) and (d.records.get(id).offset >= d.trimOffsets.get(d.records.get(id).shard) or d.retainedRecords.contains(id)))
  pure def operationOK(d) = d.issue.retries >= 0 and (d.issue.phase == "idle" or (Set("prepared", "dispatched", "unknown", "maybe_applied", "acknowledged").contains(d.issue.phase) and Set("op-parent", "op-child").contains(d.issue.id) and d.issue.token == tokenForIssue(d.issue.id) and d.issue.frontier == tokenFrontier(d.issue.token) and (not(d.issue.acknowledged) or d.issue.applied) and (not(d.completedOperations.contains(d.issue.id)) or d.issue.phase == "acknowledged" or d.stage == 11)))
  pure def operationAtomic(d) = not(d.issue.applied) or (d.issuedTokens.contains(d.issue.token) and d.tokens.get(d.issue.token).issued and d.tokens.get(d.issue.token).frontier == d.issue.frontier)
  pure def replayOK(s) = s.lastOutcome != "replay_reconciled" or (s.durable.issue.id == s.previousDurable.issue.id and s.durable.issue.token == s.previousDurable.issue.token and s.durable.issue.frontier == s.previousDurable.issue.frontier and s.durable.issue.phase == "acknowledged" and s.durable.issue.acknowledged and s.durable.replayCount == s.previousDurable.replayCount + 1 and s.durable.issuedTokens == s.previousDurable.issuedTokens and s.durable.tokens == s.previousDurable.tokens and s.durable.recordIds == s.previousDurable.recordIds and s.durable.pointerIds == s.previousDurable.pointerIds and s.durable.records == s.previousDurable.records and s.durable.pointers == s.previousDurable.pointers and s.durable.retainedRecords == s.previousDurable.retainedRecords and s.durable.trimmedRecords == s.previousDurable.trimmedRecords and s.durable.outstandingRecords == s.previousDurable.outstandingRecords and s.durable.delivered == s.previousDurable.delivered and s.durable.redelivered == s.previousDurable.redelivered and s.durable.reclaimed == s.previousDurable.reclaimed and s.durable.occurrences == s.previousDurable.occurrences and s.durable.redeliveryCounts == s.previousDurable.redeliveryCounts and s.durable.knownLineages == s.previousDurable.knownLineages and s.durable.protectedOffsets == s.previousDurable.protectedOffsets and s.durable.trimOffsets == s.previousDurable.trimOffsets and s.durable.readOffsets == s.previousDurable.readOffsets and s.durable.consumerOffsets == s.previousDurable.consumerOffsets and s.durable.recordSequence == s.previousDurable.recordSequence and s.durable.readSequence == s.previousDurable.readSequence and s.durable.activeShards == s.previousDurable.activeShards and s.durable.sealedShards == s.previousDurable.sealedShards and s.durable.completedOperations == s.previousDurable.completedOperations)
  pure def rejectionOK(s) = (s.lastOutcome != "rejected" and s.lastOutcome != "stale_route_read") or (s.durable == s.previousDurable and s.reads == s.previousReads)
  pure def manyRecords(d) = SHARDS.exists(shard => d.activeShards.contains(shard) and d.recordIds.filter(id => d.records.get(id).shard == shard).size() >= 2)
  pure def recordOffsetsAreUnique(d) = d.recordIds.forall(a => d.recordIds.filter(b => b != a and d.records.get(b).shard == d.records.get(a).shard).forall(b => d.records.get(b).offset != d.records.get(a).offset))
  pure def appendOrderIsMonotonic(d) = d.recordIds.forall(a => d.recordIds.filter(b => b != a and d.records.get(b).shard == d.records.get(a).shard and d.recordSequence.get(a) < d.recordSequence.get(b)).forall(b => d.records.get(a).offset < d.records.get(b).offset))
  pure def readOrderIsMonotonic(d) = d.delivered.forall(a => d.delivered.filter(b => b != a and d.records.get(b).shard == d.records.get(a).shard and d.readSequence.get(a) < d.readSequence.get(b)).forall(b => d.records.get(a).offset < d.records.get(b).offset))
  pure def ordering(d) = SHARDS.forall(s => d.consumerOffsets.get(s) >= d.readOffsets.get(s) and d.readOffsets.get(s) >= 0) and recordOffsetsAreUnique(d) and appendOrderIsMonotonic(d) and readOrderIsMonotonic(d)
  pure def multiRecordStateOK(d) = not(manyRecords(d)) or (recordOffsetsAreUnique(d) and appendOrderIsMonotonic(d) and readOrderIsMonotonic(d))
  pure def restartOK(d) = d.restartCount == 0 or (d.persistedScope == d.scope and d.persistedToken == "v1.token-a" and d.persistedFrontier == preSplit and d.persistedLineages == lineagesOf(preSplit) and d.issuedTokens.contains(d.persistedToken) and d.tokens.get(d.persistedToken).issued and d.tokens.get(d.persistedToken).scope == d.persistedScope and d.tokens.get(d.persistedToken).frontier == d.persistedFrontier and d.persistedFrontier.forall(position => d.knownLineages.contains(position.lineage)))

  val scopeIsVersioned = state.durable.scope.version == SCOPE_VERSION and state.durable.scope.id == SCOPE_ID and state.durable.scope.generation == GENERATION
  val positionsAreLogical = pointersOK(state.durable)
  val frontierIsBoundedCanonical = state.durable.issuedTokens.forall(t => frontierOK(state.durable.tokens.get(t).frontier))
  val tokenCatalogIsOpaque = state.durable.issuedTokens.forall(t => opaque(t))
  val issuedTokenMapsDurableFrontier = catalogOK(state.durable)
  val pointerLineageIsAtomic = pointersOK(state.durable)
  val childContinuationIsDeterministic = continuationOK(state.durable)
  val trimPreservesProtectedFrontierInvariant = trimOK(state.durable)
  val rejectedRequestsPreserveFullState = rejectionOK(state)
  val retainedRecordsAreConsistent = retainedOK(state.durable)
  val outstandingFrontierIsProtected = issuedFrontierRetained(state.durable) and state.durable.outstandingRecords.subseteq(state.durable.retainedRecords)
  val redeliveryDoesNotReturnTrimmed = redeliveryOK(state.durable)
  val restartLoadsPersistedCheckpoint = restartOK(state.durable)
  val exactReplayReexecutesIdentity = replayOK(state)
  val operationReplayIsExact = operationOK(state.durable) and operationAtomic(state.durable) and replayOK(state) and restartOK(state.durable)
  val parentCannotAppendAfterSeal = not(state.durable.parentSealed) or not(state.durable.recordIds.contains("parent@3"))
  val multipleRecordsPerShard = multiRecordStateOK(state.durable)
  val recordOffsetsUnique = recordOffsetsAreUnique(state.durable)
  val appendOrderingMonotonic = appendOrderIsMonotonic(state.durable)
  val readOrderingMonotonic = readOrderIsMonotonic(state.durable)
  val perShardOrderingOnly = ordering(state.durable)
  val deliveryAccountingIsExact = deliveriesOK(state.durable)
  val inv = all {
    scopeIsVersioned, positionsAreLogical, frontierIsBoundedCanonical, tokenCatalogIsOpaque,
    issuedTokenMapsDurableFrontier, pointerLineageIsAtomic, childContinuationIsDeterministic,
    trimPreservesProtectedFrontierInvariant, rejectedRequestsPreserveFullState, operationReplayIsExact,
    parentCannotAppendAfterSeal, multipleRecordsPerShard, recordOffsetsUnique, appendOrderingMonotonic,
    readOrderingMonotonic, retainedRecordsAreConsistent, outstandingFrontierIsProtected,
    redeliveryDoesNotReturnTrimmed, restartLoadsPersistedCheckpoint, exactReplayReexecutesIdentity,
    perShardOrderingOnly, deliveryAccountingIsExact,
  }

  pure def ready(d) = d.stage == 12 and d.currentToken == "v1.token-b" and d.issue.phase == "acknowledged"
  pure def unsafeTransition(s, d, name) = transition(s, d, s.reads, emptyRequest, name)

  action unsafePointerLineageTornAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("pointers", state.durable.pointers.set("other@1", { shard: "other", lineage: "lineage-unknown", offset: 1 })), "unsafe_pointer_lineage_torn"),
  }
  action unsafeIssuedTokenWithoutLineageAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("tokens", state.durable.tokens.set("v1.token-b", { scope: state.durable.scope, frontier: state.durable.tokens.get("v1.token-b").frontier.union(Set({ shard: "unknown", lineage: "unknown", offset: 1 })), issued: true })), "unsafe_issued_token_without_lineage"),
  }
  action unsafeDuplicateFrontierAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("tokens", state.durable.tokens.set("v1.token-b", { scope: state.durable.scope, frontier: state.durable.tokens.get("v1.token-b").frontier.union(Set({ shard: "left", lineage: "lineage-left", offset: 1 })), issued: true })), "unsafe_duplicate_frontier"),
  }
  action unsafeTrimPastProtectionAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("trimOffsets", state.durable.trimOffsets.set("left", state.durable.protectedOffsets.get("left") + 1)), "unsafe_trim_past_protection"),
  }
  action unsafeParentAppendAfterSealAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, add(state.durable, "parent@3"), "unsafe_parent_append_after_seal"),
  }
  action unsafeDuplicateChildDeliveryAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("occurrences", state.durable.occurrences.set("left@1", state.durable.occurrences.get("left@1") + 1)), "unsafe_duplicate_child_delivery"),
  }
  action unsafeChildPageGapAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("continuationPage", 2).with("readOffsets", state.durable.readOffsets.set("right", 1)), "unsafe_child_page_gap"),
  }
  action unsafePhysicalRouteIdentityAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("pointers", state.durable.pointers.set("other@1", { shard: "physical-table-9", lineage: "lineage-other", offset: 1 })), "unsafe_physical_route_identity"),
  }
  action unsafeRetryReissuesTokenAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("issue", state.durable.issue.with("phase", "dispatched").with("applied", false).with("acknowledged", false).with("retries", state.durable.issue.retries + 1)), "unsafe_retry_reissues_token"),
  }
  action unsafePointerIdentityAtReady = all {
    ready(state.durable),
    state' = unsafeTransition(state, state.durable.with("records", state.durable.records.set("other@1", { shard: "other", lineage: "lineage-other", offset: 2 })), "unsafe_pointer_identity"),
  }

  action UnsafePointerLineageTorn = any { mutationSetupStep, unsafePointerLineageTornAtReady }
  action UnsafeIssuedTokenWithoutLineage = any { mutationSetupStep, unsafeIssuedTokenWithoutLineageAtReady }
  action UnsafeDuplicateFrontier = any { mutationSetupStep, unsafeDuplicateFrontierAtReady }
  action UnsafeTrimPastProtection = any { mutationSetupStep, unsafeTrimPastProtectionAtReady }
  action UnsafeParentAppendAfterSeal = any { mutationSetupStep, unsafeParentAppendAfterSealAtReady }
  action UnsafeDuplicateChildDelivery = any { mutationSetupStep, unsafeDuplicateChildDeliveryAtReady }
  action UnsafeChildPageGap = any { mutationSetupStep, unsafeChildPageGapAtReady }
  action UnsafePhysicalRouteIdentity = any { mutationSetupStep, unsafePhysicalRouteIdentityAtReady }
  action UnsafeRetryReissuesToken = any { mutationSetupStep, unsafeRetryReissuesTokenAtReady }
  action UnsafePointerIdentity = any { mutationSetupStep, unsafePointerIdentityAtReady }
  action modelUnsafeStep = any {
    UnsafePointerLineageTorn, UnsafeIssuedTokenWithoutLineage, UnsafeDuplicateFrontier, UnsafeTrimPastProtection,
    UnsafeParentAppendAfterSeal, UnsafeDuplicateChildDelivery, UnsafeChildPageGap, UnsafePhysicalRouteIdentity,
    UnsafeRetryReissuesToken, UnsafePointerIdentity,
  }

  val scope_witness = scopeIsVersioned
  val typed_request_witness = state.lastOutcome == "rejected"
  val token_frontier_witness = state.durable.issuedTokens.contains("v1.token-a")
  val issue_reconciliation_witness = state.durable.issue.phase == "unknown" or state.durable.replayCount > 0
  val exact_replay_witness = state.lastOutcome == "replay_reconciled"
  val split_seal_witness = state.durable.parentSealed
  val incremental_continuation_witness = state.durable.continuationPage == 1 or state.durable.restartCount > 0
  val child_continuation_witness = state.durable.continuationPage == 2
  val multiple_record_witness = manyRecords(state.durable)
  val trim_frontier_witness = state.lastOutcome == "trim_at_protected_frontier"
  val redelivery_witness = state.durable.redelivered.contains("left@1")
  val reclaim_witness = state.durable.reclaimed.contains("left@1")
  val stale_route_witness = state.lastOutcome == "stale_route_read"
  val rejection_witness = state.lastOutcome == "rejected"
  val pointer_lineage_hostile_witness = not(pointersOK(state.durable))
  val issued_token_hostile_witness = not(catalogOK(state.durable))
  val duplicate_shard_hostile_witness = not(frontierIsBoundedCanonical)
  val trim_hostile_witness = not(trimPreservesProtectedFrontierInvariant)
  val parent_append_hostile_witness = not(parentCannotAppendAfterSeal)
  val duplicate_child_hostile_witness = not(deliveryAccountingIsExact)
  val child_gap_hostile_witness = not(childContinuationIsDeterministic)
  val physical_identity_hostile_witness = not(positionsAreLogical)
  val retry_identity_hostile_witness = not(operationOK(state.durable))
  val pointer_identity_hostile_witness = not(pointerLineageIsAtomic)

  pure def requestFor(token, scopeId, generation, lineage, route, legacy, hasLegacy) = {
    token: token, scopeVersion: SCOPE_VERSION, scopeId: scopeId, generation: generation,
    lineage: lineage, hasLineage: true, route: route, hasRoute: true,
    legacyCursor: legacy, hasLegacyCursor: hasLegacy,
  }
  pure def invalidTypedRequests(d) = Set(
    emptyRequest,
    requestFor("v1.unknown", d.scope.id, d.scope.generation, "lineage-parent", "logical", "", false),
    requestFor("v1.forged", d.scope.id, d.scope.generation, "lineage-parent", "logical", "", false),
    requestFor("v1.token-a", "wrong-scope", d.scope.generation, "lineage-parent", "logical", "", false),
    requestFor("v1.token-a", d.scope.id, d.scope.generation - 1, "lineage-parent", "logical", "", false),
    requestFor("v1.token-a", d.scope.id, d.scope.generation, "unknown", "logical", "", false),
    requestFor("v1.token-a", d.scope.id, d.scope.generation, "lineage-parent", "physical-stale", "", false),
    requestFor("raw-cursor", d.scope.id, d.scope.generation, "lineage-parent", "logical", "physical-parent/1", true),
  )
  run typedRequestCounterexamples = all {
    assert(state.durable.issuedTokens.size() == 0 or invalidTypedRequests(state.durable).forall(request => not(accepts(state.durable, request)))),
    assert(state.durable.issuedTokens.size() == 0 or invalidTypedRequests(state.durable).forall(request => requestReads(state.durable, Set(), request) == Set())),
  }
  run initialWitnessesAreAbsent = all {
    assert(not(typed_request_witness)), assert(not(token_frontier_witness)), assert(not(issue_reconciliation_witness)),
    assert(not(split_seal_witness)), assert(not(incremental_continuation_witness)), assert(not(child_continuation_witness)),
    assert(not(exact_replay_witness)),
    assert(not(multiple_record_witness)), assert(not(trim_frontier_witness)), assert(not(redelivery_witness)),
    assert(not(reclaim_witness)), assert(not(stale_route_witness)), assert(not(rejection_witness)),
    assert(not(pointer_lineage_hostile_witness)), assert(not(issued_token_hostile_witness)),
    assert(not(duplicate_shard_hostile_witness)), assert(not(trim_hostile_witness)), assert(not(parent_append_hostile_witness)),
    assert(not(duplicate_child_hostile_witness)), assert(not(child_gap_hostile_witness)),
    assert(not(physical_identity_hostile_witness)), assert(not(retry_identity_hostile_witness)),
    assert(not(pointer_identity_hostile_witness)),
  }
  run scopeAndFrontierExamples = all {
    assert(initialScope.version == SCOPE_VERSION),
    assert(preSplit.size() <= MAX_FRONTIER),
    assert(children.size() <= MAX_FRONTIER),
    assert(frontierOK(preSplit)),
    assert(frontierOK(children)),
    assert(tokenFrontier("v1.token-a") == preSplit),
    assert(tokenFrontier("v1.token-b") == children),
  }
}
