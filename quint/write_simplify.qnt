/// One active item, six versions, two independently retiring lineages, one table policy.
/// One clock unit is 24 hours. The ledger is an oracle, not persisted schema.
/// Native ordering, transactions, chunks and process recovery require proof.
module write_simplify {
  type Revision = { version: int, born: int, deleted: bool, previous: int, published: bool, lineage: int }
  type Retirement = { version: int, born: int, retiredAt: int, tableFor: int }
  type State = {
    now: int, tableFor: int, epoch: int, releasedAnchors: Set[int],
    revisions: List[Revision], stored: Set[int], retirements: Set[Retirement],
    tableThrough: int, otherThrough: int, physicalTableThrough: int, physicalOtherThrough: int, cleanupWork: bool, purgedThrough: int,
    pending: bool, pendingEpoch: int, pendingLineage: int, pendingThrough: int, pendingDelete: Set[int],
    systemRemoved: Set[int],
  }
  pure val FOREVER = 99
  pure val MAX_EVENTS = 6
  pure val initial: State = {
    now: 0, tableFor: 1, epoch: 1, releasedAnchors: Set(),
    revisions: List(), stored: Set(), retirements: Set(),
    tableThrough: 0, otherThrough: 0, physicalTableThrough: 0, physicalOtherThrough: 0, cleanupWork: false, purgedThrough: 0,
    pending: false, pendingEpoch: 0, pendingLineage: 0, pendingThrough: 0, pendingDelete: Set(),
    systemRemoved: Set(),
  }
  var state: State
  action init = state' = initial

  pure def maxInt(a: int, b: int): int = if (a > b) a else b
  pure def minInt(a: int, b: int): int = if (a < b) a else b
  pure def versions(s: State): Set[int] = s.revisions.foldl(Set(), (out, e) => out.union(Set(e.version)))
  pure def latest(s: State): int = s.revisions.length()
  pure def current(s: State): int = if (latest(s) == 0) 0
    else if (s.revisions.nth(latest(s) - 1).deleted) 0 else latest(s)

  // Baseline revisions publish no event. A delete control exists only in
  // this oracle ledger, not as an item-history row. Superseded current
  // unpublished or table-retired current anchors are unreferenced by any
  // later event: all existing event revisions precede the current revision.
  pure def materializeBaseline(s: State, deleted: bool): State = {
    val old = current(s)
    val discard = if (old > 0) {
      val row = s.revisions.nth(old - 1)
      if (not(row.published) or retired(s, row)) Set(old) else Set()
    } else Set()
    val revision = { version: latest(s) + 1, born: 0, deleted: deleted, previous: 0, published: false, lineage: 0 }
    val added = if (deleted) Set() else Set(revision.version)
    s.with("revisions", s.revisions.append(revision))
      .with("stored", s.stored.exclude(discard).union(added))
      .with("releasedAnchors", s.releasedAnchors.union(discard))
  }
  pure def physicalVersions(s: State): Set[int] = s.revisions
    .select(r => r.published or not(r.deleted))
    .foldl(Set(), (out, r) => out.union(Set(r.version))).exclude(s.releasedAnchors)
  // A request captures its timestamp before retry. Its eventual revision and
  // selected lineage can follow another request with a newer timestamp.
  pure def commitCaptured(s: State, deleted: bool, lineage: int, capturedAt: int): State = {
    val event = { version: latest(s) + 1, born: capturedAt, deleted: deleted,
      previous: current(s), published: true, lineage: lineage }
    s.with("revisions", s.revisions.append(event))
      .with("stored", s.stored.union(Set(event.version)))
  }
  pure def commitChange(s: State, deleted: bool): State = commitCaptured(s, deleted, 0, s.now)
  pure def through(s: State, lineage: int): int = if (lineage == 0) s.tableThrough else s.otherThrough
  pure def physicalThrough(s: State, lineage: int): int = if (lineage == 0) s.physicalTableThrough else s.physicalOtherThrough
  pure def retired(s: State, e: Revision): bool = e.published and e.version <= through(s, e.lineage)
  pure def withThrough(s: State, lineage: int, value: int): State =
    if (lineage == 0) s.with("tableThrough", value) else s.with("otherThrough", value)
  pure def withPhysical(s: State, lineage: int, value: int): State =
    if (lineage == 0) s.with("physicalTableThrough", value) else s.with("physicalOtherThrough", value)

  // Independent oracle: every unretired event owns its target and exact old
  // image. The implementation below never calls this oracle to plan deletion.
  pure def required(s: State): Set[int] = s.revisions.foldl(Set(), (out, e) => {
    val target = if (e.published and not(retired(s, e))) Set(e.version) else Set()
    val old = if (e.published and not(retired(s, e)) and e.previous > 0) Set(e.previous) else Set()
    out.union(target).union(old)
  }).union(if (current(s) > 0) Set(current(s)) else Set())
  pure def systemRequired(s: State): Set[int] = s.revisions
    .select(e => e.published and e.born >= s.now - 1 and not(s.systemRemoved.contains(e.version)))
    .foldl(Set(), (out, e) => out.union(Set(e.version)))
  // Read at most two metadata rows: one retained boundary anchor and one
  // following row. Delete at most one row, then resume from durable progress.
  // A full page is never evidence that no later unretired event exists.
  pure def metadataPage(s: State): List[Revision] = {
    val rows = s.revisions.select(e => s.stored.contains(e.version))
    val first = rows.foldl(latest(s) + 1, (out, e) => minInt(out, e.version))
    val second = rows.select(e => e.version > first).foldl(latest(s) + 1, (out, e) => minInt(out, e.version))
    rows.select(e => e.version <= second)
  }
  pure def deletionPage(s: State): Set[int] = {
    val page = metadataPage(s)
    val first = page.select(e => e.published and not(retired(s, e)))
      .foldl(latest(s) + 1, (out, e) => minInt(out, e.version))
    val scanned = page.foldl(s.purgedThrough, (out, e) => maxInt(out, e.version))
    val boundary = if (first <= latest(s)) {
      val e = s.revisions.nth(first - 1)
      if (e.previous > 0) e.previous else e.version
    } else if (page.length() < s.stored.size()) scanned
      else if (current(s) > 0) current(s) else latest(s) + 1
    val oldest = s.stored.fold(latest(s) + 1, (out, v) => minInt(out, v))
    s.stored.filter(v => v < boundary and v == oldest)
  }
  pure def remove(s: State, rows: Set[int]): State = s
    .with("stored", s.stored.exclude(rows))
    .with("purgedThrough", rows.fold(s.purgedThrough, (v, row) => maxInt(v, row)))
  pure def expiredThrough(s: State, lineage: int): int = {
    val firstYoung = s.revisions.select(e => e.published and e.lineage == lineage and
      e.version > through(s, lineage) and e.born >= s.now - s.tableFor)
      .foldl(latest(s) + 1, (v, e) => minInt(v, e.version))
    if (s.tableFor == FOREVER) through(s, lineage) else s.revisions
      .select(e => e.published and e.lineage == lineage and e.version < firstYoung)
      .foldl(through(s, lineage), (v, e) => maxInt(v, e.version))
  }
  pure def expiredTablePrefix(s: State): int = expiredThrough(s, 0)
  pure def nextBoundedThrough(s: State, lineage: int, upper: int, lower: int): int = {
    val pending = s.revisions.select(e => e.published and e.lineage == lineage and e.version > lower and e.version <= upper)
    val first = pending.foldl(latest(s) + 1, (v, e) => minInt(v, e.version))
    pending.select(e => e.version <= first + 1).foldl(lower, (v, e) => maxInt(v, e.version))
  }
  pure def prepareOn(s: State, lineage: int): State = {
    val retiredThrough = nextBoundedThrough(s, lineage, expiredThrough(s, lineage), through(s, lineage))
    val prospective = withThrough(s, lineage, retiredThrough)
    s.with("pending", true).with("pendingEpoch", s.epoch).with("pendingLineage", lineage)
      .with("pendingThrough", retiredThrough).with("pendingDelete", deletionPage(prospective))
  }
  pure def prepare(s: State): State = prepareOn(s, 0)
  pure def clearPending(s: State): State = s.with("pending", false).with("pendingEpoch", 0)
    .with("pendingLineage", 0).with("pendingThrough", 0).with("pendingDelete", Set())
  pure def applyPrepared(s: State): State = {
    val lineage = s.pendingLineage
    val retiredThrough = maxInt(through(s, lineage), s.pendingThrough)
    val prospective = withThrough(s, lineage, retiredThrough)
    val newlyRetired = s.revisions.select(e => not(retired(s, e)) and retired(prospective, e))
      .foldl(s.retirements, (out, e) => out.union(Set({ version: e.version, born: e.born,
        retiredAt: s.now, tableFor: s.tableFor })))
    val progressed = remove(prospective.with("retirements", newlyRetired), s.pendingDelete)
    val cursor = if (deletionPage(progressed).size() > 0) physicalThrough(progressed, lineage)
      else nextBoundedThrough(progressed, lineage, retiredThrough, physicalThrough(progressed, lineage))
    val physical = withPhysical(progressed, lineage, cursor)
    physical.with("cleanupWork", physical.physicalTableThrough < physical.tableThrough or physical.physicalOtherThrough < physical.otherThrough)
  }
  pure def commit(s: State): State = clearPending(
    if (s.pending and s.pendingEpoch == s.epoch) applyPrepared(s) else s)
  pure def tableSweepOn(s: State, lineage: int): State = commit(prepareOn(s, lineage))
  pure def tableSweep(s: State): State = tableSweepOn(s, 0)
  pure def systemSweep(s: State): State = s.with("systemRemoved",
    s.systemRemoved.union(s.revisions.select(e => e.published and e.born < s.now - 1)
      .foldl(Set(), (out, e) => out.union(Set(e.version)))))
  pure def setPolicy(s: State, tableFor: int): State = s.with("tableFor", tableFor).with("epoch", s.epoch + 1)
  pure def discoverTableWork(s: State): bool = s.cleanupWork or expiredThrough(s, 0) > s.tableThrough or expiredThrough(s, 1) > s.otherThrough
  pure def drain(s: State): State = 0.to(40).fold(s, (out, _) => if (discoverTableWork(out)) tableSweepOn(tableSweepOn(out, 0), 1) else out)

  pure def safe(s: State): bool = all {
    required(s).subseteq(s.stored), systemRequired(s).subseteq(s.stored),
    s.stored.subseteq(physicalVersions(s)), s.tableFor >= 1,
    s.releasedAnchors.intersect(required(s)).size() == 0,
    s.releasedAnchors.forall(v => s.revisions.select(r => r.published and r.previous == v).length() == 0),
    s.physicalTableThrough <= s.tableThrough, s.tableThrough <= latest(s),
    s.physicalOtherThrough <= s.otherThrough, s.otherThrough <= latest(s),
    s.cleanupWork == (s.physicalTableThrough < s.tableThrough or s.physicalOtherThrough < s.otherThrough),
    deletionPage(s).size() == 0 or s.cleanupWork,
    s.purgedThrough <= latest(s),
    s.retirements.forall(r => r.tableFor != FOREVER and r.born < r.retiredAt - r.tableFor),
    s.retirements.map(r => r.version) == s.revisions.select(e => retired(s, e))
      .foldl(Set(), (out, e) => out.union(Set(e.version))),
    s.revisions.indices().forall(i => {
      val e = s.revisions.nth(i)
      e.version == i + 1 and (e.previous == 0 or e.previous < e.version) and Set(0, 1).contains(e.lineage) and e.born <= s.now
        and (if (e.published) {
          e.previous == (if (i == 0) 0 else if (s.revisions.nth(i - 1).deleted) 0 else s.revisions.nth(i - 1).version)
            and (not(e.deleted) or e.previous > 0)
        } else e.previous == 0)
        and (not(e.deleted) or e.published or not(s.stored.contains(e.version)))
    }),
    s.stored.forall(v => physicalVersions(s).filter(w => w > v).subseteq(s.stored)),
    s.releasedAnchors.forall(v => {
      val row = s.revisions.nth(v - 1)
      not(row.deleted) and (not(row.published) or retired(s, row))
    }),
    s.systemRemoved.forall(v => s.revisions.nth(v - 1).published and s.revisions.nth(v - 1).born < s.now - 1),
  }
  val inv = safe(state)
  val final_delete_witness = state.revisions.select(r => r.published and r.deleted).length() > 0 and current(state) == 0 and state.stored.size() == 0
  val predecessor_witness = state.stored.exists(v => retired(state, state.revisions.nth(v - 1)) and
    state.revisions.select(e => e.published and not(retired(state, e)) and e.previous == v).length() > 0)
  val custom_table_retention_witness = state.tableFor > 1 and state.revisions.select(r => r.published).length() > 0
  val recreation_witness = state.revisions.select(e => e.published and e.version > 1 and not(e.deleted) and e.previous == 0).length() > 0
  val stale_work_witness = state.pending and state.pendingEpoch != state.epoch
  val baseline_anchor_witness = state.revisions.select(e => not(e.published) and not(e.deleted)).length() > 0
  val baseline_replacement_witness = state.releasedAnchors.size() > 0 and (if (current(state) > 0) not(state.revisions.nth(current(state) - 1).published) else false)
  val baseline_delete_witness = state.revisions.select(r => not(r.published) and r.deleted).length() > 0 and state.stored.size() == 0 and state.releasedAnchors.size() > 0
  val bounded_cleanup_witness = state.cleanupWork and state.stored.size() > 0

  val nonprefix_retirement_witness = state.revisions.select(newer => retired(state, newer) and
    state.revisions.select(older => older.published and older.version < newer.version and not(retired(state, older))).length() > 0).length() > 0
  val captured_time_inversion_witness = state.revisions.indices().exists(i => i > 0 and
    state.revisions.nth(i).published and state.revisions.nth(i - 1).published and
    state.revisions.nth(i).born < state.revisions.nth(i - 1).born)
  val partial_metadata_scan_witness = state.cleanupWork and metadataPage(state).length() == 2 and
    metadataPage(state).select(e => e.published and not(retired(state, e))).length() == 0 and state.stored.size() > 2

  action Write = {
    nondet deleted = Set(false, true).oneOf()
    nondet lineage = Set(0, 1).oneOf()
    nondet capturedAt = 0.to(state.now).oneOf()
    all { latest(state) < MAX_EVENTS,
      not(deleted) or current(state) > 0,
      state' = commitCaptured(state, deleted, lineage, capturedAt) }
  }
  action MaterializeBaseline = {
    nondet deleted = Set(false, true).oneOf()
    all { latest(state) < MAX_EVENTS, state' = materializeBaseline(state, deleted) }
  }
  action Tick = all { state.now < 12, state' = state.with("now", state.now + 1) }
  action ChangePolicy = {
    nondet tableFor = Set(1, 2, 3, FOREVER).oneOf()
    state' = setPolicy(state, tableFor)
  }
  action TableTrim = {
    nondet lineage = Set(0, 1).oneOf()
    all { discoverTableWork(state), state' = tableSweepOn(state, lineage) }
  }
  action SystemTrim = state' = systemSweep(state)
  action Prepare = {
    nondet lineage = Set(0, 1).oneOf()
    all { not(state.pending), discoverTableWork(state), state' = prepareOn(state, lineage) }
  }
  action Commit = all { state.pending, state' = commit(state) }
  action Abort = all { state.pending, state' = clearPending(state) }
  action Crash = state' = clearPending(state)
  action step = any { Write, MaterializeBaseline, Tick, ChangePolicy, TableTrim, SystemTrim, Prepare, Commit, Abort, Crash }

  run baselineImportImmediatelyCreatesOnlyAnUnpublishedAnchorTest = {
    val imported = materializeBaseline(initial, false)
    all { assert(imported.stored == Set(1)), assert(current(imported) == 1),
      assert(systemRequired(imported).size() == 0), assert(not(imported.cleanupWork)), assert(safe(imported)) }
  }
  run repeatedBaselinesKeepOnlyTheirCurrentUnpublishedAnchorTest = {
    val replaced = materializeBaseline(materializeBaseline(initial, false), false)
    all { assert(replaced.stored == Set(2)), assert(replaced.releasedAnchors == Set(1)),
      assert(systemRequired(replaced).size() == 0), assert(not(replaced.cleanupWork)), assert(safe(replaced)) }
  }
  run quietBaselineDeletionNeedsNoFutureTableEventTest = {
    val deleted = materializeBaseline(materializeBaseline(initial, false), true)
    all { assert(deleted.stored.size() == 0), assert(current(deleted) == 0),
      assert(systemRequired(deleted).size() == 0), assert(not(deleted.cleanupWork)), assert(safe(deleted)) }
  }
  run baselineReplacementPreservesPublishedEventPredecessorsTest = {
    val updated = commitChange(materializeBaseline(initial, false), false)
    val replaced = materializeBaseline(materializeBaseline(updated, false), false)
    val deleted = materializeBaseline(replaced, true)
    all { assert(replaced.stored == Set(1, 2, 4)), assert(required(replaced) == Set(1, 2, 4)),
      assert(deleted.stored == Set(1, 2)), assert(safe(replaced)), assert(safe(deleted)),
      assert(drain(deleted.with("now", 5)).stored.size() == 0) }
  }
  run baselineAfterTrimReleasesFormerLiveAnchorTest = {
    val held = tableSweep(commitChange(initial, false).with("now", 3))
    val replaced = materializeBaseline(held, false)
    all { assert(held.stored == Set(1)), assert(not(held.cleanupWork)),
      assert(replaced.stored == Set(2)), assert(not(replaced.cleanupWork)), assert(safe(replaced)) }
  }
  run baselineDeletionAfterTrimReleasesFormerLiveAnchorTest = {
    val held = tableSweep(commitChange(initial, false).with("now", 3))
    val deleted = materializeBaseline(held, true)
    all { assert(deleted.stored.size() == 0), assert(not(deleted.cleanupWork)), assert(safe(deleted)) }
  }
  run baselineCannotReleaseUnretiredPublishedCurrentTest = {
    val replaced = materializeBaseline(commitChange(initial, false), false)
    val broken = replaced.with("stored", Set(2)).with("releasedAnchors", Set(1))
    all { assert(replaced.stored == Set(1, 2)), assert(safe(replaced)), assert(not(safe(broken))) }
  }
  run baselineFirstUpdatePublishesOnlyTheNewSnapshotTest = {
    val updated = commitChange(materializeBaseline(initial, false), false)
    val cleaned = drain(updated.with("now", 5))
    all { assert(not(updated.revisions.nth(0).published)), assert(updated.revisions.nth(1).previous == 1),
      assert(systemRequired(updated) == Set(2)), assert(cleaned.stored == Set(2)), assert(safe(cleaned)) }
  }
  run baselineDeleteKeepsOldImageUntilTableRetirementTest = {
    val deleted = commitChange(materializeBaseline(initial, false), true)
    val cleaned = drain(deleted.with("now", 5))
    all { assert(required(deleted) == Set(1, 2)), assert(cleaned.stored.size() == 0), assert(safe(cleaned)) }
  }
  run predecessorSurvivesTableTrimTest = {
    val updated = commitChange(commitChange(initial, false).with("now", 1), false).with("now", 2)
    val cleaned = tableSweep(updated)
    all { assert(cleaned.tableThrough == 1), assert(cleaned.stored == Set(1, 2)), assert(safe(cleaned)) }
  }
  run customTableDurationKeepsItemHistoryTest = {
    val deleted = commitChange(commitChange(setPolicy(initial, 3), false), true).with("now", 2)
    all { assert(tableSweep(deleted).stored == Set(1, 2)), assert(tableSweep(deleted).tableThrough == 0), assert(safe(tableSweep(deleted))) }
  }
  run finalDeleteEventuallyLeavesEmptyHistoryTest = {
    val deleted = commitChange(commitChange(commitChange(initial, false), false), true).with("now", 5)
    val cleaned = drain(deleted)
    all { assert(cleaned.stored.size() == 0), assert(not(cleaned.cleanupWork)), assert(safe(cleaned)) }
  }
  run liveItemKeepsOneAnchorTest = {
    val cleaned = drain(commitChange(commitChange(initial, false), false).with("now", 5))
    all { assert(cleaned.stored == Set(2)), assert(not(cleaned.cleanupWork)), assert(safe(cleaned)) }
  }
  run recreationHasNoOldImageTest = {
    val recreated = commitChange(commitChange(commitChange(initial, false), true).with("now", 1), false)
    val cleaned = drain(recreated.with("now", 2))
    all { assert(recreated.revisions.nth(2).previous == 0), assert(cleaned.stored == Set(3)), assert(safe(cleaned)) }
  }
  run policyExtensionRejectsPreparedRetirementTest = {
    val queued = prepare(commitChange(commitChange(initial, false), true).with("now", 3))
    val changed = setPolicy(queued, FOREVER)
    val settled = commit(changed)
    all { assert(queued.pendingThrough == 2), assert(settled.tableThrough == 0), assert(settled.stored == Set(1, 2)), assert(safe(settled)) }
  }
  run retiredHistoryIsNotRevivedByExtensionTest = {
    val retired = tableSweep(commitChange(commitChange(initial, false), true).with("now", 3))
    val extended = setPolicy(retired, FOREVER)
    val cleaned = drain(extended)
    all { assert(extended.tableThrough == 2), assert(cleaned.stored.size() == 0), assert(safe(cleaned)) }
  }
  run appendSurvivesPreviouslyPreparedCleanupTest = {
    val queued = prepare(commitChange(commitChange(initial, false), false).with("now", 3))
    val settled = commit(commitChange(queued, false))
    all { assert(settled.stored == Set(2, 3)), assert(safe(settled)) }
  }
  run tableForeverKeepsDeletedHistoryTest = {
    val deleted = commitChange(commitChange(setPolicy(initial, FOREVER), false), true).with("now", 12)
    all { assert(tableSweep(deleted).stored == Set(1, 2)), assert(safe(tableSweep(deleted))) }
  }
  run systemTrimNeverDeletesHistoryTest = {
    val deleted = commitChange(commitChange(initial, false), true).with("now", 3)
    all { assert(systemSweep(deleted).stored == deleted.stored), assert(safe(systemSweep(deleted))) }
  }
  run boundedFinalDeleteSurvivesCrashTest = {
    val deleted = commitChange(commitChange(commitChange(initial, false), false), true).with("now", 5)
    val partial = tableSweep(tableSweep(deleted))
    val restarted = clearPending(prepare(partial))
    val cleaned = drain(restarted)
    all { assert(partial.cleanupWork), assert(partial.stored == Set(3)),
      assert(restarted.cleanupWork), assert(cleaned.stored.size() == 0), assert(safe(cleaned)) }
  }
  run tableForeverDoesNotExtendSystemVisibilityTest = {
    val old = commitChange(setPolicy(initial, FOREVER), false).with("now", 12)
    val swept = systemSweep(tableSweep(old))
    all { assert(swept.tableThrough == 0), assert(swept.stored == Set(1)), assert(systemRequired(swept).size() == 0), assert(safe(swept)) }
  }
  run exactCutoffRemainsRetainedTest = {
    val atCutoff = commitChange(initial, false).with("now", 1)
    val older = atCutoff.with("now", 2)
    all { assert(tableSweep(atCutoff).tableThrough == 0), assert(tableSweep(older).tableThrough == 1), assert(safe(tableSweep(older))) }
  }
  run abortPublishesNoRetirementOrDeletionTest = {
    val before = commitChange(commitChange(initial, false), true).with("now", 3)
    all { assert(clearPending(prepare(before)) == before), assert(safe(before)) }
  }
  run deletingRequiredPredecessorIsUnsafeTest = {
    val updated = tableSweep(commitChange(commitChange(initial, false).with("now", 1), false).with("now", 2))
    all { assert(safe(updated)), assert(not(safe(updated.with("stored", Set(2))))) }
  }
  run staleJobWithoutTablePolicyFenceIsUnsafeTest = {
    val queued = prepare(commitChange(commitChange(initial, false), true).with("now", 3))
    val extended = setPolicy(queued, FOREVER)
    val broken = clearPending(applyPrepared(extended))
    all { assert(safe(extended)), assert(not(safe(broken))) }
  }
  run nearestPhysicalRowCanInventOldImageTest = {
    val recreated = commitChange(commitChange(commitChange(initial, false), true), false)
    val nearest = Set(1, 3).filter(v => v < 3).fold(0, (v, old) => maxInt(v, old))
    all { assert(recreated.revisions.nth(2).previous == 0), assert(nearest == 1), assert(safe(recreated)) }
  }
  run losingBoundedWorkIsUnsafeTest = {
    val partial = tableSweep(tableSweep(commitChange(commitChange(commitChange(initial, false), false), true).with("now", 5)))
    all { assert(partial.cleanupWork), assert(safe(partial)), assert(not(safe(partial.with("cleanupWork", false)))) }
  }
  run newerRetiredLineageDoesNotAuthorizeOlderDeletionTest = {
    val first = commitCaptured(initial, false, 0, 0)
    val deleted = commitCaptured(first, true, 1, 0).with("now", 3)
    val retiredNewer = tableSweepOn(deleted, 1)
    all { assert(retiredNewer.tableThrough == 0), assert(retiredNewer.otherThrough == 2),
      assert(retiredNewer.stored == Set(1, 2)), assert(safe(retiredNewer)),
      assert(not(safe(retiredNewer.with("stored", Set())))) }
  }
  run laggingLineageEventuallyMakesTerminalDeleteReclaimableTest = {
    val split = commitCaptured(commitCaptured(initial, false, 0, 0), true, 1, 0).with("now", 3)
    val cleaned = drain(tableSweepOn(split, 1))
    all { assert(cleaned.stored.size() == 0), assert(not(cleaned.cleanupWork)), assert(safe(cleaned)) }
  }
  run capturedTimestampCanInvertRevisionOrderTest = {
    val first = commitCaptured(initial.with("now", 1), false, 0, 1)
    val laterRevision = commitCaptured(first.with("now", 2), true, 1, 0)
    val retiredNewer = tableSweepOn(laterRevision, 1)
    all { assert(laterRevision.revisions.nth(1).born < laterRevision.revisions.nth(0).born),
      assert(retiredNewer.otherThrough == 2), assert(retiredNewer.tableThrough == 0),
      assert(retiredNewer.stored == Set(1, 2)), assert(safe(retiredNewer)) }
  }
  run fullMetadataPageKeepsPredecessorForUnseenEventTest = {
    val five = 0.to(4).fold(initial, (out, _) => commitChange(out, false))
    val six = commitChange(five.with("now", 1), false).with("now", 2)
    val partial = tableSweep(tableSweep(tableSweep(six)))
    val queued = prepare(partial)
    all { assert(partial.stored == Set(4, 5, 6)), assert(metadataPage(partial).length() == 2),
      assert(queued.pendingDelete == Set(4)), assert(safe(commit(queued))),
      assert(not(safe(partial.with("stored", Set(4, 6))))), assert(safe(partial)) }
  }

}
