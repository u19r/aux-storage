/// Abstract lifecycle: one reusable name, three immutable incarnations, two
/// table scopes, nine data-family groups and two item slots. Old cleanup may
/// overlap fresh-ID creation. Native atomic fences/inventory require proof.
module table_deletion {
  type Row = { table: int, incarnation: int, origin: int, family: int, slot: int }
  type Schedule = { table: int, incarnation: int, generation: int, due: int }
  type State = {
    name: int, lastIncarnation: int, epoch: int, now: int,
    deleting: Set[int], retired: Set[int], work: Set[int],
    phases: int -> int, deletionGenerations: int -> int,
    rows: Set[Row], schedules: Set[Schedule], markers: Set[Schedule],
    writerIncarnation: int, writerEpoch: int, writerSlot: int,
    cleanupIncarnation: int, cleanupGeneration: int, cleanupPhase: int, cleanupRows: Set[Row],
    cursorIncarnation: int, cursorRejected: bool,
    writerRejected: bool, cleanupRejected: bool, restarted: bool,
  }
  pure val FAMILIES = 1.to(9)
  pure val foreignRows = Set(
    { table: 2, incarnation: 1, origin: 1, family: 1, slot: 1 },
    { table: 2, incarnation: 1, origin: 1, family: 7, slot: 1 })
  pure val foreignSchedules = Set({ table: 2, incarnation: 1, generation: 1, due: 5 })
  pure val initial: State = {
    name: 0, lastIncarnation: 0, epoch: 0, now: 0,
    deleting: Set(), retired: Set(), work: Set(),
    phases: Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0), deletionGenerations: Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0),
    rows: foreignRows, schedules: foreignSchedules, markers: foreignSchedules,
    writerIncarnation: 0, writerEpoch: 0, writerSlot: 0,
    cleanupIncarnation: 0, cleanupGeneration: 0, cleanupPhase: 0, cleanupRows: Set(),
    cursorIncarnation: 0, cursorRejected: false,
    writerRejected: false, cleanupRejected: false, restarted: false,
  }
  var state: State
  action init = state' = initial

  pure def owned(s: State, incarnation: int): Set[Row] =
    s.rows.filter(r => r.table == 1 and r.incarnation == incarnation)
  pure def visible(s: State): Set[Row] = if (s.name > 0) owned(s, s.name).filter(r => r.family != 7) else Set()
  pure def resolveCursor(s: State, incarnation: int): Set[Row] =
    if (s.name > 0 and incarnation == s.name) visible(s) else Set()
  pure def payload(incarnation: int, origin: int, slot: int): Set[Row] =
    FAMILIES.filter(f => f != 7).map(f => { table: 1, incarnation: incarnation, origin: origin, family: f, slot: slot })

  // Family 7 contains exactly one table retention state, not per-item state.
  // Name lookup is the only mutable alias. Creation never selects an old ID,
  // even when that old ID remains physically present and DELETING.
  pure def create(s: State): State = if (s.name != 0) s else {
    val id = s.lastIncarnation + 1
    val policy = { table: 1, incarnation: id, generation: 1, due: 0 }
    s.with("name", id).with("lastIncarnation", id).with("epoch", s.epoch + 1)
      .with("rows", s.rows.union(Set({ table: 1, incarnation: id, origin: id, family: 7, slot: 1 })))
      .with("schedules", s.schedules.union(Set(policy)))
  }
  pure def beginDeleteFor(s: State, incarnation: int): State =
    if (s.name != incarnation or incarnation == 0) s else s
      .with("name", 0).with("epoch", s.epoch + 1)
      .with("deleting", s.deleting.union(Set(incarnation)))
      .with("work", s.work.union(Set(incarnation)))
      .with("phases", s.phases.set(incarnation, 1))
      .with("deletionGenerations", s.deletionGenerations.set(incarnation, s.epoch + 1))
  pure def beginDelete(s: State): State = beginDeleteFor(s, s.name)

  pure def prepareWriter(s: State, slot: int): State =
    if (s.name == 0) s else s.with("writerIncarnation", s.name)
      .with("writerEpoch", s.epoch).with("writerSlot", slot)
  pure def clearWriter(s: State): State = s.with("writerIncarnation", 0)
    .with("writerEpoch", 0).with("writerSlot", 0)
  pure def commitWriter(s: State): State = {
    val allowed = s.name > 0 and s.writerIncarnation == s.name and s.writerEpoch == s.epoch
    val changed = if (allowed) s.with("rows", s.rows.union(
      payload(s.writerIncarnation, s.writerIncarnation, s.writerSlot)))
      else s.with("writerRejected", s.writerRejected or s.writerIncarnation > 0)
    clearWriter(changed)
  }
  pure def reschedule(s: State, due: int): State = {
    val existing = s.schedules.filter(p => p.table == 1 and p.incarnation == s.name)
    val generation = existing.fold(0, (g, p) => if (p.generation > g) p.generation else g) + 1
    val policy = { table: 1, incarnation: s.name, generation: generation, due: due }
    if (s.name == 0 or existing.size() == 0) s else s
      .with("schedules", s.schedules.exclude(existing).union(Set(policy)))
      .with("markers", s.markers.exclude(existing).union(if (due > 0) Set(policy) else Set()))
  }

  pure def clearCleanup(s: State): State = s.with("cleanupIncarnation", 0)
    .with("cleanupGeneration", 0).with("cleanupPhase", 0).with("cleanupRows", Set())
  pure def prepareCleanup(s: State, incarnation: int): State =
    if (not(s.deleting.contains(incarnation)) or s.phases.get(incarnation) > 9) s else {
      val phase = s.phases.get(incarnation)
      val candidates = owned(s, incarnation).filter(r => r.family == phase)
      val first = candidates.fold(3, (slot, r) => if (r.slot < slot) r.slot else slot)
      s.with("cleanupIncarnation", incarnation)
        .with("cleanupGeneration", s.deletionGenerations.get(incarnation))
        .with("cleanupPhase", phase).with("cleanupRows", candidates.filter(r => r.slot == first))
    }
  pure def commitCleanup(s: State): State = {
    val incarnation = s.cleanupIncarnation
    val allowed = s.deleting.contains(incarnation) and
      s.cleanupGeneration == s.deletionGenerations.get(incarnation) and
      s.cleanupPhase == s.phases.get(incarnation)
    val candidates = owned(s, incarnation).filter(r => r.family == s.cleanupPhase)
    val page = s.cleanupRows
    val remaining = candidates.exclude(page)
    val policies = s.schedules.filter(p => page.exists(r => r.family == 7 and
      r.table == p.table and r.incarnation == p.incarnation))
    val changed = if (allowed) s.with("rows", s.rows.exclude(page))
      .with("schedules", s.schedules.exclude(policies)).with("markers", s.markers.exclude(policies))
      .with("phases", s.phases.set(incarnation,
        if (remaining.size() == 0) s.cleanupPhase + 1 else s.cleanupPhase))
      else s.with("cleanupRejected", s.cleanupRejected or incarnation > 0)
    clearCleanup(changed)
  }
  // A second worker may progress while a fixed older page remains in flight.
  pure def cleanupPage(s: State, incarnation: int): State =
    if (not(s.deleting.contains(incarnation)) or s.phases.get(incarnation) > 9) s else {
      val progressed = commitCleanup(prepareCleanup(clearCleanup(s), incarnation))
      progressed.with("cleanupIncarnation", s.cleanupIncarnation)
        .with("cleanupGeneration", s.cleanupGeneration).with("cleanupPhase", s.cleanupPhase)
        .with("cleanupRows", s.cleanupRows)
    }
  pure def finishDelete(s: State, incarnation: int): State =
    if (not(s.deleting.contains(incarnation)) or s.phases.get(incarnation) <= 9 or
      owned(s, incarnation).size() > 0 or
      s.markers.exists(p => p.table == 1 and p.incarnation == incarnation)) s else s
        .with("retired", s.retired.union(Set(incarnation)))
        .with("deleting", s.deleting.exclude(Set(incarnation)))
        .with("work", s.work.exclude(Set(incarnation)))
        .with("phases", s.phases.set(incarnation, 0))
        .with("deletionGenerations", s.deletionGenerations.set(incarnation, 0))
  pure def restart(s: State): State = clearCleanup(clearWriter(s))
    .with("restarted", s.restarted or s.deleting.size() > 0)
  pure def drain(s: State, incarnation: int): State = finishDelete(
    0.to(20).fold(s, (progressed, _) => cleanupPage(progressed, incarnation)), incarnation)

  pure def safe(s: State): bool = all {
    s.name >= 0 and s.name <= s.lastIncarnation,
    not(s.deleting.contains(s.name)), not(s.retired.contains(s.name)),
    s.work == s.deleting,
    s.deleting.intersect(s.retired).size() == 0,
    s.deleting.union(s.retired).union(if (s.name > 0) Set(s.name) else Set()) == 1.to(s.lastIncarnation),
    1.to(3).forall(i => if (s.deleting.contains(i))
      s.phases.get(i) >= 1 and s.phases.get(i) <= 10 and s.deletionGenerations.get(i) > 0
      else s.phases.get(i) == 0 and s.deletionGenerations.get(i) == 0),
    s.retired.forall(i => s.rows.filter(r => r.table == 1 and
      (r.incarnation == i or r.origin == i)).size() == 0),
    s.rows.forall(r => r.incarnation == r.origin),
    s.rows.filter(r => r.table == 2) == foreignRows,
    s.schedules.filter(p => p.table == 2) == foreignSchedules,
    s.markers == s.schedules.filter(p => p.due > 0),
    s.schedules.forall(p => s.rows.exists(r => r.family == 7 and
      r.table == p.table and r.incarnation == p.incarnation)),
    s.rows.filter(r => r.family == 7).forall(r => s.schedules.filter(p =>
      r.table == p.table and r.incarnation == p.incarnation).size() == 1),
    visible(s).forall(r => r.incarnation == s.name and r.origin == s.name),
    1.to(3).forall(i => i == s.name or resolveCursor(s, i).size() == 0),
    s.deleting.forall(i => owned(s, i).forall(r => r.family >= s.phases.get(i))),
  }
  val inv = safe(state)
  val deleting_with_data_witness = state.deleting.exists(i => owned(state, i).size() > 0)
  val completed_delete_witness = state.retired.size() > 0
  val recreated_empty_witness = state.name > 1 and visible(state).size() == 0
  val concurrent_cleanup_witness = state.name > 0 and state.deleting.size() > 0
  val stale_writer_witness = state.writerRejected
  val stale_cleanup_witness = state.cleanupRejected
  val stale_cursor_witness = state.cursorRejected
  val restart_witness = state.restarted

  action Create = all { state.name == 0, state.lastIncarnation < 3, state' = create(state) }
  action Delete = all { state.name > 0, state' = beginDelete(state) }
  action PrepareWriter = {
    nondet slot = Set(1, 2).oneOf()
    all { state.name > 0, state.writerIncarnation == 0, state' = prepareWriter(state, slot) }
  }
  action CommitWriter = all { state.writerIncarnation > 0, state' = commitWriter(state) }
  action Reschedule = {
    nondet due = Set(0, 1, 5).oneOf()
    all { state.name > 0, state' = reschedule(state, due) }
  }
  action Cleanup = {
    nondet incarnation = 1.to(3).oneOf()
    all { state.deleting.contains(incarnation), state.phases.get(incarnation) <= 9,
      state' = cleanupPage(state, incarnation) }
  }
  action PrepareCleanup = {
    nondet incarnation = 1.to(3).oneOf()
    all { state.deleting.contains(incarnation), state.phases.get(incarnation) <= 9,
      state.cleanupIncarnation == 0, state' = prepareCleanup(state, incarnation) }
  }
  action CommitCleanup = all { state.cleanupIncarnation > 0, state' = commitCleanup(state) }
  action CaptureCursor = all {
    state.name > 0, state.cursorIncarnation == 0, state' = state.with("cursorIncarnation", state.name),
  }
  action ReadCursor = all {
    state.cursorIncarnation > 0,
    state' = state.with("cursorRejected", state.cursorRejected or state.name == 0 or state.cursorIncarnation != state.name),
  }
  action Finish = {
    nondet incarnation = 1.to(3).oneOf()
    all { state.deleting.contains(incarnation), state.phases.get(incarnation) > 9,
      state' = finishDelete(state, incarnation) }
  }
  action Tick = all { state.now < 20, state' = state.with("now", state.now + 1) }
  action Crash = state' = restart(state)
  action step = any { Create, Delete, PrepareWriter, CommitWriter, Reschedule, Cleanup, PrepareCleanup, CommitCleanup, CaptureCursor, ReadCursor, Finish, Tick, Crash }

  run recreationDoesNotWaitForOldCleanupTest = {
    val loaded = commitWriter(prepareWriter(create(initial), 1))
    val deleting = beginDelete(loaded)
    val recreated = create(deleting)
    all {
      assert(recreated.name == 2), assert(recreated.deleting.contains(1)),
      assert(owned(recreated, 1) == owned(loaded, 1)), assert(visible(recreated).size() == 0), assert(safe(recreated)),
    }
  }
  run allFamiliesMustDrainBeforeRetiringOldIdTest = {
    val loaded = commitWriter(prepareWriter(commitWriter(prepareWriter(create(initial), 1)), 2))
    val partial = cleanupPage(beginDelete(loaded), 1)
    val recreated = create(partial)
    val finished = drain(recreated, 1)
    all {
      assert(finishDelete(partial, 1) == partial), assert(partial.deleting.contains(1)),
      assert(finished.name == 2), assert(finished.retired.contains(1)), assert(owned(finished, 1).size() == 0), assert(safe(finished)),
    }
  }
  run restartResumesDurableDeletionTest = {
    val partial = create(cleanupPage(beginDelete(commitWriter(prepareWriter(create(initial), 1))), 1))
    val resumed = restart(partial)
    all {
      assert(resumed.work.contains(1)), assert(resumed.phases == partial.phases), assert(resumed.rows == partial.rows),
      assert(drain(resumed, 1).retired.contains(1)), assert(drain(resumed, 1).name == 2), assert(safe(drain(resumed, 1))),
    }
  }
  run recreationStartsEmptyWithFreshIdentityTest = {
    val loaded = commitWriter(prepareWriter(create(initial), 1))
    val recreated = create(beginDelete(loaded))
    all { assert(recreated.name == 2), assert(visible(recreated).size() == 0), assert(safe(recreated)) }
  }
  run inFlightWriterCannotCommitDuringDeletionTest = {
    val deleting = beginDelete(prepareWriter(create(initial), 1))
    val rejected = commitWriter(deleting)
    all { assert(rejected.rows == deleting.rows), assert(rejected.writerRejected), assert(safe(rejected)) }
  }
  run oldWriterCannotRebindAfterRecreationTest = {
    val recreated = create(beginDelete(prepareWriter(create(initial), 1)))
    val rejected = commitWriter(recreated)
    all { assert(rejected.rows == recreated.rows), assert(rejected.writerRejected), assert(safe(rejected)) }
  }
  run staleCleanupCannotDeleteNewIncarnationTest = {
    val deleting = prepareCleanup(beginDelete(commitWriter(prepareWriter(create(initial), 1))), 1)
    val recreated = commitWriter(prepareWriter(create(drain(deleting, 1)), 1))
    val rejected = commitCleanup(recreated)
    all { assert(rejected.rows == recreated.rows), assert(rejected.cleanupRejected), assert(safe(rejected)) }
  }
  run oldCleanupPreservesNewWritesAndNameTest = {
    val old = beginDelete(commitWriter(prepareWriter(create(initial), 1)))
    val recreated = commitWriter(prepareWriter(create(old), 1))
    val finished = drain(recreated, 1)
    all {
      assert(finished.name == recreated.name), assert(finished.epoch == recreated.epoch),
      assert(visible(finished) == visible(recreated)), assert(safe(finished)),
    }
  }
  run oldCleanupDoesNotInvalidateNewPreparedWriterTest = {
    val prepared = prepareWriter(create(beginDelete(create(initial))), 1)
    val settled = commitWriter(drain(prepared, 1))
    all { assert(visible(settled).size() > 0), assert(not(settled.writerRejected)), assert(safe(settled)) }
  }
  run deletionCanFinishOutOfOrderTest = {
    val first = beginDelete(commitWriter(prepareWriter(create(initial), 1)))
    val second = beginDelete(commitWriter(prepareWriter(create(first), 1)))
    val third = commitWriter(prepareWriter(create(second), 1))
    val finishedSecond = drain(third, 2)
    val finishedBoth = drain(finishedSecond, 1)
    all {
      assert(finishedSecond.deleting.contains(1)), assert(finishedSecond.retired.contains(2)),
      assert(finishedBoth.retired == Set(1, 2)), assert(visible(finishedBoth) == visible(third)), assert(safe(finishedBoth)),
    }
  }
  run staleSamePhasePageCannotSkipRemainingRowsTest = {
    val loaded = commitWriter(prepareWriter(commitWriter(prepareWriter(create(initial), 1)), 2))
    val prepared = prepareCleanup(beginDelete(loaded), 1)
    val concurrent = cleanupPage(prepared, 1)
    val settled = commitCleanup(concurrent)
    all {
      assert(concurrent.phases.get(1) == 1), assert(settled.phases.get(1) == 1),
      assert(owned(settled, 1).exists(r => r.family == 1 and r.slot == 2)), assert(safe(settled)),
    }
  }
  run staleCursorCannotResolveReplacementTableTest = {
    val loaded = commitWriter(prepareWriter(create(initial), 1))
    val recreated = commitWriter(prepareWriter(create(beginDelete(loaded)), 2))
    all { assert(resolveCursor(recreated, 1).size() == 0), assert(visible(recreated).size() > 0), assert(safe(recreated)) }
  }
  run tableDeletionOverridesForeverHistoryTest = {
    val loaded = commitWriter(prepareWriter(create(initial), 1))
    val finished = drain(beginDelete(loaded), 1)
    all { assert(loaded.markers == foreignSchedules), assert(owned(finished, 1).size() == 0), assert(safe(finished)) }
  }
  run repeatedOldIdDeleteCannotDeleteReplacementTest = {
    val partial = cleanupPage(beginDelete(commitWriter(prepareWriter(create(initial), 1))), 1)
    val recreated = create(partial)
    all { assert(beginDeleteFor(recreated, 1) == recreated), assert(safe(recreated)) }
  }
  run rebindingNameToDeletingIdIsUnsafeTest = {
    val deleting = beginDelete(commitWriter(prepareWriter(create(initial), 1)))
    all { assert(safe(deleting)), assert(not(safe(deleting.with("name", 1)))) }
  }
  run cachedNameReplayCanResurrectOldPayloadTest = {
    val recreated = create(beginDelete(prepareWriter(create(initial), 1)))
    val broken = recreated.with("rows", recreated.rows.union(payload(recreated.name, 1, 1)))
    all { assert(safe(recreated)), assert(not(safe(broken))) }
  }
  run droppingDeletionWorkIsUnsafeTest = {
    val deleting = beginDelete(commitWriter(prepareWriter(create(initial), 1)))
    all { assert(safe(deleting)), assert(not(safe(deleting.with("work", Set())))) }
  }
  run crossTableCleanupIsUnsafeTest = {
    val deleting = beginDelete(create(initial))
    val broken = deleting.with("rows", Set())
    all { assert(safe(deleting)), assert(not(safe(broken))) }
  }
  run multipleItemWritesShareOneTableScheduleTest = {
    val loaded = commitWriter(prepareWriter(commitWriter(prepareWriter(create(initial), 1)), 2))
    all {
      assert(loaded.schedules.filter(p => p.table == 1).size() == 1),
      assert(owned(loaded, 1).filter(r => r.family == 7).size() == 1), assert(safe(loaded)),
    }
  }
  run rescheduleReplacesExactlyOneMarkerTest = {
    val loaded = commitWriter(prepareWriter(create(initial), 1))
    val scheduled = reschedule(reschedule(loaded, 5), 1)
    val forever = reschedule(scheduled, 0)
    all {
      assert(scheduled.markers.filter(p => p.table == 1).size() == 1),
      assert(forever.markers.filter(p => p.table == 1).size() == 0), assert(safe(forever)),
    }
  }
  run deletionRemovesFutureMarkersWithTheirPoliciesTest = {
    val loaded = reschedule(commitWriter(prepareWriter(create(initial), 1)), 99)
    val finished = drain(beginDelete(loaded), 1)
    all {
      assert(finished.now == 0), assert(finished.retired.contains(1)),
      assert(finished.markers == foreignSchedules), assert(finished.schedules == foreignSchedules), assert(safe(finished)),
    }
  }
  run retainingOldMarkerOnRescheduleIsUnsafeTest = {
    val loaded = reschedule(commitWriter(prepareWriter(create(initial), 1)), 5)
    val changed = reschedule(loaded, 1)
    val broken = changed.with("markers", changed.markers.union(loaded.markers))
    all { assert(safe(changed)), assert(not(safe(broken))) }
  }
  run droppingPolicyWithoutItsMarkerIsUnsafeTest = {
    val deleting = beginDelete(reschedule(commitWriter(prepareWriter(create(initial), 1)), 5))
    val broken = deleting.with("rows", deleting.rows.filter(r => r.table != 1 or r.family != 7))
      .with("schedules", foreignSchedules)
    all { assert(safe(deleting)), assert(not(safe(broken))) }
  }
}
