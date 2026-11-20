/// Serialized RocksDB sequence arithmetic, not a physical WAL/fsync proof.
/// durable_ack.qnt / durable_ack_impl_local owns ticket publication, holds,
/// settlement and crash behavior. Here its proven obligation is abstracted as
/// answer <= completed durable prefix; sync captures before completion.
module rocks_stream_positions {
  type Entry = { position: int, end: int, parent: bool }
  type State = {
    mode: str, latest: int, durable: int, queued: bool, captured: int,
    active: bool, read: int, entries: List[Entry], seal: int,
    cursor: int, answered: int, barrier: int, ordered: bool,
    delayed: bool, dedup: bool, hole: bool, aborted: bool,
    unknownApplied: bool, unknownLost: bool, crashed: bool,
  }
  var s: State
  pure def initial(mode: str): State = {
    mode: mode, latest: 0, durable: 0, queued: false, captured: 0,
    active: false, read: 0, entries: List(), seal: -1,
    cursor: -1, answered: -1, barrier: -1, ordered: true,
    delayed: false, dedup: false, hole: false, aborted: false,
    unknownApplied: false, unknownLost: false, crashed: false,
  }
  action init = s' = initial("correct")
  action initEarlySnapshot = s' = initial("earlySnapshot")
  action initTooFewHeads = s' = initial("tooFewHeads")
  action initInclusiveSeal = s' = initial("inclusiveSeal")
  action initEarlyAnswer = s' = initial("earlyAnswer")

  action queue = all {
    not(s.queued), not(s.active), s.latest < 30, not(s.crashed),
    s' = s.with("queued", true).with("captured", s.latest),
  }
  // A preceding serialized worker can commit after async submission but before
  // this attempt enters the native worker. It emits no entry.
  action predecessor = all {
    s.queued, not(s.active), s.latest < 30,
    s' = s.with("latest", s.latest + 1).with("hole", true),
  }
  action admit = all {
    s.queued, not(s.active),
    s' = s.with("queued", false).with("active", true)
      .with("read", if (s.mode == "earlySnapshot") s.captured else s.latest)
      .with("delayed", s.delayed or s.captured < s.latest),
  }
  action finish(n: int, extra: int, outcome: str): bool = {
    val applies = Set("ok", "unknownApplied").contains(outcome)
    // N distinct entry heads survive even if every projection key deduplicates.
    val mutations = if (s.mode == "tooFewHeads" and n > 0) n - 1 else n + extra
    val end = s.latest + mutations
    val positions = List(0, 1, 2).slice(0, n).foldl(List(), (entries, i) => entries.append({
      position: s.read + i + 1, end: end, parent: s.seal < 0,
    }))
    val previousMax = s.entries.foldl(0, (m, e) => if (e.position > m) e.position else m)
    all {
      s.active, s.latest < 30, n >= 0, n <= 3, extra >= 0, n + extra > 0,
      s' = s.with("active", false)
        .with("latest", if (applies) end else s.latest)
        .with("entries", if (applies) s.entries.concat(positions) else s.entries)
        .with("ordered", s.ordered and (not(applies) or n == 0 or s.read + 1 > previousMax))
        .with("dedup", s.dedup or (applies and n >= 2 and extra == 1))
        .with("hole", s.hole or (applies and n == 0))
        .with("aborted", s.aborted or outcome == "abort")
        .with("unknownApplied", s.unknownApplied or outcome == "unknownApplied")
        .with("unknownLost", s.unknownLost or outcome == "unknownLost"),
    }
  }
  action split = all {
    s.active, s.seal < 0,
    s' = s.with("active", false).with("latest", s.latest + 1)
      .with("seal", s.read + (if (s.mode == "inclusiveSeal") 0 else 1)),
  }
  action observeCursor = all {
    not(s.active), not(s.crashed),
    s' = s.with("cursor", s.latest),
  }
  action startSync = all {
    s.barrier < 0, not(s.crashed),
    s' = s.with("barrier", s.latest),
  }
  action completeSync = all {
    s.barrier >= 0,
    s' = s.with("durable", s.barrier).with("barrier", -1),
  }
  action answerCursor = all {
    s.cursor >= 0, s.mode == "earlyAnswer" or s.cursor <= s.durable,
    s' = s.with("answered", s.cursor),
  }
  action powerCrash = all {
    not(s.active), not(s.crashed),
    s' = s.with("latest", s.durable).with("crashed", true)
      .with("queued", false).with("barrier", -1),
  }
  action step = any {
    queue, predecessor, admit,
    nondet n = 0.to(3).oneOf()
    nondet extra = 0.to(2).oneOf()
    nondet outcome = Set("ok", "abort", "unknownApplied", "unknownLost").oneOf()
    finish(n, extra, outcome),
    split, observeCursor, startSync, completeSync, answerCursor, powerCrash,
  }
  val positionsWithinCommit = s.entries.indices().forall(i => s.entries[i].position <= s.entries[i].end)
  val positionsOrdered = s.ordered
  val admittedSnapshot = not(s.active) or s.read == s.latest
  val exclusiveParentSeal = s.seal < 0 or s.entries.indices().forall(i => not(s.entries[i].parent) or s.entries[i].position < s.seal)
  val cursorDurable = s.answered <= s.durable and (not(s.crashed) or s.answered <= s.latest)
  val inv = positionsWithinCommit and positionsOrdered and admittedSnapshot and exclusiveParentSeal and cursorDurable
  val delayedAdmission = s.delayed
  val deduplicatedProjection = s.dedup
  val metadataHole = s.hole
  val knownAbort = s.aborted
  val unknownApplied = s.unknownApplied
  val unknownLost = s.unknownLost
  val splitReached = s.seal >= 0 and s.entries.indices().exists(i => s.entries[i].parent)
  val durableCursor = s.answered > 0 and s.crashed

  run givenQueuedAttemptWhenPredecessorCommitsThenAdmissionUsesLatestTest =
    init.then(queue).then(predecessor).then(admit)
      .expect(s.read == 1 and s.delayed and inv)
  run givenRepeatedProjectionWhenBatchCommitsThenPositionsFitTest =
    init.then(queue).then(admit).then(finish(3, 1, "ok"))
      .expect(s.entries.length() == 3 and s.latest == 4 and inv)
  run givenMetadataWhenCommitThenCursorSurvivesSyncedCrashTest =
    init.then(queue).then(admit).then(finish(0, 1, "ok"))
      .then(observeCursor).then(startSync).then(completeSync)
      .then(answerCursor).then(powerCrash)
      .expect(s.entries.length() == 0 and s.answered == 1 and inv)
  run givenUnknownLostWhenNewAttemptStartsThenItReadsActualStateTest =
    init.then(queue).then(admit).then(finish(2, 1, "unknownLost"))
      .then(queue).then(admit).then(finish(2, 1, "unknownApplied"))
      .then(queue).then(admit)
      .expect(s.unknownLost and s.unknownApplied and s.read == 3 and inv)
  run givenParentEntriesWhenSplitThenSealIsExclusiveTest =
    init.then(queue).then(admit).then(finish(2, 1, "ok"))
      .then(queue).then(admit).then(split)
      .then(queue).then(admit).then(finish(1, 1, "ok"))
      .expect(s.seal == 4 and s.entries.nth(s.entries.length() - 1).parent == false and inv)

  run givenKnownAbortWhenFinishedThenNoEntriesArePublishedTest =
    init.then(queue).then(admit).then(finish(3, 0, "abort"))
      .expect(s.aborted and s.latest == 0 and s.entries.length() == 0 and inv)
  run givenPreAdmissionSnapshotWhenDelayedThenSnapshotRuleFailsTest =
    initEarlySnapshot.then(queue).then(predecessor).then(admit)
      .expect(not(admittedSnapshot))
  run givenTooFewHeadsWhenCommittedThenEntryExceedsEndTest =
    initTooFewHeads.then(queue).then(admit).then(finish(2, 0, "ok"))
      .expect(not(positionsWithinCommit))
  run givenInclusiveSealWhenLastEntryEqualsReadThenParentIsExcludedTest =
    initInclusiveSeal.then(queue).then(admit).then(finish(2, 0, "ok"))
      .then(queue).then(admit).then(split).expect(not(exclusiveParentSeal))
  run givenEarlyAnswerWhenUnsyncedThenCursorIsNotDurableTest =
    initEarlyAnswer.then(queue).then(admit).then(finish(0, 1, "ok"))
      .then(observeCursor).then(answerCursor).then(powerCrash)
      .expect(not(cursorDurable))
}
