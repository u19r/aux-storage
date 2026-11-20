/// Retain imported history and its addresses. Opening excludes writers.
/// FDB scalar encoding preserves (version8, batch2, eventOrdinal2) order.
/// PG uses p=(x+offset)*65536+ordinal, F=(xmin+offset)*65536.
module restored_stream_positions {
  type State = { provider: str, mode: str, phase: int, clock: int,
    offset: int, entries: Set[int], saved: Set[int], authority: int,
    backupVersion: int, first: int, second: int, pending: Set[int],
    added: Set[int], high: int, committed: Set[int] }
  var s: State
  pure val width = 65536
  pure def initial(provider, mode) = { provider: provider, mode: mode,
    phase: 0, clock: 0, offset: 0, entries: Set(), saved: Set(), authority: 0,
    backupVersion: 0, first: 0, second: 0, pending: Set(), added: Set(),
    high: -1, committed: Set() }
  action init = s' = initial("pg", "correct")
  action initPriorOffset = s' = initial("pg", "correct").with("offset", 3)
  action initFdb = s' = initial("fdb", "correct")
  action initUuidOnly = s' = initial("pg", "uuidOnly")
  action initReadWithoutOffset = s' = initial("pg", "readWithoutOffset")
  action initClamp = s' = initial("pg", "clamp")
  action initNoNativeAdvance = s' = initial("fdb", "noNativeAdvance")
  val unit = if (s.provider == "pg") width else width * width
  def event(x, batch, ordinal) = (x + s.offset) * unit + batch * width + ordinal

  // Two commits in one FDB version use different batches; each has two events.
  // PG transactions have distinct xids. Extreme ordinals exercise the boundary.
  action appendSource = all { s.phase == 0,
    s' = s.with("phase", 1).with("entries", Set(event(10, 0, 0),
      event(10, 0, width - 1),
      event(if (s.provider == "pg") 11 else 10,
        if (s.provider == "pg") 0 else width - 1, 0),
      event(if (s.provider == "pg") 11 else 10,
        if (s.provider == "pg") 0 else width - 1, width - 1))) }
  // A sealed/retired bound can exceed every surviving entry. Backup version
  // covers that bound as well as commits for explicit streams and parent ends.
  action takeBackup = all { s.phase == 1,
    s' = s.with("phase", 2).with("saved", s.entries)
      .with("authority", (12 + s.offset) * unit + unit - 1).with("backupVersion", 12) }
  action restore = {
    nondet target = Set(2, 12, 20).oneOf()
    all { s.phase == 2,
      s' = s.with("phase", 3).with("clock", target) }
  }
  action openRestoredDatabase = {
    val raisedOffset = if (s.authority / unit - s.clock > s.offset)
      s.authority / unit - s.clock else s.offset
    val advancedVersion = if (s.clock > s.backupVersion)
      s.clock else s.backupVersion + 1
    all { s.phase == 3,
      s' = s.with("phase", 4)
        .with("offset", if (s.provider == "pg" and
          (s.mode == "correct" or s.mode == "readWithoutOffset")) raisedOffset else 0)
        .with("clock", if (s.provider == "fdb" and s.mode == "correct")
          advancedVersion else s.clock) }
  }
  // Both writers begin after exclusive open. They can commit in either order.
  action beginWriters = all { s.phase == 4,
    s' = s.with("phase", 5).with("first", s.clock + 1)
      .with("second", s.clock + 2).with("pending", Set(s.clock + 1, s.clock + 2))
      .with("clock", s.clock + 3) }
  def writerEvents(x) = if (s.mode == "clamp") Set(s.authority, s.authority + 1)
    else Set((x + s.offset) * unit, (x + s.offset) * unit + width - 1)
  action commitWriter = {
    nondet x = s.pending.oneOf()
    val events = writerEvents(x)
    all { s.phase == 5, s.pending.size() > 0,
      s' = s.with("pending", s.pending.exclude(Set(x)))
        .with("entries", s.entries.union(events)).with("added", s.added.union(events))
        .with("committed", s.committed.union(Set(x))) }
  }
  // xmin is the oldest pending xid, or next xid when no writers remain.
  // FDB reads likewise close only fully published native versions.
  action readClosedPrefix = {
    val oldest = if (s.pending.contains(s.first)) s.first
      else if (s.pending.contains(s.second)) s.second else s.clock
    val readOffset = if (s.mode == "readWithoutOffset") 0 else s.offset
    all { s.phase == 5,
      s' = s.with("high", (oldest + readOffset) * unit - 1)
        .with("phase", if (s.pending.size() == 0) 6 else 5) }
  }
  action step = any { appendSource, takeBackup, restore, openRestoredDatabase,
    beginWriters, commitWriter, readClosedPrefix, s' = s }
  val historyPreserved = s.phase < 2 or s.saved.subseteq(s.entries)
  val oldBeforeNew = s.added.forall(p => p > s.authority)
  val uniqueAddresses = s.added.size() == 2 * s.committed.size() and
    s.added.intersect(s.saved).size() == 0
  val closedPrefix = s.pending.forall(x => writerEvents(x).forall(p => p > s.high))
  val allVisible = s.phase < 6 or s.added.forall(p => p <= s.high)
  val inv = historyPreserved and oldBeforeNew and uniqueAddresses and closedPrefix and allVisible
  val lowerClock = s.phase == 3 and s.clock == 2
  val equalClock = s.phase == 3 and s.clock == 12
  val higherClock = s.phase == 3 and s.clock == 20
  val laterCommitFirst = s.phase == 5 and s.committed == Set(s.second)
  val partialRead = laterCommitFirst and s.high >= 0 and closedPrefix
  val completeRead = s.phase == 6 and s.committed.size() == 2 and inv
  action throughOpen = appendSource.then(takeBackup).then(restore)
    .then(openRestoredDatabase).then(beginWriters)
  run retainedHistoryTest = init.then(throughOpen).then(commitWriter)
    .then(commitWriter).then(readClosedPrefix).expect(completeRead)
  run nativeAdvanceTest = initFdb.then(throughOpen).then(commitWriter)
    .then(commitWriter).then(readClosedPrefix).expect(completeRead)
}
