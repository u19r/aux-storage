/// Candidate only: FDB split seal, complete native batches, and protected topology.
/// Native resolver checks earlier accepted writes, INCLUDING within the same V.
/// Native positions are (version8, batch2, eventOrdinal2).
/// One writer, one parent split, two commit versions, and one fixed operation ID.
/// Token inequality abstracts a protected catalog epoch change, not a raw10 codec.
module native_stream_seal {
  type Stamp = { version: int, batch: int }
  type Event = { version: int, batch: int, ordinal: int }
  type Attempt = { phase: str, read: int, token: Stamp }
  type State = {
    published: int, version: int, batch: int, isOpen: bool,
    token: Stamp, visibleToken: Stamp,
    writer: Attempt, split: Attempt,
    events: Set[Event], visible: Set[Event], seal: int, visibleSeal: int,
    splitStamp: Stamp, lateParent: bool, high: int, captured: bool,
    capturedEvents: Set[Event], mode: str,
    receipt: bool, children: Set[str], publications: int, recovery: str,
    childProgress: int, loadedEnd: int, loadedReceipt: bool, loadedChildren: Set[str],
    replayEnd: int, replayChildren: Set[str],
  }
  var s: State
  pure val emptyToken = { version: 1, batch: 0 }
  pure val idle = { phase: "idle", read: 0, token: emptyToken }
  pure def initial(mode) = {
    published: 1, version: 1, batch: 0, isOpen: false,
    token: emptyToken, visibleToken: emptyToken,
    writer: idle, split: idle, events: Set(), visible: Set(),
    seal: -1, visibleSeal: -1, splitStamp: { version: -1, batch: -1 },
    lateParent: false, high: -1, captured: false, capturedEvents: Set(), mode: mode,
    receipt: false, children: Set(), publications: 0, recovery: "idle",
    childProgress: 0, loadedEnd: -1, loadedReceipt: false, loadedChildren: Set(),
    replayEnd: -1, replayChildren: Set(),
  }
  action init = s' = initial("commit")
  action initReadSeal = s' = initial("read")
  action initPreviousSeal = s' = initial("previous")
  action initNoConflict = s' = initial("noConflict")
  action initMissingReceipt = s' = initial("missingReceipt")
  action initSnapshotEnd = s' = initial("snapshotEnd")

  // Begin does not observe authority, route, stage, or resolve a transaction.
  action beginWriter = all {
    s.writer.phase == "idle",
    s' = s.with("writer", idle.with("phase", "begun").with("read", s.published)),
  }
  action beginSplit = all {
    s.split.phase == "idle",
    s' = s.with("split", idle.with("phase", "begun").with("read", s.published)),
  }
  // Snapshots cannot observe a partial unpublished resolver batch. Observations
  // below refer to the captured snapshot, which precedes the sole topology change.
  action observeWriter = all {
    s.writer.phase == "begun", s.writer.read == 1,
    s' = s.with("writer", s.writer.with("phase", "observed").with("token", emptyToken)),
  }
  action observeSplit = all {
    s.split.phase == "begun", s.split.read == 1,
    s' = s.with("split", s.split.with("phase", "observed").with("token", emptyToken)),
  }
  action stageWriter = all {
    s.writer.phase == "observed",
    s' = s.with("writer", s.writer.with("phase", "staged")),
  }
  action stageSplit = all {
    s.split.phase == "observed",
    s' = s.with("split", s.split.with("phase", "staged")),
  }
  action openBatch = all {
    not(s.isOpen), s.published < 3,
    s' = s.with("version", s.published + 1).with("batch", 0).with("isOpen", true),
  }
  // token is the resolver's accepted write ledger, not merely published state.
  // A protected metadata read conflicts with an earlier accepted split write.
  action resolveWriter = {
    val hasConflict = s.mode != "noConflict" and s.writer.token != s.token
    val emitted = Set(
      { version: s.version, batch: s.batch, ordinal: 0 },
      { version: s.version, batch: s.batch, ordinal: 65535 })
    all {
      s.isOpen, s.writer.phase == "staged", s.version > s.writer.read,
      s' = s.with("writer", s.writer.with("phase", if (hasConflict) "aborted" else "accepted"))
        .with("events", if (hasConflict) s.events else s.events.union(emitted))
        .with("lateParent", s.lateParent or (not(hasConflict) and s.seal >= 0))
        .with("batch", s.batch + 1),
    }
  }
  action resolveSplit = {
    val boundary = if (s.mode == "read") s.split.read
      else if (s.mode == "previous") s.version - 1 else s.version
    all {
      s.isOpen, s.split.phase == "staged", s.version > s.split.read,
      s.split.token == s.token,
      s' = s.with("split", s.split.with("phase", "accepted"))
        .with("token", { version: s.version, batch: s.batch })
        .with("splitStamp", { version: s.version, batch: s.batch })
        .with("seal", boundary).with("batch", s.batch + 1),
    }
  }
  // All accepted transactions at V become visible together. There is no state
  // with snapshot high V and only part of the batch visible. Once published,
  // no later resolve can receive V (openBatch strictly advances the version).
  action publishBatch = all {
    s.isOpen, s.batch > 0,
    s' = s.with("published", s.version).with("isOpen", false)
      .with("visible", s.events).with("visibleSeal", s.seal).with("visibleToken", s.token)
      .with("receipt", s.seal >= 0 and s.mode != "missingReceipt")
      .with("children", if (s.seal >= 0) Set("left", "right") else Set())
      .with("publications", if (s.visibleSeal < 0 and s.seal >= 0) s.publications + 1 else s.publications),
  }
  // Parent envelope, EndAtCommit receipt marker, children, and changed epoch
  // publish in the same snapshot above. The public result contains no end number.
  action loseCommittedReply = all {
    s.visibleSeal >= 0, s.recovery == "idle",
    s' = s.with("recovery", "unknown"),
  }
  // Abstract child append followed by child split. Neither changes the sealed
  // parent's row or the fixed parent operation's receipt/child identities.
  action advanceChildren = all {
    s.isOpen, s.visibleSeal >= 0, s.childProgress < 2,
    s' = s.with("childProgress", s.childProgress + 1).with("batch", s.batch + 1),
  }
  action reopenAndRead = all {
    Set("unknown", "replayed").contains(s.recovery), not(s.isOpen), s.childProgress == 2,
    s' = s.with("recovery", "loaded").with("loadedReceipt", s.receipt)
      .with("loadedChildren", s.children)
      .with("loadedEnd", if (s.mode == "snapshotEnd") s.published + 1 else s.visibleSeal + 1),
  }
  action replay = all {
    s.recovery == "loaded", s.loadedReceipt,
    s' = s.with("recovery", "replayed").with("replayEnd", s.loadedEnd)
      .with("replayChildren", s.loadedChildren),
  }
  action captureHigh = all {
    not(s.captured),
    s' = s.with("high", s.published).with("captured", true).with("capturedEvents", s.visible),
  }
  action step = any {
    beginWriter, beginSplit, observeWriter, observeSplit, stageWriter, stageSplit,
    openBatch, resolveWriter, resolveSplit, publishBatch, captureHigh,
    loseCommittedReply, advanceChildren, reopenAndRead, replay,
  }

  // Inclusive version boundary is equivalent to sealed_after = seal + 1;
  // neither transaction batch order nor event ordinal truncates this prefix.
  val sealCoversParent = s.seal < 0 or s.events.forall(e => e.version <= s.seal)
  val protectedTopology = not(s.lateParent)
  val capturedHighIsClosed = not(s.captured) or
    s.events.filter(e => e.version <= s.high) == s.capturedEvents
  val publicationIsAtomic = (s.visibleSeal >= 0) == s.receipt and
    (s.visibleSeal >= 0) == (s.children == Set("left", "right")) and s.publications <= 1
  val replayIsExact = s.recovery != "replayed" or
    (s.replayEnd == s.splitStamp.version + 1 and s.replayChildren == Set("left", "right") and s.publications == 1)
  val inv = sealCoversParent and protectedTopology and capturedHighIsClosed and
    publicationIsAtomic and replayIsExact
  val replayAfterChildSplit = s.recovery == "replayed" and s.childProgress == 2 and s.published == 3
  val writerBeforeSplitSameVersion = s.splitStamp.version == 2 and
    s.splitStamp.batch == 1 and s.events.size() == 2 and sealCoversParent
  val staleWriterRejectedSameVersion = s.splitStamp.version == 2 and
    s.splitStamp.batch == 0 and s.writer.phase == "aborted" and s.version == 2
  val writerBeforeSplitLaterVersion = s.splitStamp.version == 3 and
    s.events.exists(e => e.version == 2) and sealCoversParent
  val oldHighRemainsClosed = s.captured and s.high == 1 and s.events.size() == 2
  val newHighIncludesWholeBatch = s.captured and s.high == 2 and s.capturedEvents.size() == 2

  // These are actual action paths from init, also enabled by step. No fabricated
  // bad starting state or pure-function shortcut supplies the hostile schedule.
  action prepareRace = beginSplit.then(observeSplit).then(stageSplit)
    .then(beginWriter).then(observeWriter).then(stageWriter)
  action writerThenSplit = prepareRace.then(openBatch).then(resolveWriter).then(resolveSplit)
  action splitThenWriter = prepareRace.then(openBatch).then(resolveSplit).then(resolveWriter)
  action splitThenLaterWriter = prepareRace.then(openBatch).then(resolveSplit)
    .then(publishBatch).then(openBatch).then(resolveWriter)
  run sameVersionAllOrdinalsTest = init.then(writerThenSplit)
    .expect(writerBeforeSplitSameVersion and inv)
    .then(publishBatch).then(captureHigh).expect(newHighIncludesWholeBatch and inv)
  run laterVersionSealTest = init.then(prepareRace).then(openBatch).then(resolveWriter)
    .then(publishBatch).then(openBatch).then(resolveSplit)
    .expect(writerBeforeSplitLaterVersion and inv)
  run staleSameVersionRejectedTest = init.then(splitThenWriter)
    .expect(staleWriterRejectedSameVersion and inv)
  run staleLaterVersionRejectedTest = init.then(splitThenLaterWriter)
    .expect(s.writer.phase == "aborted" and s.version == 3 and inv)
  run oldSnapshotClosedTest = init.then(captureHigh).then(writerThenSplit)
    .expect(oldHighRemainsClosed and inv)
  run readSealMutationTest = initReadSeal.then(writerThenSplit).expect(not(sealCoversParent))
  run previousSealMutationTest = initPreviousSeal.then(writerThenSplit).expect(not(sealCoversParent))
  run omittedConflictLaterVersionMutationTest = initNoConflict.then(splitThenLaterWriter)
    .expect(not(protectedTopology) and not(sealCoversParent))
  run omittedConflictMutationTest = initNoConflict.then(splitThenWriter).expect(not(protectedTopology))
  action committedThenReopened = beginSplit.then(observeSplit).then(stageSplit)
    .then(openBatch).then(resolveSplit).then(publishBatch).then(loseCommittedReply)
    .then(openBatch).then(advanceChildren).then(advanceChildren).then(publishBatch)
    .then(reopenAndRead).then(replay)
  run replayAfterChildSplitTest = init.then(committedThenReopened)
    .expect(replayAfterChildSplit and inv)
  run repeatedReplayTest = init.then(committedThenReopened).then(reopenAndRead).then(replay)
    .expect(replayAfterChildSplit and inv)
  run missingReceiptMutationTest = initMissingReceipt.then(beginSplit).then(observeSplit)
    .then(stageSplit).then(openBatch).then(resolveSplit).then(publishBatch)
    .expect(not(publicationIsAtomic))
  run snapshotEndMutationTest = initSnapshotEnd.then(committedThenReopened)
    .expect(not(replayIsExact))
}

