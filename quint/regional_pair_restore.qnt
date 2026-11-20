/// Coverage after disabled ordinary writes. Native capture/hold admission is atomic.
/// New-pair baseline order 0 is below every captured current-pair write.
module regional_pair_restore_model {
  const BROKEN: str
  type Packet = { region: int, requestPair: int, emissionPair: int, position: int, value: int }
  type State = { phase: int, pair: int, cursor: int,
    captured: bool, scanned: Set[int], scanValues: int -> int,
    sourceValue: int, written: bool, delivered: bool, done: bool,
    rows: int -> int, orders: int -> int, oldRejected: bool,
    oldPageCaptured: bool, oldPageAttempted: bool, reopened: bool, reopenedPair: int }
  var s: State
  // Old G1 history contains PUTs at positions 1,2 with winner orders 90,99.
  // While disabled: ordinary PUT(0,10), DELETE(1). Neither emits G2 history.
  action init = s' = { phase: 0, pair: 1, cursor: 0,
    captured: false, scanned: Set(), scanValues: Map(0 -> -1, 1 -> -1),
    sourceValue: 10, written: false, delivered: false, done: false,
    rows: Map(0 -> -1, 1 -> -1), orders: Map(0 -> -1, 1 -> -1),
    oldRejected: false, oldPageCaptured: false, oldPageAttempted: false,
    reopened: false, reopenedPair: 0 }
  // Fresh hidden target, durable hold at position 0 and G2 capture become
  // visible together. A real native closed cut may replace generation filtering.
  action enable = all { s.phase == 0,
    s' = s.with("phase", 1).with("pair", 2).with("captured", true) }
  action write = all { s.phase == 1, not(s.written), s.captured,
    s' = s.with("written", true).with("sourceValue", 20) }
  action scan(key: int): bool = {
    val value = if (key == 0) s.sourceValue else -1
    val order = if (value == 20) 1 else 0
    all { s.phase == 1, not(s.scanned.contains(key)),
      s' = s.with("scanned", s.scanned.union(Set(key)))
        .with("scanValues", s.scanValues.set(key, value))
        .with("rows", if (s.orders.get(key) < order) s.rows.set(key, value) else s.rows)
        .with("orders", if (s.orders.get(key) < order) s.orders.set(key, order) else s.orders) }
  }
  // Request authority is G2, but the retained payload was emitted under G1.
  // Checking only the request pair would admit these old higher-order PUTs.
  action oldHistory(key: int): bool = {
    val packet = { region: 1, requestPair: 2, emissionPair: 1, position: key + 1, value: key + 1 }
    val admitted = packet.region == 1 and packet.requestPair == s.pair and
      (packet.emissionPair == s.pair or BROKEN == "oldGenerationHistory")
    all { s.phase == 1,
      s' = if (admitted) s.with("rows", s.rows.set(key, packet.value))
        .with("orders", s.orders.set(key, 90 + key))
        else s.with("oldRejected", true) }
  }
  action finishScan = all { s.phase == 1, s.scanned == Set(0, 1), not(s.done),
    s' = s.with("done", true).with("cursor", if (BROKEN == "resumeAtHead") 3 else s.cursor) }
  // Native position 3 and winner order 1 are intentionally different numbers.
  action deliver = all { s.phase == 1, s.written, s.cursor < 3,
    s' = s.with("cursor", 3).with("delivered", true)
      .with("rows", s.rows.set(0, 20)).with("orders", s.orders.set(0, 1)) }
  action reopen = all { s.phase == 1, not(s.reopened), s' = s.with("reopened", true).with("reopenedPair", s.pair) }
  // An honest G2 page exists before restore. Its captured fields never change.
  action captureOldPage = all { s.phase == 1, s.done, not(s.oldPageCaptured),
    s' = s.with("oldPageCaptured", true) }
  // Supported import installs a genuinely fresh pair before regional serving.
  // Normal reopen above preserves it. Native scope/lineage are separate premises.
  action restore = all { s.phase == 1, s.done, s.oldPageCaptured,
    s' = s.with("phase", 2).with("pair", if (BROKEN == "reusePairAfterRestore") 2 else 3)
      .with("cursor", if (BROKEN == "reuseProgressAfterRestore") s.cursor else 0)
      .with("rows", Map(0 -> -1, 1 -> -1)) }
  action receiveRestored(packet: Packet): bool = {
    val admitted = packet.region == 1 and packet.requestPair == s.pair and packet.emissionPair == s.pair
    val observed = s.with("oldPageAttempted", s.oldPageAttempted or packet.position == 5)
    all { s.phase == 2,
      s' = if (admitted and packet.position > s.cursor)
        observed.with("cursor", packet.position).with("rows", s.rows.set(0, packet.value))
        else observed }
  }
  action oldSourcePage = all { s.oldPageCaptured,
    receiveRestored({ region: 1, requestPair: 2, emissionPair: 2, position: 5, value: 40 }) }
  // Newly captured after restore, under its actual pair; never relabel old history.
  action newSourcePage = receiveRestored({ region: 1, requestPair: s.pair, emissionPair: s.pair, position: 2, value: 30 })
  action step = { nondet key = Set(0, 1).oneOf()
    any { enable, write, scan(key), oldHistory(key), finishScan, deliver, reopen,
      captureOldPage, restore, oldSourcePage, newSourcePage, s' = s } }
  // Oracle uses only current source observations and valid new-pair delivery.
  // Old retained payloads never contribute, however high their winner order.
  val expected0 = if (s.delivered or s.scanValues.get(0) == 20) 20 else s.scanValues.get(0)
  val visibleData = s.rows.get(1) == -1 and s.rows.get(0) ==
    (if (s.phase == 2) if (s.cursor == 2) 30 else -1 else expected0)
  val retainedCoverage = s.phase != 1 or not(s.done and s.written) or
    s.rows.get(0) == 20 or (s.captured and s.cursor < 3)
  val restoreCoverage = s.phase != 2 or s.rows.get(0) == 30 or s.cursor < 2
  val reopenIdentity = not(s.reopened) or s.phase == 2 or
    s.pair == s.reopenedPair
  val inv = visibleData and retainedCoverage and restoreCoverage and reopenIdentity
  val disabledDeleteSafe = s.done and s.oldRejected and s.rows.get(1) == -1 and inv
  val fuzzyWriteCovered = s.done and s.written and s.scanValues.get(0) == 10 and
    not(s.delivered) and retainedCoverage and inv
  val currentWriteVisible = s.done and s.delivered and s.rows.get(0) == 20 and inv
  val restoredOldRejected = s.phase == 2 and s.oldPageAttempted and s.cursor == 0 and inv
  val restoredNewVisible = s.phase == 2 and s.rows.get(0) == 30 and s.cursor == 2 and inv
  run disableReenableTest = init.then(enable).then(scan(0)).then(scan(1))
    .then(finishScan).then(oldHistory(0)).then(oldHistory(1)).expect(disabledDeleteSafe)
  run fuzzyCaptureTest = init.then(enable).then(scan(0)).then(write).then(scan(1))
    .then(finishScan).expect(fuzzyWriteCovered)
  run fuzzyDeliveryTest = init.then(enable).then(scan(0)).then(write).then(scan(1))
    .then(finishScan).then(deliver).expect(currentWriteVisible)
  run restartIdentityTest = init.then(enable).then(reopen)
    .expect(s.pair == 2 and reopenIdentity)
  run restoreIdentityTest = init.then(enable).then(scan(0)).then(scan(1))
    .then(finishScan).then(reopen).then(captureOldPage).then(restore).then(oldSourcePage).expect(restoredOldRejected)
  run restoredLowerPositionTest = init.then(enable).then(scan(0)).then(scan(1))
    .then(finishScan).then(captureOldPage).then(restore).then(oldSourcePage).then(newSourcePage).expect(restoredNewVisible)
  run oldProgressResetTest = init.then(enable).then(scan(0)).then(write).then(scan(1))
    .then(finishScan).then(deliver).then(captureOldPage).then(restore)
    .then(newSourcePage).expect(restoredNewVisible)
}
module regional_pair_restore { import regional_pair_restore_model(BROKEN = "none").* }
module regional_pair_restore_oldGenerationHistory { import regional_pair_restore_model(BROKEN = "oldGenerationHistory").* }
module regional_pair_restore_resumeAtHead { import regional_pair_restore_model(BROKEN = "resumeAtHead").* }
module regional_pair_restore_reusePairAfterRestore { import regional_pair_restore_model(BROKEN = "reusePairAfterRestore").* }
module regional_pair_restore_reuseProgressAfterRestore { import regional_pair_restore_model(BROKEN = "reuseProgressAfterRestore").* }
