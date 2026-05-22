/// Documents the current active-active multi-region conflict rule.
///
/// The model intentionally shows that wall-clock skew changes winners because
/// local writes synthesize HLC logical time as zero and compare physical time
/// before region and sequence tie breakers.
module multi_region_clock_skew_lww_mbt {
  type ClockSkewCase = {
    currentPhysicalMs: int,
    currentRegion: str,
    currentSequence: int,
    incomingPhysicalMs: int,
    incomingRegion: str,
    incomingSequence: int,
  }

  var lastCase: ClockSkewCase
  var lastDecision: str

  pure def regionRank(region) =
    if (region == "region-a" or region == "region-fast") {
      1
    } else {
      2
    }

  pure def compareTuple(lhsPhysicalMs, lhsRegion, lhsSequence, rhsPhysicalMs, rhsRegion, rhsSequence) =
    if (lhsPhysicalMs > rhsPhysicalMs) {
      "greater"
    } else if (lhsPhysicalMs < rhsPhysicalMs) {
      "less"
    } else if (regionRank(lhsRegion) > regionRank(rhsRegion)) {
      "greater"
    } else if (regionRank(lhsRegion) < regionRank(rhsRegion)) {
      "less"
    } else if (lhsSequence > rhsSequence) {
      "greater"
    } else if (lhsSequence < rhsSequence) {
      "less"
    } else {
      "equal"
    }

  pure def lwwDecision(c) =
    if (compareTuple(
      c.currentPhysicalMs,
      c.currentRegion,
      c.currentSequence,
      c.incomingPhysicalMs,
      c.incomingRegion,
      c.incomingSequence
    ) == "greater") {
      "skip_stale"
    } else if (compareTuple(
      c.currentPhysicalMs,
      c.currentRegion,
      c.currentSequence,
      c.incomingPhysicalMs,
      c.incomingRegion,
      c.incomingSequence
    ) == "equal") {
      "skip_duplicate"
    } else {
      "apply"
    }

  action init = all {
    lastCase' = {
      currentPhysicalMs: 0,
      currentRegion: "region-a",
      currentSequence: 0,
      incomingPhysicalMs: 0,
      incomingRegion: "region-a",
      incomingSequence: 0,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet currentPhysicalMs = Set(1, 2).oneOf()
    nondet incomingPhysicalMs = Set(1, 2).oneOf()
    nondet currentRegion = Set("region-a", "region-b").oneOf()
    nondet incomingRegion = Set("region-a", "region-b").oneOf()
    nondet currentSequence = Set(1, 2).oneOf()
    nondet incomingSequence = Set(1, 2).oneOf()
    val c = {
      currentPhysicalMs: currentPhysicalMs,
      currentRegion: currentRegion,
      currentSequence: currentSequence,
      incomingPhysicalMs: incomingPhysicalMs,
      incomingRegion: incomingRegion,
      incomingSequence: incomingSequence,
    }
    all {
      lastCase' = c,
      lastDecision' = lwwDecision(c),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "apply" or lastCase.incomingPhysicalMs >= lastCase.currentPhysicalMs,
      lastDecision != "skip_stale" or lastCase.currentPhysicalMs >= lastCase.incomingPhysicalMs,
    }

  run fastClockCurrentBeatsSlowClockLaterSequence =
    true.expect(lwwDecision({
      currentPhysicalMs: 2,
      currentRegion: "region-fast",
      currentSequence: 1,
      incomingPhysicalMs: 1,
      incomingRegion: "region-slow",
      incomingSequence: 2,
    }) == "skip_stale")

  run fastClockIncomingBeatsSlowClockCurrent =
    true.expect(lwwDecision({
      currentPhysicalMs: 1,
      currentRegion: "region-slow",
      currentSequence: 1,
      incomingPhysicalMs: 2,
      incomingRegion: "region-fast",
      incomingSequence: 2,
    }) == "apply")
}
