/// Item-versioned stream model.
///
/// Protects Invariant Inventory IDs STREAM-1, STREAM-2, STREAM-3, STREAM-4,
/// STREAM-5, and STORAGE-1 from docs/quint-backfill.md.
module item_versioned_stream {
  pure val KEYS = Set("a", "b")
  pure val NONE_VERSION = 0
  pure val OLD_FORMAT_VERSION = -1

  var current: str -> int
  var highWatermark: str -> int
  var pointerVersion: int
  var protectedVersion: int
  var trimBefore: int
  var oldFormatAccepted: bool
  var lastPointerAccepted: bool

  pure def initialCurrent = Map("a" -> NONE_VERSION, "b" -> NONE_VERSION)

  pure def initialHighWatermark = Map("a" -> NONE_VERSION, "b" -> NONE_VERSION)

  def keyVersionIsValid(key) =
    current.get(key) <= highWatermark.get(key)

  def versionsMonotonic =
    KEYS.forall(key => 0 <= highWatermark.get(key) and keyVersionIsValid(key))

  def pointerCarriesItemVersion =
    pointerVersion == NONE_VERSION or KEYS.exists(key => highWatermark.get(key) == pointerVersion)

  def trimPreservesProtectedBoundary =
    trimBefore <= protectedVersion

  def oldFormatRejected =
    not(oldFormatAccepted)

  def streamSafety =
    versionsMonotonic
      and pointerCarriesItemVersion
      and trimPreservesProtectedBoundary
      and oldFormatRejected

  pure def nextVersionFor(state, key) =
    state.get(key) + 1

  pure def putItem(state, high, key) = {
    pure val nextVersion = nextVersionFor(high, key)
    {
      current: state.set(key, nextVersion),
      highWatermark: high.set(key, nextVersion),
      pointerVersion: nextVersion,
    }
  }

  pure def deleteItem(state, high, key) = {
    pure val nextVersion = nextVersionFor(high, key)
    {
      current: state.set(key, NONE_VERSION),
      highWatermark: high.set(key, nextVersion),
      pointerVersion: nextVersion,
    }
  }

  def pointerPayloadAccepted(version) =
    version == NONE_VERSION or KEYS.exists(key => highWatermark.get(key) == version)

  action init = all {
    current' = initialCurrent,
    highWatermark' = initialHighWatermark,
    pointerVersion' = NONE_VERSION,
    protectedVersion' = NONE_VERSION,
    trimBefore' = NONE_VERSION,
    oldFormatAccepted' = false,
    lastPointerAccepted' = true,
  }

  action Put = {
    nondet key = KEYS.oneOf()
    val outcome = putItem(current, highWatermark, key)
    all {
      current' = outcome.current,
      highWatermark' = outcome.highWatermark,
      pointerVersion' = outcome.pointerVersion,
      protectedVersion' = protectedVersion,
      trimBefore' = trimBefore,
      oldFormatAccepted' = oldFormatAccepted,
      lastPointerAccepted' = true,
    }
  }

  action Delete = {
    nondet key = KEYS.oneOf()
    val outcome = deleteItem(current, highWatermark, key)
    all {
      current' = outcome.current,
      highWatermark' = outcome.highWatermark,
      pointerVersion' = outcome.pointerVersion,
      protectedVersion' = protectedVersion,
      trimBefore' = trimBefore,
      oldFormatAccepted' = oldFormatAccepted,
      lastPointerAccepted' = true,
    }
  }

  action ProtectBoundary = {
    nondet key = KEYS.oneOf()
    all {
      highWatermark.get(key) >= trimBefore,
      current' = current,
      highWatermark' = highWatermark,
      pointerVersion' = pointerVersion,
      protectedVersion' = highWatermark.get(key),
      trimBefore' = trimBefore,
      oldFormatAccepted' = oldFormatAccepted,
      lastPointerAccepted' = true,
    }
  }

  action Trim = {
    nondet version = Set(0, 1, 2).oneOf()
    all {
      version <= protectedVersion,
      current' = current,
      highWatermark' = highWatermark,
      pointerVersion' = pointerVersion,
      protectedVersion' = protectedVersion,
      trimBefore' = version,
      oldFormatAccepted' = oldFormatAccepted,
      lastPointerAccepted' = true,
    }
  }

  action ReadPointer = {
    nondet version = Set(OLD_FORMAT_VERSION, 0, 1, 2).oneOf()
    all {
      current' = current,
      highWatermark' = highWatermark,
      pointerVersion' = if (pointerPayloadAccepted(version)) version else pointerVersion,
      protectedVersion' = protectedVersion,
      trimBefore' = trimBefore,
      oldFormatAccepted' = oldFormatAccepted,
      lastPointerAccepted' = pointerPayloadAccepted(version),
    }
  }

  action step =
    any {
      Put,
      Delete,
      ProtectBoundary,
      Trim,
      ReadPointer,
    }

  val inv = streamSafety

  val put_success_witness =
    KEYS.exists(key => current.get(key) > NONE_VERSION)

  val old_format_rejected_witness =
    not(lastPointerAccepted)

  val trim_boundary_protected_witness =
    protectedVersion > NONE_VERSION and trimBefore == protectedVersion

  val pointer_uses_item_version_witness =
    pointerVersion > NONE_VERSION and pointerCarriesItemVersion

  run putIncrementsItemVersion =
    true.expect(putItem(initialCurrent, initialHighWatermark, "a").highWatermark.get("a") == 1)

  run deleteWritesVersionedTombstone =
    true
      .expect(deleteItem(initialCurrent.set("a", 1), initialHighWatermark.set("a", 1), "a").current.get("a") == NONE_VERSION)
      .expect(deleteItem(initialCurrent.set("a", 1), initialHighWatermark.set("a", 1), "a").highWatermark.get("a") == 2)

  run oldFormatPointerIsRejected =
    false.expect(pointerPayloadAccepted(OLD_FORMAT_VERSION))
}
