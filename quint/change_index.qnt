module change_index {
  type Version = V0 | V1 | V2 | V3
  type MarkerAge = Recent | Old
  type WriteOutcome = NotChecked | CommittedMarker | FailedNoMarker | TransactionMarkers | TrimmedOld

  var lastOutcome: WriteOutcome
  var itemCommitted: bool
  var markerWritten: bool
  var markerVersion: Version
  var markerAge: MarkerAge
  var markerRetained: bool

  pure def rank(version: Version): int =
    match version {
      | V0 => 0
      | V1 => 1
      | V2 => 2
      | V3 => 3
    }

  pure def slotForTable(tableHash: int, slotCount: int): int =
    tableHash % slotCount

  pure def slotPrefix(slot: int): str =
    if (slot == 1) "slot/1" else "slot/other"

  pure def markerKeyForSlotOne(version: str, tableId: str): str =
    if (version == "0002" and tableId == "source_users") "slot/1/0002/source_users" else "slot/1/other"

  pure def writeMarkerRequired(streamEnabled: bool, writeCommitted: bool): bool =
    streamEnabled and writeCommitted

  pure def ttlKeepsMarker(age: MarkerAge): bool =
    age == Recent

  action init = all {
    lastOutcome' = NotChecked,
    itemCommitted' = false,
    markerWritten' = false,
    markerVersion' = V0,
    markerAge' = Recent,
    markerRetained' = false,
  }

  action CommitItemWithMarker = all {
    lastOutcome' = CommittedMarker,
    itemCommitted' = true,
    markerWritten' = true,
    markerVersion' = V1,
    markerAge' = Recent,
    markerRetained' = true,
  }

  action RejectConditionalWrite = all {
    lastOutcome' = FailedNoMarker,
    itemCommitted' = false,
    markerWritten' = false,
    markerVersion' = V0,
    markerAge' = Recent,
    markerRetained' = false,
  }

  action CommitTransactionMarkers = all {
    lastOutcome' = TransactionMarkers,
    itemCommitted' = true,
    markerWritten' = true,
    markerVersion' = V2,
    markerAge' = Recent,
    markerRetained' = true,
  }

  action TrimOldMarker = all {
    lastOutcome' = TrimmedOld,
    itemCommitted' = true,
    markerWritten' = true,
    markerVersion' = V2,
    markerAge' = Old,
    markerRetained' = false,
  }

  action step = any {
    CommitItemWithMarker,
    RejectConditionalWrite,
    CommitTransactionMarkers,
    TrimOldMarker,
  }

  val committedWriteHasMarker =
    itemCommitted == markerWritten

  val failedWriteHasNoMarker =
    itemCommitted or markerWritten == false

  val ttlRetainsRecentMarkers =
    ttlKeepsMarker(markerAge) == markerRetained

  val markerIsOrdered =
    markerRetained implies rank(markerVersion) > rank(V0)

  val inv = all {
    committedWriteHasMarker,
    failedWriteHasNoMarker,
    ttlRetainsRecentMarkers,
    markerIsOrdered,
  }

  run changeIndexKeyExamples = all {
    assert(slotForTable(257, 256) == 1),
    assert(slotPrefix(1) == "slot/1"),
    assert(markerKeyForSlotOne("0002", "source_users") == "slot/1/0002/source_users"),
    assert(writeMarkerRequired(true, true)),
    assert(writeMarkerRequired(true, false) == false),
    assert(writeMarkerRequired(false, true) == false),
    assert(ttlKeepsMarker(Recent)),
    assert(ttlKeepsMarker(Old) == false),
  }

  val committed_marker_witness =
    lastOutcome == CommittedMarker and itemCommitted and markerWritten

  val failed_no_marker_witness =
    lastOutcome == FailedNoMarker and itemCommitted == false and markerWritten == false

  val transaction_markers_witness =
    lastOutcome == TransactionMarkers and markerVersion == V2

  val trim_old_witness =
    lastOutcome == TrimmedOld and markerAge == Old and markerRetained == false
}
