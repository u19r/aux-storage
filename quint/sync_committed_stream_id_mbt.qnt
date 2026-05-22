/// MBT surface for committed resolved-sync stream id application.
///
/// Followers must materialize the stream id chosen by the leader/resolver.
/// Allocating a local stream id while applying a committed entry violates the
/// no-local-metadata-allocation rule in `sync_resolved_mutation.qnt`.
module sync_committed_stream_id_mbt {
  type CommitCase = {
    targetVersion: int,
  }

  var lastCase: CommitCase
  var appliedStreamId: int
  var localMetadataAllocated: bool
  var lastDecision: str

  pure def expectedDecision(commitCase, appliedId, localAllocated) =
    if (commitCase.targetVersion <= 0) {
      "rejected_zero_target"
    } else if (localAllocated) {
      "rejected_local_metadata"
    } else if (appliedId != commitCase.targetVersion) {
      "rejected_target_mismatch"
    } else {
      "accepted"
    }

  action init = all {
    lastCase' = { targetVersion: 1 },
    appliedStreamId' = 1,
    localMetadataAllocated' = false,
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet targetVersion = Set(1, 2, 7, 43, 255, 65535).oneOf()
    val commitCase = { targetVersion: targetVersion }
    all {
      lastCase' = commitCase,
      appliedStreamId' = targetVersion,
      localMetadataAllocated' = false,
      lastDecision' = expectedDecision(commitCase, targetVersion, false),
    }
  }

  action step = Check

  val inv =
    lastDecision != "accepted"
      or all {
        lastCase.targetVersion > 0,
        appliedStreamId == lastCase.targetVersion,
        not(localMetadataAllocated),
      }

  run committedTargetIsAccepted =
    true.expect(expectedDecision({ targetVersion: 43 }, 43, false) == "accepted")

  run localMetadataAllocationRejected =
    true.expect(expectedDecision({ targetVersion: 43 }, 44, true) == "rejected_local_metadata")

  run targetMismatchRejected =
    true.expect(expectedDecision({ targetVersion: 43 }, 44, false) == "rejected_target_mismatch")
}
