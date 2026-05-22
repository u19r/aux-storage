/// Compact MBT surface for sync crash/reopen recovery boundaries.
///
/// The model records the client-visible recovery rule for four crash points:
/// after request admission, after Raft append before apply, after apply before
/// response, and follower crash during catchup.
module sync_crash_recovery_mbt {
  type Gate = {
    boundary: str,
    entryCommitted: bool,
    applyDurable: bool,
    responseSent: bool,
    catchupCheckpointDurable: bool,
  }

  var lastGate: Gate
  var lastDecision: str

  pure def expectedDecision(gate) =
    if (gate.boundary == "after_admission_before_append") {
      "retry_no_acknowledgement"
    } else if (gate.boundary == "after_raft_append_before_apply") {
      if (gate.entryCommitted) {
        "replay_committed_entry"
      } else {
        "retry_no_acknowledgement"
      }
    } else if (gate.boundary == "after_apply_before_response") {
      if (gate.applyDurable) {
        "return_durable_response"
      } else if (gate.entryCommitted) {
        "replay_committed_entry"
      } else {
        "retry_no_acknowledgement"
      }
    } else {
      if (gate.catchupCheckpointDurable) {
        "resume_catchup_from_checkpoint"
      } else {
        "restart_catchup_chunk_idempotently"
      }
    }

  action init = all {
    lastGate' = {
      boundary: "after_admission_before_append",
      entryCommitted: false,
      applyDurable: false,
      responseSent: false,
      catchupCheckpointDurable: false,
    },
    lastDecision' = "not_checked",
  }

  action Check = {
    nondet boundary = Set(
      "after_admission_before_append",
      "after_raft_append_before_apply",
      "after_apply_before_response",
      "follower_crash_during_catchup"
    ).oneOf()
    nondet entryCommitted = Set(false, true).oneOf()
    nondet applyDurable = Set(false, true).oneOf()
    nondet responseSent = Set(false, true).oneOf()
    nondet catchupCheckpointDurable = Set(false, true).oneOf()
    val gate = {
      boundary: boundary,
      entryCommitted: entryCommitted,
      applyDurable: applyDurable,
      responseSent: responseSent,
      catchupCheckpointDurable: catchupCheckpointDurable,
    }
    all {
      lastGate' = gate,
      lastDecision' = expectedDecision(gate),
    }
  }

  action step = Check

  val inv =
    all {
      lastDecision != "return_durable_response" or lastGate.applyDurable,
      lastDecision != "replay_committed_entry" or lastGate.entryCommitted,
      lastDecision != "resume_catchup_from_checkpoint" or lastGate.catchupCheckpointDurable,
    }

  run admissionCrashHasNoAck =
    true.expect(expectedDecision({
      boundary: "after_admission_before_append",
      entryCommitted: true,
      applyDurable: true,
      responseSent: false,
      catchupCheckpointDurable: true,
    }) == "retry_no_acknowledgement")

  run committedAppendCrashReplays =
    true.expect(expectedDecision({
      boundary: "after_raft_append_before_apply",
      entryCommitted: true,
      applyDurable: false,
      responseSent: false,
      catchupCheckpointDurable: false,
    }) == "replay_committed_entry")

  run appliedBeforeResponseReturnsDurableResponse =
    true.expect(expectedDecision({
      boundary: "after_apply_before_response",
      entryCommitted: true,
      applyDurable: true,
      responseSent: false,
      catchupCheckpointDurable: false,
    }) == "return_durable_response")

  run catchupCrashWithoutCheckpointReplaysChunk =
    true.expect(expectedDecision({
      boundary: "follower_crash_during_catchup",
      entryCommitted: false,
      applyDurable: false,
      responseSent: false,
      catchupCheckpointDurable: false,
    }) == "restart_catchup_chunk_idempotently")
}
