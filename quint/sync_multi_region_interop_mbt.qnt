/// Compact MBT surface for sync-backed multi-region interop.
///
/// Models the failure found while wiring the simulation harness: a sync-backed
/// region may be standby for outbound async sender ownership, but inbound
/// multi-region apply still has to accept fresh remote mutations and converge.
module sync_multi_region_interop_mbt {
  type InteropCase = {
    syncRole: str,
    currentVersion: int,
    incomingVersion: int,
  }

  var lastCase: InteropCase
  var lastOutboundDecision: str
  var lastInboundDecision: str
  var lastStoredVersion: int

  pure def outboundDecision(role) =
    if (role == "leader") {
      "owns_sender"
    } else {
      "standby"
    }

  pure def inboundApplyDecision(currentVersion, incomingVersion) =
    if (incomingVersion > currentVersion) {
      "apply"
    } else {
      "skip_stale"
    }

  pure def storedVersionAfterInbound(currentVersion, incomingVersion) =
    if (inboundApplyDecision(currentVersion, incomingVersion) == "apply") {
      incomingVersion
    } else {
      currentVersion
    }

  action init = all {
    lastCase' = {
      syncRole: "disabled",
      currentVersion: 0,
      incomingVersion: 0,
    },
    lastOutboundDecision' = "not_checked",
    lastInboundDecision' = "not_checked",
    lastStoredVersion' = 0,
  }

  action Check = {
    nondet syncRole = Set("disabled", "learner", "follower", "candidate", "leader").oneOf()
    nondet currentVersion = Set(0, 1, 2).oneOf()
    nondet incomingVersion = Set(0, 1, 2, 3).oneOf()
    all {
      lastCase' = {
        syncRole: syncRole,
        currentVersion: currentVersion,
        incomingVersion: incomingVersion,
      },
      lastOutboundDecision' = outboundDecision(syncRole),
      lastInboundDecision' = inboundApplyDecision(currentVersion, incomingVersion),
      lastStoredVersion' = storedVersionAfterInbound(currentVersion, incomingVersion),
    }
  }

  action step = Check

  val inv =
    all {
      lastOutboundDecision != "owns_sender" or lastCase.syncRole == "leader",
      lastInboundDecision != "apply" or lastCase.incomingVersion > lastCase.currentVersion,
      lastStoredVersion >= lastCase.currentVersion,
      lastStoredVersion >= lastCase.incomingVersion
        or lastCase.incomingVersion <= lastCase.currentVersion,
    }

  run syncFollowerStandsByForOutboundSender =
    true.expect(outboundDecision("follower") == "standby")

  run syncFollowerAcceptsFreshInboundApply =
    true.expect(all {
      outboundDecision("follower") == "standby",
      inboundApplyDecision(1, 2) == "apply",
      storedVersionAfterInbound(1, 2) == 2,
    })

  run syncLeaderOwnsOutboundSender =
    true.expect(outboundDecision("leader") == "owns_sender")

  run staleInboundApplyIsSkipped =
    true.expect(all {
      inboundApplyDecision(2, 1) == "skip_stale",
      storedVersionAfterInbound(2, 1) == 2,
    })
}
