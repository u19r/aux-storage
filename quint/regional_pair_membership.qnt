/// One durable catalog transition per action. Pair identity is fresh across
/// replacement, even when the same named peer returns. No native claim.
module regional_pair_membership_model {
  const BROKEN: str
  type Packet = { pair: int, scan: bool }
  type State = { phase: str, pair: int, packets: Set[Packet], acceptedOld: bool,
    occupied: Set[int], rejected: Set[str], replaced: bool }
  var s: State
  action init = s' = { phase: "Joining", pair: 1, packets: Set(),
    acceptedOld: false, occupied: Set(0, 1), rejected: Set(), replaced: false }
  action send(scan: bool): bool = all { s.phase != "Absent",
    s' = s.with("packets", s.packets.union(Set({ pair: s.pair, scan: scan }))) }
  action activate = all { s.phase == "Joining", s' = s.with("phase", "Active") }
  action leave = all { s.phase == "Active", s' = s.with("phase", "Leaving") }
  action finishRemoval = all { s.phase == "Leaving", s' = s.with("phase", "Absent").with("occupied", Set(0)) }
  action add = all { s.phase == "Absent", s.pair == 1,
    s' = s.with("phase", "Joining").with("pair", 2).with("replaced", true).with("occupied", Set(0, 2)) }
  action rejectThird = all { s.phase != "Absent",
    s' = s.with("rejected", s.rejected.union(Set(s.phase)))
      .with("occupied", if (BROKEN == "activeOnly" and s.phase != "Active")
        s.occupied.union(Set(3)) else s.occupied) }
  action receive(packet: Packet): bool = {
    val admitted = s.phase != "Absent" and
      (packet.pair == s.pair or BROKEN == "staleIdentity") and
      (not(packet.scan) or s.phase == "Joining" or BROKEN == "lateScan")
    all { s.packets.contains(packet), s' = s.with("acceptedOld", s.acceptedOld or
      (admitted and (packet.pair != s.pair or (packet.scan and s.phase != "Joining")))) }
  }
  action step = { nondet scan = Set(true, false).oneOf()
    nondet pair = Set(1, 2).oneOf()
    any { send(scan), activate, leave, finishRemoval, add, rejectThird, receive({ pair: pair, scan: scan }) } }
  val inv = s.occupied.size() <= 2 and not(s.acceptedOld)
  val rejectedEveryPhase = s.rejected == Set("Joining", "Active", "Leaving") and inv
  val replacedWithOldPackets = s.replaced and s.packets.exists(p => p.pair == 1 and p.scan)
    and s.packets.exists(p => p.pair == 1 and not(p.scan)) and inv
  run hardTwoTest = init.then(rejectThird).then(activate).then(rejectThird)
    .then(leave).then(rejectThird).expect(rejectedEveryPhase)
  run replacementIdentityTest = init.then(send(true)).then(send(false)).then(activate)
    .then(leave).then(finishRemoval).then(add).then(receive({ pair: 1, scan: true }))
    .then(receive({ pair: 1, scan: false }))
    .expect(replacedWithOldPackets)
}
module regional_pair_membership { import regional_pair_membership_model(BROKEN = "none").* }
module regional_pair_membership_activeOnly { import regional_pair_membership_model(BROKEN = "activeOnly").* }
module regional_pair_membership_staleIdentity { import regional_pair_membership_model(BROKEN = "staleIdentity").* }
module regional_pair_membership_lateScan { import regional_pair_membership_model(BROKEN = "lateScan").* }
