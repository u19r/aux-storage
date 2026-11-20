/// Self-contained history entries. Identities are oracle labels, not item revisions
/// or a native-position allocator. Provider position models own ordering proof.
module write_simplify_model {
  const BROKEN: str
  type Entry = { id: int, born: int, oldImage: int, newImage: int }
  type State = {
    now: int, duration: int, epoch: int, image: int, entries: Set[Entry],
    expected: Set[Entry], retired: Set[int], systemRemoved: Set[int],
    pending: Set[int], pendingEpoch: int, prepared: bool, baselineCount: int,
    foreverDeleted: bool,
  }
  pure val FOREVER = 99
  pure val initial: State = {
    now: 0, duration: 2, epoch: 0, image: 0, entries: Set(), expected: Set(),
    retired: Set(), systemRemoved: Set(), pending: Set(), pendingEpoch: 0,
    prepared: false, baselineCount: 0, foreverDeleted: false,
  }
  var s: State
  action init = s' = initial
  pure def writeEntry(st: State, image: int, captured: int): State = {
    val id = st.expected.size() + 1
    val entry = { id: id, born: captured, oldImage: st.image, newImage: image }
    val stored = if (BROKEN == "image") entry.with("oldImage", 0) else entry
    st.with("image", image).with("entries", st.entries.union(Set(stored)))
      .with("expected", st.expected.union(Set(entry)))
  }
  pure def expired(st: State): Set[int] = st.entries.filter(e =>
    st.duration != FOREVER and e.born < st.now - st.duration).map(e => e.id)
  pure def prepare(st: State): State = {
    val ids = expired(st)
    val selected = if (BROKEN == "prefix" and ids.size() > 0)
      st.entries.filter(e => ids.exists(id => e.id <= id)).map(e => e.id) else ids
    st.with("pending", selected).with("pendingEpoch", st.epoch).with("prepared", true)
  }
  pure def trim(st: State): State = {
    val ids = if (BROKEN == "policy" or st.pendingEpoch == st.epoch) st.pending else Set()
    st.with("entries", st.entries.filter(e => not(ids.contains(e.id))))
      .with("retired", st.retired.union(ids))
      .with("foreverDeleted", st.foreverDeleted or (st.duration == FOREVER and ids.size() > 0))
      .with("prepared", false).with("pending", Set())
  }
  pure def systemTrim(st: State): State = {
    val ids = st.entries.filter(e => e.born < st.now - 1).map(e => e.id)
    st.with("systemRemoved", st.systemRemoved.union(ids))
      .with("entries", if (BROKEN == "system") st.entries.filter(e => not(ids.contains(e.id))) else st.entries)
  }
  val exactImages = s.entries.subseteq(s.expected)
  val retainedEntries = s.expected.filter(e => not(s.retired.contains(e.id))).subseteq(s.entries)
  val retirementAge = s.retired.forall(id => s.expected.exists(e => e.id == id and
    e.born < s.now - 2))
  val foreverSafe = not(s.foreverDeleted)
  val inv = all { exactImages, retainedEntries, retirementAge, foreverSafe }
  val baseline_witness = s.baselineCount > 0 and s.expected.size() == 0
  val retained_old_image_witness = s.entries.exists(e => e.oldImage > 0)
  val captured_time_inversion_witness = s.expected.exists(a => s.expected.exists(b =>
    a.id < b.id and a.born > b.born))
  val independent_retention_witness = s.retired.exists(id => s.entries.exists(e => e.id < id))
  val system_visibility_witness = s.systemRemoved.size() > 0 and s.entries.size() > 0
  val forever_old_rows_witness = s.duration == FOREVER and s.entries.exists(e => e.born < s.now - 2)
  val stale_work_witness = s.prepared and s.pending.size() > 0 and s.pendingEpoch != s.epoch
  action Write = {
    nondet image = Set(0, 1, 2).oneOf()
    nondet captured = 0.to(s.now).oneOf()
    all { s.expected.size() < 5, image != 0 or s.image != 0, s' = writeEntry(s, image, captured) }
  }
  action Baseline = all { s.baselineCount < 2, s' = s.with("image", 2).with("baselineCount", s.baselineCount + 1) }
  action Tick = all { s.now < 8, s' = s.with("now", s.now + 1) }
  action Policy = s' = s.with("duration", FOREVER).with("epoch", s.epoch + 1)
  action Prepare = all { not(s.prepared), s' = prepare(s) }
  action Trim = all { s.prepared, s' = trim(s) }
  action SystemTrim = s' = systemTrim(s)
  action step = any { Write, Baseline, Tick, Policy, Prepare, Trim, SystemTrim }
  run baselineHasNoHistoryTest = all {
    assert(initial.with("image", 2).entries.size() == 0),
    assert(initial.with("image", 2).expected.size() == 0),
  }
  run independentImagesTest = {
    val first = writeEntry(initial, 1, 0)
    val second = writeEntry(first, 2, 0)
    val retained = second.entries.filter(e => e.id == 2)
    all { assert(retained.size() == 1), assert(retained.forall(e => e.oldImage == 1 and e.newImage == 2)) }
  }
}
module write_simplify { import write_simplify_model(BROKEN = "none").* }
module write_simplify_missing_image { import write_simplify_model(BROKEN = "image").* }
module write_simplify_system_delete { import write_simplify_model(BROKEN = "system").* }
module write_simplify_prefix_delete { import write_simplify_model(BROKEN = "prefix").* }
module write_simplify_stale_policy { import write_simplify_model(BROKEN = "policy").* }
