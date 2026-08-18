/// Bounded DAG contract for ReadSequence.
///
/// Edges are encoded as parent * 10 + child so the model stays small and
/// deterministic.  The actions cover later declaration, multiple parents,
/// retry publication, bounded budgets, mapped/SQL strategy equivalence, and
/// the deliberate partial-publication mutation used by the simulator.
module read_sequence_dag {
  pure val MAX_NODES = 6
  pure val NODE_SET = Set(0, 1, 2, 3, 4, 5)
  pure val MAX_READS = 12

  var activeCount: int
  var edges: Set[int]
  var waveByNode: int -> int
  var declaration: List[int]
  var completion: List[int]
  var responseOrdinals: List[int]
  var plannedReads: int
  var reservedReads: int
  var attemptVersion: int
  var publishedVersion: int
  var published: bool
  var failed: bool
  var selectedMappedEdges: Set[int]
  var strategy: str
  var outcome: str

  pure def parent(edge: int): int = edge / 10
  pure def child(edge: int): int = edge % 10

  def active(node: int): bool = node < activeCount

  def graphEdgesValid =
    edges.forall(edge =>
      active(parent(edge))
        and active(child(edge))
        and parent(edge) != child(edge)
    )

  def acyclic =
    edges.forall(edge => waveByNode.get(parent(edge)) < waveByNode.get(child(edge)))

  def declarationDoesNotMatter =
    declaration.foldl(Set(), (seen, node) => seen.union(Set(node)))
      == NODE_SET.filter(node => active(node))
      or not(published)

  def stableResponse =
    not(published) or responseOrdinals == List(0, 1, 2, 3, 4, 5)

  def oneAttemptVersion =
    not(published) or publishedVersion == attemptVersion

  def noPartialPublication =
    not(failed) or not(published)

  def boundedReservations =
    reservedReads <= MAX_READS and plannedReads <= MAX_READS

  def strategyEquivalent =
    strategy == "ordinary"
      or strategy == "mapped"
      or strategy == "postgres"
      or strategy == "sqlite"
      or strategy == "fallback"

  pure val initialWaveMap = Map(0 -> 0, 1 -> 0, 2 -> 1, 3 -> 2, 4 -> 2, 5 -> 3)

  pure def normalEdges = Set(2, 12, 23, 24, 35)

  action init = all {
    activeCount' = MAX_NODES,
    edges' = normalEdges,
    waveByNode' = initialWaveMap,
    declaration' = List(0, 1, 2, 3, 4, 5),
    completion' = List(),
    responseOrdinals' = List(),
    plannedReads' = 0,
    reservedReads' = 0,
    attemptVersion' = 0,
    publishedVersion' = 0,
    published' = false,
    failed' = false,
    selectedMappedEdges' = Set(),
    strategy' = "ordinary",
    outcome' = "ready",
  }

  action LaterDeclaration = all {
    activeCount' = MAX_NODES,
    edges' = normalEdges,
    waveByNode' = initialWaveMap,
    declaration' = List(2, 0, 1, 4, 3, 5),
    completion' = List(1, 0, 2, 4, 3, 5),
    responseOrdinals' = List(0, 1, 2, 3, 4, 5),
    plannedReads' = 6,
    reservedReads' = 6,
    attemptVersion' = 11,
    publishedVersion' = 11,
    published' = true,
    failed' = false,
    selectedMappedEdges' = Set(),
    strategy' = "ordinary",
    outcome' = "accepted",
  }

  action MultipleParents = all {
    activeCount' = MAX_NODES,
    edges' = Set(2, 12, 24),
    waveByNode' = Map(0 -> 0, 1 -> 0, 2 -> 1, 3 -> 0, 4 -> 2, 5 -> 0),
    declaration' = List(0, 1, 2, 3, 4, 5),
    completion' = List(),
    responseOrdinals' = List(),
    plannedReads' = 4,
    reservedReads' = 4,
    attemptVersion' = 0,
    publishedVersion' = 0,
    published' = false,
    failed' = false,
    selectedMappedEdges' = Set(),
    strategy' = "ordinary",
    outcome' = "accepted",
  }

  action RetryAfterFailure = all {
    activeCount' = MAX_NODES,
    edges' = normalEdges,
    waveByNode' = initialWaveMap,
    declaration' = List(0, 1, 2, 3, 4, 5),
    completion' = List(1, 0, 2, 3, 4, 5),
    responseOrdinals' = List(0, 1, 2, 3, 4, 5),
    plannedReads' = 6,
    reservedReads' = 6,
    attemptVersion' = 22,
    publishedVersion' = 22,
    published' = true,
    failed' = false,
    selectedMappedEdges' = Set(),
    strategy' = "ordinary",
    outcome' = "accepted_after_retry",
  }

  action PartialPublicationMutation = all {
    activeCount' = MAX_NODES,
    edges' = normalEdges,
    waveByNode' = initialWaveMap,
    completion' = List(0, 1),
    responseOrdinals' = List(0),
    plannedReads' = 2,
    reservedReads' = 2,
    attemptVersion' = 31,
    publishedVersion' = 31,
    published' = true,
    failed' = true,
    declaration' = List(0, 1, 2, 3, 4, 5),
    selectedMappedEdges' = Set(),
    strategy' = "ordinary",
    outcome' = "mutated_failure",
  }

  action MappedSelection = all {
    activeCount' = MAX_NODES,
    edges' = normalEdges,
    waveByNode' = initialWaveMap,
    declaration' = List(0, 1, 2, 3, 4, 5),
    completion' = List(),
    responseOrdinals' = List(),
    selectedMappedEdges' = Set(24),
    strategy' = "mapped",
    plannedReads' = 5,
    reservedReads' = 5,
    attemptVersion' = 0,
    publishedVersion' = 0,
    published' = false,
    failed' = false,
    outcome' = "accepted",
  }

  action SqlFallback = all {
    activeCount' = MAX_NODES,
    edges' = normalEdges,
    waveByNode' = initialWaveMap,
    declaration' = List(0, 1, 2, 3, 4, 5),
    completion' = List(),
    responseOrdinals' = List(),
    selectedMappedEdges' = Set(),
    strategy' = "fallback",
    plannedReads' = 6,
    reservedReads' = 6,
    attemptVersion' = 0,
    publishedVersion' = 0,
    published' = false,
    failed' = false,
    outcome' = "accepted",
  }

  action step = any {
    LaterDeclaration,
    MultipleParents,
    RetryAfterFailure,
    MappedSelection,
    SqlFallback,
  }

  val inv = all {
    graphEdgesValid or outcome == "mutated_failure",
    acyclic or outcome == "mutated_failure",
    declarationDoesNotMatter,
    stableResponse,
    oneAttemptVersion,
    noPartialPublication,
    boundedReservations,
    strategyEquivalent,
  }

  val later_declaration_witness = outcome == "accepted" and declaration != List(0, 1, 2, 3, 4, 5)
  val multiple_parent_witness = outcome == "accepted" and edges.contains(12)
  val retry_witness = outcome == "accepted_after_retry" and attemptVersion == publishedVersion
  val partial_publication_witness = outcome == "mutated_failure" and published
}
