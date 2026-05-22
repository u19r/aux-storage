module stream_partition_split_tests {
  import stream_partition_split.* from "stream_partition_split"

  run initialFamilyCoversEveryHashOnce =
    true.expect(hashCoveredExactlyOnce(baseFamily))

  run splitPreservesHashCoverage =
    true.expect(hashCoveredExactlyOnce(splitPartition(baseFamily, 0, 10)))

  run initialFamilyPreservesItemLevelTotalOrdering =
    true.expect(totalOrderingPreservedAtItemLevel(baseFamily))

  run splitPreservesItemLevelTotalOrdering =
    true.expect(totalOrderingPreservedAtItemLevel(splitPartition(baseFamily, 0, 10)))

  run splitRoutesParentRangeAcrossChildren =
    true
      .expect(routeKey(splitPartition(baseFamily, 0, 10), 1) == Set(2))
      .expect(routeKey(splitPartition(baseFamily, 0, 10), 5) == Set(3))
      .expect(routeKey(splitPartition(baseFamily, 0, 10), 12) == Set(1))

  run splitLeavesOneClosedParentAndTwoOpenChildren =
    true
      .expect(splitPartition(baseFamily, 0, 10).partitions.get(0).state == "write_closed")
      .expect(splitPartition(baseFamily, 0, 10).partitions.get(2).state == "open")
      .expect(splitPartition(baseFamily, 0, 10).partitions.get(3).state == "open")
      .expect(openCount(splitPartition(baseFamily, 0, 10)) == 3)

  run forwardFanInKeepsParentBeforeBoundary =
    true.expect(forwardReadIds(splitPartition(baseFamily, 0, 10), 9).contains(0))

  run forwardFanInDropsParentAfterBoundary =
    true.expect(not(forwardReadIds(splitPartition(baseFamily, 0, 10), 10).contains(0)))

  run reverseFanInKeepsChildrenAfterBoundary =
    true
      .expect(reverseReadIds(splitPartition(baseFamily, 0, 10), 11).contains(2))
      .expect(reverseReadIds(splitPartition(baseFamily, 0, 10), 11).contains(3))

  run reverseFanInDropsChildrenAtBoundary =
    true
      .expect(not(reverseReadIds(splitPartition(baseFamily, 0, 10), 10).contains(2)))
      .expect(not(reverseReadIds(splitPartition(baseFamily, 0, 10), 10).contains(3)))

  run hotSingleRoutingBucketDoesNotAutosplit =
    true.expect(not(canAutosplit(hotFamily.config, SPLIT_THRESHOLD, openCount(hotFamily), 1)))

  run hotDiverseRoutingBucketsCanAutosplit =
    true.expect(canAutosplit(hotFamily.config, SPLIT_THRESHOLD, openCount(hotFamily), 2))

  run freezeBlocksAutosplit =
    true.expect(
      not(
        canAutosplit(
          hotFamily.config.with("freeze", true),
          SPLIT_THRESHOLD,
          openCount(hotFamily),
          2,
        )
      )
    )

  run cooldownBlocksAutosplit =
    true.expect(
      not(
        canAutosplit(
          hotFamily.config.with("cooldownActive", true),
          SPLIT_THRESHOLD,
          openCount(hotFamily),
          2,
        )
      )
    )
}
