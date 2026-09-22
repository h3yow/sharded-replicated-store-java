package com.github.h3yow.shardedreplicatedstorejava.store;

/** 拓扑非法的可区分原因。 */
public enum TopologyViolation {
    EMPTY_TOPOLOGY,
    DUPLICATE_SHARD_ID,
    RANGE_OUT_OF_BOUNDS,
    RANGE_INVALID_BOUNDARY,
    OVERLAPPING_RANGES,
    UNCOVERED_HASH_GAP,
    REPLICA_SET_INVALID
}
