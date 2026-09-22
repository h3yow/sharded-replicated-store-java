package com.github.h3yow.shardedreplicatedstorejava.store;

/** 一次写入的对外结论，三类失败必须可区分。 */
public enum WriteStatus {
    COMMITTED,
    DUPLICATE,
    INSUFFICIENT_ACKS,
    REPLICA_UNAVAILABLE
}
