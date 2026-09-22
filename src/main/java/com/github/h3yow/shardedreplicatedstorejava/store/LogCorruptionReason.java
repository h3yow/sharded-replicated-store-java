package com.github.h3yow.shardedreplicatedstorejava.store;

/** 日志损坏的可区分原因。 */
public enum LogCorruptionReason {
    TRUNCATED_ENTRY,
    MISSING_INDEX,
    INDEX_OUT_OF_ORDER,
    CHECKSUM_MISMATCH,
    MALFORMED_LINE
}
