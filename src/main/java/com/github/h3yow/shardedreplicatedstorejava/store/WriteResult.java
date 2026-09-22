package com.github.h3yow.shardedreplicatedstorejava.store;

/** 写入结果：结论 + 位点/版本 + 判定依据说明。 */
public record WriteResult(WriteStatus status, String shardId, String requestId,
                          long logIndex, long version, int acks, int quorum,
                          String decisionReason) {
}
