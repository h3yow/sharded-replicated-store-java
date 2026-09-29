package com.github.h3yow.shardedreplicatedstorejava.store;

/**
 * 写入结果：结论 + 位点/版本 + 判定依据说明。
 *
 * @param key 该请求写入的键（再平衡时按 key 归属迁移 requestId 首次结论）
 */
public record WriteResult(WriteStatus status, String shardId, String requestId,
                          String key, long logIndex, long version, int acks,
                          int quorum, String decisionReason) {

    /** 兼容构造：不带 key（key 未知时用于包装/中间结果）。 */
    public WriteResult(WriteStatus status, String shardId, String requestId,
                       long logIndex, long version, int acks, int quorum,
                       String decisionReason) {
        this(status, shardId, requestId, null, logIndex, version, acks, quorum,
                decisionReason);
    }
}
