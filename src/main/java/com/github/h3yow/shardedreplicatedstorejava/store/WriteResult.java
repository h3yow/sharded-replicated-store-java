package com.github.h3yow.shardedreplicatedstorejava.store;

/**
 * 写入结果：结论 + 位点/版本 + 判定依据说明。
 *
 * @param contestPosition 该请求在同键决胜批中的序号（0 起，按 requestId 升序）；
 *                        非决胜（单个/批量/失败）路径为 -1
 * @param contestSize     该请求所属决胜批的竞争者总数；不在决胜批中为 0
 * @param winnerRequestId 决胜批最终赢家的 requestId；非决胜路径为 {@code null}。
 *                        赢家自己 {@code winnerRequestId().equals(requestId())}
 */
public record WriteResult(WriteStatus status, String shardId, String requestId,
                          long logIndex, long version, int acks, int quorum,
                          int contestPosition, int contestSize, String winnerRequestId,
                          String decisionReason) {

    /** 非决胜路径（单个写 / 批量写 / 失败）使用的便捷构造。 */
    public WriteResult(WriteStatus status, String shardId, String requestId,
                       long logIndex, long version, int acks, int quorum,
                       String decisionReason) {
        this(status, shardId, requestId, logIndex, version, acks, quorum,
                -1, 0, null, decisionReason);
    }
}
