package com.github.h3yow.shardedreplicatedstorejava.store;

/**
 * 一次写入的对外结论，失败与“决胜落败”必须可区分。
 *
 * <ul>
 *   <li>{@link #COMMITTED}：多数派确认；在普通并发决胜路径上，还表示<strong>本请求
 *       就是同批同键决胜的最终赢家</strong>（它的值就是当前对外可见值）。</li>
 *   <li>{@link #SUPERSEDED}：本请求已多数派落盘（拿到自己确定的 index/version），
 *       但在同一决胜批中被 requestId 更大的同键提交覆盖，<strong>不是最终可见值</strong>。
 *       客户端据此可把“我成功了”和“最终值是我写的”区分开，不会有人拿着成功却读到
 *       别人的值而不自知。</li>
 *   <li>{@link #DUPLICATE}：同一 requestId 重复提交，返回首次结论（含首次的成败/落败）。</li>
 *   <li>{@link #INSUFFICIENT_ACKS}：副本在线但本轮确认数不足多数派，未提交、不可见。</li>
 *   <li>{@link #REPLICA_UNAVAILABLE}：leader 不可用，或在线副本数本身凑不够多数派。</li>
 * </ul>
 */
public enum WriteStatus {
    COMMITTED,
    SUPERSEDED,
    DUPLICATE,
    INSUFFICIENT_ACKS,
    REPLICA_UNAVAILABLE
}
