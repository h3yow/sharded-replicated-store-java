package com.github.h3yow.shardedreplicatedstorejava.store;

/** 一次写入的对外结论，失败、落败与重复都必须可区分。 */
public enum WriteStatus {
    /** 多数派确认，对外可见。 */
    COMMITTED,
    /** 同一 requestId 重复提交，返回该 requestId 的首次结论。 */
    DUPLICATE,
    /** 副本在线但本轮确认数不足多数派，条目未提交、不可见。 */
    INSUFFICIENT_ACKS,
    /** leader 不可用或在线副本数本身凑不够多数派。 */
    REPLICA_UNAVAILABLE,
    /**
     * 同一键的并发决胜中落败：本批存在 requestId 更大的提交，本提交不复制、
     * 不占版本、不对外可见。客户端据此可与 COMMITTED 区分，不会“拿到成功却读到
     * 别人的值”。
     */
    SUPERSEDED
}
