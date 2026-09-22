package com.github.h3yow.shardedreplicatedstorejava.model;

/**
 * 分片日志中的一条条目。index 在每个分片内从 1 开始严格连续递增。
 * requestId 用于幂等去重；同一 requestId 的内容必须完全一致。
 */
public record LogEntry(long index, long term, String requestId, String key,
                       String value, Operation op, long checksum) {
    public enum Operation { PUT, DELETE }
}
