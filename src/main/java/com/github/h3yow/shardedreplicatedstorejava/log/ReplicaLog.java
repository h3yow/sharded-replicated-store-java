package com.github.h3yow.shardedreplicatedstorejava.log;

import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;

import java.util.List;
import java.util.Optional;

/**
 * 单个副本上单个分片的持久化日志（本地文件模拟）。
 */
public interface ReplicaLog {
    long lastIndex();

    Optional<LogEntry> entryAt(long index);

    List<LogEntry> entriesAfter(long index);

    /** 幂等追加：同 requestId 已存在则返回既有条目，不重复写入。 */
    LogEntry append(LogEntry entry);

    /** 按 requestId 查找既有条目（幂等重试判定用）。 */
    Optional<LogEntry> findByRequestId(String requestId);

    void truncateAfter(long index);

    /** 启动时校验：返回最后一个连续一致位点，发现损坏停止并记录原因。 */
    long verifyAndRecover();

    String corruptionReason();

    void close();
}
