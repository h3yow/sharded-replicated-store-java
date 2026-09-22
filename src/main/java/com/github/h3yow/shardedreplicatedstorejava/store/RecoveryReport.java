package com.github.h3yow.shardedreplicatedstorejava.store;

/** 一次日志恢复的结论：恢复停在哪个位点、应用了多少条、是否因损坏而停止。 */
public record RecoveryReport(String shardId, String replicaId,
                             long lastConsistentIndex, int appliedCount,
                             boolean clean, LogCorruptionReason stopReason) {
}
