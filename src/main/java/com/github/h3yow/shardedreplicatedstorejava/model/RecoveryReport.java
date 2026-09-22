package com.github.h3yow.shardedreplicatedstorejava.model;

import java.util.Map;

/**
 * 一次恢复的报告：每个 (副本, 分片) 停在的最后一致位点及是否遇到损坏。
 */
public record RecoveryReport(long generation,
                             Map<String, Long> lastConsistentIndex,
                             boolean corruptionDetected,
                             String corruptionReason) {
}
