package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.cluster.Cluster;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.model.RecoveryReport;
import com.github.h3yow.shardedreplicatedstorejava.tests.support.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 日志损坏场景（针对 r0 领导日志，3 副本）：
 * - 校验和不符（位翻转）：LOG_CHECKSUM_MISMATCH；
 * - 物理截断（半截）：LOG_TRUNCATED；
 * - 缺号（index 跳号）：LOG_INDEX_GAP。
 * 恢复都必须停在最后一个一致位点、截断损坏尾部，并按多数派决定提交位点；
 * 其余两个完好副本上若多数派仍持有后续条目，则该条目仍为已确认。
 */
class LogRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(LogRecoveryTest.class);

    private Path root;
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        root = TestKit.newTempDir();
        cluster = new Cluster(root);
        cluster.bootstrap(TestKit.topology(1, 1));
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    private String writeFive() {
        for (int i = 1; i <= 5; i++) {
            cluster.write("rec-" + i, "k", "v" + i, LogEntry.Operation.PUT);
        }
        // 决胜规则：requestId 字典序最大为最终版本 -> rec-5
        return "rec-5";
    }

    @Test
    @DisplayName("校验和损坏：第 4 行 checksum 翻转 -> 停在位点 3，原因可区分")
    void checksumCorruptionStopsAtConsistentPoint() {
        writeFive();
        Path leaderLog = TestKit.logFile(root, 1, "s0", "r0");
        TestKit.corruptChecksum(leaderLog, 3); // 0 基：第 4 条

        RecoveryReport report = cluster.restart();
        assertTrue(report.corruptionDetected());
        assertTrue(report.corruptionReason().contains("LOG_CHECKSUM_MISMATCH"),
                "实际: " + report.corruptionReason());
        assertEquals(3L, report.lastConsistentIndex().get("r0/s0"));
        // r1/r2 完好且多数派仍持有全部 5 条，最终版本不变
        assertEquals(Optional.of("v5"), cluster.read("k"));
        log.info("[位点] 校验和损坏 r0 停在 3；多数派提交位点={} 最终版本依据=requestId 最大(rec-5)，判定={}",
                cluster.coordinator("s0").commitIndex(), report.corruptionReason());
    }

    @Test
    @DisplayName("物理截断：日志只剩 2 行 -> 领导停在位点 2，仍由多数派保住后续数据")
    void truncationStopsAtConsistentPoint() {
        writeFive();
        Path leaderLog = TestKit.logFile(root, 1, "s0", "r0");
        TestKit.truncateLines(leaderLog, 2);

        RecoveryReport report = cluster.restart();
        // 2 个完整行不算“内容损坏”，但领导位点落后；跟随者多数派持有 1..5
        assertEquals(2L, report.lastConsistentIndex().get("r0/s0"));
        assertEquals(5L, report.lastConsistentIndex().get("r1/s0"));
        assertEquals(5L, cluster.coordinator("s0").commitIndex(),
                "r1/r2 两个副本仍构成多数派，提交位点应为 5");
        assertEquals(Optional.of("v5"), cluster.read("k"));
        // 恢复重建多数派提交位点时，权威副本自动把落后领导补齐
        var leaderAfter = cluster.coordinator("s0").replicas().stream()
                .filter(r -> r.replicaId().equals("r0")).findFirst().orElseThrow();
        assertEquals(5L, leaderAfter.lastIndex(), "恢复时领导应已自动追赶到提交位点");
        log.info("[位点] 截断恢复：r0=2 r1=5 r2=5 -> commitIndex=5，恢复期间 r0 自动补齐至 5");
    }

    @Test
    @DisplayName("缺号：第 3 条 index 被改成 4 -> LOG_INDEX_GAP，停在位点 2")
    void indexGapDetected() {
        writeFive();
        Path leaderLog = TestKit.logFile(root, 1, "s0", "r0");
        TestKit.rewriteIndex(leaderLog, 2, 4); // 第 3 条 index 3 -> 4，形成 1,2,4 跳号

        RecoveryReport report = cluster.restart();
        assertTrue(report.corruptionDetected());
        assertTrue(report.corruptionReason().contains("LOG_INDEX_GAP"),
                "实际: " + report.corruptionReason());
        assertEquals(2L, report.lastConsistentIndex().get("r0/s0"));
        // 关键保证：损坏点之后的 3 条没有被静默信任，扫描阶段 r0 停在 2 并截断；
        // 随后恢复流程从健康多数派（r1/r2）以追赶方式重新复制 3..5，而不是直接跳过读取
        assertEquals(2L, report.lastConsistentIndex().get("r0/s0"),
                "扫描报告必须记录 r0 停在损坏前的最后一致位点 2");
        var leader = cluster.coordinator("s0").replicas().stream()
                .filter(r -> r.replicaId().equals("r0")).findFirst().orElseThrow();
        assertEquals(5L, leader.lastIndex(),
                "r0 被截断后由健康多数派重新追赶至提交位点（非静默跳过）");
        assertEquals(Optional.of("v5"), cluster.read("k"), "多数派仍持有完整数据");
        log.info("[位点] 缺号恢复：扫描停点 r0=2（判定依据={}，尾部先截断再从 r1/r2 重拷贝），多数派 commitIndex={}",
                report.corruptionReason(), cluster.coordinator("s0").commitIndex());
    }

    @Test
    @DisplayName("多数派同时损坏尾部：提交位点必须回退到最后多数派一致位点")
    void quorumLossRollsBackCommit() {
        writeFive();
        // r0 与 r1 都只保留前 2 条（模拟两台机器崩溃丢尾部），只剩 r2 持有 3..5
        TestKit.truncateLines(TestKit.logFile(root, 1, "s0", "r0"), 2);
        TestKit.truncateLines(TestKit.logFile(root, 1, "s0", "r1"), 2);

        RecoveryReport report = cluster.restart();
        assertEquals(2L, cluster.coordinator("s0").commitIndex(),
                "位点 3 只在 1 个副本上，不足多数派，必须回退到 2");
        assertEquals(Optional.of("v2"), cluster.read("k"),
                "回退后最终版本应为 rec-2 对应值 v2");
        // r2 的未确认尾部必须被截断，不会重放
        var r2 = cluster.coordinator("s0").replicas().stream()
                .filter(r -> r.replicaId().equals("r2")).findFirst().orElseThrow();
        assertEquals(2L, r2.lastIndex());
        log.info("[位点] 多数派丢失：提交位点回退 5 -> 2，最终版本=rec-2，r2 未确认尾部已截断，损坏={}",
                report.corruptionDetected());
    }
}
