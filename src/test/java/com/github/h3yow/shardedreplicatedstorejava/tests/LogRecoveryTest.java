package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.store.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重启日志重放与损坏恢复：
 * 截断、缺号、乱序、校验和损坏都不得被静默跳过，恢复必须停在最后一个一致位点；
 * 损坏副本随后可从 leader 追赶，重新对齐并追上最新提交。
 */
class LogRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(LogRecoveryTest.class);

    @TempDir
    Path tempDir;

    private Cluster cluster3Replicas() throws Exception {
        int max = KeyHasher.HASH_SPACE;
        Topology topology = new Topology(1,
                List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2")));
        return new Cluster(tempDir, topology);
    }

    /** 提交 5 个版本，返回每个版本的值。 */
    private List<String> publishFiveVersions(Cluster cluster) {
        List<String> values = new java.util.ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            String v = "v" + i;
            WriteResult r = cluster.write("req-" + i, "k", v);
            assertThat(r.status()).isEqualTo(WriteStatus.COMMITTED);
            assertThat(r.version()).isEqualTo(i);
            values.add(v);
        }
        return values;
    }

    @Test
    void cleanReplayRebuildsExactState() throws Exception {
        Cluster cluster = cluster3Replicas();
        publishFiveVersions(cluster);

        List<RecoveryReport> reports = cluster.restartAll();
        assertThat(reports).hasSize(3);
        assertThat(reports).allMatch(RecoveryReport::clean);
        assertThat(reports).allMatch(r -> r.lastConsistentIndex() == 5);
        assertThat(cluster.read("k").value()).isEqualTo("v5");
        assertThat(cluster.read("k").version()).isEqualTo(5);
        for (RecoveryReport r : reports) {
            log.info("[位点/版本] 干净重放 {}/{}: lastConsistentIndex={} applied={} 可见值=v5/version=5",
                    r.shardId(), r.replicaId(), r.lastConsistentIndex(), r.appliedCount());
        }
    }

    @Test
    void truncatedLastEntryStopsRecoveryAtLastConsistentIndex() throws Exception {
        Cluster cluster = cluster3Replicas();
        publishFiveVersions(cluster);
        ShardReplicaSet set = cluster.shard("s1");
        Replica victim = set.replica("r1");

        String raw = victim.rawLogTextForTest();
        int lastNl = raw.lastIndexOf('\n', raw.length() - 2);
        // 第 5 行完整保留内容但去掉结尾换行符 -> 扫描器判定为写入截断，
        // 而不是把半行解析成畸形结构。
        String truncated = raw.substring(0, lastNl + 1)
                + raw.substring(lastNl + 1).replaceAll("\\n$", "");
        victim.overwriteRawLogForTest(truncated);

        RecoveryReport report = victim.recover();
        assertThat(report.clean()).isFalse();
        assertThat(report.stopReason()).isEqualTo(LogCorruptionReason.TRUNCATED_ENTRY);
        assertThat(report.lastConsistentIndex()).isEqualTo(4);
        // 停点为 index=4，且 commit.txt 为 5 时可见位点取 min(4,5)=4 -> 不展示第5版
        assertThat(victim.get("k").version()).isEqualTo(4);
        assertThat(victim.get("k").value()).isEqualTo("v4");
        log.warn("[位点/版本] 截断日志: lastConsistentIndex={} 可见 version=4(v4) 判定依据=末行无换行符，未静默跳过",
                report.lastConsistentIndex());

        // leader 仍完整，损坏副本追赶后补齐到 index=5
        int appended = cluster.catchUpReplica("s1", "r1");
        assertThat(appended).isEqualTo(1);
        assertThat(victim.get("k").version()).isEqualTo(5);
        assertThat(victim.get("k").value()).isEqualTo("v5");
        RecoveryReport after = victim.recover();
        assertThat(after.clean()).isTrue();
        assertThat(after.lastConsistentIndex()).isEqualTo(5);
        log.info("[位点/版本] 追赶后 lastConsistentIndex={} 可见 version=5(v5) 判定依据=与leader逐index对齐",
                after.lastConsistentIndex());
    }

    @Test
    void missingIndexAndOutOfOrderAreDetectedAtCorrectStopPoint() throws Exception {
        Cluster cluster = cluster3Replicas();
        publishFiveVersions(cluster);
        Replica victim = cluster.shard("s1").replica("r2");

        // 场景 A：删掉第 3 行 -> 期望 index=3 时遇到 index=4，缺号，停点=2
        String[] lines = victim.rawLogTextForTest().split("\n", -1);
        StringBuilder missing = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            if (i == 2) {
                continue;
            }
            missing.append(lines[i]).append('\n');
        }
        victim.overwriteRawLogForTest(missing.toString());
        RecoveryReport missingReport = victim.recover();
        assertThat(missingReport.clean()).isFalse();
        assertThat(missingReport.stopReason()).isEqualTo(LogCorruptionReason.MISSING_INDEX);
        assertThat(missingReport.lastConsistentIndex()).isEqualTo(2);
        log.warn("[位点/版本] 缺号: 恢复停在 index=2 stopReason=MISSING_INDEX（缺失 index=3，未跳过）");

        // 场景 B：构造乱序/重复序列 1,2,2,4,5（重复一条自洽的第 2 行，跳过第 3 行）。
        // 扫描到第三条时期望 index=3、实到 index=2 -> INDEX_OUT_OF_ORDER，停点=2。
        // 直接改 index 文本会先破坏校验和，因此只能用“重复已有自洽行”的方式制造。
        StringBuilder duplicated = new StringBuilder();
        String[] dupOrder = {lines[0], lines[1], lines[1], lines[3], lines[4]};
        for (String line : dupOrder) {
            duplicated.append(line).append('\n');
        }
        victim.overwriteRawLogForTest(duplicated.toString());
        RecoveryReport dupReport = victim.recover();
        assertThat(dupReport.clean()).isFalse();
        assertThat(dupReport.stopReason()).isEqualTo(LogCorruptionReason.INDEX_OUT_OF_ORDER);
        assertThat(dupReport.lastConsistentIndex()).isEqualTo(2);
        log.warn("[位点/版本] 乱序/重复: 序列1,2,2,4,5 在第三条停于 index=2 stopReason=INDEX_OUT_OF_ORDER（未回退应用）");

        // 两种损坏都可通过从 leader 追赶修复；序列停点=2，故补齐 3 条
        int appended = cluster.catchUpReplica("s1", "r2");
        assertThat(appended).isEqualTo(3);
        assertThat(victim.get("k").version()).isEqualTo(5);
        log.info("[位点/版本] 乱序副本追赶补齐 {} 条后 version=5，与 leader 结论一致", appended);
    }

    @Test
    void checksumMismatchIsDetectedAndNeverApplied() throws Exception {
        Cluster cluster = cluster3Replicas();
        publishFiveVersions(cluster);
        Replica victim = cluster.shard("s1").replica("r1");

        // 翻转第 3 行 value 的 Base64 载荷字符但保留校验和 -> CHECKSUM_MISMATCH，停点=2
        String[] lines = victim.rawLogTextForTest().split("\n", -1);
        String badLine = lines[2];
        int valAt = badLine.indexOf("val=");
        char flipped = badLine.charAt(valAt + 4) == 'A' ? 'B' : 'A';
        lines[2] = badLine.substring(0, valAt + 4) + flipped + badLine.substring(valAt + 5);
        victim.overwriteRawLogForTest(String.join("\n", lines) + "\n");

        RecoveryReport report = victim.recover();
        assertThat(report.clean()).isFalse();
        assertThat(report.stopReason()).isEqualTo(LogCorruptionReason.CHECKSUM_MISMATCH);
        assertThat(report.lastConsistentIndex()).isEqualTo(2);
        assertThat(victim.get("k").version()).isEqualTo(2);
        log.warn("[位点/版本] 校验和不匹配: 停点 index=2 version=2，被篡改条目未应用、未跳过");

        int appended = cluster.catchUpReplica("s1", "r1");
        assertThat(appended).isEqualTo(3);
        assertThat(victim.get("k").value()).isEqualTo("v5");
        log.info("[位点/版本] 校验和损坏副本追赶补齐 {} 条后恢复到 v5", appended);
    }

    @Test
    void laggingReplicaCatchesUpAfterDowntime() throws Exception {
        Cluster cluster = cluster3Replicas();
        ShardReplicaSet set = cluster.shard("s1");
        // r2 宕机期间提交 3 个版本（leader+r1 构成多数派）
        set.replica("r2").setStatus(ReplicaStatus.DOWN);
        for (int i = 1; i <= 3; i++) {
            WriteResult r = cluster.write("down-req-" + i, "hot", "hot-v" + i);
            assertThat(r.status()).isEqualTo(WriteStatus.COMMITTED);
            assertThat(r.acks()).isEqualTo(2);
        }
        Replica follower = set.replica("r2");
        assertThat(follower.lastLogIndex()).isEqualTo(0);
        log.info("[位点] r2 宕机期间 leader 已提交 3 条，follower lastLogIndex=0");

        // 恢复后追赶
        follower.setStatus(ReplicaStatus.UP);
        int appended = cluster.catchUpReplica("s1", "r2");
        assertThat(appended).isEqualTo(3);
        assertThat(follower.get("hot").value()).isEqualTo("hot-v3");
        assertThat(follower.get("hot").version()).isEqualTo(3);
        assertThat(follower.commitIndex()).isEqualTo(3);

        // 追赶后立即参与下一次多数派写入
        WriteResult next = cluster.write("req-after-catchup", "hot", "hot-v4");
        assertThat(next.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(next.acks()).isEqualTo(3);
        log.info("[位点/版本] r2 追赶补齐 {} 条并参与提交 hot version=4 index={} acks=3/3",
                appended, next.logIndex());
    }
}
