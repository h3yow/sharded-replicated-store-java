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
 * 写入多数派规则：
 * 多数派确认才可见；确认不足 / 副本不可用 / 重复提交必须被明确区分；
 * 重复提交不得重复应用、回退版本或造成不一致。
 */
class WriteConsensusTest {

    private static final Logger log = LoggerFactory.getLogger(WriteConsensusTest.class);

    @TempDir
    Path tempDir;

    private Cluster singleShardCluster3() throws Exception {
        int max = KeyHasher.HASH_SPACE;
        Topology topology = new Topology(1,
                List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2")));
        return new Cluster(tempDir, topology);
    }

    @Test
    void writeIsVisibleOnlyAfterQuorumAck() throws Exception {
        Cluster cluster = singleShardCluster3();
        ShardReplicaSet set = cluster.shard("s1");

        // 初始全部 UP：3 副本，多数派 2
        WriteResult ok = cluster.write("req-1", "alpha", "one");
        assertThat(ok.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(ok.acks()).isEqualTo(3);
        assertThat(ok.quorum()).isEqualTo(2);
        assertThat(ok.version()).isEqualTo(1);
        assertThat(cluster.read("alpha").value()).isEqualTo("one");
        log.info("[位点/版本] 首次提交 requestId={} index={} version={} acks={}/{} 判定依据={}",
                ok.requestId(), ok.logIndex(), ok.version(), ok.acks(), 3, ok.decisionReason());

        // 1 个副本 DOWN：2/3 仍达多数派，可见
        set.replica("r2").setStatus(ReplicaStatus.DOWN);
        WriteResult stillOk = cluster.write("req-2", "alpha", "two");
        assertThat(stillOk.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(stillOk.acks()).isEqualTo(2);
        assertThat(stillOk.version()).isEqualTo(2);
        assertThat(cluster.read("alpha").value()).isEqualTo("two");
        log.info("[位点/版本] 1副本宕机仍提交 requestId={} index={} version={} acks=2 判定依据={}",
                stillOk.requestId(), stillOk.logIndex(), stillOk.version(),
                stillOk.decisionReason());

        // 2 个副本 DOWN：只剩 leader 1 票 < quorum=2 -> REPLICA_UNAVAILABLE
        set.replica("r1").setStatus(ReplicaStatus.DOWN);
        WriteResult unavailable = cluster.write("req-3", "alpha", "three");
        assertThat(unavailable.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        // 未提交对外不可见：值仍是 two，版本仍是 2
        assertThat(cluster.read("alpha").value()).isEqualTo("two");
        assertThat(cluster.read("alpha").version()).isEqualTo(2);
        log.warn("[判定依据] 2副本宕机 requestId={} status={} acks={} 说明='{}'（写入对外不可见）",
                unavailable.requestId(), unavailable.status(), unavailable.acks(),
                unavailable.decisionReason());

        // r1 在线但慢(SLOW)、r2 宕机(DOWN)：在线副本数=2 够多数派，但本轮只有
        // leader 1 个 ack -> INSUFFICIENT_ACKS（区别于“连投票资格都不够”的不可用）。
        set.replica("r1").setStatus(ReplicaStatus.SLOW);
        WriteResult slow = cluster.write("req-4", "alpha", "four");
        assertThat(slow.status()).isEqualTo(WriteStatus.INSUFFICIENT_ACKS);
        assertThat(slow.acks()).isEqualTo(1);
        assertThat(cluster.read("alpha").value()).isEqualTo("two");
        log.warn("[判定依据] SLOW在线但未确认 requestId={} status={} acks={} 说明='{}'",
                slow.requestId(), slow.status(), slow.acks(), slow.decisionReason());
    }

    @Test
    void distinguishesInsufficientAcksFromReplicaUnavailable() throws Exception {
        // 5 副本，多数派=3
        int max = KeyHasher.HASH_SPACE;
        Topology topology = new Topology(1,
                List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2", "r3", "r4")));
        Cluster cluster = new Cluster(tempDir, topology);
        ShardReplicaSet set = cluster.shard("s1");

        // 3 个 follower SLOW（在线但本轮不确认）：5 副本都在线，但只有
        // leader + 1 个 UP follower 共 2 个 ack < quorum=3 -> INSUFFICIENT_ACKS。
        set.replica("r2").setStatus(ReplicaStatus.SLOW);
        set.replica("r3").setStatus(ReplicaStatus.SLOW);
        set.replica("r4").setStatus(ReplicaStatus.SLOW);
        WriteResult insufficient = cluster.write("req-slow", "k", "v");
        assertThat(insufficient.status()).isEqualTo(WriteStatus.INSUFFICIENT_ACKS);
        assertThat(insufficient.acks()).isEqualTo(2);
        assertThat(cluster.read("k")).isNull();
        log.warn("[判定依据] 5副本中3个SLOW: status={} acks=2/quorum=3 在线数够但确认不足，未提交不可见",
                insufficient.status());

        // 3 个副本 DOWN：在线只剩 2 < 3 -> REPLICA_UNAVAILABLE
        set.replica("r2").setStatus(ReplicaStatus.DOWN);
        set.replica("r3").setStatus(ReplicaStatus.DOWN);
        set.replica("r4").setStatus(ReplicaStatus.DOWN);
        WriteResult unavailable = cluster.write("req-down", "k", "v");
        assertThat(unavailable.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        log.warn("[判定依据] 5副本中3个DOWN: status={} 在线副本数本身不足多数派", unavailable.status());
    }

    @Test
    void duplicateSubmitReturnsFirstOutcomeAndNeverAppliesTwice() throws Exception {
        Cluster cluster = singleShardCluster3();
        ShardReplicaSet set = cluster.shard("s1");

        WriteResult first = cluster.write("dup-req", "k", "v1");
        assertThat(first.status()).isEqualTo(WriteStatus.COMMITTED);
        long firstIndex = first.logIndex();
        long firstVersion = first.version();

        // 同一 requestId 以不同 value 重试：必须返回首次结论，value 不被覆盖
        WriteResult second = cluster.write("dup-req", "k", "v2-evil");
        assertThat(second.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(second.logIndex()).isEqualTo(firstIndex);
        assertThat(second.version()).isEqualTo(firstVersion);
        assertThat(cluster.read("k").value()).isEqualTo("v1");
        assertThat(cluster.read("k").version()).isEqualTo(firstVersion);
        log.info("[位点/版本] 重复提交 requestId=dup-req 首次 index={} version={}；重试结论={} 复用index={} version={} 判定依据={}",
                firstIndex, firstVersion, second.status(), second.logIndex(),
                second.version(), second.decisionReason());

        // leader 日志中只有一条该 requestId 的条目（没有重复应用）
        long count = set.leader().logEntries().stream()
                .filter(e -> e.requestId().equals("dup-req")).count();
        assertThat(count).isEqualTo(1);

        // 再用新 requestId 写同 key：版本正常推进到 2，说明重复识别只影响同一 requestId
        WriteResult next = cluster.write("dup-req-2", "k", "v2");
        assertThat(next.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(next.version()).isEqualTo(2);
        assertThat(cluster.read("k").value()).isEqualTo("v2");
        log.info("[位点/版本] 新 requestId 正常推进 version=2 index={}，重复识别不误伤其他提交",
                next.logIndex());

        // 幂等表持久化：重启集群后同一 requestId 仍然是 DUPLICATE
        List<RecoveryReport> reports = cluster.restartAll();
        assertThat(reports).allMatch(RecoveryReport::clean);
        WriteResult afterRestart = cluster.write("dup-req", "k", "v3");
        assertThat(afterRestart.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(afterRestart.logIndex()).isEqualTo(firstIndex);
        assertThat(cluster.read("k").value()).isEqualTo("v2");
        log.info("[判定依据] 重启后重复提交仍返回首次结论 index={}，当前值保持 v2 -> 去重表持久化且无重复应用",
                afterRestart.logIndex());
    }

    @Test
    void failedRequestIdIsAlsoDistinctAndNotConfusedWithDuplicateOrCommit() throws Exception {
        Cluster cluster = singleShardCluster3();
        ShardReplicaSet set = cluster.shard("s1");
        set.replica("r1").setStatus(ReplicaStatus.DOWN);
        set.replica("r2").setStatus(ReplicaStatus.DOWN);

        WriteResult failed = cluster.write("req-x", "k", "v");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);

        // 同一个失败 requestId 重试：返回的是首次失败结论（DUPLICATE 包装），
        // 不会因为副本恢复而悄悄变成 COMMITTED —— 对外结论一致可解释。
        WriteResult retry = cluster.write("req-x", "k", "v");
        assertThat(retry.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(retry.logIndex()).isEqualTo(failed.logIndex());
        log.warn("[判定依据] 失败 requestId 重试 -> DUPLICATE(首次={}) index={}；客户端须换新 requestId 重提",
                failed.status(), retry.logIndex());

        // 副本恢复后用新 requestId 提交成功，与失败 requestId 的结论互不混淆
        set.replica("r1").setStatus(ReplicaStatus.UP);
        set.replica("r2").setStatus(ReplicaStatus.UP);
        WriteResult committed = cluster.write("req-y", "k", "v");
        assertThat(committed.status()).isEqualTo(WriteStatus.COMMITTED);
        log.info("[判定依据] 新 requestId 提交成功 index={} version={}，与失败请求明确区分",
                committed.logIndex(), committed.version());
    }
}
