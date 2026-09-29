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
 * 场景一：写入因副本掉线 / 确认数不足失败后，换一个新 requestId 安全重提。
 *
 * <p>结论：</p>
 * <ul>
 *   <li>重提成功值最终可见，版本严格大于此前所有已确认版本；</li>
 *   <li>失败提交留在日志里的值，在副本恢复、追赶、整集群重启之后都不能再出现；</li>
 *   <li>旧 requestId 永远保留首次失败结论，重提它只得到 DUPLICATE，不会被当成
 *       一次新的成功提交，也不会重复应用。</li>
 * </ul>
 */
class SafeRetryAfterFailureTest {

    private static final Logger log = LoggerFactory.getLogger(SafeRetryAfterFailureTest.class);

    @TempDir
    Path tempDir;

    private Cluster cluster3() throws Exception {
        int max = KeyHasher.HASH_SPACE;
        return new Cluster(tempDir, new Topology(1,
                List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2"))));
    }

    @Test
    void failedValueNeverResurfacesAfterSafeRetryWithNewRequestId() throws Exception {
        Cluster cluster = cluster3();
        ShardReplicaSet set = cluster.shard("s1");

        // 已确认基线：alpha=base, version=1
        WriteResult base = cluster.write("req-base", "alpha", "base");
        assertThat(base.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(base.version()).isEqualTo(1);

        // 两个 follower 掉线：leader 独自写入失败（REPLICA_UNAVAILABLE），
        // 失败条目只可能落在 leader 日志的未提交尾部。
        set.replica("r1").setStatus(ReplicaStatus.DOWN);
        set.replica("r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("req-fail", "alpha", "EVIL-FAILED");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        long failedIndex = failed.logIndex();
        assertThat(cluster.read("alpha").value()).isEqualTo("base");
        assertThat(cluster.read("alpha").version()).isEqualTo(1);
        log.warn("[位点/版本] 失败提交 requestId=req-fail status={} leaderIndex={} 未提交不可见，当前 version=1/base",
                failed.status(), failedIndex);

        // 旧 requestId 立即重提：仍是首次失败结论（DUPLICATE 包装），不会变成功。
        WriteResult duplicateOfFailed = cluster.write("req-fail", "alpha", "EVIL-FAILED-2");
        assertThat(duplicateOfFailed.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(duplicateOfFailed.logIndex()).isEqualTo(failedIndex);
        log.info("[判定依据] 旧失败 requestId 重提 -> {} 复用 index={}，不产生新提交",
                duplicateOfFailed.status(), duplicateOfFailed.logIndex());

        // 只恢复一个 follower（2/3 多数派），用“新 requestId”重提同一 key。
        set.replica("r1").setStatus(ReplicaStatus.UP);
        WriteResult retry = cluster.write("req-retry", "alpha", "recovered");
        assertThat(retry.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(retry.version()).isEqualTo(2); // 严格大于此前已确认版本 1
        assertThat(retry.acks()).isEqualTo(2);
        assertThat(cluster.read("alpha").value()).isEqualTo("recovered");
        assertThat(cluster.read("alpha").version()).isEqualTo(2);
        log.info("[位点/版本] 新 requestId=req-retry 安全重提成功 index={} version=2 acks=2/3（失败尾部已被安全中止）",
                retry.logIndex());

        // leader 日志连续、无失败值残留：req-fail 不应出现在任何日志中。
        for (Replica r : set.replicas()) {
            boolean hasFailedReq = r.logEntries().stream()
                    .anyMatch(e -> e.requestId().equals("req-fail"));
            assertThat(hasFailedReq)
                    .as("副本 %s 上失败请求 req-fail 的条目必须已被物理移除", r.replicaId())
                    .isFalse();
        }
        log.info("[判定依据] 所有副本日志均不含 req-fail（leader 截断 + 在线 follower 对齐）");

        // 第二个 follower 恢复并追赶：失败值不得随分叉日志重新冒出来。
        set.replica("r2").setStatus(ReplicaStatus.UP);
        int appended = cluster.catchUpReplica("s1", "r2");
        assertThat(set.replica("r2").get("alpha").value()).isEqualTo("recovered");
        assertThat(set.replica("r2").get("alpha").version()).isEqualTo(2);
        assertThat(set.replica("r2").logEntries().stream()
                .noneMatch(e -> e.requestId().equals("req-fail"))).isTrue();
        log.info("[位点/版本] r2 恢复追赶补齐 {} 条后 alpha=recovered/version=2，无失败值复活", appended);

        // 整集群重启重放：结论不变。
        List<RecoveryReport> reports = cluster.restartAll();
        assertThat(reports).allMatch(RecoveryReport::clean);
        assertThat(cluster.read("alpha").value()).isEqualTo("recovered");
        assertThat(cluster.read("alpha").version()).isEqualTo(2);
        log.info("[判定依据] 整集群重启后 alpha=recovered/version=2，失败值未复活、版本未回退");

        // 旧 requestId 在重启后仍然保留首次失败结论。
        WriteResult afterRestart = cluster.write("req-fail", "alpha", "whatever");
        assertThat(afterRestart.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(afterRestart.logIndex()).isEqualTo(failedIndex);
        assertThat(cluster.read("alpha").value()).isEqualTo("recovered");
        log.info("[判定依据] 重启后重提 req-fail 仍是 {}（首次结论持久化），当前值保持 recovered",
                afterRestart.status());
    }

    @Test
    void insufficientAcksFailureIsAlsoAbortedBeforeSafeRetry() throws Exception {
        // 5 副本：3 个 SLOW 造成 INSUFFICIENT_ACKS（在线够多数派但本轮 acks 不足）。
        int max = KeyHasher.HASH_SPACE;
        Cluster cluster = new Cluster(tempDir, new Topology(1,
                List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2", "r3", "r4"))));
        ShardReplicaSet set = cluster.shard("s1");
        set.replica("r2").setStatus(ReplicaStatus.SLOW);
        set.replica("r3").setStatus(ReplicaStatus.SLOW);
        set.replica("r4").setStatus(ReplicaStatus.SLOW);

        WriteResult failed = cluster.write("req-slow-fail", "k", "slow-failed");
        assertThat(failed.status()).isEqualTo(WriteStatus.INSUFFICIENT_ACKS);
        log.warn("[位点/版本] 确认不足 requestId=req-slow-fail acks={}/quorum={} index={} 未提交",
                failed.acks(), failed.quorum(), failed.logIndex());

        // 全部恢复后新 requestId 重提：成功，版本=1，失败值不存在。
        set.replica("r2").setStatus(ReplicaStatus.UP);
        set.replica("r3").setStatus(ReplicaStatus.UP);
        set.replica("r4").setStatus(ReplicaStatus.UP);
        WriteResult ok = cluster.write("req-slow-retry", "k", "ok");
        assertThat(ok.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(ok.version()).isEqualTo(1);
        assertThat(ok.logIndex()).isEqualTo(1); // 失败尾部被中止，位点从 1 重新连续
        assertThat(cluster.read("k").value()).isEqualTo("ok");
        assertThat(set.leader().logEntries().stream()
                .noneMatch(e -> e.requestId().equals("req-slow-fail"))).isTrue();
        log.info("[位点/版本] 确认不足后新 requestId 重提 index=1 version=1（失败 index 被中止，无跳号）");

        cluster.restartAll();
        assertThat(cluster.read("k").value()).isEqualTo("ok");
        assertThat(cluster.read("k").version()).isEqualTo(1);
        log.info("[判定依据] 重启后 k=ok/version=1，确认不足的失败值未复活");
    }
}
