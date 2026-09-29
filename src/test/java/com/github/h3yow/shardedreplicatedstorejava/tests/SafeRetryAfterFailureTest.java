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
 * 场景：写失败之后“安全重提”。
 *
 * <p>某轮写入因副本掉线 / 确认数不足失败（条目可能已在 leader 本地落盘），
 * 之后副本恢复，客户端<strong>换新 requestId</strong> 重写同一 key：</p>
 * <ul>
 *   <li>成功的重提必须成为最终可见值，版本严格大于此前所有已确认版本；</li>
 *   <li>失败轮次的值无论经过重启、追赶还是再平衡，都不能被任何读取看到；</li>
 *   <li>原失败 requestId 永远保留首次结论：重提它只得到 DUPLICATE(首次失败)，
 *       不会因副本恢复变成新的成功提交，也不会重复应用。</li>
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
    void failedWriteThenNewRequestIdRetryWinsAndFailedValueNeverResurfaces() throws Exception {
        Cluster cluster = cluster3();
        ShardReplicaSet set = cluster.shard("s1");

        // 先有一个已确认版本 v1/version=1 作为“此前所有已确认版本”的基准。
        WriteResult base = cluster.write("req-base", "k", "v1");
        assertThat(base.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(base.version()).isEqualTo(1);
        long committedVersionBefore = cluster.read("k").version();

        // 两个 follower 掉线：leader 单独 1 票 < quorum=2 -> REPLICA_UNAVAILABLE。
        // 失败条目只落 leader 本地（index=2,version=2），永不提交。
        set.replica("r1").setStatus(ReplicaStatus.DOWN);
        set.replica("r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("req-fail", "k", "EVIL-FAILED");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        assertThat(failed.version()).isEqualTo(LogEntry.NO_VERSION);
        assertThat(cluster.read("k").value()).isEqualTo("v1");
        log.warn("[判定依据] 失败轮 requestId={} status={} leader残留index={}（未提交不可见，当前仍 v1/version=1）",
                failed.requestId(), failed.status(), failed.logIndex());

        // 副本恢复后，用【新 requestId】重提同一 key：必须成功并成为最终可见值。
        set.replica("r1").setStatus(ReplicaStatus.UP);
        set.replica("r2").setStatus(ReplicaStatus.UP);
        WriteResult retry = cluster.write("req-retry-new", "k", "v2-good");
        assertThat(retry.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(retry.version()).isGreaterThan(committedVersionBefore);
        assertThat(retry.version()).isEqualTo(2);
        assertThat(cluster.read("k").value()).isEqualTo("v2-good");
        assertThat(cluster.read("k").version()).isEqualTo(2);
        log.info("[位点/版本] 新 requestId={} 重提成功 index={} version={} 最终值=v2-good（版本严格大于已确认基准1）",
                retry.requestId(), retry.logIndex(), retry.version());

        // 关键：所有副本（含此前掉线者）日志里都不能出现失败值，状态机也从未应用它。
        for (Replica r : set.replicas()) {
            boolean hasFailedEntry = r.logEntries().stream()
                    .anyMatch(e -> e.requestId().equals("req-fail"));
            assertThat(hasFailedEntry)
                    .as("副本 %s 上失败残留 req-fail 必须已在重提前清场", r.replicaId())
                    .isFalse();
            assertThat(r.get("k").value())
                    .as("副本 %s 状态机不得看到失败值", r.replicaId())
                    .isEqualTo("v2-good");
        }
        log.info("[判定依据] 3 个副本日志均无 req-fail，状态机最终值统一为 v2-good -> 失败值未复活");

        // 整集群重启重放：结论保持，失败值不冒出。
        List<RecoveryReport> reports = cluster.restartAll();
        assertThat(reports).allMatch(RecoveryReport::clean);
        assertThat(cluster.read("k").value()).isEqualTo("v2-good");
        assertThat(cluster.read("k").version()).isEqualTo(2);
        log.info("[判定依据] 重启重放后读 k = v2-good/version=2，失败值未在重放中复活");

        // 原失败 requestId 重提：只能得到首次失败结论（DUPLICATE 包装），
        // 不会因副本恢复变成成功，也不产生第二条日志。
        WriteResult failedAgain = cluster.write("req-fail", "k", "EVIL-FAILED-2");
        assertThat(failedAgain.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(failedAgain.logIndex()).isEqualTo(failed.logIndex());
        long failEntryCount = set.leader().logEntries().stream()
                .filter(e -> e.requestId().equals("req-fail")).count();
        assertThat(failEntryCount).isZero();
        assertThat(cluster.read("k").value()).isEqualTo("v2-good");
        log.warn("[判定依据] 失败 requestId 重提 -> DUPLICATE(首次={}) 复用index={}，无新日志、不重复应用、值仍 v2-good",
                failed.status(), failedAgain.logIndex());
    }

    @Test
    void insufficientAcksFailureThenRetryAfterCatchupAlsoWins() throws Exception {
        Cluster cluster = cluster3();
        ShardReplicaSet set = cluster.shard("s1");

        // 初始确认版本 v1（version=1）。
        cluster.write("base", "k", "v1");

        // r2 掉线：leader+r1 仍多数派可提交；制造“在线但确认不足”：
        // r2 DOWN（在线数=2 够多数派），r1 SLOW（本轮不确认）-> 只有 leader 1 ack。
        set.replica("r2").setStatus(ReplicaStatus.DOWN);
        set.replica("r1").setStatus(ReplicaStatus.SLOW);
        WriteResult slow = cluster.write("req-slow-fail", "k", "SLOW-EVIL");
        assertThat(slow.status()).isEqualTo(WriteStatus.INSUFFICIENT_ACKS);
        assertThat(slow.acks()).isEqualTo(1);
        assertThat(cluster.read("k").value()).isEqualTo("v1");
        log.warn("[判定依据] 确认不足 requestId={} status={} acks=1/2，失败值 SLOW-EVIL 不可见",
                slow.requestId(), slow.status());

        // 恢复两个 follower（r1 由 SLOW->UP，r2 DOWN->UP），r2 先追赶对齐已提交前缀。
        set.replica("r1").setStatus(ReplicaStatus.UP);
        set.replica("r2").setStatus(ReplicaStatus.UP);
        int caughtUp = cluster.catchUpReplica("s1", "r2");
        log.info("[位点] r2 恢复性追赶补齐 {} 条（只应含已提交 v1，不含 SLOW-EVIL）", caughtUp);
        assertThat(set.replica("r2").get("k").value()).isEqualTo("v1");

        // 换新 requestId 重提：成功且失败值不在任何副本。
        WriteResult ok = cluster.write("req-retry-2", "k", "v2-good");
        assertThat(ok.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(ok.version()).isEqualTo(2);
        assertThat(cluster.read("k").value()).isEqualTo("v2-good");
        for (Replica r : set.replicas()) {
            assertThat(r.logEntries().stream()
                    .noneMatch(e -> "SLOW-EVIL".equals(e.value())))
                    .as("副本 %s 不得残留确认不足轮次的值", r.replicaId())
                    .isTrue();
        }
        log.info("[位点/版本] 确认不足后新 requestId 重提成功 version=2 index={}，3 副本均无 SLOW-EVIL",
                ok.logIndex());
    }
}
