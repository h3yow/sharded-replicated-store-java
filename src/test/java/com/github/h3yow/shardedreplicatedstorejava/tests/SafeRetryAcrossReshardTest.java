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
 * 场景：写失败/成功后的 requestId 结论，在<strong>分片归属调整（再平衡）</strong>之后
 * 重提必须保持首次结论，不得被当成全新写入重复应用。
 *
 * <ul>
 *   <li>首次失败的 requestId：调整后重提仍返回首次失败结论，失败值永不对外可见；</li>
 *   <li>首次成功的 requestId：调整后重提仍返回首次成功结论，不产生新版本/新日志；</li>
 *   <li>换新 requestId 安全重提：版本严格大于此前所有已确认版本。</li>
 * </ul>
 */
class SafeRetryAcrossReshardTest {

    private static final Logger log = LoggerFactory.getLogger(SafeRetryAcrossReshardTest.class);

    @TempDir
    Path tempDir;

    private static List<ShardRange> twoShards() {
        int max = KeyHasher.HASH_SPACE;
        return List.of(new ShardRange("old-a", 0, max / 2),
                new ShardRange("old-b", max / 2, max));
    }

    private static List<ShardRange> fourShards() {
        int max = KeyHasher.HASH_SPACE;
        int q = max / 4;
        return List.of(new ShardRange("n0", 0, q),
                new ShardRange("n1", q, 2 * q),
                new ShardRange("n2", 2 * q, 3 * q),
                new ShardRange("n3", 3 * q, max));
    }

    private static Map<String, List<String>> replicas(String... shardIds) {
        Map<String, List<String>> m = new java.util.LinkedHashMap<>();
        for (String s : shardIds) {
            m.put(s, List.of(s + "-r0", s + "-r1", s + "-r2"));
        }
        return m;
    }

    @Test
    void failedRequestIdRetriedAfterReshardKeepsFirstFailureVerdict() throws Exception {
        Cluster cluster = new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));

        // 1) 先确认一条基线，再制造一轮多数派不足的失败写入。
        WriteResult base = cluster.write("base-req", "alpha", "base");
        assertThat(base.status()).isEqualTo(WriteStatus.COMMITTED);
        String oldShard = cluster.topology().shardForKey("alpha").shardId();
        ShardReplicaSet oldSet = cluster.shard(oldShard);
        oldSet.replica(oldShard + "-r1").setStatus(ReplicaStatus.DOWN);
        oldSet.replica(oldShard + "-r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("fail-req", "alpha", "FAIL-A");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        oldSet.replica(oldShard + "-r1").setStatus(ReplicaStatus.UP);
        oldSet.replica(oldShard + "-r2").setStatus(ReplicaStatus.UP);
        log.info("[判定依据] 调整前: fail-req 首次结论={}（多数派不足），失败值 FAIL-A 未提交", failed.status());

        // 2) 换新 requestId 安全重提成功，版本严格大于此前已确认版本。
        WriteResult good = cluster.write("retry-req", "alpha", "good");
        assertThat(good.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(good.version()).isGreaterThan(base.version());
        log.info("[位点/版本] 安全重提: retry-req 提交 good version={}（> 已确认 version={}）",
                good.version(), base.version());

        // 3) 再平衡：alpha 被调到新分片。
        cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3"));
        String newShard = cluster.topology().shardForKey("alpha").shardId();
        log.info("[位点] 再平衡完成 epoch=2: alpha 归属 {} -> {}", oldShard, newShard);

        // 4) 调整后重提失败的 requestId：必须返回首次失败结论，不得变成新提交。
        WriteResult retried = cluster.write("fail-req", "alpha", "FAIL-A");
        assertThat(retried.status()).isEqualTo(WriteStatus.DUPLICATE);
        log.info("[判定依据] 调整后重提 fail-req -> {}（首次结论 {}），不重复应用",
                retried.status(), failed.status());

        // 失败值永不对外可见；可见值仍是安全重提的 good，版本不回退。
        VersionedValue visible = cluster.read("alpha");
        assertThat(visible.value()).isEqualTo("good");
        assertThat(visible.version()).isEqualTo(good.version());
        for (ShardReplicaSet set : cluster.shards()) {
            for (Replica r : set.replicas()) {
                assertThat(r.logEntries().stream().noneMatch(e -> "FAIL-A".equals(e.value())))
                        .as("调整后 %s/%s 不得出现失败值 FAIL-A", set.shardId(), r.replicaId())
                        .isTrue();
            }
        }
        log.info("[判定依据] 重提后可见值仍为 good/version={}，所有副本日志均无 FAIL-A", visible.version());
    }

    @Test
    void committedRequestIdRetriedAfterReshardKeepsFirstSuccessVerdict() throws Exception {
        Cluster cluster = new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));

        // 1) 一条成功提交，记录其位点与版本。
        WriteResult committed = cluster.write("ok-req", "beta", "v1");
        assertThat(committed.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(committed.version()).isEqualTo(1);
        String oldShard = cluster.topology().shardForKey("beta").shardId();
        long oldLeaderLogSize = cluster.shard(oldShard).leader().logEntries().size();
        log.info("[位点/版本] 调整前: ok-req 首次结论=COMMITTED index={} version={} 分片={}",
                committed.logIndex(), committed.version(), oldShard);

        // 2) 再平衡后重提同一 requestId（即使换更大的值也不能生效）。
        cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3"));
        String newShard = cluster.topology().shardForKey("beta").shardId();
        WriteResult retried = cluster.write("ok-req", "beta", "v2-should-not-apply");
        assertThat(retried.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(retried.version()).isEqualTo(committed.version());
        log.info("[判定依据] 调整后重提 ok-req -> DUPLICATE（首次结论 COMMITTED version={}），不重复应用",
                committed.version());

        // 3) 不产生新版本、新值不可见、新分片日志没有重复应用同一 requestId。
        VersionedValue visible = cluster.read("beta");
        assertThat(visible.value()).isEqualTo("v1");
        assertThat(visible.version()).isEqualTo(1);
        ShardReplicaSet newSet = cluster.shard(newShard);
        long occurrences = newSet.leader().logEntries().stream()
                .filter(e -> "ok-req".equals(e.requestId())).count();
        assertThat(occurrences).isZero();
        assertThat(newSet.leader().logEntries().stream()
                .noneMatch(e -> "v2-should-not-apply".equals(e.value()))).isTrue();
        log.info("[位点/版本] 新分片 {} 日志中 ok-req 出现 {} 次（应为 0），可见值仍为 v1/version=1；"
                + "旧分片日志条目数={} 未增长={}", newShard, occurrences, oldLeaderLogSize,
                cluster.shard(oldShard).leader().logEntries().size() == oldLeaderLogSize);
    }

    @Test
    void verdictsSurviveRestartReshardAndCatchup() throws Exception {
        Cluster cluster = new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));

        // 1) gamma：成功提交；delta：先失败再换新 requestId 安全重提成功。
        WriteResult ok = cluster.write("ok-req", "gamma", "g1");
        assertThat(ok.status()).isEqualTo(WriteStatus.COMMITTED);
        String deltaShard = cluster.topology().shardForKey("delta").shardId();
        ShardReplicaSet deltaSet = cluster.shard(deltaShard);
        deltaSet.replica(deltaShard + "-r1").setStatus(ReplicaStatus.DOWN);
        deltaSet.replica(deltaShard + "-r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("fail-req", "delta", "FAIL-D");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        deltaSet.replica(deltaShard + "-r1").setStatus(ReplicaStatus.UP);
        deltaSet.replica(deltaShard + "-r2").setStatus(ReplicaStatus.UP);
        WriteResult good = cluster.write("retry-req", "delta", "d-good");
        assertThat(good.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(good.version()).isEqualTo(1);
        log.info("[位点/版本] 调整前: gamma=ok-req/COMMITTED/v{}；delta=fail-req/{} 后 retry-req/COMMITTED/v{}",
                ok.version(), failed.status(), good.version());

        // 2) 整集群重启重放 -> 再平衡 -> 再次整集群重启（从磁盘重载拓扑）。
        cluster.restartAll();
        cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3"));
        cluster.restartAll();
        Cluster reopened = Cluster.open(tempDir);
        assertThat(reopened.topology().epoch()).isEqualTo(2);
        log.info("[位点] 重启+再平衡+重启完成 epoch=2，拓扑已从磁盘重载");

        // 3) 落后副本追赶：把 gamma 所属新分片的一个 follower 截回空位点再追赶。
        String gammaShard = reopened.topology().shardForKey("gamma").shardId();
        Replica laggard = reopened.shard(gammaShard).replicas().get(1);
        laggard.simulateLagBehind(0);
        int appended = reopened.catchUpReplica(gammaShard, laggard.replicaId());
        assertThat(appended).isGreaterThan(0);
        assertThat(laggard.get("gamma").value()).isEqualTo("g1");
        log.info("[位点/版本] 落后副本 {}/{} 追赶补齐 {} 条，gamma=g1/version={}",
                gammaShard, laggard.replicaId(), appended, laggard.get("gamma").version());

        // 4) 追赶之后再重提两个旧 requestId：结论必须与首次一致。
        WriteResult dupOk = reopened.write("ok-req", "gamma", "g2-should-not-apply");
        assertThat(dupOk.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(dupOk.version()).isEqualTo(ok.version());
        WriteResult dupFail = reopened.write("fail-req", "delta", "FAIL-D");
        assertThat(dupFail.status()).isEqualTo(WriteStatus.DUPLICATE);
        log.info("[判定依据] 重启+再平衡+追赶后重提: ok-req -> {}（首次 COMMITTED），fail-req -> {}（首次 {}）",
                dupOk.status(), dupFail.status(), failed.status());

        // 5) 已确认数据不丢不重；失败值 FAIL-D 在任何副本上都不可见。
        assertThat(reopened.read("gamma").value()).isEqualTo("g1");
        assertThat(reopened.read("gamma").version()).isEqualTo(1);
        assertThat(reopened.read("delta").value()).isEqualTo("d-good");
        assertThat(reopened.read("delta").version()).isEqualTo(1);
        for (ShardReplicaSet set : reopened.shards()) {
            for (Replica r : set.replicas()) {
                assertThat(r.logEntries().stream().noneMatch(e -> "FAIL-D".equals(e.value())))
                        .as("%s/%s 不得出现失败值 FAIL-D", set.shardId(), r.replicaId())
                        .isTrue();
            }
        }
        log.info("[判定依据] 最终一致: gamma=g1/1、delta=d-good/1，FAIL-D 未复活，两旧 requestId 均复用首次结论");

        // 6) 换新 requestId 再写 delta：版本必须严格大于此前所有已确认版本。
        WriteResult next = reopened.write("next-req", "delta", "d2");
        assertThat(next.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(next.version()).isGreaterThan(good.version());
        log.info("[位点/版本] 新 requestId 写入 delta version={}（严格大于此前已确认 version={}）",
                next.version(), good.version());
    }

    @Test
    void verdictsStableWhenReshardFailsMidwayAndIsRetried() throws Exception {
        Cluster cluster = new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));

        // 1) 一成功一失败两个 requestId，分属不同键。
        WriteResult ok = cluster.write("ok-req", "epsilon", "e1");
        assertThat(ok.status()).isEqualTo(WriteStatus.COMMITTED);
        String zetaShard = cluster.topology().shardForKey("zeta").shardId();
        ShardReplicaSet zetaSet = cluster.shard(zetaShard);
        zetaSet.replica(zetaShard + "-r1").setStatus(ReplicaStatus.DOWN);
        zetaSet.replica(zetaShard + "-r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("fail-req", "zeta", "FAIL-Z");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        zetaSet.replica(zetaShard + "-r1").setStatus(ReplicaStatus.UP);
        zetaSet.replica(zetaShard + "-r2").setStatus(ReplicaStatus.UP);
        log.info("[判定依据] 调整前: ok-req=COMMITTED/v{}，fail-req={}（FAIL-Z 未提交）",
                ok.version(), failed.status());

        // 2) 注入“调整中途”故障：所有新分片都凑不够多数派 -> 首个迁移键即失败并回滚。
        cluster.setBeforeMigrateHook(c -> {
            for (String s : List.of("n0", "n1", "n2", "n3")) {
                c.shard(s).replica(s + "-r1").setStatus(ReplicaStatus.DOWN);
                c.shard(s).replica(s + "-r2").setStatus(ReplicaStatus.DOWN);
            }
        });
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("多数派");
        assertThat(cluster.topology().epoch()).isEqualTo(1);
        log.warn("[判定依据] 再平衡中途失败并回滚到 epoch=1（钩子注入 n0 两副本 DOWN）");

        // 3) 回滚后（旧拓扑）重提两个旧 requestId：仍是首次结论。
        WriteResult dupOkOld = cluster.write("ok-req", "epsilon", "e-x");
        assertThat(dupOkOld.status()).isEqualTo(WriteStatus.DUPLICATE);
        WriteResult dupFailOld = cluster.write("fail-req", "zeta", "FAIL-Z");
        assertThat(dupFailOld.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.read("epsilon").value()).isEqualTo("e1");
        assertThat(cluster.read("zeta")).isNull();
        log.info("[判定依据] 回滚后旧拓扑重提: ok-req -> DUPLICATE，fail-req -> DUPLICATE，FAIL-Z 仍不可见");

        // 4) 恢复副本后重试再平衡：成功；旧拓扑已确认数据原样迁移。
        cluster.setBeforeMigrateHook(null);
        for (String s : List.of("n0", "n1", "n2", "n3")) {
            cluster.shard(s).replica(s + "-r1").setStatus(ReplicaStatus.UP);
            cluster.shard(s).replica(s + "-r2").setStatus(ReplicaStatus.UP);
        }
        cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3"));
        assertThat(cluster.topology().epoch()).isEqualTo(2);
        log.info("[位点] 恢复后重试再平衡成功 epoch=2");

        // 5) 新拓扑下再重提：结论仍不变；失败值不复活；已确认数据不丢不重。
        WriteResult dupOkNew = cluster.write("ok-req", "epsilon", "e-y");
        assertThat(dupOkNew.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(dupOkNew.version()).isEqualTo(ok.version());
        WriteResult dupFailNew = cluster.write("fail-req", "zeta", "FAIL-Z");
        assertThat(dupFailNew.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.read("epsilon").value()).isEqualTo("e1");
        assertThat(cluster.read("epsilon").version()).isEqualTo(1);
        assertThat(cluster.read("zeta")).isNull();
        for (ShardReplicaSet set : cluster.shards()) {
            for (Replica r : set.replicas()) {
                // 已提交前缀（index <= commitIndex）绝不含失败值；状态机也不可见。
                // （旧分片 leader 上允许存在未提交的物理残留，它永不提交、永不可见，
                //   下次该分片有写入时会被“重提前清场”截断。）
                assertThat(r.logEntries().stream()
                        .filter(e -> e.index() <= r.commitIndex())
                        .noneMatch(e -> "FAIL-Z".equals(e.value())))
                        .as("%s/%s 已提交前缀不得出现失败值 FAIL-Z", set.shardId(), r.replicaId())
                        .isTrue();
                assertThat(r.get("zeta"))
                        .as("%s/%s 状态机不得可见 FAIL-Z", set.shardId(), r.replicaId())
                        .isNull();
            }
        }
        log.info("[判定依据] 失败-回滚-重试再平衡后: 两旧 requestId 仍复用首次结论，epsilon=e1/1，FAIL-Z 未复活");

        // 6) 换新 requestId 写 zeta（此前从未确认过）：版本从 1 起，严格大于已确认版本（无）。
        WriteResult fresh = cluster.write("fresh-req", "zeta", "z1");
        assertThat(fresh.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(fresh.version()).isEqualTo(1);
        WriteResult nextEps = cluster.write("eps-req-2", "epsilon", "e2");
        assertThat(nextEps.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(nextEps.version()).isGreaterThan(ok.version());
        log.info("[位点/版本] 新 requestId: zeta=z1/v{}，epsilon=e2/v{}（严格大于已确认 v{}）",
                fresh.version(), nextEps.version(), ok.version());
    }
}
