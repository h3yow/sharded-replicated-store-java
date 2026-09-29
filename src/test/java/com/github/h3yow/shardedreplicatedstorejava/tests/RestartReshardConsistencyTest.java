package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.store.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 场景：安全重提 + 普通路径并发决胜的结论，在<strong>整集群重启 + 分片归属重新划分
 * （再平衡） + 落后副本追赶</strong>之后依然成立。
 *
 * <ul>
 *   <li>最终胜出的值、版本、所在分片与调整前一致；已确认数据不丢、不重复；</li>
 *   <li>失败轮次的值在迁移/重启/追赶后不会重新冒出来；</li>
 *   <li>再平衡中途失败（迁移未获多数派）时，旧拓扑下的已确认数据原样保留，
 *       磁盘拓扑不被替换，可在恢复后重新再平衡成功。</li>
 * </ul>
 */
class RestartReshardConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(RestartReshardConsistencyTest.class);

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
    void winnerVersionAndShardRemainStableAcrossRestartReshardAndCatchup() throws Exception {
        Cluster cluster = new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));

        // 1) 基础已确认数据。
        WriteResult base = cluster.write("base-req", "alpha", "base");
        assertThat(base.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(base.version()).isEqualTo(1);
        String baseShard = cluster.topology().shardForKey("alpha").shardId();

        // 2) 安全重提：让 alpha 的一轮写入失败，再换新 requestId 重提成功。
        //    失败值 FAIL-A 绝不可见，最终值 good，version=2。
        ShardReplicaSet baseSet = cluster.shard(baseShard);
        baseSet.replica(baseShard + "-r1").setStatus(ReplicaStatus.DOWN);
        baseSet.replica(baseShard + "-r2").setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("fail-req", "alpha", "FAIL-A");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        baseSet.replica(baseShard + "-r1").setStatus(ReplicaStatus.UP);
        baseSet.replica(baseShard + "-r2").setStatus(ReplicaStatus.UP);
        WriteResult good = cluster.write("retry-req", "alpha", "good");
        assertThat(good.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(good.version()).isEqualTo(2);
        log.info("[位点/版本] 安全重提: alpha 最终 good/version=2（失败 FAIL-A 已清场），原分片={}", baseShard);

        // 3) 普通路径并发决胜：对 beta 起 12 路并发写。
        int n = 12;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<WriteResult>> futures = new ArrayList<>();
        for (int i = n; i >= 1; i--) {
            String req = String.format("beta-req-%02d", i);
            String val = String.format("beta-%02d", i);
            futures.add(pool.submit(() -> {
                start.await();
                return cluster.write(req, "beta", val);
            }));
        }
        start.countDown();
        String winnerRequest = null;
        int committed = 0;
        int superseded = 0;
        for (Future<WriteResult> f : futures) {
            WriteResult r = f.get(30, TimeUnit.SECONDS);
            if (r.status() == WriteStatus.COMMITTED) {
                committed++;
                winnerRequest = r.requestId();
            } else if (r.status() == WriteStatus.SUPERSEDED) {
                superseded++;
            }
        }
        pool.shutdownNow();
        assertThat(committed).isEqualTo(1);
        assertThat(superseded).isEqualTo(n - 1);
        String expectedWinner = String.format("beta-req-%02d", n);
        String expectedBeta = String.format("beta-%02d", n);
        assertThat(winnerRequest).isEqualTo(expectedWinner);
        VersionedValue betaBefore = cluster.read("beta");
        assertThat(betaBefore.value()).isEqualTo(expectedBeta);
        assertThat(betaBefore.version()).isEqualTo(n);
        String betaShardBefore = cluster.topology().shardForKey("beta").shardId();
        log.info("[判定依据] 并发决胜: beta 赢家={} 最终值={} version={} 分片={}（1 COMMITTED / {} SUPERSEDED）",
                winnerRequest, expectedBeta, n, betaShardBefore, n - 1);

        // 调整前快照：所有键的（值、版本）。
        record Kv(String key, String value, long version, String shard) {
        }
        List<Kv> before = List.of(
                new Kv("alpha", "good", 2, baseShard),
                new Kv("beta", expectedBeta, n, betaShardBefore));

        // 4) 再平衡 2 分片 -> 4 分片。
        int migrated = cluster.reshard(fourShards(),
                replicas("n0", "n1", "n2", "n3"));
        assertThat(cluster.topology().epoch()).isEqualTo(2);
        log.info("[位点] 再平衡完成 epoch=2 迁移键数={}", migrated);

        // 值/版本不变，归属为新拓扑分片（可能仍是同名区间，但一定是新分片对象）。
        for (Kv kv : before) {
            VersionedValue vv = cluster.read(kv.key());
            assertThat(vv.value()).isEqualTo(kv.value());
            assertThat(vv.version()).isEqualTo(kv.version());
            String ownerNow = cluster.topology().shardForKey(kv.key()).shardId();
            assertThat(ownerNow).startsWith("n");
            log.info("[位点/版本] 再平衡后 key={} value={} version={} 归属分片={}",
                    kv.key(), vv.value(), vv.version(), ownerNow);
        }

        // 失败值不得通过迁移冒出来：所有新分片副本上都没有 FAIL-A。
        for (ShardReplicaSet set : cluster.shards()) {
            for (Replica r : set.replicas()) {
                assertThat(r.logEntries().stream().noneMatch(e -> "FAIL-A".equals(e.value())))
                        .as("再平衡后 %s/%s 不得出现失败值 FAIL-A", set.shardId(), r.replicaId())
                        .isTrue();
            }
        }

        // 5) 整集群重启 + 从磁盘重载拓扑。
        cluster.restartAll();
        Cluster reopened = Cluster.open(tempDir);
        assertThat(reopened.topology().epoch()).isEqualTo(2);
        for (Kv kv : before) {
            VersionedValue vv = reopened.read(kv.key());
            assertThat(vv.value()).isEqualTo(kv.value());
            assertThat(vv.version()).isEqualTo(kv.version());
            assertThat(reopened.topology().shardForKey(kv.key()).shardId()).startsWith("n");
        }
        log.info("[判定依据] 重启+重载后 alpha=good/2、beta={}/{}，归属仍为 n* 新分片", expectedBeta, n);

        // 6) 落后副本追赶：把 beta 所属新分片的一个 follower 日志截回空位点，再追赶。
        String betaShard = reopened.topology().shardForKey("beta").shardId();
        ShardReplicaSet betaSet = reopened.shard(betaShard);
        Replica laggard = betaSet.replicas().get(1);
        // 模拟该副本在迁移全程都落后：日志与提交位点都回到 0，状态机随之停在 0。
        laggard.simulateLagBehind(0);
        assertThat(laggard.get("beta")).isNull();
        int appended = reopened.catchUpReplica(betaShard, laggard.replicaId());
        assertThat(appended).isGreaterThan(0);
        assertThat(laggard.get("beta").value()).isEqualTo(expectedBeta);
        assertThat(laggard.get("beta").version()).isEqualTo(n);
        log.info("[位点/版本] 落后副本 {}/{} 追赶补齐 {} 条后 beta={}/version={}，与赢家一致",
                betaShard, laggard.replicaId(), appended,
                laggard.get("beta").value(), laggard.get("beta").version());

        // 最终再读一次：结论与调整前完全一致。
        assertThat(reopened.read("alpha").value()).isEqualTo("good");
        assertThat(reopened.read("alpha").version()).isEqualTo(2);
        assertThat(reopened.read("beta").value()).isEqualTo(expectedBeta);
        assertThat(reopened.read("beta").version()).isEqualTo(n);
        log.info("[判定依据] 重启+再平衡+追赶后，最终值/版本/归属与调整前一致，失败值未复活");
    }

    @Test
    void failedReshardKeepsCommittedDataUntouchedAndCanRetrySuccessfully() throws Exception {
        Cluster cluster = new Cluster(tempDir,
                new Topology(1, twoShards(), replicas("old-a", "old-b")));
        for (int i = 0; i < 10; i++) {
            WriteResult r = cluster.write("k-req-" + i, "key-" + i, "v" + i);
            assertThat(r.status()).isEqualTo(WriteStatus.COMMITTED);
        }

        // 注入“调整中途”故障：新分片激活后、正式迁移前，把覆盖旧 old-a 哈希区间的
        // 目标分片 n0 的两个 follower 置 DOWN -> 迁移到 n0 时（key-3/5/8）拿不到多数派。
        cluster.setBeforeMigrateHook(c -> {
            ShardReplicaSet n0 = c.shard("n0");
            n0.replica("n0-r1").setStatus(ReplicaStatus.DOWN);
            n0.replica("n0-r2").setStatus(ReplicaStatus.DOWN);
        });

        assertThatThrownBy(() -> cluster.reshard(fourShards(),
                replicas("n0", "n1", "n2", "n3")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("多数派");
        log.warn("[判定依据] 再平衡中途因目标分片 n0 多数派不足失败（钩子注入 2 follower DOWN）");

        // 内存路由已回滚到旧世代；磁盘拓扑文件仍是 epoch=1。
        assertThat(cluster.topology().epoch()).isEqualTo(1);
        Cluster onDisk = Cluster.open(tempDir);
        assertThat(onDisk.topology().epoch()).isEqualTo(1);

        // 旧拓扑下已确认数据原样保留、可读、版本不变。
        for (int i = 0; i < 10; i++) {
            VersionedValue vv = onDisk.read("key-" + i);
            assertThat(vv).isNotNull();
            assertThat(vv.value()).isEqualTo("v" + i);
            assertThat(vv.version()).isEqualTo(1);
            assertThat(onDisk.topology().shardForKey("key-" + i).shardId()).startsWith("old-");
        }
        log.info("[判定依据] 失败再平衡后旧拓扑 epoch=1 的 10 个已确认键原样保留，未丢失/重复");

        // 目标副本恢复、撤销故障注入后重新再平衡：成功，数据按新拓扑归属且值/版本不变。
        cluster.setBeforeMigrateHook(null);
        cluster.shard("n0").replica("n0-r1").setStatus(ReplicaStatus.UP);
        cluster.shard("n0").replica("n0-r2").setStatus(ReplicaStatus.UP);
        int migrated = cluster.reshard(fourShards(),
                replicas("n0", "n1", "n2", "n3"));
        assertThat(migrated).isEqualTo(10);
        assertThat(cluster.topology().epoch()).isEqualTo(2);
        for (int i = 0; i < 10; i++) {
            VersionedValue vv = cluster.read("key-" + i);
            assertThat(vv.value()).isEqualTo("v" + i);
            assertThat(vv.version()).isEqualTo(1);
            assertThat(cluster.topology().shardForKey("key-" + i).shardId()).startsWith("n");
        }
        log.info("[判定依据] 恢复后重提再平衡成功 epoch=2 迁移 {} 键，值/版本不变、归属更新（失败 MIGRATE 未复活）", migrated);
    }
}
