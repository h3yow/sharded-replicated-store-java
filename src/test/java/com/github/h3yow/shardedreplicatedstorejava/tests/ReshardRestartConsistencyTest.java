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
 * 场景三：安全重提与同键并发决胜的结论，在<b>整集群重启、落后副本追赶、
 * 分片归属重新划分</b>之后依然成立。
 *
 * <ul>
 *   <li>最终胜出值、版本、胜者 requestId 与所在分片在调整前后一致；</li>
 *   <li>已确认数据不丢、不重复；失败提交的值不会在再平衡/追赶后重新冒出；</li>
 *   <li>再平衡中途因非法目标拓扑失败时，原已确认数据原样保留。</li>
 * </ul>
 */
class ReshardRestartConsistencyTest {

    private static final Logger log = LoggerFactory.getLogger(ReshardRestartConsistencyTest.class);

    @TempDir
    Path tempDir;

    private Map<String, List<String>> replicas(String... shardIds) {
        Map<String, List<String>> m = new java.util.LinkedHashMap<>();
        for (String s : shardIds) {
            m.put(s, List.of(s + "-r0", s + "-r1", s + "-r2"));
        }
        return m;
    }

    private List<ShardRange> fourShards() {
        int max = KeyHasher.HASH_SPACE;
        int q = max / 4;
        return List.of(
                new ShardRange("n0", 0, q),
                new ShardRange("n1", q, 2 * q),
                new ShardRange("n2", 2 * q, 3 * q),
                new ShardRange("n3", 3 * q, max));
    }

    @Test
    void winnerValueVersionOwnerAndWinnerRequestIdSurviveRestartAndReshardAndCatchup()
            throws Exception {
        int max = KeyHasher.HASH_SPACE;
        Cluster cluster = new Cluster(tempDir, new Topology(1,
                List.of(new ShardRange("old-a", 0, max / 2),
                        new ShardRange("old-b", max / 2, max)),
                replicas("old-a", "old-b")));

        // ---- 1) 失败后安全重提：在某分片留下一个被中止的失败提交 ----
        // 找到落在 old-a 的键
        String safeKey = "alpha";
        ShardRange before = cluster.topology().shardForKey(safeKey);
        String safeShard = before.shardId();
        ShardReplicaSet safeSet = cluster.shard(safeShard);
        WriteResult base = cluster.write("req-base", safeKey, "base");
        assertThat(base.status()).isEqualTo(WriteStatus.COMMITTED);
        long baseVersion = base.version();

        Replica f1 = safeSet.replicas().get(1);
        Replica f2 = safeSet.replicas().get(2);
        f1.setStatus(ReplicaStatus.DOWN);
        f2.setStatus(ReplicaStatus.DOWN);
        WriteResult failed = cluster.write("req-fail", safeKey, "EVIL");
        assertThat(failed.status()).isEqualTo(WriteStatus.REPLICA_UNAVAILABLE);
        f1.setStatus(ReplicaStatus.UP);
        // 只恢复 1 个 follower（多数派），换新 requestId 重提成功
        WriteResult recovered = cluster.write("req-win-retry", safeKey, "retry-wins");
        assertThat(recovered.status()).isEqualTo(WriteStatus.COMMITTED);
        assertThat(recovered.version()).isEqualTo(baseVersion + 1);
        log.info("[位点/版本] 安全重提成功 key={} version={} 胜者=req-win-retry 分片={}",
                safeKey, recovered.version(), safeShard);

        // ---- 2) 普通接口下同键 12 路并发决胜 ----
        String contended = "contended-key";
        int n = 12;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch fire = new CountDownLatch(1);
        List<Future<WriteResult>> futures = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            String req = String.format("req-%03d", i);
            futures.add(pool.submit(() -> {
                ready.countDown();
                fire.await();
                return cluster.write(req, contended, "val-" + req);
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        fire.countDown();
        int committed = 0;
        int superseded = 0;
        for (Future<WriteResult> f : futures) {
            WriteResult r = f.get(30, TimeUnit.SECONDS);
            if (r.status() == WriteStatus.COMMITTED) {
                committed++;
            } else if (r.status() == WriteStatus.SUPERSEDED) {
                superseded++;
            }
        }
        pool.shutdown();
        assertThat(committed).isEqualTo(1);
        assertThat(superseded).isEqualTo(n - 1);
        String contendedShard = cluster.topology().shardForKey(contended).shardId();
        VersionedValue contendedBefore = cluster.read(contended);
        assertThat(contendedBefore.value()).isEqualTo("val-req-012");
        assertThat(contendedBefore.winnerRequestId()).isEqualTo("req-012");
        log.info("[判定依据] 并发决胜: 1 COMMITTED + {} SUPERSEDED，胜者=req-012 value={} version={} 分片={}",
                superseded, contendedBefore.value(), contendedBefore.version(), contendedShard);

        // 记录调整前所有可见键的快照（值/版本/胜者）
        VersionedValue safeBefore = cluster.read(safeKey);
        String safeOwnerBefore = cluster.topology().shardForKey(safeKey).shardId();
        String contendedOwnerBefore = cluster.topology().shardForKey(contended).shardId();

        // ---- 3) 整集群重启重放 ----
        List<RecoveryReport> reports = cluster.restartAll();
        assertThat(reports).allMatch(RecoveryReport::clean);
        assertThat(cluster.read(safeKey)).isEqualTo(safeBefore);
        assertThat(cluster.read(contended)).isEqualTo(contendedBefore);
        log.info("[判定依据] 重启后安全重提值与决胜值不变: {} / {}",
                safeBefore, contendedBefore);

        // ---- 4) 落后副本追赶：把之前一直 DOWN 的 f2 恢复并追赶 ----
        f2.setStatus(ReplicaStatus.UP);
        int appended = cluster.catchUpReplica(safeShard, f2.replicaId());
        assertThat(f2.get(safeKey).value()).isEqualTo("retry-wins");
        assertThat(f2.get(safeKey).winnerRequestId()).isEqualTo("req-win-retry");
        assertThat(f2.logEntries().stream().noneMatch(e -> e.requestId().equals("req-fail")))
                .isTrue();
        log.info("[位点/版本] 落后副本 {} 追赶补齐 {} 条，安全重提值在、失败值 req-fail 不在",
                f2.replicaId(), appended);

        // ---- 5) 再平衡为 4 分片 ----
        int migrated = cluster.reshard(fourShards(), replicas("n0", "n1", "n2", "n3"));
        assertThat(cluster.topology().epoch()).isEqualTo(2);
        log.info("[位点/版本] 再平衡完成，迁移键数={} epoch=2", migrated);

        VersionedValue safeAfter = cluster.read(safeKey);
        VersionedValue contendedAfter = cluster.read(contended);
        String safeOwnerAfter = cluster.topology().shardForKey(safeKey).shardId();
        String contendedOwnerAfter = cluster.topology().shardForKey(contended).shardId();

        // 值、版本、胜者三者不变
        assertThat(safeAfter.value()).isEqualTo(safeBefore.value());
        assertThat(safeAfter.version()).isEqualTo(safeBefore.version());
        assertThat(safeAfter.winnerRequestId()).isEqualTo(safeBefore.winnerRequestId());
        assertThat(contendedAfter).isEqualTo(contendedBefore);
        log.info("[位点/版本] 再平衡后 safeKey: value={} version={} winner={} 分片 {}->{}",
                safeAfter.value(), safeAfter.version(), safeAfter.winnerRequestId(),
                safeOwnerBefore, safeOwnerAfter);
        log.info("[位点/版本] 再平衡后 contended: value={} version={} winner={} 分片 {}->{}",
                contendedAfter.value(), contendedAfter.version(),
                contendedAfter.winnerRequestId(), contendedOwnerBefore, contendedOwnerAfter);

        // 归属按新拓扑更新（用哈希重算期望分片，不硬编码）
        assertThat(safeOwnerAfter).isEqualTo(
                new Topology(2, fourShards(), Map.of()).shardForKey(safeKey).shardId());
        assertThat(contendedOwnerAfter).isEqualTo(
                new Topology(2, fourShards(), Map.of()).shardForKey(contended).shardId());

        // 新分片中也不存在失败请求的条目
        for (ShardReplicaSet set : cluster.shards()) {
            for (Replica r : set.replicas()) {
                assertThat(r.logEntries().stream().noneMatch(e -> e.requestId().equals("req-fail")))
                        .as("再平衡后分片 %s 副本 %s 不得出现失败值", set.shardId(), r.replicaId())
                        .isTrue();
            }
        }

        // ---- 6) 再从磁盘重开集群：跨重启结论仍成立 ----
        Cluster reopened = Cluster.open(tempDir);
        assertThat(reopened.topology().epoch()).isEqualTo(2);
        assertThat(reopened.read(safeKey)).isEqualTo(safeAfter);
        assertThat(reopened.read(contended)).isEqualTo(contendedAfter);
        assertThat(reopened.read(safeKey).winnerRequestId()).isEqualTo("req-win-retry");
        assertThat(reopened.read(contended).winnerRequestId()).isEqualTo("req-012");
        log.info("[判定依据] 重开集群(epoch=2)后值/版本/胜者/归属全部一致，失败值未复活");

        // 旧失败 requestId 依旧保留首次失败结论
        WriteResult stale = reopened.write("req-fail", safeKey, "EVIL2");
        assertThat(stale.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(reopened.read(safeKey).value()).isEqualTo("retry-wins");
        log.info("[判定依据] 再平衡+重启后重提 req-fail 仍为 DUPLICATE，可见值保持 retry-wins");
    }

    @Test
    void failedReshardKeepsCommittedDataIntact() throws Exception {
        int max = KeyHasher.HASH_SPACE;
        Cluster cluster = new Cluster(tempDir, new Topology(1,
                List.of(new ShardRange("old-a", 0, max / 2),
                        new ShardRange("old-b", max / 2, max)),
                replicas("old-a", "old-b")));
        for (int i = 0; i < 10; i++) {
            assertThat(cluster.write("req-" + i, "keep-" + i, "v" + i).status())
                    .isEqualTo(WriteStatus.COMMITTED);
        }
        // 非法新拓扑（两个分片之间留空洞）：必须被拒绝，世代与数据保持原样
        List<ShardRange> gappy = List.of(
                new ShardRange("n0", 0, max / 4),
                new ShardRange("n1", max / 4 + 100, max));
        assertThatThrownBy(() -> cluster.reshard(gappy, replicas("n0", "n1")))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.UNCOVERED_HASH_GAP);
        assertThat(cluster.topology().epoch()).isEqualTo(1);
        for (int i = 0; i < 10; i++) {
            VersionedValue vv = cluster.read("keep-" + i);
            assertThat(vv).isNotNull();
            assertThat(vv.value()).isEqualTo("v" + i);
            assertThat(vv.version()).isEqualTo(1);
        }
        log.info("[判定依据] 非法再平衡(空洞)被拒绝 epoch 仍=1，10 个已确认键原样保留");
    }
}
