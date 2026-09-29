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

/**
 * 场景：<strong>不声明批次</strong>，直接走普通 {@code write} 路径的同键并发决胜。
 *
 * <p>判定依据：普通写入路径内置“同键决胜闸门”，静默窗口内到达的同键请求自动成批、
 * 按 requestId 升序决胜。因此同一批并发提交的最终值只由请求内容决定（requestId 最大
 * 者的值），与线程调度/到达先后/副本处理顺序无关；每个客户端结论可区分：赢家
 * COMMITTED、落败但已落盘者 SUPERSEDED；高并发下不死锁、不超时、版本不重复/跳号/回退。</p>
 */
class OrdinaryPathContentionTest {

    private static final Logger log = LoggerFactory.getLogger(OrdinaryPathContentionTest.class);

    @TempDir
    Path tempDir;

    private Topology topology() {
        int max = KeyHasher.HASH_SPACE;
        return new Topology(1, List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2")));
    }

    private record Outcome(String requestId, WriteStatus status, long index, long version,
                           int position, int size, String winnerRequestId) {
    }

    /** 在独立目录跑一次 n 路并发同键写（同一起跑线触发，任务逆序提交）。 */
    private List<Outcome> runOnce(Path dir, int n) throws Exception {
        Cluster cluster = new Cluster(dir, topology());
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<WriteResult>> futures = new ArrayList<>();
        for (int i = n; i >= 1; i--) {
            String req = String.format("req-%02d", i);
            String val = String.format("value-%02d", i);
            futures.add(pool.submit(() -> {
                start.await();
                return cluster.write(req, "contended", val);
            }));
        }
        start.countDown(); // 同时放行，最大化到达顺序的不确定性
        List<Outcome> outcomes = new ArrayList<>();
        try {
            for (Future<WriteResult> f : futures) {
                WriteResult r = f.get(30, TimeUnit.SECONDS);
                outcomes.add(new Outcome(r.requestId(), r.status(), r.logIndex(), r.version(),
                        r.contestPosition(), r.contestSize(), r.winnerRequestId()));
            }
        } finally {
            pool.shutdownNow();
        }
        return outcomes;
    }

    @Test
    void ordinaryPathConcurrentWinnersAreDeterministicAcrossRepeatedRuns() throws Exception {
        int n = 16; // 十几路同时压同一个 key
        List<Outcome> run1 = runOnce(tempDir.resolve("run-1"), n);
        List<Outcome> run2 = runOnce(tempDir.resolve("run-2"), n);
        List<Outcome> run3 = runOnce(tempDir.resolve("run-3"), n);

        // 按 requestId 排序后的逐字段结果跨 3 次运行必须完全一致（index/version/position
        // /size/winner/status）。Outcome 是值对象，equals 做逐字段比较。
        List<Outcome> t1 = sortedByRequest(run1);
        List<Outcome> t2 = sortedByRequest(run2);
        List<Outcome> t3 = sortedByRequest(run3);
        assertThat(t2).isEqualTo(t1);
        assertThat(t3).isEqualTo(t1);
        log.info("[判定依据] 3 次 {}-路并发，requestId -> (index,version,position,status) 完全一致: {}",
                n, formatOutcomes(t1));

        // index 恰好 1..n 不重复不跳号；version 恰好 1..n 不重复不跳号。
        List<Long> indexes = run1.stream().map(o -> o.index).sorted().toList();
        List<Long> versions = run1.stream().map(o -> o.version).sorted().toList();
        for (long v = 1; v <= n; v++) {
            assertThat(indexes.get((int) v - 1)).isEqualTo(v);
            assertThat(versions.get((int) v - 1)).isEqualTo(v);
        }
        log.info("[位点/版本] index 集合=1..{} 无重复/跳号，version 集合=1..{} 无重复/跳号/回退", n, n);

        // 恰好 1 个赢家 COMMITTED，其余 n-1 个已落盘者 SUPERSEDED，结论可区分。
        long committed = run1.stream().filter(o -> o.status == WriteStatus.COMMITTED).count();
        long superseded = run1.stream().filter(o -> o.status == WriteStatus.SUPERSEDED).count();
        assertThat(committed).isEqualTo(1);
        assertThat(superseded).isEqualTo(n - 1);

        // 每个结果的 winnerRequestId 一致，且等于 requestId 最大者。
        String winner = run1.get(0).winnerRequestId;
        assertThat(winner).isEqualTo(String.format("req-%02d", n));
        assertThat(run1).allMatch(o -> o.winnerRequestId.equals(winner));
        log.info("[判定依据] 唯一赢家={}（{} 个落败者 SUPERSEDED 可区分，不存在'拿成功却读到别人值'）",
                winner, superseded);
    }

    @Test
    void finalValueVersionAndShardSurviveRestart() throws Exception {
        int n = 12;
        Path dir = tempDir.resolve("restart");
        List<Outcome> outcomes = runOnce(dir, n);

        Cluster cluster = Cluster.open(dir);
        VersionedValue vv = cluster.read("contended");
        String expectedWinner = String.format("req-%02d", n);
        String expectedValue = String.format("value-%02d", n);
        assertThat(vv.value()).isEqualTo(expectedValue);
        assertThat(vv.version()).isEqualTo(n);
        log.info("[位点/版本] 并发决胜后最终值={} version={}（赢家 {}）", vv.value(), vv.version(),
                expectedWinner);

        // 重启重放后结论完全一致。
        cluster.restartAll();
        VersionedValue after = cluster.read("contended");
        assertThat(after.value()).isEqualTo(expectedValue);
        assertThat(after.version()).isEqualTo(n);
        // 最终值所在分片仍是 s1（单分片拓扑），归属稳定。
        ShardRange owner = cluster.topology().shardForKey("contended");
        assertThat(owner.shardId()).isEqualTo("s1");
        log.info("[判定依据] 整集群重启后最终值={} version={} 归属分片={} 不变",
                after.value(), after.version(), owner.shardId());

        // 落败者用同一 requestId 重提：仍是首次 SUPERSEDED 结论，不会重复应用、不会翻盘。
        WriteResult loserAgain = cluster.write("req-01", "contended", "value-01");
        assertThat(loserAgain.status()).isEqualTo(WriteStatus.DUPLICATE);
        Outcome loserFirst = outcomes.stream().filter(o -> o.requestId.equals("req-01")).findFirst().orElseThrow();
        assertThat(loserAgain.logIndex()).isEqualTo(loserFirst.index);
        assertThat(loserAgain.version()).isEqualTo(loserFirst.version);
        assertThat(cluster.read("contended").value()).isEqualTo(expectedValue);
        log.info("[判定依据] 落败 requestId=req-01 重提 -> DUPLICATE(首次SUPERSEDED index={} version={})，不翻盘、不重复应用",
                loserAgain.logIndex(), loserAgain.version());
    }

    private List<Outcome> sortedByRequest(List<Outcome> outcomes) {
        List<Outcome> sorted = new ArrayList<>(outcomes);
        sorted.sort(java.util.Comparator.comparing(Outcome::requestId));
        return sorted;
    }

    private String formatOutcomes(List<Outcome> sorted) {
        StringBuilder sb = new StringBuilder("{");
        for (Outcome o : sorted) {
            sb.append(o.requestId()).append("->(idx=").append(o.index())
                    .append(",ver=").append(o.version())
                    .append(",pos=").append(o.position())
                    .append(",").append(o.status()).append(") ");
        }
        return sb.append("}").toString();
    }
}
