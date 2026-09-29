package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.store.*;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 场景二：<b>不声明批次</b>，直接走普通 {@code write} 接口时同一键并发写入的决胜。
 *
 * <p>判定依据：同批并发竞争者由分片内的“安静窗口决胜门”自动识别，最终胜出值
 * 只由本批提交内容（requestId 字典序最大者的值）决定，与线程调度、到达先后、
 * 副本处理顺序无关；每个客户端拿到可区分的结论（恰好 1 个 COMMITTED，其余
 * SUPERSEDED），COMMITTED 客户端随后读到的一定是自己的值；不出现死锁、超时、
 * 版本重复/跳号/回退。</p>
 */
class ContentionArbitrationTest {

    private static final Logger log = LoggerFactory.getLogger(ContentionArbitrationTest.class);

    @TempDir
    Path tempDir;

    private Cluster cluster3(Path dir) throws Exception {
        int max = KeyHasher.HASH_SPACE;
        return new Cluster(dir, new Topology(1,
                List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2"))));
    }

    private record ClientOutcome(String requestId, WriteStatus status,
                                 long index, long version, String value) {
    }

    /**
     * n 路线程在同一个屏障后同时写同一 key（普通 write 接口，不传批大小）。
     */
    private List<ClientOutcome> contend(Cluster cluster, String key, int n)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch fire = new CountDownLatch(1);
        List<Future<ClientOutcome>> futures = new ArrayList<>();
        // 乱序提交任务，刻意打乱 requestId 与任务创建顺序
        List<Integer> order = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            order.add(i);
        }
        java.util.Collections.shuffle(order);
        for (int i : order) {
            String req = String.format("req-%03d", i);
            String val = "val-" + req;
            futures.add(pool.submit(() -> {
                ready.countDown();
                fire.await();
                WriteResult r = cluster.write(req, key, val);
                return new ClientOutcome(req, r.status(), r.logIndex(), r.version(), val);
            }));
        }
        assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
        fire.countDown(); // 同时放行
        List<ClientOutcome> outcomes = new ArrayList<>();
        for (Future<ClientOutcome> f : futures) {
            outcomes.add(f.get(30, TimeUnit.SECONDS));
        }
        pool.shutdown();
        return outcomes;
    }

    @RepeatedTest(20)
    void ordinaryWriteArbitratesSameKeyConcurrentlyWithoutBatchHint() throws Exception {
        int n = 15;
        Cluster cluster = cluster3(tempDir.resolve("iter-" + System.nanoTime()));

        List<ClientOutcome> outcomes = contend(cluster, "hot", n);

        // 恰好一个 COMMITTED，其余全部 SUPERSEDED —— 每个客户端结论可区分。
        List<ClientOutcome> committed = outcomes.stream()
                .filter(o -> o.status() == WriteStatus.COMMITTED).toList();
        List<ClientOutcome> superseded = outcomes.stream()
                .filter(o -> o.status() == WriteStatus.SUPERSEDED).toList();
        assertThat(committed).hasSize(1);
        assertThat(superseded).hasSize(n - 1);

        // 胜者必须是 requestId 字典序最大者，胜出值只由提交内容决定。
        ClientOutcome winner = committed.get(0);
        String expectedWinnerReq = String.format("req-%03d", n);
        assertThat(winner.requestId()).isEqualTo(expectedWinnerReq);

        VersionedValue visible = cluster.read("hot");
        assertThat(visible.value()).isEqualTo(winner.value());
        assertThat(visible.winnerRequestId()).isEqualTo(expectedWinnerReq);
        assertThat(visible.version()).isEqualTo(1); // 落败者不占版本，无重复/跳号
        log.info("[位点/版本] {} 路同键并发: 唯一胜者={} index={} version=1，SUPERSEDED={} 个，可见值={}（判定依据: requestId 字典序最大）",
                n, winner.requestId(), winner.index(), superseded.size(), visible.value());

        // 落败者均无 index/version，且其 requestId 不会出现在日志里。
        for (ClientOutcome o : superseded) {
            assertThat(o.index()).isEqualTo(0);
            assertThat(o.version()).isEqualTo(LogEntry.NO_VERSION);
        }
        assertThat(cluster.shard("s1").leader().logEntries()).hasSize(1);

        // 重提落败 requestId：首次结论 SUPERSEDED 被持久化，返回 DUPLICATE 而非重新提交。
        WriteResult loserRetry = cluster.write("req-001", "hot", "val-req-001");
        assertThat(loserRetry.status()).isEqualTo(WriteStatus.DUPLICATE);
        assertThat(cluster.read("hot").value()).isEqualTo(winner.value());
        log.info("[判定依据] 落败 requestId 重提 -> DUPLICATE(首次=SUPERSEDED)，胜出值不变");
    }

    @Test
    void repeatedRunsAlwaysConvergeToSameWinnerAcrossIndependentDirectories() throws Exception {
        // 三个独立目录各跑一次相同负载：最终胜者/值必须完全一致（内容决定，与调度无关）。
        TreeMap<String, String> finals = new TreeMap<>();
        for (int run = 0; run < 3; run++) {
            Cluster cluster = cluster3(tempDir.resolve("run-" + run));
            List<ClientOutcome> outcomes = contend(cluster, "k", 12);

            // 每个独立集群内：恰好 1 个 COMMITTED、11 个 SUPERSEDED
            List<ClientOutcome> committed = outcomes.stream()
                    .filter(o -> o.status() == WriteStatus.COMMITTED).toList();
            List<ClientOutcome> superseded = outcomes.stream()
                    .filter(o -> o.status() == WriteStatus.SUPERSEDED).toList();
            assertThat(committed).hasSize(1);
            assertThat(superseded).hasSize(11);

            String winnerReq = committed.get(0).requestId();
            assertThat(winnerReq).isEqualTo("req-012");
            assertThat(cluster.read("k").value()).isEqualTo("val-req-012");
            finals.put("run-" + run, winnerReq + "/" + cluster.read("k").value()
                    + "/v" + cluster.read("k").version());
        }
        assertThat(finals.values().stream().distinct().count())
                .as("多次独立运行的最终胜者/值/版本必须完全一致: %s", finals)
                .isEqualTo(1);
        assertThat(finals.firstEntry().getValue()).isEqualTo("req-012/val-req-012/v1");
        log.info("[判定依据] 3 次独立并发运行最终结论完全一致: {}", finals);
    }

    @Test
    void sequentialWritesAfterContentionKeepMonotonicVersions() throws Exception {
        Cluster cluster = cluster3(tempDir.resolve("seq"));
        // 先一批并发
        contend(cluster, "k", 5);
        assertThat(cluster.read("k").version()).isEqualTo(1);
        // 之后的顺序写（requestId 更大）正常逐版推进，不回退不跳号
        for (int i = 1; i <= 3; i++) {
            String req = String.format("req-later-%d", i);
            WriteResult r = cluster.write(req, "k", "later-" + i);
            assertThat(r.status()).isEqualTo(WriteStatus.COMMITTED);
            assertThat(r.version()).isEqualTo(1 + i);
        }
        assertThat(cluster.read("k").version()).isEqualTo(4);
        assertThat(cluster.read("k").value()).isEqualTo("later-3");
        log.info("[位点/版本] 并发决胜后顺序写版本 2,3,4 单调推进，最终 version=4/later-3");
    }
}
