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
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 同一键的并发写入：最终版本必须确定、可复现。
 *
 * <p>判定依据：所有并发请求通过“批量严格全序”写入接口提交（声明批大小），
 * 锁在本批竞争者到齐后严格按 requestId 升序放行，因此日志 index、版本号
 * 与最终值与线程调度无关，重复运行逐次一致。</p>
 */
class ConcurrentWriteTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentWriteTest.class);

    @TempDir
    Path tempDir;

    private Topology topology() {
        int max = KeyHasher.HASH_SPACE;
        return new Topology(1, List.of(new ShardRange("s1", 0, max)),
                Map.of("s1", List.of("r0", "r1", "r2")));
    }

    private record Outcome(String requestId, long index, long version) {
    }

    @Test
    void concurrentWritesToSameKeyProduceDeterministicFinalVersion() throws Exception {
        // 用两个不同目录独立运行两次完全相同的并发负载，结果必须逐次一致。
        List<Outcome> run1 = runOnce(tempDir.resolve("run-a"), 8);
        List<Outcome> run2 = runOnce(tempDir.resolve("run-b"), 8);

        TreeMap<String, Long> versions1 = new TreeMap<>();
        TreeMap<String, Long> indexes1 = new TreeMap<>();
        for (Outcome o : run1) {
            versions1.put(o.requestId, o.version);
            indexes1.put(o.requestId, o.index);
        }
        TreeMap<String, Long> versions2 = new TreeMap<>();
        TreeMap<String, Long> indexes2 = new TreeMap<>();
        for (Outcome o : run2) {
            versions2.put(o.requestId, o.version);
            indexes2.put(o.requestId, o.index);
        }
        assertThat(versions2).isEqualTo(versions1);
        assertThat(indexes2).isEqualTo(indexes1);
        log.info("[判定依据] 两次并发运行 requestId->(index,version) 完全一致: {}", indexes1);

        // 严格全序：index 顺序必须恰好等于 requestId 字典序
        List<Map.Entry<String, Long>> byIndex = new ArrayList<>(indexes1.entrySet());
        byIndex.sort(Map.Entry.comparingByValue());
        List<String> requestIdsInIndexOrder = byIndex.stream().map(Map.Entry::getKey).toList();
        assertThat(requestIdsInIndexOrder).isEqualTo(new ArrayList<>(indexes1.keySet()));
        log.info("[位点/版本] 日志位点顺序 == requestId 字典序: {}", requestIdsInIndexOrder);
    }

    @Test
    void finalValueOfContendedKeyEqualsHighestRequestIdValueAcrossRuns() throws Exception {
        // 三次独立并发运行，最终值必须一致且等于 requestId 最大者写入的值
        String f1 = contendedFinalValue(tempDir.resolve("a"));
        String f2 = contendedFinalValue(tempDir.resolve("b"));
        String f3 = contendedFinalValue(tempDir.resolve("c"));
        assertThat(f1).isEqualTo(f2).isEqualTo(f3).isEqualTo("value-03");
        log.info("[判定依据] 3 次并发运行 key=contended 最终值均为 {} -> 最终版本确定可复现", f1);
    }

    private String contendedFinalValue(Path dir) throws Exception {
        Cluster cluster = new Cluster(dir, topology());
        int n = 3;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        List<Future<WriteResult>> futures = new ArrayList<>();
        // 逆序创建任务，增加到达顺序的不确定性；锁在到齐后仍按 requestId 排序
        for (int i = n; i >= 1; i--) {
            String req = String.format("req-%02d", i);
            String val = String.format("value-%02d", i);
            futures.add(pool.submit(() ->
                    cluster.writeInBatch(req, "contended", val, n)));
        }
        for (Future<WriteResult> f : futures) {
            assertThat(f.get(10, TimeUnit.SECONDS).status()).isEqualTo(WriteStatus.COMMITTED);
        }
        pool.shutdown();
        VersionedValue vv = cluster.read("contended");
        assertThat(vv.version()).isEqualTo(n);
        return vv.value();
    }

    private List<Outcome> runOnce(Path dir, int threads) throws Exception {
        Cluster cluster = new Cluster(dir, topology());
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<WriteResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            // 逆序创建任务
            String req = String.format("req-%02d", threads - i);
            futures.add(pool.submit(() ->
                    cluster.writeInBatch(req, "same-key", "v-from-" + req, threads)));
        }
        List<Outcome> outcomes = new ArrayList<>();
        for (Future<WriteResult> f : futures) {
            WriteResult r = f.get(10, TimeUnit.SECONDS);
            assertThat(r.status()).isEqualTo(WriteStatus.COMMITTED);
            outcomes.add(new Outcome(r.requestId(), r.logIndex(), r.version()));
        }
        pool.shutdown();

        // 版本必须恰好覆盖 1..threads（无重复应用、无跳号），最终版本=threads
        List<Long> versions = outcomes.stream().map(Outcome::version).sorted().toList();
        List<Long> expectedVersions = new ArrayList<>();
        for (long v = 1; v <= threads; v++) {
            expectedVersions.add(v);
        }
        assertThat(versions).isEqualTo(expectedVersions);
        assertThat(cluster.read("same-key").version()).isEqualTo(threads);
        log.info("[位点/版本] 并发 {} 提交全部提交，版本集合=1..{}，最终 version={} 最终值={}",
                threads, threads, threads, cluster.read("same-key").value());
        return outcomes;
    }
}
