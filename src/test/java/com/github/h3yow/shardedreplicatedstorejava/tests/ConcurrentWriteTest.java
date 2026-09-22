package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.cluster.Cluster;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.tests.support.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 并发写入场景：
 * - 多线程对同一 key 用不同 requestId 并发写：无论调度顺序如何，
 *   最终版本恒为 requestId 字典序最大者（确定、可复现），且所有副本一致；
 * - 多线程用同一 requestId 并发重复提交：恰好应用一次，位点不重复；
 * - 跨重启结论保持一致。
 */
class ConcurrentWriteTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentWriteTest.class);

    private Path root;
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        root = TestKit.newTempDir();
        cluster = new Cluster(root);
        cluster.bootstrap(TestKit.topology(1, 2));
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    @Test
    @DisplayName("12 线程并发写同一 key：最终版本恒为字典序最大 requestId，多轮可复现")
    void concurrentWinnersDeterministic() throws Exception {
        int threads = 12;
        for (int round = 0; round < 5; round++) {
            String key = "hot-key-" + round;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            List<String> requestIds = Collections.synchronizedList(new ArrayList<>());
            for (int i = 0; i < threads; i++) {
                final int n = i;
                // 固定补零保证字典序与数值序一致，最大者为 cw-{round}-11
                String req = String.format("cw-%d-%02d", round, n);
                requestIds.add(req);
                futures.add(pool.submit((Callable<Void>) () -> {
                    cluster.write(req, key, "value-" + n, LogEntry.Operation.PUT);
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdownNow();

            String expectedReq = requestIds.stream().max(String::compareTo).orElseThrow();
            String expectedValue = "value-11";
            assertEquals(Optional.of(expectedValue), cluster.read(key),
                    "第 " + round + " 轮并发后最终值确定");
            String expectedVersion = expectedReq;
            var coord = cluster.coordinatorFor(key);
            for (String rid : new String[]{"r0", "r1", "r2"}) {
                var replica = coord.replicas().stream()
                        .filter(r -> r.replicaId().equals(rid)).findFirst().orElseThrow();
                assertEquals(expectedValue, replica.read(key).orElse(null),
                        "副本 " + rid + " 最终值不一致");
                assertEquals(Optional.of(expectedVersion), replica.stateMachine().versionOf(key),
                        "副本 " + rid + " 最终版本号不一致");
            }
            log.info("[版本] 第{}轮 12 并发写 key={} commitIndex={} 最终版本={} 判定依据=requestId 字典序最大（与调度无关）",
                    round, key, coord.commitIndex(), expectedVersion);
        }

        // 重启后最终结论仍一致且可复现
        cluster.restart();
        assertEquals(Optional.of("value-11"), cluster.read("hot-key-4"));
        log.info("[位点] 重启后 hot-key-4 仍为 value-11（版本 cw-4-11），结论可复现");
    }

    @Test
    @DisplayName("同一 requestId 10 线程并发重复提交：仅一次有效应用，位点唯一")
    void concurrentDuplicateSubmission() throws Exception {
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                var r = cluster.write("same-req", "idem", "X", LogEntry.Operation.PUT);
                return r.duplicate();
            }));
        }
        int duplicates = 0;
        for (Future<Boolean> f : futures) {
            if (f.get(30, TimeUnit.SECONDS)) {
                duplicates++;
            }
        }
        pool.shutdownNow();
        assertEquals(threads - 1, duplicates, "恰好 1 次首次提交，其余全部判为重复");
        var coord = cluster.coordinatorFor("idem");
        assertEquals(1L, coord.commitIndex(), "重复提交不得产生多个位点");
        assertEquals(Optional.of("X"), cluster.read("idem"));
        // 状态机内该键只应有一个版本
        for (String rid : new String[]{"r0", "r1", "r2"}) {
            var replica = coord.replicas().stream()
                    .filter(r -> r.replicaId().equals(rid)).findFirst().orElseThrow();
            assertEquals(Optional.of("same-req"), replica.stateMachine().versionOf("idem"));
        }
        log.info("[版本] 10 个并发同 requestId：首次 1 次 + 重复 {} 次，commitIndex=1，无重复应用",
                duplicates);
    }
}
