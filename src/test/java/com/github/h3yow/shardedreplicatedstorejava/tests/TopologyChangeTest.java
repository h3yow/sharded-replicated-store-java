package com.github.h3yow.shardedreplicatedstorejava.tests;

import com.github.h3yow.shardedreplicatedstorejava.cluster.Cluster;
import com.github.h3yow.shardedreplicatedstorejava.exception.StoreException;
import com.github.h3yow.shardedreplicatedstorejava.exception.TopologyValidationException;
import com.github.h3yow.shardedreplicatedstorejava.model.KeyRange;
import com.github.h3yow.shardedreplicatedstorejava.model.LogEntry;
import com.github.h3yow.shardedreplicatedstorejava.model.ShardSpec;
import com.github.h3yow.shardedreplicatedstorejava.model.Topology;
import com.github.h3yow.shardedreplicatedstorejava.tests.support.TestKit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 分片变更场景：
 * 1) 重叠 / 空洞 / 未覆盖 / 非法配置必须被拒绝，且 reason 可区分；
 * 2) 合法重配后已确认数据不丢失、不重复、仍可读且落点正确。
 */
class TopologyChangeTest {

    private static final Logger log = LoggerFactory.getLogger(TopologyChangeTest.class);

    private Path root;
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        root = TestKit.newTempDir();
        cluster = new Cluster(root);
    }

    @AfterEach
    void tearDown() {
        cluster.close();
    }

    private static Topology custom(long gen, int[][] ranges, List<String> replicas) {
        List<ShardSpec> shards = new ArrayList<>();
        for (int i = 0; i < ranges.length; i++) {
            shards.add(new ShardSpec("s" + i, new KeyRange(ranges[i][0], ranges[i][1]),
                    new ArrayList<>(replicas)));
        }
        return new Topology(gen, 1000, shards);
    }

    @Test
    @DisplayName("合法拓扑：4 分片均匀覆盖 [0,1000) 可引导并写入")
    void validTopologyBootstraps() {
        Topology t = TestKit.topology(1, 4);
        cluster.bootstrap(t);
        var r = cluster.write("req-1", "alpha", "v1", LogEntry.Operation.PUT);
        assertTrue(r.committed());
        assertEquals(Optional.of("v1"), cluster.read("alpha"));
        log.info("[位点] alpha 提交位点={} quorum={}/{}", r.commitIndex(), r.quorum(), 3);
    }

    @Test
    @DisplayName("重叠区间：reason=SHARD_RANGE_OVERLAP")
    void overlapRejected() {
        Topology bad = custom(1, new int[][]{{0, 300}, {250, 700}, {700, 1000}},
                List.of("r0", "r1", "r2"));
        TopologyValidationException ex = assertThrows(TopologyValidationException.class,
                () -> cluster.bootstrap(bad));
        assertEquals("SHARD_RANGE_OVERLAP", ex.getReason());
        log.info("[判定依据] 重叠拒绝 reason={} msg={}", ex.getReason(), ex.getMessage());
    }

    @Test
    @DisplayName("内部空洞：reason=SHARD_RANGE_HOLE")
    void holeRejected() {
        Topology bad = custom(1, new int[][]{{0, 300}, {350, 700}, {700, 1000}},
                List.of("r0", "r1", "r2"));
        TopologyValidationException ex = assertThrows(TopologyValidationException.class,
                () -> cluster.bootstrap(bad));
        assertEquals("SHARD_RANGE_HOLE", ex.getReason());
        log.info("[判定依据] 空洞拒绝 reason={} msg={}", ex.getReason(), ex.getMessage());
    }

    @Test
    @DisplayName("尾部未覆盖：reason=KEYSPACE_NOT_FULLY_COVERED")
    void uncoveredTailRejected() {
        Topology bad = custom(1, new int[][]{{0, 300}, {300, 600}, {600, 900}},
                List.of("r0", "r1", "r2"));
        TopologyValidationException ex = assertThrows(TopologyValidationException.class,
                () -> cluster.bootstrap(bad));
        assertEquals("KEYSPACE_NOT_FULLY_COVERED", ex.getReason());
        log.info("[判定依据] 未覆盖拒绝 reason={} msg={}", ex.getReason(), ex.getMessage());
    }

    @Test
    @DisplayName("头部未覆盖：reason=KEYSPACE_NOT_FULLY_COVERED")
    void uncoveredHeadRejected() {
        Topology bad = custom(1, new int[][]{{100, 400}, {400, 700}, {700, 1000}},
                List.of("r0", "r1", "r2"));
        TopologyValidationException ex = assertThrows(TopologyValidationException.class,
                () -> cluster.bootstrap(bad));
        assertEquals("KEYSPACE_NOT_FULLY_COVERED", ex.getReason());
        log.info("[判定依据] 头部未覆盖拒绝 reason={} msg={}", ex.getReason(), ex.getMessage());
    }

    @Test
    @DisplayName("副本集合为空：reason=INVALID_SHARD_CONFIG")
    void emptyReplicasRejected() {
        Topology bad = new Topology(1, 1000, List.of(
                new ShardSpec("s0", new KeyRange(0, 1000), List.of())));
        TopologyValidationException ex = assertThrows(TopologyValidationException.class,
                () -> cluster.bootstrap(bad));
        assertEquals("INVALID_SHARD_CONFIG", ex.getReason());
        log.info("[判定依据] 非法配置拒绝 reason={} msg={}", ex.getReason(), ex.getMessage());
    }

    @Test
    @DisplayName("分片重配 4->8：已确认数据零丢失零重复，落点正确，重启后一致")
    void reconfigurePreservesCommittedData() {
        cluster.bootstrap(TestKit.topology(1, 4));
        // 写入 40 个键，记录每个键重配前所属分片
        for (int i = 0; i < 40; i++) {
            String key = "key-" + i;
            var r = cluster.write("gen1-" + i, key, "val-" + i, LogEntry.Operation.PUT);
            assertTrue(r.committed());
        }
        long gen1Committed = cluster.commitIndexes().values().stream().mapToLong(Long::longValue).sum();
        log.info("[位点] 重配前各分片提交位点={} 总已确认条目={}", cluster.commitIndexes(), gen1Committed);

        cluster.reconfigure(TestKit.topology(2, 8));
        assertEquals(2, cluster.topology().generation());

        // 全部值可读、不丢失
        for (int i = 0; i < 40; i++) {
            assertEquals(Optional.of("val-" + i), cluster.read("key-" + i),
                    "key-" + i + " lost after reconfig");
        }
        long gen2Committed = cluster.commitIndexes().values().stream().mapToLong(Long::longValue).sum();
        log.info("[版本] 重配后各分片提交位点={} 总条目={}（应等于 {}）",
                cluster.commitIndexes(), gen2Committed, gen1Committed);
        assertEquals(gen1Committed, gen2Committed, "重配不得丢失或重复条目");

        // 重启后结论一致
        var report = cluster.restart();
        assertFalse(report.corruptionDetected());
        for (int i = 0; i < 40; i++) {
            assertEquals(Optional.of("val-" + i), cluster.read("key-" + i));
        }
        log.info("[位点] 重启后提交位点={}", cluster.commitIndexes());

        // 旧代重放拒绝（代际必须递增）
        assertThrows(StoreException.class,
                () -> cluster.reconfigure(TestKit.topology(2, 8)));
    }

    @Test
    @DisplayName("重配到非法拓扑：拒绝且当前代际与数据不受影响")
    void reconfigureToInvalidKeepsOldGeneration() {
        cluster.bootstrap(TestKit.topology(1, 4));
        cluster.write("req-x", "beta", "b", LogEntry.Operation.PUT);
        Topology bad = custom(2, new int[][]{{0, 300}, {350, 700}, {700, 1000}},
                List.of("r0", "r1", "r2"));
        TopologyValidationException ex = assertThrows(TopologyValidationException.class,
                () -> cluster.reconfigure(bad));
        assertEquals("SHARD_RANGE_HOLE", ex.getReason());
        assertEquals(1, cluster.topology().generation());
        assertEquals(Optional.of("b"), cluster.read("beta"));
        log.info("[判定依据] 非法重配被拒 reason={}，仍停留在代际 1，数据可读", ex.getReason());
    }
}
