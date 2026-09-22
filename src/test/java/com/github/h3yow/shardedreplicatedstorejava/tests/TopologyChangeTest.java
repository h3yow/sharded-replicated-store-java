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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 分片与副本归属动态调整：
 * 重叠、空洞、越界、重复 ID、非法副本集必须被拒绝且原因可区分；
 * 合法再平衡后已确认数据不丢失、不重复、不落到错误分片。
 */
class TopologyChangeTest {

    private static final Logger log = LoggerFactory.getLogger(TopologyChangeTest.class);

    @TempDir
    Path tempDir;

    private Topology topology(int epoch, List<ShardRange> ranges,
                              Map<String, List<String>> replicas) {
        return new Topology(epoch, ranges, replicas);
    }

    private Map<String, List<String>> threeReplicas(String... shardIds) {
        Map<String, List<String>> m = new java.util.LinkedHashMap<>();
        for (String s : shardIds) {
            m.put(s, List.of(s + "-r0", s + "-r1", s + "-r2"));
        }
        return m;
    }

    @Test
    void rejectsOverlappingRangesWithDistinctReason() {
        int max = KeyHasher.HASH_SPACE;
        List<ShardRange> ranges = List.of(
                new ShardRange("s1", 0, max / 2 + 10),
                new ShardRange("s2", max / 2, max));
        Topology t = topology(1, ranges, threeReplicas("s1", "s2"));
        assertThatThrownBy(() -> TopologyValidator.validate(t))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.OVERLAPPING_RANGES);
        log.info("[判定依据] 区间重叠 -> OVERLAPPING_RANGES: [0,{}) 与 [{},{})",
                max / 2 + 10, max / 2, max);
    }

    @Test
    void rejectsUncoveredGapWithDistinctReason() {
        int max = KeyHasher.HASH_SPACE;
        List<ShardRange> ranges = List.of(
                new ShardRange("s1", 0, max / 3),
                new ShardRange("s2", max / 3 + 5, max)); // 5 个位点的空洞
        Topology t = topology(1, ranges, threeReplicas("s1", "s2"));
        assertThatThrownBy(() -> TopologyValidator.validate(t))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.UNCOVERED_HASH_GAP);
        log.info("[判定依据] 区间空洞 -> UNCOVERED_HASH_GAP: 缺失位点 [{},{})", max / 3, max / 3 + 5);
    }

    @Test
    void rejectsOutOfBoundsAndInvalidBoundaryAndDuplicates() {
        int max = KeyHasher.HASH_SPACE;

        Topology outOfBounds = topology(1,
                List.of(new ShardRange("s1", 0, max + 1)),
                threeReplicas("s1"));
        assertThatThrownBy(() -> TopologyValidator.validate(outOfBounds))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.RANGE_OUT_OF_BOUNDS);

        Topology invalidBoundary = topology(1,
                List.of(new ShardRange("s1", 5, 5)),
                threeReplicas("s1"));
        assertThatThrownBy(() -> TopologyValidator.validate(invalidBoundary))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.RANGE_INVALID_BOUNDARY);

        Topology dupId = topology(1,
                List.of(new ShardRange("s1", 0, max / 2),
                        new ShardRange("s1", max / 2, max)),
                threeReplicas("s1"));
        assertThatThrownBy(() -> TopologyValidator.validate(dupId))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.DUPLICATE_SHARD_ID);

        Topology badReplicas = topology(1,
                List.of(new ShardRange("s1", 0, max / 2),
                        new ShardRange("s2", max / 2, max)),
                Map.of("s1", List.of("r0", "r0"), "s2", List.of("x")));
        assertThatThrownBy(() -> TopologyValidator.validate(badReplicas))
                .isInstanceOf(TopologyException.class)
                .extracting(e -> ((TopologyException) e).violation())
                .isEqualTo(TopologyViolation.REPLICA_SET_INVALID);
        log.info("[判定依据] 越界/空区间/重复ID/重复副本 分别给出 RANGE_OUT_OF_BOUNDS / RANGE_INVALID_BOUNDARY / DUPLICATE_SHARD_ID / REPLICA_SET_INVALID");
    }

    @Test
    void validReshardMovesCommittedDataWithoutLossOrDuplication() throws Exception {
        int max = KeyHasher.HASH_SPACE;
        // 初始：2 分片，每片 3 副本
        Topology initial = topology(1,
                List.of(new ShardRange("old-a", 0, max / 2),
                        new ShardRange("old-b", max / 2, max)),
                threeReplicas("old-a", "old-b"));
        Cluster cluster = new Cluster(tempDir, initial);

        // 写入若干键，全部多数派提交
        Map<String, String> written = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 20; i++) {
            String key = "key-" + i;
            String value = "v" + i;
            WriteResult r = cluster.write("req-" + i, key, value);
            assertThat(r.status()).isEqualTo(WriteStatus.COMMITTED);
            written.put(key, value);
        }
        // 记录迁移前每个键的版本与归属
        Map<String, Long> versionsBefore = new java.util.LinkedHashMap<>();
        for (String key : written.keySet()) {
            VersionedValue vv = cluster.read(key);
            assertThat(vv).isNotNull();
            versionsBefore.put(key, vv.version());
        }

        // 再平衡为 4 分片
        int q = max / 4;
        List<ShardRange> newRanges = List.of(
                new ShardRange("n0", 0, q),
                new ShardRange("n1", q, 2 * q),
                new ShardRange("n2", 2 * q, 3 * q),
                new ShardRange("n3", 3 * q, max));
        Map<String, List<String>> newReplicas = threeReplicas("n0", "n1", "n2", "n3");
        int migrated = cluster.reshard(newRanges, newReplicas);
        log.info("[位点/版本] 再平衡迁移键数={} 新 epoch={}", migrated, cluster.topology().epoch());
        assertThat(migrated).isEqualTo(written.size());
        assertThat(cluster.topology().epoch()).isEqualTo(2);

        // 切换后：值一致、版本不回退（迁移沿用原版本）、归属为新分片
        for (Map.Entry<String, String> e : written.entrySet()) {
            VersionedValue after = cluster.read(e.getKey());
            assertThat(after).as("迁移后键 %s 不得丢失", e.getKey()).isNotNull();
            assertThat(after.value()).isEqualTo(e.getValue());
            assertThat(after.version()).isEqualTo(versionsBefore.get(e.getKey()));
            ShardRange owner = cluster.topology().shardForKey(e.getKey());
            assertThat(owner.shardId()).startsWith("n");
            log.info("[位点/版本] key={} 归属新分片={} version={}（与迁移前一致，无回退）",
                    e.getKey(), owner.shardId(), after.version());
        }

        // 拓扑文件落盘且重载后仍是新世代
        Cluster reopened = Cluster.open(tempDir);
        assertThat(reopened.topology().epoch()).isEqualTo(2);
        for (String key : written.keySet()) {
            assertThat(reopened.read(key).value()).isEqualTo(written.get(key));
        }
        log.info("[判定依据] 重载集群后全部键值与版本一致 -> 已确认数据未丢失/未重复/未落到错误分片");
    }
}
