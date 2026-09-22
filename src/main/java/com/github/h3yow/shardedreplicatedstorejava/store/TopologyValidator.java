package com.github.h3yow.shardedreplicatedstorejava.store;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 校验分片拓扑：
 * <ol>
 *   <li>不能为空；</li>
 *   <li>分片 ID 不得重复；</li>
 *   <li>区间边界必须落在 [0, HASH_SPACE] 内且 start &lt; end；</li>
 *   <li>区间两两不得重叠；</li>
 *   <li>区间必须首尾相接、完整覆盖 [0, HASH_SPACE)，不允许空洞；</li>
 *   <li>每个分片必须有 1 个以上互不重复的副本 ID。</li>
 * </ol>
 * 每一类失败抛出携带不同 {@link TopologyViolation} 的 {@link TopologyException}。
 */
public final class TopologyValidator {

    private TopologyValidator() {
    }

    public static void validate(Topology topology) {
        List<ShardRange> ranges = topology.ranges();

        if (ranges.isEmpty()) {
            throw new TopologyException(TopologyViolation.EMPTY_TOPOLOGY,
                    "拓扑不包含任何分片，拒绝生效");
        }

        Set<String> ids = new HashSet<>();
        for (ShardRange range : ranges) {
            if (!ids.add(range.shardId())) {
                throw new TopologyException(TopologyViolation.DUPLICATE_SHARD_ID,
                        "分片 ID 重复: " + range.shardId());
            }
        }

        for (ShardRange range : ranges) {
            if (range.startInclusive() < 0 || range.endExclusive() > KeyHasher.HASH_SPACE
                    || range.startInclusive() >= KeyHasher.HASH_SPACE) {
                throw new TopologyException(TopologyViolation.RANGE_OUT_OF_BOUNDS,
                        "分片 " + range.shardId() + " 区间越界: ["
                                + range.startInclusive() + ", " + range.endExclusive()
                                + ")，合法空间 [0, " + KeyHasher.HASH_SPACE + ")");
            }
            if (range.startInclusive() >= range.endExclusive()) {
                throw new TopologyException(TopologyViolation.RANGE_INVALID_BOUNDARY,
                        "分片 " + range.shardId() + " 区间起点不小于终点: ["
                                + range.startInclusive() + ", " + range.endExclusive() + ")");
            }
        }

        List<ShardRange> sorted = topology.sortedRanges();

        // 排序后相邻区间一旦有交叉即重叠（含端点相接的反向区间，已在上面排除）。
        for (int i = 1; i < sorted.size(); i++) {
            ShardRange prev = sorted.get(i - 1);
            ShardRange cur = sorted.get(i);
            if (cur.startInclusive() < prev.endExclusive()) {
                throw new TopologyException(TopologyViolation.OVERLAPPING_RANGES,
                        "分片区间重叠: " + prev.shardId() + "["
                                + prev.startInclusive() + "," + prev.endExclusive()
                                + ") 与 " + cur.shardId() + "["
                                + cur.startInclusive() + "," + cur.endExclusive() + ")");
            }
        }

        // 必须从 0 开始、首尾相接、到 HASH_SPACE 结束，任何空隙都是未覆盖空洞。
        int expected = 0;
        for (ShardRange range : sorted) {
            if (range.startInclusive() != expected) {
                throw new TopologyException(TopologyViolation.UNCOVERED_HASH_GAP,
                        "哈希空间存在空洞: 期望位点 " + expected + "，实际起始 "
                                + range.startInclusive() + "（分片 " + range.shardId() + "）");
            }
            expected = range.endExclusive();
        }
        if (expected != KeyHasher.HASH_SPACE) {
            throw new TopologyException(TopologyViolation.UNCOVERED_HASH_GAP,
                    "哈希空间尾部未覆盖: 覆盖到 " + expected + "，空间上界 "
                            + KeyHasher.HASH_SPACE);
        }

        for (ShardRange range : ranges) {
            List<String> replicas = topology.replicasOf(range.shardId());
            if (replicas.isEmpty()) {
                throw new TopologyException(TopologyViolation.REPLICA_SET_INVALID,
                        "分片 " + range.shardId() + " 没有任何副本");
            }
            Set<String> unique = new HashSet<>(replicas);
            if (unique.size() != replicas.size()) {
                throw new TopologyException(TopologyViolation.REPLICA_SET_INVALID,
                        "分片 " + range.shardId() + " 的副本列表包含重复 ID: " + replicas);
            }
        }
    }
}
