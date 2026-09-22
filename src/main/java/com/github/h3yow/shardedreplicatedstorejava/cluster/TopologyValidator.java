package com.github.h3yow.shardedreplicatedstorejava.cluster;

import com.github.h3yow.shardedreplicatedstorejava.exception.TopologyValidationException;
import com.github.h3yow.shardedreplicatedstorejava.model.KeyRange;
import com.github.h3yow.shardedreplicatedstorejava.model.ShardSpec;
import com.github.h3yow.shardedreplicatedstorejava.model.Topology;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 拓扑静态校验。拒绝时抛出 {@link TopologyValidationException}，其 reason 可区分：
 * <ul>
 *   <li>{@code INVALID_SHARD_CONFIG}：ringSize 非法、分片 id 缺失/重复、副本集合为空、区间越界；</li>
 *   <li>{@code SHARD_RANGE_OVERLAP}：两个分片区间存在重叠；</li>
 *   <li>{@code SHARD_RANGE_HOLE}：相邻分片之间存在内部空洞；</li>
 *   <li>{@code KEYSPACE_NOT_FULLY_COVERED}：区间没有恰好覆盖整个 [0, ringSize)
 *       （首个分片不从 0 开始，或最后一个分片不以 ringSize 结束）。</li>
 * </ul>
 */
public final class TopologyValidator {

    public static final String INVALID_CONFIG = "INVALID_SHARD_CONFIG";
    public static final String OVERLAP = "SHARD_RANGE_OVERLAP";
    public static final String HOLE = "SHARD_RANGE_HOLE";
    public static final String NOT_FULLY_COVERED = "KEYSPACE_NOT_FULLY_COVERED";

    private TopologyValidator() {
    }

    public static void validate(Topology topology) {
        if (topology == null) {
            throw new TopologyValidationException(INVALID_CONFIG, "topology is null");
        }
        if (topology.ringSize() <= 0) {
            throw new TopologyValidationException(INVALID_CONFIG,
                    "ringSize must be positive: " + topology.ringSize());
        }
        List<ShardSpec> shards = topology.shards();
        if (shards.isEmpty()) {
            throw new TopologyValidationException(INVALID_CONFIG, "topology has no shards");
        }

        Set<String> ids = new HashSet<>();
        for (ShardSpec shard : shards) {
            if (shard.shardId() == null || shard.shardId().isBlank()) {
                throw new TopologyValidationException(INVALID_CONFIG, "shard with blank id");
            }
            if (!ids.add(shard.shardId())) {
                throw new TopologyValidationException(INVALID_CONFIG,
                        "duplicate shard id: " + shard.shardId());
            }
            if (shard.replicas() == null || shard.replicas().isEmpty()) {
                throw new TopologyValidationException(INVALID_CONFIG,
                        "shard " + shard.shardId() + " has no replicas");
            }
            if (new HashSet<>(shard.replicas()).size() != shard.replicas().size()) {
                throw new TopologyValidationException(INVALID_CONFIG,
                        "shard " + shard.shardId() + " has duplicate replica entries");
            }
            KeyRange r = shard.range();
            if (r.start() < 0 || r.end() > topology.ringSize()) {
                throw new TopologyValidationException(INVALID_CONFIG,
                        "shard " + shard.shardId() + " range " + r
                                + " out of keyspace [0," + topology.ringSize() + ")");
            }
            if (r.length() == 0) {
                throw new TopologyValidationException(INVALID_CONFIG,
                        "shard " + shard.shardId() + " has empty range " + r);
            }
        }

        List<ShardSpec> sorted = shards.stream()
                .sorted(Comparator.comparingInt(s -> s.range().start()))
                .toList();

        // 起点未覆盖 [0, ...)
        KeyRange first = sorted.get(0).range();
        if (first.start() != 0) {
            throw new TopologyValidationException(NOT_FULLY_COVERED,
                    "keyspace [0," + first.start() + ") is not assigned to any shard");
        }

        int cursor = first.end();
        for (int i = 1; i < sorted.size(); i++) {
            KeyRange prev = sorted.get(i - 1).range();
            KeyRange cur = sorted.get(i).range();
            if (cur.start() < prev.end()) {
                throw new TopologyValidationException(OVERLAP,
                        "ranges overlap: " + prev + " vs " + cur
                                + " (shards " + sorted.get(i - 1).shardId()
                                + "," + sorted.get(i).shardId() + ")");
            }
            if (cur.start() > cursor) {
                throw new TopologyValidationException(HOLE,
                        "uncovered hole [" + cursor + "," + cur.start()
                                + ") between shards " + sorted.get(i - 1).shardId()
                                + " and " + sorted.get(i).shardId());
            }
            cursor = cur.end();
        }

        KeyRange last = sorted.get(sorted.size() - 1).range();
        if (last.end() != topology.ringSize()) {
            throw new TopologyValidationException(NOT_FULLY_COVERED,
                    "keyspace [" + last.end() + "," + topology.ringSize()
                            + ") is not assigned to any shard");
        }
    }
}
