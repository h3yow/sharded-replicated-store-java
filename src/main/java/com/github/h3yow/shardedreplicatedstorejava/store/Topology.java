package com.github.h3yow.shardedreplicatedstorejava.store;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 某个世代（epoch）的分片拓扑：分片区间 + 每个分片的副本列表。
 *
 * <p>拓扑对象本身是不可变的；是否合法由 {@link TopologyValidator} 负责，
 * 构造时不强制校验，以便测试中构造出非法拓扑并被拒绝。</p>
 */
public final class Topology {

    private final int epoch;
    private final List<ShardRange> ranges;
    private final Map<String, List<String>> replicasByShard;

    public Topology(int epoch, List<ShardRange> ranges,
                    Map<String, List<String>> replicasByShard) {
        this.epoch = epoch;
        this.ranges = List.copyOf(ranges);
        this.replicasByShard = Map.copyOf(replicasByShard);
    }

    public int epoch() {
        return epoch;
    }

    public List<ShardRange> ranges() {
        return ranges;
    }

    public List<String> replicasOf(String shardId) {
        return replicasByShard.getOrDefault(shardId, List.of());
    }

    /** 返回覆盖给定哈希位点的分片区间；未覆盖返回 null（随后由上层拒绝）。 */
    public ShardRange shardForHash(int hash) {
        for (ShardRange range : ranges) {
            if (range.contains(hash)) {
                return range;
            }
        }
        return null;
    }

    /** 按键路由到所属分片；未覆盖返回 null。 */
    public ShardRange shardForKey(String key) {
        Objects.requireNonNull(key, "key");
        return shardForHash(KeyHasher.hash(key));
    }

    /** 区间按起始位点排序后的拷贝，供校验与迁移按序遍历。 */
    public List<ShardRange> sortedRanges() {
        return ranges.stream()
                .sorted(Comparator.comparingInt(ShardRange::startInclusive))
                .toList();
    }
}
