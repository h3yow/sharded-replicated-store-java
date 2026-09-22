package com.github.h3yow.shardedreplicatedstorejava.model;

import java.util.Comparator;
import java.util.List;

/**
 * 分片拓扑：一代（generation）配置，定义全部分片与副本归属。
 *
 * <p>代际号单调递增，用于区分重配前后的配置；路由仅按区间查找，
 * 是否合法（重叠/空洞/未覆盖）由 TopologyValidator 负责。
 */
public record Topology(long generation, int ringSize, List<ShardSpec> shards) {

    public Topology {
        shards = List.copyOf(shards);
    }

    /** 返回 hash 所属分片；落在空洞或边界外（未覆盖）时返回 null。 */
    public ShardSpec shardForHash(int hash) {
        for (ShardSpec shard : shards) {
            if (shard.range().contains(hash)) {
                return shard;
            }
        }
        return null;
    }

    public boolean covers(int hash) {
        return shardForHash(hash) != null;
    }

    /** 按区间起点排序的副本，供恢复/重配做确定性遍历。 */
    public List<ShardSpec> shardsSortedByRange() {
        return shards.stream()
                .sorted(Comparator.comparingInt(s -> s.range().start()))
                .toList();
    }
}
