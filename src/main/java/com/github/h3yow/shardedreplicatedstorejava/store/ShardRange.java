package com.github.h3yow.shardedreplicatedstorejava.store;

/**
 * 一个分片在哈希空间上的左闭右开区间 [startInclusive, endExclusive)。
 *
 * @param shardId        分片标识
 * @param startInclusive 起始哈希位点（含）
 * @param endExclusive   结束哈希位点（不含）
 */
public record ShardRange(String shardId, int startInclusive, int endExclusive) {

    /** 该区间是否包含给定哈希位点。 */
    public boolean contains(int hash) {
        return hash >= startInclusive && hash < endExclusive;
    }

    /** 区间长度（覆盖的位点个数）。 */
    public int length() {
        return endExclusive - startInclusive;
    }
}
